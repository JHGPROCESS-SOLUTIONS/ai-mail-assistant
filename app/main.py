import os
import base64
from email.mime.text import MIMEText
from urllib.parse import quote

import httpx
import stripe
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse

from app.billing import router as billing_router

load_dotenv()

app = FastAPI(title="AI Mail Assistant API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://officeflow-site-one.vercel.app",
        "https://officeflowcompany.com",
        "https://www.officeflowcompany.com",
        "http://localhost:3000",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(billing_router)

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

FRONTEND_SUCCESS_URL = os.getenv(
    "FRONTEND_SUCCESS_URL",
    "https://officeflowcompany.com/onboarding/success",
)
FRONTEND_PRICING_URL = os.getenv(
    "FRONTEND_PRICING_URL",
    "https://officeflowcompany.com/pricing",
)

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

GMAIL_SCOPE = "openid email profile https://www.googleapis.com/auth/gmail.modify"
GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

OFFICEFLOW_LABELS = [
    "OfficeFlow/To Respond",
    "OfficeFlow/FYI",
    "OfficeFlow/Notification",
    "OfficeFlow/Marketing",
]

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def require_env(value: str | None, name: str) -> str:
    if not value:
        raise HTTPException(status_code=500, detail=f"Missing environment variable: {name}")
    return value


def decode_base64(data: str | None) -> str | None:
    if not data:
        return None

    padding = len(data) % 4
    if padding:
        data += "=" * (4 - padding)

    try:
        decoded_bytes = base64.urlsafe_b64decode(data)
        return decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return None


def get_header_value(headers: list, name: str) -> str | None:
    for header in headers:
        if header.get("name", "").lower() == name.lower():
            return header.get("value")
    return None


def extract_email_address(from_header: str | None) -> str | None:
    if not from_header:
        return None

    if "<" in from_header and ">" in from_header:
        return from_header.split("<")[1].split(">")[0].strip()

    return from_header.strip()


def extract_plain_text_from_payload(payload: dict) -> str | None:
    if not payload:
        return None

    if payload.get("mimeType") == "text/plain":
        return decode_base64(payload.get("body", {}).get("data"))

    for part in payload.get("parts", []):
        if part.get("mimeType") == "text/plain":
            return decode_base64(part.get("body", {}).get("data"))

        for nested_part in part.get("parts", []):
            if nested_part.get("mimeType") == "text/plain":
                return decode_base64(nested_part.get("body", {}).get("data"))

    return decode_base64(payload.get("body", {}).get("data"))


def build_pricing_redirect(reason: str) -> str:
    return f"{FRONTEND_PRICING_URL}?reason={quote(reason)}"


def supabase_headers() -> dict:
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
    }


async def supabase_get(
    path_and_query: str,
    *,
    timeout: float = 30.0,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(
            f"{supabase_url}{path_and_query}",
            headers=supabase_headers(),
        )

    data = response.json()

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase GET failed: {data}")

    return data


async def supabase_post(
    path_and_query: str,
    payload,
    *,
    prefer: str = "return=representation",
    timeout: float = 30.0,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")

    headers = supabase_headers()
    headers["Content-Type"] = "application/json"
    headers["Prefer"] = prefer

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{supabase_url}{path_and_query}",
            headers=headers,
            json=payload,
        )

    data = response.json()

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase POST failed: {data}")

    return data


async def supabase_patch(
    path_and_query: str,
    payload,
    *,
    prefer: str = "return=representation",
    timeout: float = 30.0,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")

    headers = supabase_headers()
    headers["Content-Type"] = "application/json"
    headers["Prefer"] = prefer

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.patch(
            f"{supabase_url}{path_and_query}",
            headers=headers,
            json=payload,
        )

    data = response.json()

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase PATCH failed: {data}")

    return data


# ----------------------------
# Supabase mailbox helpers
# ----------------------------

async def supabase_get_mailbox_by_email(email: str, provider: str = "gmail"):
    encoded_email = quote(email, safe="")
    encoded_provider = quote(provider, safe="")

    queries = [
        (
            "/rest/v1/mailboxes"
            f"?address=eq.{encoded_email}"
            f"&provider=eq.{encoded_provider}"
            "&select=id,tenant_id,provider,address,oauth_access_token,oauth_refresh_token,oauth_token_expires_at,provider_mailbox_id,provider_status"
        ),
        (
            "/rest/v1/mailboxes"
            f"?email_address=eq.{encoded_email}"
            f"&provider=eq.{encoded_provider}"
            "&select=id,tenant_id,provider,email_address,oauth_access_token,oauth_refresh_token,oauth_token_expires_at,provider_mailbox_id,provider_status"
        ),
    ]

    last_error = None

    for query in queries:
        try:
            data = await supabase_get(query)
            if isinstance(data, list) and data:
                return data[0]
        except HTTPException as exc:
            detail = str(exc.detail)
            last_error = exc

            if "column mailboxes.address does not exist" in detail:
                continue
            if "column mailboxes.email_address does not exist" in detail:
                continue

            raise

    if last_error and "does not exist" not in str(last_error.detail):
        raise last_error

    return None


async def supabase_update_mailbox_tokens(
    mailbox_id: str,
    *,
    access_token: str | None = None,
    refresh_token: str | None = None,
    token_expires_at: str | None = None,
    provider_mailbox_id: str | None = None,
):
    payload = {}

    if access_token is not None:
        payload["oauth_access_token"] = access_token
    if refresh_token is not None:
        payload["oauth_refresh_token"] = refresh_token
    if token_expires_at is not None:
        payload["oauth_token_expires_at"] = token_expires_at
    if provider_mailbox_id is not None:
        payload["provider_mailbox_id"] = provider_mailbox_id

    if not payload:
        return None

    data = await supabase_patch(
        f"/rest/v1/mailboxes?id=eq.{quote(mailbox_id, safe='')}",
        payload,
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_gmail_label(
    tenant_id: str,
    label_name: str,
    label_id: str,
):
    data = await supabase_post(
        "/rest/v1/gmail_labels?on_conflict=tenant_id,label_name",
        [
            {
                "tenant_id": tenant_id,
                "label_name": label_name,
                "label_id": label_id,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_insert_message(
    tenant_id: str,
    mailbox_id: str,
    provider_message_id: str,
    thread_id: str | None,
    provider: str = "gmail",
):
    data = await supabase_post(
        "/rest/v1/messages?on_conflict=provider,provider_message_id",
        [
            {
                "tenant_id": tenant_id,
                "mailbox_id": mailbox_id,
                "provider": provider,
                "provider_message_id": provider_message_id,
                "thread_id": thread_id,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_insert_mail_draft(
    tenant_id: str,
    mailbox_id: str,
    message_id: str | None,
    gmail_draft_id: str | None,
    draft_body: str,
):
    payload = {
        "tenant_id": tenant_id,
        "mailbox_id": mailbox_id,
        "draft_body": draft_body,
        "provider_draft_id": gmail_draft_id,
    }

    if message_id:
        payload["message_id"] = message_id

    data = await supabase_post(
        "/rest/v1/mail_drafts",
        [payload],
        prefer="return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


# ----------------------------
# Access / context
# ----------------------------

async def ensure_mailbox_access(email: str):
    if not email:
        raise HTTPException(status_code=401, detail="Missing email")

    mailbox = await supabase_get_mailbox_by_email(email=email, provider="gmail")

    if not mailbox:
        raise HTTPException(status_code=404, detail="Mailbox not found")

    tenant_id = mailbox.get("tenant_id")

    if not tenant_id and mailbox.get("id"):
        mailbox_id = quote(str(mailbox["id"]), safe="")
        refetch = await supabase_get(
            f"/rest/v1/mailboxes?id=eq.{mailbox_id}&select=id,tenant_id,provider,address,oauth_access_token,oauth_refresh_token,oauth_token_expires_at,provider_mailbox_id,provider_status"
        )
        if isinstance(refetch, list) and refetch:
            mailbox = refetch[0]
            tenant_id = mailbox.get("tenant_id")

    if not tenant_id:
        raise HTTPException(
            status_code=400,
            detail=f"Mailbox exists but tenant_id is missing. Mailbox row: {mailbox}",
        )

    return mailbox


# ----------------------------
# Gmail helpers
# ----------------------------

async def refresh_google_access_token(mailbox: dict):
    client_id = require_env(GOOGLE_CLIENT_ID, "GOOGLE_CLIENT_ID")
    client_secret = require_env(GOOGLE_CLIENT_SECRET, "GOOGLE_CLIENT_SECRET")

    refresh_token = mailbox.get("oauth_refresh_token")
    mailbox_id = mailbox.get("id")

    if not refresh_token:
        raise HTTPException(status_code=401, detail="Missing Google refresh token")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )

        data = response.json()

        new_access_token = data.get("access_token")
        if not new_access_token:
            raise HTTPException(status_code=401, detail=f"Google token refresh failed: {data}")

        await supabase_update_mailbox_tokens(
            mailbox_id=mailbox_id,
            access_token=new_access_token,
            token_expires_at=str(data.get("expires_in")) if data.get("expires_in") is not None else None,
        )

        mailbox["oauth_access_token"] = new_access_token
        return new_access_token


async def gmail_get_json_for_mailbox(
    mailbox: dict,
    url: str,
    params: dict | None = None,
):
    access_token = mailbox.get("oauth_access_token")
    refresh_token = mailbox.get("oauth_refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing Google access token")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        )

        if response.status_code == 401 and refresh_token:
            access_token = await refresh_google_access_token(mailbox)
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                params=params,
            )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=f"Gmail API error: {data}")

        return data


async def gmail_post_json_for_mailbox(
    mailbox: dict,
    url: str,
    payload: dict,
):
    access_token = mailbox.get("oauth_access_token")
    refresh_token = mailbox.get("oauth_refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing Google access token")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )

        if response.status_code == 401 and refresh_token:
            access_token = await refresh_google_access_token(mailbox)
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=f"Gmail API error: {data}")

        return data


# ----------------------------
# Labels
# ----------------------------

async def setup_gmail_labels_for_mailbox(mailbox: dict):
    tenant_id = mailbox.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Mailbox tenant_id missing for label setup")

    existing = await gmail_get_json_for_mailbox(
        mailbox=mailbox,
        url=f"{GMAIL_API_BASE}/labels",
    )

    existing_map = {
        label["name"]: label["id"]
        for label in existing.get("labels", [])
    }

    results = []

    for label_name in OFFICEFLOW_LABELS:
        if label_name in existing_map:
            label_id = existing_map[label_name]
        else:
            created = await gmail_post_json_for_mailbox(
                mailbox=mailbox,
                url=f"{GMAIL_API_BASE}/labels",
                payload={
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            )
            label_id = created["id"]

        saved = await supabase_upsert_gmail_label(
            tenant_id=tenant_id,
            label_name=label_name,
            label_id=label_id,
        )

        results.append(
            {
                "label_name": label_name,
                "label_id": label_id,
                "db_row": saved,
            }
        )

    return results


# ----------------------------
# AI
# ----------------------------

async def generate_ai_reply(subject: str | None, sender: str | None, body_text: str | None) -> str:
    api_key = require_env(OPENAI_API_KEY, "OPENAI_API_KEY")

    prompt = f"""
Je bent een slimme e-mailassistent.

Schrijf een korte, natuurlijke en professionele reply op deze e-mail.
Hou de toon vriendelijk en menselijk.
Verzin geen feiten.
Als de mail vooral informatief is en geen duidelijke vraag bevat, schrijf dan een korte beleefde ontvangstbevestiging.

Van: {sender}
Onderwerp: {subject}

E-mail:
{body_text}
""".strip()

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "system",
                        "content": "Je schrijft korte, duidelijke zakelijke e-mails in natuurlijk Nederlands."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.5,
            },
        )

        data = response.json()

        if "choices" not in data:
            raise HTTPException(status_code=500, detail=f"OpenAI error: {data}")

        return data["choices"][0]["message"]["content"]


# ----------------------------
# Routes
# ----------------------------

@app.get("/")
def home():
    return {
        "name": "AI Mail Assistant API",
        "status": "online",
        "message": "API is running."
    }


@app.get("/privacy")
def privacy():
    return {
        "title": "Privacy Policy",
        "content": (
            "We use Google account data only to authenticate users, read selected Gmail messages, "
            "generate AI-based email drafts, and create Gmail draft replies. "
            "We do not sell user data or share Gmail content with third parties except where required "
            "to provide the AI drafting service."
        ),
    }


@app.get("/terms")
def terms():
    return {
        "title": "Terms of Service",
        "content": (
            "This service helps generate AI email drafts. Users are responsible for reviewing all drafts "
            "before sending. We do not guarantee correctness, completeness, or suitability of generated content."
        ),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/billing/status")
async def billing_status():
    return JSONResponse(
        status_code=501,
        content={"detail": "billing/status is not implemented in this main.py"},
    )


@app.get("/auth/google/start")
def google_login():
    client_id = require_env(GOOGLE_CLIENT_ID, "GOOGLE_CLIENT_ID")
    redirect_uri = require_env(GOOGLE_REDIRECT_URI, "GOOGLE_REDIRECT_URI")

    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        "&response_type=code"
        f"&scope={quote(GMAIL_SCOPE, safe=':/')}"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url)


@app.get("/auth/google/callback")
async def google_callback(code: str):
    client_id = require_env(GOOGLE_CLIENT_ID, "GOOGLE_CLIENT_ID")
    client_secret = require_env(GOOGLE_CLIENT_SECRET, "GOOGLE_CLIENT_SECRET")
    redirect_uri = require_env(GOOGLE_REDIRECT_URI, "GOOGLE_REDIRECT_URI")

    async with httpx.AsyncClient(timeout=60.0) as client:
        token_response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        token_data = token_response.json()

        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in")

        if not access_token:
            raise HTTPException(status_code=400, detail=f"Google token error: {token_data}")

        user_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user = user_response.json()

    user_email = user.get("email")
    google_account_id = user.get("id")

    if not user_email:
        raise HTTPException(status_code=400, detail=f"Google userinfo error: {user}")

    mailbox = await supabase_get_mailbox_by_email(user_email, provider="gmail")
    if not mailbox:
        return RedirectResponse(
            url=build_pricing_redirect("mailbox_not_preprovisioned"),
            status_code=302,
        )

    await supabase_update_mailbox_tokens(
        mailbox_id=mailbox["id"],
        access_token=access_token,
        refresh_token=refresh_token,
        token_expires_at=str(expires_in) if expires_in is not None else None,
        provider_mailbox_id=google_account_id,
    )

    mailbox["oauth_access_token"] = access_token
    if refresh_token:
        mailbox["oauth_refresh_token"] = refresh_token

    mailbox = await ensure_mailbox_access(user_email)
    tenant_id = mailbox["tenant_id"]

    gmail_response = await gmail_get_json_for_mailbox(
        mailbox=mailbox,
        url=f"{GMAIL_API_BASE}/messages",
        params={
            "maxResults": 1,
            "labelIds": "INBOX",
            "q": "-category:social -category:promotions -category:updates",
        },
    )

    messages = gmail_response.get("messages", [])
    if not messages:
        return RedirectResponse(url=FRONTEND_SUCCESS_URL, status_code=302)

    first_message_id = messages[0]["id"]
    first_thread_id = messages[0].get("threadId")

    first_email = await gmail_get_json_for_mailbox(
        mailbox=mailbox,
        url=f"{GMAIL_API_BASE}/messages/{first_message_id}",
    )

    payload = first_email.get("payload", {})
    headers = payload.get("headers", [])
    body_text = extract_plain_text_from_payload(payload)
    subject = get_header_value(headers, "Subject")
    sender = get_header_value(headers, "From")

    to_email = extract_email_address(sender)

    message_row = None
    try:
        message_row = await supabase_insert_message(
            tenant_id=tenant_id,
            mailbox_id=mailbox["id"],
            provider_message_id=first_message_id,
            thread_id=first_thread_id,
            provider="gmail",
        )
    except Exception:
        message_row = None

    ai_reply = await generate_ai_reply(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    if to_email and ai_reply:
        message = MIMEText(ai_reply)
        message["to"] = to_email
        message["subject"] = f"Re: {subject}" if subject else "Re:"
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        draft_result = await gmail_post_json_for_mailbox(
            mailbox=mailbox,
            url=f"{GMAIL_API_BASE}/drafts",
            payload={"message": {"raw": raw_message}},
        )

        gmail_draft_id = draft_result.get("id")

        try:
            await supabase_insert_mail_draft(
                tenant_id=tenant_id,
                mailbox_id=mailbox["id"],
                message_id=message_row["id"] if message_row else None,
                gmail_draft_id=gmail_draft_id,
                draft_body=ai_reply,
            )
        except Exception:
            pass

    return RedirectResponse(url=FRONTEND_SUCCESS_URL, status_code=302)


@app.get("/test/protected")
async def test_protected(email: str):
    mailbox = await ensure_mailbox_access(email)
    return {
        "status": "allowed",
        "tenant_id": mailbox["tenant_id"],
        "mailbox_id": mailbox["id"],
    }


@app.get("/test/protected-ui")
async def test_protected_ui(email: str):
    mailbox = await ensure_mailbox_access(email)
    return {
        "status": "allowed",
        "email": email,
        "tenant_id": mailbox["tenant_id"],
        "mailbox_id": mailbox["id"],
        "message": "Protected route accessible",
    }


@app.post("/ai/reply")
async def ai_reply_route(
    email: str = Body(...),
    subject: str | None = Body(default=None),
    sender: str | None = Body(default=None),
    body_text: str | None = Body(default=None),
):
    await ensure_mailbox_access(email)

    reply = await generate_ai_reply(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    return {
        "status": "ok",
        "reply": reply,
    }


@app.get("/gmail/inbox")
async def gmail_inbox(email: str, max_results: int = 10):
    mailbox = await ensure_mailbox_access(email)

    if max_results < 1:
        max_results = 1
    if max_results > 20:
        max_results = 20

    gmail_data = await gmail_get_json_for_mailbox(
        mailbox=mailbox,
        url=f"{GMAIL_API_BASE}/messages",
        params={
            "maxResults": max_results,
            "labelIds": "INBOX",
        },
    )

    messages = gmail_data.get("messages", [])
    results = []

    for message in messages:
        message_id = message.get("id")
        if not message_id:
            continue

        message_data = await gmail_get_json_for_mailbox(
            mailbox=mailbox,
            url=f"{GMAIL_API_BASE}/messages/{message_id}",
        )

        payload = message_data.get("payload", {})
        headers = payload.get("headers", [])

        from_header = get_header_value(headers, "From")
        subject = get_header_value(headers, "Subject")
        body_text = extract_plain_text_from_payload(payload)

        results.append(
            {
                "gmail_message_id": message_data.get("id"),
                "gmail_thread_id": message_data.get("threadId"),
                "subject": subject,
                "from_name": from_header,
                "from_email": extract_email_address(from_header),
                "snippet": message_data.get("snippet"),
                "body_text": body_text[:500] if body_text else None,
            }
        )

    return {
        "status": "ok",
        "count": len(results),
        "messages": results,
    }


@app.post("/gmail/draft")
async def gmail_draft_route(
    email: str = Body(...),
    to_email: str = Body(...),
    subject: str | None = Body(default=None),
    body: str = Body(...),
):
    mailbox = await ensure_mailbox_access(email)

    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = f"Re: {subject}" if subject else "Re:"

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    draft_result = await gmail_post_json_for_mailbox(
        mailbox=mailbox,
        url=f"{GMAIL_API_BASE}/drafts",
        payload={"message": {"raw": raw_message}},
    )

    return {
        "status": "ok",
        "gmail_draft_id": draft_result.get("id"),
        "draft": draft_result,
    }


@app.post("/internal/setup-labels")
async def setup_labels(email: str = Body(...)):
    mailbox = await ensure_mailbox_access(email)
    tenant_id = mailbox["tenant_id"]

    result = await setup_gmail_labels_for_mailbox(mailbox)

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "mailbox_id": mailbox["id"],
        "labels": result,
    }


@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    webhook_secret = require_env(STRIPE_WEBHOOK_SECRET, "STRIPE_WEBHOOK_SECRET")

    try:
        payload = await request.body()
        sig_header = request.headers.get("stripe-signature")

        if not sig_header:
            return JSONResponse(
                status_code=400,
                content={"error": "Missing Stripe signature header"},
            )

        stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret,
        )

        return {"received": True}

    except stripe.error.SignatureVerificationError:
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe signature"})
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe payload"})
    except Exception as e:
        print("STRIPE WEBHOOK FATAL ERROR:", repr(e))
        return JSONResponse(status_code=500, content={"error": "Webhook handler failed"})