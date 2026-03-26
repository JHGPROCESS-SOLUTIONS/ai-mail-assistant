import os
import base64
from email.mime.text import MIMEText
from urllib.parse import quote

import httpx
import stripe
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
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
ALLOWED_SUBSCRIPTION_STATUSES = {"active", "trialing"}

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


def has_active_access(user: dict | None) -> bool:
    if not user:
        return False

    access_allowed = bool(user.get("access_allowed"))
    subscription_status = user.get("subscription_status")

    return access_allowed and subscription_status in ALLOWED_SUBSCRIPTION_STATUSES


def build_pricing_redirect(reason: str) -> str:
    return f"{FRONTEND_PRICING_URL}?reason={quote(reason)}"


async def supabase_get_user_by_email(email: str):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")
    encoded_email = quote(email, safe="")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{supabase_url}/rest/v1/users?email=eq.{encoded_email}&select=*",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
            },
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase get user failed: {data}",
            )

        if isinstance(data, list) and data:
            return data[0]

        return None


async def supabase_get_user_by_stripe_customer_id(customer_id: str):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")
    encoded_customer_id = quote(customer_id, safe="")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{supabase_url}/rest/v1/users?stripe_customer_id=eq.{encoded_customer_id}&select=*",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
            },
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase get user by stripe_customer_id failed: {data}",
            )

        if isinstance(data, list) and data:
            return data[0]

        return None


async def supabase_insert_user(email: str, full_name: str | None):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/users",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
            json=[
                {
                    "email": email,
                    "full_name": full_name,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase users insert failed: {data}",
            )

        if not data or not isinstance(data, list):
            raise HTTPException(status_code=500, detail="Supabase users insert returned no rows")

        return data[0]


async def supabase_update_user_profile(
    user_id: str,
    full_name: str | None = None,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    payload = {}
    if full_name is not None:
        payload["full_name"] = full_name

    if not payload:
        return None

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.patch(
            f"{supabase_url}/rest/v1/users?id=eq.{user_id}",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
            json=payload,
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase user profile update failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_user(email: str, full_name: str | None):
    existing_user = await supabase_get_user_by_email(email)

    if existing_user:
        return existing_user

    return await supabase_insert_user(email, full_name)


async def supabase_update_user_subscription(
    user_id: str,
    subscription_status: str | None,
    access_allowed: bool,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    payload = {
        "subscription_status": subscription_status,
        "access_allowed": access_allowed,
    }

    if stripe_customer_id is not None:
        payload["stripe_customer_id"] = stripe_customer_id

    if stripe_subscription_id is not None:
        payload["stripe_subscription_id"] = stripe_subscription_id

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.patch(
            f"{supabase_url}/rest/v1/users?id=eq.{user_id}",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
            json=payload,
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase subscription update failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_oauth_account(
    user_id: str,
    provider: str,
    provider_account_id: str | None,
    access_token: str | None,
    refresh_token: str | None,
    expiry_date: str | None,
    scope: str | None,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/oauth_accounts?on_conflict=user_id,provider",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[
                {
                    "user_id": user_id,
                    "provider": provider,
                    "provider_account_id": provider_account_id,
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expiry_date": expiry_date,
                    "scope": scope,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase oauth_accounts upsert failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_mailbox(
    user_id: str,
    provider: str,
    email_address: str,
    status: str = "connected",
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/mailboxes?on_conflict=user_id,provider,email_address",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[
                {
                    "user_id": user_id,
                    "provider": provider,
                    "email_address": email_address,
                    "status": status,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase mailboxes upsert failed: {data}",
            )

        if not data or not isinstance(data, list):
            raise HTTPException(status_code=500, detail="Supabase mailboxes upsert returned no rows")

        return data[0]


async def supabase_upsert_onboarding_state(
    user_id: str,
    gmail_connected: bool,
    profile_completed: bool,
    initial_sync_completed: bool,
    first_draft_generated: bool,
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/onboarding_state?on_conflict=user_id",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[
                {
                    "user_id": user_id,
                    "gmail_connected": gmail_connected,
                    "profile_completed": profile_completed,
                    "initial_sync_completed": initial_sync_completed,
                    "first_draft_generated": first_draft_generated,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase onboarding_state upsert failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


async def supabase_insert_email(
    user_id: str,
    mailbox_id: str,
    gmail_message_id: str,
    gmail_thread_id: str | None,
    subject: str | None,
    from_email: str | None,
    from_name: str | None,
    snippet: str | None,
    status: str = "new",
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/emails?on_conflict=gmail_message_id",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[
                {
                    "user_id": user_id,
                    "mailbox_id": mailbox_id,
                    "gmail_message_id": gmail_message_id,
                    "gmail_thread_id": gmail_thread_id,
                    "subject": subject,
                    "from_email": from_email,
                    "from_name": from_name,
                    "snippet": snippet,
                    "status": status,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase emails insert failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


async def supabase_insert_draft(
    user_id: str,
    email_id: str,
    gmail_draft_id: str | None,
    subject: str | None,
    draft_body: str,
    status: str = "generated",
):
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/drafts",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
            json=[
                {
                    "user_id": user_id,
                    "email_id": email_id,
                    "gmail_draft_id": gmail_draft_id,
                    "subject": subject,
                    "draft_body": draft_body,
                    "status": status,
                }
            ],
        )

        data = response.json()

        if response.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase drafts insert failed: {data}",
            )

        return data[0] if isinstance(data, list) and data else data


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
            return f"AI fout: {data}"

        return data["choices"][0]["message"]["content"]


async def create_gmail_draft(
    access_token: str,
    to_email: str,
    subject: str | None,
    body: str,
):
    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = f"Re: {subject}" if subject else "Re:"

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/drafts",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={
                "message": {
                    "raw": raw_message
                }
            },
        )

        return response.json()


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
async def billing_status(email: str):
    user = await supabase_get_user_by_email(email)

    if not user:
        return JSONResponse(
            status_code=404,
            content={
                "email": email,
                "found": False,
                "subscription_status": None,
                "access_allowed": False,
            },
        )

    return {
        "email": user.get("email"),
        "found": True,
        "subscription_status": user.get("subscription_status"),
        "access_allowed": bool(user.get("access_allowed")),
        "stripe_customer_id": user.get("stripe_customer_id"),
        "stripe_subscription_id": user.get("stripe_subscription_id"),
    }


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
        scope = token_data.get("scope")
        expires_in = token_data.get("expires_in")
        expiry_date = None

        if expires_in:
            expiry_date = None

        if not access_token:
            raise HTTPException(status_code=400, detail=f"Google token error: {token_data}")

        user_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user = user_response.json()

        user_email = user.get("email")
        user_name = user.get("name")

        if not user_email:
            raise HTTPException(status_code=400, detail=f"Google userinfo error: {user}")

    existing_user = await supabase_get_user_by_email(user_email)

    if not existing_user:
        return RedirectResponse(
            url=build_pricing_redirect("no_subscription_record"),
            status_code=302,
        )

    if not has_active_access(existing_user):
        return RedirectResponse(
            url=build_pricing_redirect("subscription_required"),
            status_code=302,
        )

    user_id = existing_user["id"]

    if user_name and user_name != existing_user.get("full_name"):
        await supabase_update_user_profile(
            user_id=user_id,
            full_name=user_name,
        )

    first_email = None
    body_text = None
    subject = None
    sender = None
    first_message_id = None
    first_thread_id = None

    async with httpx.AsyncClient(timeout=60.0) as client:
        gmail_response = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "maxResults": 5,
                "labelIds": "INBOX",
                "q": "-category:social -category:promotions -category:updates",
            },
        )
        gmail_data = gmail_response.json()

        messages = gmail_data.get("messages", [])

        if messages:
            first_message_id = messages[0]["id"]
            first_thread_id = messages[0].get("threadId")

            email_response = await client.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{first_message_id}",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            first_email = email_response.json()

            payload = first_email.get("payload", {})
            headers = payload.get("headers", [])

            body_text = extract_plain_text_from_payload(payload)
            subject = get_header_value(headers, "Subject")
            sender = get_header_value(headers, "From")

    await supabase_upsert_oauth_account(
        user_id=user_id,
        provider="google",
        provider_account_id=user.get("id"),
        access_token=access_token,
        refresh_token=refresh_token,
        expiry_date=expiry_date,
        scope=scope,
    )

    mailbox = await supabase_upsert_mailbox(
        user_id=user_id,
        provider="gmail",
        email_address=user_email,
        status="connected",
    )

    mailbox_id = mailbox["id"]

    await supabase_upsert_onboarding_state(
        user_id=user_id,
        gmail_connected=True,
        profile_completed=True,
        initial_sync_completed=False,
        first_draft_generated=False,
    )

    email_row = None
    draft_result = None
    ai_reply = None

    if first_email:
        payload = first_email.get("payload", {})
        headers = payload.get("headers", [])
        from_header = get_header_value(headers, "From")
        from_email = extract_email_address(from_header)

        email_row = await supabase_insert_email(
            user_id=user_id,
            mailbox_id=mailbox_id,
            gmail_message_id=first_message_id,
            gmail_thread_id=first_thread_id,
            subject=subject,
            from_email=from_email,
            from_name=from_header,
            snippet=body_text[:500] if body_text else None,
            status="new",
        )

        ai_reply = await generate_ai_reply(
            subject=subject,
            sender=sender,
            body_text=body_text,
        )

        to_email = extract_email_address(sender)

        if to_email and ai_reply and not ai_reply.startswith("AI fout:"):
            draft_result = await create_gmail_draft(
                access_token=access_token,
                to_email=to_email,
                subject=subject,
                body=ai_reply,
            )

            gmail_draft_id = draft_result.get("id")

            if email_row and gmail_draft_id:
                await supabase_insert_draft(
                    user_id=user_id,
                    email_id=email_row["id"],
                    gmail_draft_id=gmail_draft_id,
                    subject=subject,
                    draft_body=ai_reply,
                    status="generated",
                )

                await supabase_upsert_onboarding_state(
                    user_id=user_id,
                    gmail_connected=True,
                    profile_completed=True,
                    initial_sync_completed=False,
                    first_draft_generated=True,
                )

    return RedirectResponse(url=FRONTEND_SUCCESS_URL, status_code=302)


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

        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret,
        )

        event_type = event["type"]
        data = event["data"]["object"]

        print("Stripe webhook received:", event_type)
        print("Stripe webhook data type:", type(data))

        if event_type == "checkout.session.completed":
            email = (
                getattr(data, "customer_email", None)
                or getattr(data, "client_reference_id", None)
            )
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "subscription", None)

            print("checkout.session.completed email:", email)
            print("checkout.session.completed customer_id:", customer_id)
            print("checkout.session.completed subscription_id:", subscription_id)

            if not email:
                print("No email found on checkout.session.completed")
                return {"received": True}

            user = await supabase_get_user_by_email(email)
            print("user found:", user)

            if not user:
                user = await supabase_insert_user(
                    email=email,
                    full_name=None,
                )
                print("user created:", user)

            updated_user = await supabase_update_user_subscription(
                user_id=user["id"],
                subscription_status="active",
                access_allowed=True,
                stripe_customer_id=customer_id,
                stripe_subscription_id=subscription_id,
            )
            print("updated user:", updated_user)

        elif event_type == "customer.subscription.updated":
            status = getattr(data, "status", None)
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "id", None)

            print("subscription.updated status:", status)
            print("subscription.updated customer_id:", customer_id)
            print("subscription.updated subscription_id:", subscription_id)

            if customer_id:
                user = await supabase_get_user_by_stripe_customer_id(customer_id)
                print("user by stripe_customer_id:", user)

                if user:
                    access_allowed = status in ALLOWED_SUBSCRIPTION_STATUSES

                    updated_user = await supabase_update_user_subscription(
                        user_id=user["id"],
                        subscription_status=status,
                        access_allowed=access_allowed,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=subscription_id,
                    )
                    print("updated user:", updated_user)

        elif event_type == "customer.subscription.deleted":
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "id", None)

            print("subscription.deleted customer_id:", customer_id)
            print("subscription.deleted subscription_id:", subscription_id)

            if customer_id:
                user = await supabase_get_user_by_stripe_customer_id(customer_id)
                print("user by stripe_customer_id:", user)

                if user:
                    updated_user = await supabase_update_user_subscription(
                        user_id=user["id"],
                        subscription_status="canceled",
                        access_allowed=False,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=subscription_id,
                    )
                    print("updated user:", updated_user)

        return {"received": True}

    except stripe.error.SignatureVerificationError as e:
        print("STRIPE SIGNATURE ERROR:", repr(e))
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe signature"})
    except ValueError as e:
        print("STRIPE PAYLOAD ERROR:", repr(e))
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe payload"})
    except Exception as e:
        print("STRIPE WEBHOOK FATAL ERROR:", repr(e))
        return JSONResponse(status_code=500, content={"error": "Webhook handler failed"})