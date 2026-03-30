import os
import json
import base64
from email.mime.text import MIMEText
from urllib.parse import quote
from typing import Any

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
        "https://officeflow-site2.vercel.app",
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
ALLOWED_SUBSCRIPTION_STATUSES = {"active", "trialing"}

OFFICEFLOW_LABELS = [
    "OfficeFlow/Priority",
    "OfficeFlow/To Respond",
    "OfficeFlow/FYI",
    "OfficeFlow/Notification",
    "OfficeFlow/Marketing",
    "OfficeFlow/Spam",
]

LABEL_RULES = {
    "OfficeFlow/Priority": {"generate_draft": True},
    "OfficeFlow/To Respond": {"generate_draft": True},
    "OfficeFlow/FYI": {"generate_draft": False},
    "OfficeFlow/Notification": {"generate_draft": False},
    "OfficeFlow/Marketing": {"generate_draft": False},
    "OfficeFlow/Spam": {"generate_draft": False},
}

LABEL_COLORS = {
    "OfficeFlow/Priority": {
        "textColor": "#ffffff",
        "backgroundColor": "#cc3a21",
    },
    "OfficeFlow/To Respond": {
        "textColor": "#ffffff",
        "backgroundColor": "#3c78d8",
    },
    "OfficeFlow/FYI": {
        "textColor": "#000000",
        "backgroundColor": "#f3f3f3",
    },
    "OfficeFlow/Notification": {
        "textColor": "#ffffff",
        "backgroundColor": "#8e63ce",
    },
    "OfficeFlow/Marketing": {
        "textColor": "#000000",
        "backgroundColor": "#fad165",
    },
    "OfficeFlow/Spam": {
        "textColor": "#ffffff",
        "backgroundColor": "#822111",
    },
}

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


def get_header_value(headers: list[dict[str, Any]], name: str) -> str | None:
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


def extract_plain_text_from_payload(payload: dict[str, Any]) -> str | None:
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


def parse_response_data(response: httpx.Response) -> Any:
    if not response.text:
        return None

    try:
        return response.json()
    except Exception:
        return {"raw": response.text}


def supabase_headers() -> dict[str, str]:
    service_role_key = require_env(SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
    }


async def supabase_get(path_and_query: str, timeout: float = 30.0) -> Any:
    supabase_url = require_env(SUPABASE_URL, "SUPABASE_URL")

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(
            f"{supabase_url}{path_and_query}",
            headers=supabase_headers(),
        )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase GET failed: {data}")

    return data


async def supabase_post(
    path_and_query: str,
    payload: Any,
    prefer: str = "return=representation",
    timeout: float = 30.0,
) -> Any:
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

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase POST failed: {data}")

    return data


async def supabase_patch(
    path_and_query: str,
    payload: Any,
    prefer: str = "return=representation",
    timeout: float = 30.0,
) -> Any:
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

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Supabase PATCH failed: {data}")

    return data


# ----------------------------
# Users helpers
# ----------------------------

async def supabase_get_user_by_email(email: str) -> dict[str, Any] | None:
    data = await supabase_get(
        f"/rest/v1/users?email=eq.{quote(email, safe='')}&select=*"
    )

    if isinstance(data, list) and data:
        return data[0]

    return None


async def supabase_get_user_by_stripe_customer_id(customer_id: str) -> dict[str, Any] | None:
    data = await supabase_get(
        f"/rest/v1/users?stripe_customer_id=eq.{quote(customer_id, safe='')}&select=*"
    )

    if isinstance(data, list) and data:
        return data[0]

    return None


async def supabase_insert_user(email: str, full_name: str | None) -> dict[str, Any]:
    data = await supabase_post(
        "/rest/v1/users",
        [
            {
                "email": email,
                "full_name": full_name,
            }
        ],
    )

    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=500, detail="Supabase users insert returned no rows")

    return data[0]


async def supabase_update_user_profile(user_id: str, full_name: str | None = None) -> dict[str, Any] | None:
    payload: dict[str, Any] = {}

    if full_name is not None:
        payload["full_name"] = full_name

    if not payload:
        return None

    data = await supabase_patch(
        f"/rest/v1/users?id=eq.{quote(user_id, safe='')}",
        payload,
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_update_user_subscription(
    user_id: str,
    subscription_status: str | None,
    access_allowed: bool,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
) -> dict[str, Any] | None:
    payload: dict[str, Any] = {
        "subscription_status": subscription_status,
        "access_allowed": access_allowed,
    }

    if stripe_customer_id is not None:
        payload["stripe_customer_id"] = stripe_customer_id

    if stripe_subscription_id is not None:
        payload["stripe_subscription_id"] = stripe_subscription_id

    data = await supabase_patch(
        f"/rest/v1/users?id=eq.{quote(user_id, safe='')}",
        payload,
    )

    return data[0] if isinstance(data, list) and data else data


async def ensure_user_has_access(email: str) -> dict[str, Any]:
    if not email:
        raise HTTPException(status_code=401, detail="Missing user email")

    user = await supabase_get_user_by_email(email)

    if not user:
        raise HTTPException(status_code=403, detail="No user record found")

    access_allowed = user.get("access_allowed")
    subscription_status = user.get("subscription_status")

    has_access_columns = ("access_allowed" in user) or ("subscription_status" in user)

    if has_access_columns:
        if access_allowed is False:
            raise HTTPException(status_code=403, detail="Active subscription required")

        if subscription_status and subscription_status not in ALLOWED_SUBSCRIPTION_STATUSES:
            raise HTTPException(status_code=403, detail="Active subscription required")

    return user


# ----------------------------
# Mailbox helpers
# ----------------------------

async def supabase_get_mailbox_by_user_and_email(
    user_id: str,
    email_address: str,
    provider: str = "gmail",
) -> dict[str, Any] | None:
    data = await supabase_get(
        (
            "/rest/v1/mailboxes"
            f"?user_id=eq.{quote(user_id, safe='')}"
            f"&provider=eq.{quote(provider, safe='')}"
            f"&email_address=eq.{quote(email_address, safe='')}"
            "&select=*"
        )
    )

    if isinstance(data, list) and data:
        return data[0]

    return None


async def supabase_get_mailbox_by_user_id(
    user_id: str,
    provider: str = "gmail",
) -> dict[str, Any] | None:
    data = await supabase_get(
        (
            "/rest/v1/mailboxes"
            f"?user_id=eq.{quote(user_id, safe='')}"
            f"&provider=eq.{quote(provider, safe='')}"
            "&select=*"
        )
    )

    if isinstance(data, list) and data:
        return data[0]

    return None


async def supabase_upsert_mailbox(
    user_id: str,
    provider: str,
    email_address: str,
    status: str = "connected",
) -> dict[str, Any]:
    data = await supabase_post(
        "/rest/v1/mailboxes?on_conflict=user_id,provider,email_address",
        [
            {
                "user_id": user_id,
                "provider": provider,
                "email_address": email_address,
                "status": status,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=500, detail="Supabase mailboxes upsert returned no rows")

    return data[0]


# ----------------------------
# OAuth account helpers
# ----------------------------

async def supabase_get_oauth_account(
    user_id: str,
    provider: str = "google",
) -> dict[str, Any] | None:
    data = await supabase_get(
        (
            "/rest/v1/oauth_accounts"
            f"?user_id=eq.{quote(user_id, safe='')}"
            f"&provider=eq.{quote(provider, safe='')}"
            "&select=*"
        )
    )

    if isinstance(data, list) and data:
        return data[0]

    return None


async def supabase_update_oauth_account_tokens(
    user_id: str,
    provider: str,
    access_token: str | None = None,
    refresh_token: str | None = None,
) -> dict[str, Any] | None:
    payload: dict[str, Any] = {}

    if access_token is not None:
        payload["access_token"] = access_token

    if refresh_token is not None:
        payload["refresh_token"] = refresh_token

    if not payload:
        return None

    data = await supabase_patch(
        (
            "/rest/v1/oauth_accounts"
            f"?user_id=eq.{quote(user_id, safe='')}"
            f"&provider=eq.{quote(provider, safe='')}"
        ),
        payload,
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_oauth_account(
    user_id: str,
    provider: str,
    provider_account_id: str | None,
    access_token: str | None,
    refresh_token: str | None,
) -> dict[str, Any]:
    existing = await supabase_get_oauth_account(user_id=user_id, provider=provider)
    existing_refresh_token = existing.get("refresh_token") if existing else None
    effective_refresh_token = refresh_token if refresh_token else existing_refresh_token

    data = await supabase_post(
        "/rest/v1/oauth_accounts?on_conflict=user_id,provider",
        [
            {
                "user_id": user_id,
                "provider": provider,
                "provider_account_id": provider_account_id,
                "access_token": access_token,
                "refresh_token": effective_refresh_token,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=500, detail="Supabase oauth_accounts upsert returned no rows")

    return data[0]


# ----------------------------
# Onboarding / email / drafts
# ----------------------------

async def supabase_upsert_onboarding_state(
    user_id: str,
    gmail_connected: bool,
    profile_completed: bool,
    initial_sync_completed: bool,
    first_draft_generated: bool,
) -> dict[str, Any] | None:
    data = await supabase_post(
        "/rest/v1/onboarding_state?on_conflict=user_id",
        [
            {
                "user_id": user_id,
                "gmail_connected": gmail_connected,
                "profile_completed": profile_completed,
                "initial_sync_completed": initial_sync_completed,
                "first_draft_generated": first_draft_generated,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_insert_email(
    user_id: str,
    mailbox_id: str,
    gmail_message_id: str,
    gmail_thread_id: str | None,
    subject: str | None,
) -> dict[str, Any] | None:
    data = await supabase_post(
        "/rest/v1/emails?on_conflict=gmail_message_id",
        [
            {
                "user_id": user_id,
                "mailbox_id": mailbox_id,
                "gmail_message_id": gmail_message_id,
                "gmail_thread_id": gmail_thread_id,
                "subject": subject,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


async def supabase_get_drafts_by_email_id(email_id: str) -> list[dict[str, Any]]:
    data = await supabase_get(
        f"/rest/v1/drafts?email_id=eq.{quote(email_id, safe='')}&select=*"
    )
    return data if isinstance(data, list) else []


async def supabase_insert_draft(
    user_id: str,
    email_id: str,
    gmail_draft_id: str | None,
    subject: str | None,
    draft_body: str,
    status: str = "generated",
) -> dict[str, Any] | None:
    data = await supabase_post(
        "/rest/v1/drafts",
        [
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

    return data[0] if isinstance(data, list) and data else data


async def supabase_upsert_gmail_label(
    user_id: str,
    mailbox_id: str,
    label_name: str,
    label_id: str,
) -> dict[str, Any] | None:
    data = await supabase_post(
        "/rest/v1/gmail_labels?on_conflict=user_id,mailbox_id,label_name",
        [
            {
                "user_id": user_id,
                "mailbox_id": mailbox_id,
                "label_name": label_name,
                "label_id": label_id,
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    )

    return data[0] if isinstance(data, list) and data else data


# ----------------------------
# Gmail auth / context helpers
# ----------------------------

async def get_gmail_context_by_email(email: str) -> dict[str, Any]:
    user = await ensure_user_has_access(email)
    user_id = user["id"]

    mailbox = await supabase_get_mailbox_by_user_and_email(
        user_id=user_id,
        email_address=email,
        provider="gmail",
    )

    if not mailbox:
        mailbox = await supabase_get_mailbox_by_user_id(
            user_id=user_id,
            provider="gmail",
        )

    if not mailbox:
        raise HTTPException(status_code=404, detail="Gmail mailbox not found")

    oauth = await supabase_get_oauth_account(
        user_id=user_id,
        provider="google",
    )

    if not oauth:
        raise HTTPException(status_code=400, detail="Google account not connected")

    return {
        "user": user,
        "mailbox": mailbox,
        "oauth": oauth,
    }


async def refresh_google_access_token(user_id: str, refresh_token: str) -> str:
    client_id = require_env(GOOGLE_CLIENT_ID, "GOOGLE_CLIENT_ID")
    client_secret = require_env(GOOGLE_CLIENT_SECRET, "GOOGLE_CLIENT_SECRET")

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

    data = parse_response_data(response)

    new_access_token = data.get("access_token") if isinstance(data, dict) else None

    if not new_access_token:
        raise HTTPException(status_code=401, detail=f"Google token refresh failed: {data}")

    await supabase_update_oauth_account_tokens(
        user_id=user_id,
        provider="google",
        access_token=new_access_token,
    )

    return new_access_token


async def gmail_get_json_for_user(user_id: str, url: str, params: dict[str, Any] | None = None) -> Any:
    oauth = await supabase_get_oauth_account(user_id=user_id, provider="google")

    if not oauth:
        raise HTTPException(status_code=400, detail="Google account not connected")

    access_token = oauth.get("access_token")
    refresh_token = oauth.get("refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing Google access token")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        )

        if response.status_code == 401 and refresh_token:
            access_token = await refresh_google_access_token(
                user_id=user_id,
                refresh_token=refresh_token,
            )
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                params=params,
            )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=f"Gmail API error: {data}")

    return data


async def gmail_post_json_for_user(user_id: str, url: str, payload: dict[str, Any]) -> Any:
    oauth = await supabase_get_oauth_account(user_id=user_id, provider="google")

    if not oauth:
        raise HTTPException(status_code=400, detail="Google account not connected")

    access_token = oauth.get("access_token")
    refresh_token = oauth.get("refresh_token")

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
            access_token = await refresh_google_access_token(
                user_id=user_id,
                refresh_token=refresh_token,
            )
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=f"Gmail API error: {data}")

    return data


async def gmail_patch_json_for_user(user_id: str, url: str, payload: dict[str, Any]) -> Any:
    oauth = await supabase_get_oauth_account(user_id=user_id, provider="google")

    if not oauth:
        raise HTTPException(status_code=400, detail="Google account not connected")

    access_token = oauth.get("access_token")
    refresh_token = oauth.get("refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing Google access token")

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.patch(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )

        if response.status_code == 401 and refresh_token:
            access_token = await refresh_google_access_token(
                user_id=user_id,
                refresh_token=refresh_token,
            )
            response = await client.patch(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=f"Gmail API error: {data}")

    return data


async def get_gmail_label_id_by_name(user_id: str, label_name: str) -> str | None:
    labels_response = await gmail_get_json_for_user(
        user_id=user_id,
        url=f"{GMAIL_API_BASE}/labels",
    )

    for label in labels_response.get("labels", []):
        if label.get("name") == label_name:
            return label.get("id")

    return None


async def apply_gmail_label_to_message(user_id: str, gmail_message_id: str, label_id: str) -> Any:
    return await gmail_post_json_for_user(
        user_id=user_id,
        url=f"{GMAIL_API_BASE}/messages/{gmail_message_id}/modify",
        payload={
            "addLabelIds": [label_id],
        },
    )


async def update_gmail_label_color(user_id: str, label_id: str, label_name: str) -> Any:
    color = LABEL_COLORS.get(label_name)
    if not color:
        return None

    return await gmail_patch_json_for_user(
        user_id=user_id,
        url=f"{GMAIL_API_BASE}/labels/{label_id}",
        payload={
            "color": {
                "textColor": color["textColor"],
                "backgroundColor": color["backgroundColor"],
            }
        },
    )


# ----------------------------
# Labels
# ----------------------------

async def setup_gmail_labels_for_mailbox(user_id: str, mailbox_id: str) -> list[dict[str, Any]]:
    existing = await gmail_get_json_for_user(
        user_id=user_id,
        url=f"{GMAIL_API_BASE}/labels",
    )

    existing_map = {
        label["name"]: label
        for label in existing.get("labels", [])
    }

    results: list[dict[str, Any]] = []

    for label_name in OFFICEFLOW_LABELS:
        color = LABEL_COLORS.get(label_name)

        if label_name in existing_map:
            label_obj = existing_map[label_name]
            label_id = label_obj["id"]

            if color:
                await gmail_patch_json_for_user(
                    user_id=user_id,
                    url=f"{GMAIL_API_BASE}/labels/{label_id}",
                    payload={
                        "color": {
                            "textColor": color["textColor"],
                            "backgroundColor": color["backgroundColor"],
                        }
                    },
                )
        else:
            payload = {
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            }

            if color:
                payload["color"] = {
                    "textColor": color["textColor"],
                    "backgroundColor": color["backgroundColor"],
                }

            created = await gmail_post_json_for_user(
                user_id=user_id,
                url=f"{GMAIL_API_BASE}/labels",
                payload=payload,
            )
            label_id = created["id"]

        saved = await supabase_upsert_gmail_label(
            user_id=user_id,
            mailbox_id=mailbox_id,
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

async def classify_email(subject: str | None, sender: str | None, body_text: str | None) -> dict[str, Any]:
    api_key = require_env(OPENAI_API_KEY, "OPENAI_API_KEY")

    prompt = f"""
Je bent een e-mail classifier voor OfficeFlow.

Kies exact 1 label uit deze lijst:
- OfficeFlow/Priority
- OfficeFlow/To Respond
- OfficeFlow/FYI
- OfficeFlow/Notification
- OfficeFlow/Marketing
- OfficeFlow/Spam

Regels:
- Priority: belangrijke mail met urgentie, klantwaarde, deadline of directe business impact
- To Respond: normale mail waar een antwoord op nodig is
- FYI: informatief, geen antwoord nodig
- Notification: automatische melding, statusupdate, systeemmail
- Marketing: nieuwsbrief, promotie, sales outreach, aanbieding
- Spam: irrelevant, ongewenst, rommel of duidelijk lage kwaliteit

Geef alleen geldige JSON terug in exact dit formaat:
{{
  "label": "OfficeFlow/To Respond",
  "reason": "Korte reden"
}}

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
                        "content": "Je classificeert zakelijke e-mails en geeft alleen geldige JSON terug.",
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    },
                ],
                "temperature": 0,
            },
        )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"OpenAI classify error: {data}")

    try:
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except Exception:
        raise HTTPException(status_code=500, detail=f"Invalid classifier response: {data}")

    label = parsed.get("label")
    reason = parsed.get("reason")

    if label not in LABEL_RULES:
        raise HTTPException(status_code=500, detail=f"Classifier returned invalid label: {label}")

    return {
        "label": label,
        "reason": reason,
        "generate_draft": LABEL_RULES[label]["generate_draft"],
    }


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
                        "content": "Je schrijft korte, duidelijke zakelijke e-mails in natuurlijk Nederlands.",
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    },
                ],
                "temperature": 0.5,
            },
        )

    data = parse_response_data(response)

    if response.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"OpenAI error: {data}")

    if not isinstance(data, dict) or "choices" not in data:
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
        "message": "API is running.",
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
        "access_allowed": user.get("access_allowed", True),
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
        token_data = parse_response_data(token_response)

        access_token = token_data.get("access_token") if isinstance(token_data, dict) else None
        refresh_token = token_data.get("refresh_token") if isinstance(token_data, dict) else None

        if not access_token:
            raise HTTPException(status_code=400, detail=f"Google token error: {token_data}")

        user_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user_info = parse_response_data(user_response)

    user_email = user_info.get("email") if isinstance(user_info, dict) else None
    user_name = user_info.get("name") if isinstance(user_info, dict) else None
    provider_account_id = user_info.get("id") if isinstance(user_info, dict) else None

    if not user_email:
        raise HTTPException(status_code=400, detail=f"Google userinfo error: {user_info}")

    try:
        existing_user = await ensure_user_has_access(user_email)
    except HTTPException as exc:
        if exc.status_code == 403:
            reason = "subscription_required"
            if exc.detail == "No user record found":
                reason = "no_subscription_record"

            return RedirectResponse(
                url=build_pricing_redirect(reason),
                status_code=302,
            )
        raise

    user_id = existing_user["id"]

    if user_name and user_name != existing_user.get("full_name"):
        await supabase_update_user_profile(
            user_id=user_id,
            full_name=user_name,
        )

    await supabase_upsert_oauth_account(
        user_id=user_id,
        provider="google",
        provider_account_id=provider_account_id,
        access_token=access_token,
        refresh_token=refresh_token,
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

    await setup_gmail_labels_for_mailbox(
        user_id=user_id,
        mailbox_id=mailbox_id,
    )

    gmail_response = await gmail_get_json_for_user(
        user_id=user_id,
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

    first_email = await gmail_get_json_for_user(
        user_id=user_id,
        url=f"{GMAIL_API_BASE}/messages/{first_message_id}",
    )

    payload = first_email.get("payload", {})
    headers = payload.get("headers", [])
    body_text = extract_plain_text_from_payload(payload)
    subject = get_header_value(headers, "Subject")
    sender = get_header_value(headers, "From")

    email_row = await supabase_insert_email(
        user_id=user_id,
        mailbox_id=mailbox_id,
        gmail_message_id=first_message_id,
        gmail_thread_id=first_thread_id,
        subject=subject,
    )

    classification = await classify_email(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    label_name = classification["label"]
    generate_draft = classification["generate_draft"]

    label_id = await get_gmail_label_id_by_name(
        user_id=user_id,
        label_name=label_name,
    )

    if label_id:
        await apply_gmail_label_to_message(
            user_id=user_id,
            gmail_message_id=first_message_id,
            label_id=label_id,
        )

    gmail_label_ids = first_email.get("labelIds", [])
    is_marketing = "CATEGORY_PROMOTIONS" in gmail_label_ids
    is_social = "CATEGORY_SOCIAL" in gmail_label_ids
    is_updates = "CATEGORY_UPDATES" in gmail_label_ids

    should_generate_draft = generate_draft and not is_marketing and not is_social and not is_updates

    if should_generate_draft and email_row:
        existing_drafts = await supabase_get_drafts_by_email_id(email_row["id"])
        if not existing_drafts:
            ai_reply = await generate_ai_reply(
                subject=subject,
                sender=sender,
                body_text=body_text,
            )

            to_email = extract_email_address(sender)

            if to_email and ai_reply:
                message = MIMEText(ai_reply)
                message["to"] = to_email
                message["subject"] = f"Re: {subject}" if subject else "Re:"
                raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

                draft_result = await gmail_post_json_for_user(
                    user_id=user_id,
                    url=f"{GMAIL_API_BASE}/drafts",
                    payload={"message": {"raw": raw_message}},
                )

                gmail_draft_id = draft_result.get("id")

                if gmail_draft_id:
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


@app.get("/test/protected")
async def test_protected(email: str):
    user = await ensure_user_has_access(email)
    mailbox = await supabase_get_mailbox_by_user_id(user_id=user["id"], provider="gmail")

    return {
        "status": "allowed",
        "user_id": user["id"],
        "mailbox_id": mailbox["id"] if mailbox else None,
    }


@app.get("/test/protected-ui")
async def test_protected_ui(email: str):
    user = await ensure_user_has_access(email)
    mailbox = await supabase_get_mailbox_by_user_id(user_id=user["id"], provider="gmail")

    return {
        "status": "allowed",
        "email": email,
        "user_id": user["id"],
        "mailbox_id": mailbox["id"] if mailbox else None,
        "message": "Protected route accessible",
    }


@app.post("/ai/reply")
async def ai_reply_route(
    email: str = Body(...),
    subject: str | None = Body(default=None),
    sender: str | None = Body(default=None),
    body_text: str | None = Body(default=None),
):
    await ensure_user_has_access(email)

    reply = await generate_ai_reply(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    return {
        "status": "ok",
        "reply": reply,
    }


@app.post("/gmail/classify")
async def gmail_classify_route(
    email: str = Body(...),
    subject: str | None = Body(default=None),
    sender: str | None = Body(default=None),
    body_text: str | None = Body(default=None),
):
    await ensure_user_has_access(email)

    result = await classify_email(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    return {
        "status": "ok",
        "classification": result,
    }


@app.get("/gmail/inbox")
async def gmail_inbox(email: str, max_results: int = 10):
    context = await get_gmail_context_by_email(email)
    user = context["user"]
    mailbox = context["mailbox"]

    if max_results < 1:
        max_results = 1
    if max_results > 20:
        max_results = 20

    await setup_gmail_labels_for_mailbox(
        user_id=user["id"],
        mailbox_id=mailbox["id"],
    )

    labels_response = await gmail_get_json_for_user(
        user_id=user["id"],
        url=f"{GMAIL_API_BASE}/labels",
    )
    label_name_to_id = {
        label["name"]: label["id"]
        for label in labels_response.get("labels", [])
    }

    officeflow_label_ids = {
        label_id
        for label_name, label_id in label_name_to_id.items()
        if label_name.startswith("OfficeFlow/")
    }

    gmail_data = await gmail_get_json_for_user(
        user_id=user["id"],
        url=f"{GMAIL_API_BASE}/messages",
        params={
            "maxResults": max_results,
            "labelIds": "INBOX",
        },
    )

    messages = gmail_data.get("messages", [])
    results: list[dict[str, Any]] = []

    for message in messages:
        message_id = message.get("id")
        if not message_id:
            continue

        message_data = await gmail_get_json_for_user(
            user_id=user["id"],
            url=f"{GMAIL_API_BASE}/messages/{message_id}",
        )

        payload = message_data.get("payload", {})
        headers = payload.get("headers", [])

        from_header = get_header_value(headers, "From")
        subject = get_header_value(headers, "Subject")
        body_text = extract_plain_text_from_payload(payload)

        email_row = await supabase_insert_email(
            user_id=user["id"],
            mailbox_id=mailbox["id"],
            gmail_message_id=message_id,
            gmail_thread_id=message_data.get("threadId"),
            subject=subject,
        )

        classification = await classify_email(
            subject=subject,
            sender=from_header,
            body_text=body_text,
        )

        officeflow_label_name = classification["label"]
        officeflow_label_id = label_name_to_id.get(officeflow_label_name)

        current_label_ids = set(message_data.get("labelIds", []))
        has_any_officeflow_label = any(label_id in current_label_ids for label_id in officeflow_label_ids)

        if officeflow_label_id and officeflow_label_id not in current_label_ids:
            await apply_gmail_label_to_message(
                user_id=user["id"],
                gmail_message_id=message_id,
                label_id=officeflow_label_id,
            )

            message_data = await gmail_get_json_for_user(
                user_id=user["id"],
                url=f"{GMAIL_API_BASE}/messages/{message_id}",
            )
            current_label_ids = set(message_data.get("labelIds", []))

        is_marketing = "CATEGORY_PROMOTIONS" in current_label_ids
        is_social = "CATEGORY_SOCIAL" in current_label_ids
        is_updates = "CATEGORY_UPDATES" in current_label_ids

        should_generate_draft = (
            classification["generate_draft"]
            and not is_marketing
            and not is_social
            and not is_updates
        )

        draft_created = False
        gmail_draft_id = None

        if should_generate_draft and email_row:
            existing_drafts = await supabase_get_drafts_by_email_id(email_row["id"])

            if not existing_drafts:
                ai_reply = await generate_ai_reply(
                    subject=subject,
                    sender=from_header,
                    body_text=body_text,
                )

                to_email = extract_email_address(from_header)

                if to_email and ai_reply:
                    draft_message = MIMEText(ai_reply)
                    draft_message["to"] = to_email
                    draft_message["subject"] = f"Re: {subject}" if subject else "Re:"
                    raw_message = base64.urlsafe_b64encode(draft_message.as_bytes()).decode()

                    draft_result = await gmail_post_json_for_user(
                        user_id=user["id"],
                        url=f"{GMAIL_API_BASE}/drafts",
                        payload={"message": {"raw": raw_message}},
                    )

                    gmail_draft_id = draft_result.get("id")

                    if gmail_draft_id:
                        await supabase_insert_draft(
                            user_id=user["id"],
                            email_id=email_row["id"],
                            gmail_draft_id=gmail_draft_id,
                            subject=subject,
                            draft_body=ai_reply,
                            status="generated",
                        )
                        draft_created = True

                        await supabase_upsert_onboarding_state(
                            user_id=user["id"],
                            gmail_connected=True,
                            profile_completed=True,
                            initial_sync_completed=False,
                            first_draft_generated=True,
                        )

        results.append(
            {
                "gmail_message_id": message_data.get("id"),
                "gmail_thread_id": message_data.get("threadId"),
                "subject": subject,
                "from_name": from_header,
                "from_email": extract_email_address(from_header),
                "snippet": message_data.get("snippet"),
                "body_text": body_text[:500] if body_text else None,
                "label_ids": list(current_label_ids),
                "officeflow_label": officeflow_label_name,
                "generate_draft": should_generate_draft,
                "classification_reason": classification["reason"],
                "draft_created": draft_created,
                "gmail_draft_id": gmail_draft_id,
                "already_had_officeflow_label": has_any_officeflow_label,
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
    email_id: str | None = Body(default=None),
):
    context = await get_gmail_context_by_email(email)
    user = context["user"]

    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = f"Re: {subject}" if subject else "Re:"

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    draft_result = await gmail_post_json_for_user(
        user_id=user["id"],
        url=f"{GMAIL_API_BASE}/drafts",
        payload={"message": {"raw": raw_message}},
    )

    gmail_draft_id = draft_result.get("id")

    saved_draft = None
    if email_id and gmail_draft_id:
        saved_draft = await supabase_insert_draft(
            user_id=user["id"],
            email_id=email_id,
            gmail_draft_id=gmail_draft_id,
            subject=subject,
            draft_body=body,
            status="generated",
        )

    return {
        "status": "ok",
        "gmail_draft_id": gmail_draft_id,
        "draft": draft_result,
        "saved_draft": saved_draft,
    }


@app.post("/internal/setup-labels")
async def setup_labels(email: str = Body(...)):
    context = await get_gmail_context_by_email(email)
    user = context["user"]
    mailbox = context["mailbox"]

    result = await setup_gmail_labels_for_mailbox(
        user_id=user["id"],
        mailbox_id=mailbox["id"],
    )

    return {
        "status": "ok",
        "user_id": user["id"],
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

        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret,
        )

        event_type = event["type"]
        data = event["data"]["object"]

        if event_type == "checkout.session.completed":
            email = (
                getattr(data, "customer_email", None)
                or getattr(data, "client_reference_id", None)
            )
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "subscription", None)

            if not email:
                return {"received": True}

            user = await supabase_get_user_by_email(email)

            if not user:
                user = await supabase_insert_user(
                    email=email,
                    full_name=None,
                )

            await supabase_update_user_subscription(
                user_id=user["id"],
                subscription_status="active",
                access_allowed=True,
                stripe_customer_id=customer_id,
                stripe_subscription_id=subscription_id,
            )

        elif event_type == "customer.subscription.updated":
            status = getattr(data, "status", None)
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "id", None)

            if customer_id:
                user = await supabase_get_user_by_stripe_customer_id(customer_id)

                if user:
                    access_allowed = status in ALLOWED_SUBSCRIPTION_STATUSES

                    await supabase_update_user_subscription(
                        user_id=user["id"],
                        subscription_status=status,
                        access_allowed=access_allowed,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=subscription_id,
                    )

        elif event_type == "customer.subscription.deleted":
            customer_id = getattr(data, "customer", None)
            subscription_id = getattr(data, "id", None)

            if customer_id:
                user = await supabase_get_user_by_stripe_customer_id(customer_id)

                if user:
                    await supabase_update_user_subscription(
                        user_id=user["id"],
                        subscription_status="canceled",
                        access_allowed=False,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=subscription_id,
                    )

        return {"received": True}

    except stripe.error.SignatureVerificationError:
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe signature"})
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid Stripe payload"})
    except Exception as e:
        print("STRIPE WEBHOOK FATAL ERROR:", repr(e))
        return JSONResponse(status_code=500, content={"error": "Webhook handler failed"})