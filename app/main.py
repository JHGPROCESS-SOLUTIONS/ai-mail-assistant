import os
import base64
import httpx
from email.mime.text import MIMEText
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def decode_base64(data):
    if not data:
        return None

    padding = len(data) % 4
    if padding:
        data += "=" * (4 - padding)

    decoded_bytes = base64.urlsafe_b64decode(data)
    return decoded_bytes.decode("utf-8", errors="ignore")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/auth/google/start")
def google_login():
    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&scope=openid email profile https://www.googleapis.com/auth/gmail.modify"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url)


async def supabase_upsert_user(email: str, full_name: str | None):
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"{SUPABASE_URL}/rest/v1/users",
            headers={
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[
                {
                    "email": email,
                    "full_name": full_name,
                }
            ],
        )
        return res.json()


def extract_plain_text_from_payload(payload: dict):
    if not payload:
        return None

    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data")
        return decode_base64(data)

    parts = payload.get("parts", [])
    for part in parts:
        if part.get("mimeType") == "text/plain":
            data = part.get("body", {}).get("data")
            return decode_base64(data)

        nested_parts = part.get("parts", [])
        for nested_part in nested_parts:
            if nested_part.get("mimeType") == "text/plain":
                data = nested_part.get("body", {}).get("data")
                return decode_base64(data)

    data = payload.get("body", {}).get("data")
    return decode_base64(data)


def get_header_value(headers: list, name: str):
    for header in headers:
        if header.get("name", "").lower() == name.lower():
            return header.get("value")
    return None


def extract_email_address(from_header: str | None):
    if not from_header:
        return None

    if "<" in from_header and ">" in from_header:
        return from_header.split("<")[1].split(">")[0].strip()

    return from_header.strip()


async def generate_ai_reply(subject: str | None, sender: str | None, body_text: str | None):
    if not OPENAI_API_KEY:
        return "OPENAI_API_KEY ontbreekt in .env"

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
        res = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
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
                "temperature": 0.5
            },
        )

        data = res.json()

        if "choices" not in data:
            return f"AI fout: {data}"

        return data["choices"][0]["message"]["content"]


async def create_gmail_draft(access_token: str, to_email: str, subject: str | None, body: str):
    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = f"Re: {subject}" if subject else "Re:"

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
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
        return res.json()


@app.get("/auth/google/callback")
async def google_callback(code: str):
    async with httpx.AsyncClient() as client:
        token_res = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )
        token_json = token_res.json()
        access_token = token_json.get("access_token")

        user_res = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={
                "Authorization": f"Bearer {access_token}"
            },
        )
        user = user_res.json()

        gmail_res = await client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers={
                "Authorization": f"Bearer {access_token}"
            },
            params={
                "maxResults": 5,
                "labelIds": "INBOX",
                "q": "-category:social -category:promotions -category:updates",
            },
        )
        gmail_data = gmail_res.json()

        messages = gmail_data.get("messages", [])
        body_text = None
        subject = None
        sender = None
        first_email = None

        if messages:
            first_id = messages[0]["id"]

            email_res = await client.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{first_id}",
                headers={
                    "Authorization": f"Bearer {access_token}"
                },
            )
            first_email = email_res.json()

            payload = first_email.get("payload", {})
            headers = payload.get("headers", [])

            body_text = extract_plain_text_from_payload(payload)
            subject = get_header_value(headers, "Subject")
            sender = get_header_value(headers, "From")

    saved_user = await supabase_upsert_user(
        email=user.get("email"),
        full_name=user.get("name"),
    )

    ai_reply = await generate_ai_reply(
        subject=subject,
        sender=sender,
        body_text=body_text,
    )

    to_email = extract_email_address(sender)
    draft_result = None

    if to_email and ai_reply and not str(ai_reply).startswith("AI fout:"):
        draft_result = await create_gmail_draft(
            access_token=access_token,
            to_email=to_email,
            subject=subject,
            body=ai_reply,
        )

    return {
        "google_user": {
            "email": user.get("email"),
            "name": user.get("name"),
        },
        "gmail_messages": gmail_data,
        "subject": subject,
        "from": sender,
        "email_preview": body_text[:500] if body_text else None,
        "ai_reply": ai_reply,
        "draft_result": draft_result,
        "supabase_user": saved_user,
    }