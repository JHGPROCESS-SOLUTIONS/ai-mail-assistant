import os
import stripe

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse

router = APIRouter()

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

PRICE_MONTHLY = os.getenv("STRIPE_PRICE_MONTHLY")
PRICE_YEARLY = os.getenv("STRIPE_PRICE_YEARLY")

# Legacy env vars — kept as fallback so old links/tests keep working during switch.
PRICE_STARTER = os.getenv("STRIPE_PRICE_STARTER")
PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")

SUCCESS_URL = os.getenv(
    "STRIPE_SUCCESS_URL",
    "https://officeflowcompany.com/payment/success",
)
CANCEL_URL = os.getenv(
    "STRIPE_CANCEL_URL",
    "https://officeflowcompany.com",
)


def require_stripe():
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Missing environment variable: STRIPE_SECRET_KEY")


def get_price_id(plan: str) -> str:
    normalized_plan = (plan or "").strip().lower()

    # New canonical plans
    if normalized_plan in ("monthly", "month", "maandelijks"):
        if not PRICE_MONTHLY:
            raise HTTPException(status_code=500, detail="Missing env var: STRIPE_PRICE_MONTHLY")
        return PRICE_MONTHLY

    if normalized_plan in ("yearly", "year", "annual", "jaarlijks"):
        if not PRICE_YEARLY:
            raise HTTPException(status_code=500, detail="Missing env var: STRIPE_PRICE_YEARLY")
        return PRICE_YEARLY

    # Legacy fallback (remove once all links updated)
    if normalized_plan == "starter" and PRICE_STARTER:
        return PRICE_STARTER
    if normalized_plan == "pro" and PRICE_PRO:
        return PRICE_PRO

    raise HTTPException(status_code=400, detail="Invalid plan. Use 'monthly' or 'yearly'.")


def normalize_email(email: str | None) -> str:
    if not email:
        raise HTTPException(status_code=400, detail="Email is required to start checkout.")

    cleaned = email.strip().lower()

    if not cleaned or "@" not in cleaned:
        raise HTTPException(status_code=400, detail="A valid email is required to start checkout.")

    return cleaned


def create_stripe_checkout_session(plan: str, email: str):
    require_stripe()

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[
                {
                    "price": get_price_id(plan),
                    "quantity": 1,
                }
            ],
            success_url=SUCCESS_URL,
            cancel_url=CANCEL_URL,
            customer_email=email,
            client_reference_id=email,
            allow_promotion_codes=True,
            metadata={
                "plan": plan,
                "email": email,
            },
            subscription_data={
                "metadata": {
                    "plan": plan,
                    "email": email,
                },
            },
        )
        return session

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")


@router.post("/billing/create-checkout-session")
async def create_checkout_session(
    plan: str = "monthly",
    email: str | None = None,
):
    normalized_email = normalize_email(email)
    normalized_plan = (plan or "").strip().lower()

    session = create_stripe_checkout_session(
        plan=normalized_plan,
        email=normalized_email,
    )

    return JSONResponse({"url": session.url})


@router.get("/billing/start-checkout")
async def start_checkout(
    plan: str = "monthly",
    email: str | None = Query(default=None),
):
    normalized_email = normalize_email(email)
    normalized_plan = (plan or "").strip().lower()

    session = create_stripe_checkout_session(
        plan=normalized_plan,
        email=normalized_email,
    )

    return RedirectResponse(url=session.url, status_code=303)