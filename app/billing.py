import os
import stripe

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse

router = APIRouter()

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

PRICE_STARTER = "price_1TEU9gPBSJU3A3dnTg1vAPCx"
PRICE_PRO = "price_1TEUABPBSJU3A3dnFlIlIxzr"

SUCCESS_URL = "https://officeflow-site-one.vercel.app/payment/success"
CANCEL_URL = "https://officeflow-site-one.vercel.app"


def get_price_id(plan: str) -> str:
    return PRICE_STARTER if plan == "starter" else PRICE_PRO


@router.post("/billing/create-checkout-session")
async def create_checkout_session(
    plan: str = "starter",
    email: str | None = None,
):
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
            customer_email=email if email else None,
            client_reference_id=email if email else None,
            metadata={
                "plan": plan,
                "email": email or "",
            },
        )

        return JSONResponse({"url": session.url})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")


@router.get("/billing/start-checkout")
async def start_checkout(
    plan: str = "starter",
    email: str | None = Query(default=None),
):
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
            customer_email=email if email else None,
            client_reference_id=email if email else None,
            metadata={
                "plan": plan,
                "email": email or "",
            },
        )

        return RedirectResponse(url=session.url, status_code=303)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")