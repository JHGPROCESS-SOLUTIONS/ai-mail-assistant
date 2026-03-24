# billing.py

import os
import stripe

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

PRICE_STARTER = "price_1TEU9gPBSJU3A3dnTg1vAPCx"
PRICE_PRO = "price_1TEUABPBSJU3A3dnFlIlIxzr"


@router.post("/billing/create-checkout-session")
async def create_checkout_session(plan: str = "starter"):
    price_id = PRICE_STARTER if plan == "starter" else PRICE_PRO

    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        mode="subscription",
        line_items=[
            {
                "price": price_id,
                "quantity": 1,
            }
        ],
        success_url="https://jouwdomein.vercel.app/payment/success",
        cancel_url="https://jouwdomein.vercel.app/payment/cancel",
    )

    return JSONResponse({"url": session.url})