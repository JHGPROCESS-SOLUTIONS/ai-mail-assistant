import os
import stripe

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse

router = APIRouter()

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ---- Solo Mailbox Manager prices ----
PRICE_MONTHLY = os.getenv("STRIPE_PRICE_MONTHLY")
PRICE_YEARLY = os.getenv("STRIPE_PRICE_YEARLY")

# Legacy env vars — kept as fallback so old links/tests keep working during switch.
PRICE_STARTER = os.getenv("STRIPE_PRICE_STARTER")
PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")

# ---- Team tier prices ----
PRICE_TEAM_S_MONTHLY = os.getenv("STRIPE_PRICE_TEAM_S_MONTHLY")
PRICE_TEAM_S_YEARLY = os.getenv("STRIPE_PRICE_TEAM_S_YEARLY")
PRICE_TEAM_PRO_MONTHLY = os.getenv("STRIPE_PRICE_TEAM_PRO_MONTHLY")
PRICE_TEAM_PRO_YEARLY = os.getenv("STRIPE_PRICE_TEAM_PRO_YEARLY")
PRICE_BUSINESS_MONTHLY = os.getenv("STRIPE_PRICE_BUSINESS_MONTHLY")
PRICE_BUSINESS_YEARLY = os.getenv("STRIPE_PRICE_BUSINESS_YEARLY")

# tier-config: seats per tier + pricing env vars per billing period
TEAM_TIERS: dict[str, dict] = {
    "team_s": {
        "seats": 3,
        "monthly_env": "STRIPE_PRICE_TEAM_S_MONTHLY",
        "yearly_env": "STRIPE_PRICE_TEAM_S_YEARLY",
        "monthly": PRICE_TEAM_S_MONTHLY,
        "yearly": PRICE_TEAM_S_YEARLY,
    },
    "team_pro": {
        "seats": 10,
        "monthly_env": "STRIPE_PRICE_TEAM_PRO_MONTHLY",
        "yearly_env": "STRIPE_PRICE_TEAM_PRO_YEARLY",
        "monthly": PRICE_TEAM_PRO_MONTHLY,
        "yearly": PRICE_TEAM_PRO_YEARLY,
    },
    "business": {
        "seats": 25,
        "monthly_env": "STRIPE_PRICE_BUSINESS_MONTHLY",
        "yearly_env": "STRIPE_PRICE_BUSINESS_YEARLY",
        "monthly": PRICE_BUSINESS_MONTHLY,
        "yearly": PRICE_BUSINESS_YEARLY,
    },
}

# BTW tarief (21% NL) — Stripe Tax Rate ID, e.g. txr_1Ab...
# Prices in Stripe are EXCLUSIVE of BTW; Stripe voegt het tarief bovenop toe bij checkout.
TAX_RATE_NL = os.getenv("STRIPE_TAX_RATE_NL")

SUCCESS_URL = os.getenv(
    "STRIPE_SUCCESS_URL",
    "https://officeflowcompany.com/payment/success",
)
CANCEL_URL = os.getenv(
    "STRIPE_CANCEL_URL",
    "https://officeflowcompany.com",
)

# Separate URL for team success page (lands on team-setup wizard)
TEAM_SUCCESS_URL = os.getenv(
    "STRIPE_TEAM_SUCCESS_URL",
    "https://officeflowcompany.com/team.html?welcome=1",
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


def normalize_team_tier(tier: str) -> str:
    normalized = (tier or "").strip().lower().replace("-", "_")
    # Accept a few friendly aliases
    aliases = {
        "s": "team_s",
        "starter": "team_s",
        "teamstarter": "team_s",
        "team_starter": "team_s",
        "pro": "team_pro",
        "teampro": "team_pro",
        "biz": "business",
        "team_business": "business",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in TEAM_TIERS:
        raise HTTPException(
            status_code=400,
            detail="Invalid team tier. Use 'team_s', 'team_pro', or 'business'.",
        )
    return normalized


def normalize_billing_period(period: str) -> str:
    normalized = (period or "").strip().lower()
    if normalized in ("monthly", "month", "maandelijks"):
        return "monthly"
    if normalized in ("yearly", "year", "annual", "jaarlijks"):
        return "yearly"
    raise HTTPException(status_code=400, detail="Invalid billing period. Use 'monthly' or 'yearly'.")


def get_team_price_id(tier: str, billing_period: str) -> tuple[str, int]:
    """Return (price_id, seats) for given team tier + period."""
    tier_config = TEAM_TIERS[tier]
    price_id = tier_config.get(billing_period)
    if not price_id:
        env_var = tier_config.get(f"{billing_period}_env", "unknown")
        raise HTTPException(status_code=500, detail=f"Missing env var: {env_var}")
    return price_id, tier_config["seats"]


def normalize_email(email: str | None) -> str:
    if not email:
        raise HTTPException(status_code=400, detail="Email is required to start checkout.")

    cleaned = email.strip().lower()

    if not cleaned or "@" not in cleaned:
        raise HTTPException(status_code=400, detail="A valid email is required to start checkout.")

    return cleaned


def create_stripe_checkout_session(plan: str, email: str):
    """Solo Mailbox Manager checkout — onveranderd gedrag."""
    require_stripe()

    line_item: dict = {
        "price": get_price_id(plan),
        "quantity": 1,
    }
    # Apply 21% BTW if configured. Prices are exclusive of tax,
    # so the subtotal + 21% becomes the total the customer pays.
    if TAX_RATE_NL:
        line_item["tax_rates"] = [TAX_RATE_NL]

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card", "ideal", "bancontact"],
            mode="subscription",
            line_items=[line_item],
            success_url=SUCCESS_URL,
            cancel_url=CANCEL_URL,
            customer_email=email,
            client_reference_id=email,
            allow_promotion_codes=True,
            metadata={
                "plan": plan,
                "email": email,
                "product_family": "officeflow_solo",
            },
            subscription_data={
                "metadata": {
                    "plan": plan,
                    "email": email,
                    "product_family": "officeflow_solo",
                },
            },
        )
        return session

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")


def create_team_stripe_checkout_session(
    tier: str,
    billing_period: str,
    email: str | None = None,
    team_name: str | None = None,
):
    """Team-tier checkout. Metadata bevat tier + seats zodat de webhook
    weet dat dit een TEAM subscription is en de juiste Supabase-inserts doet.
    Email is optioneel — Stripe vraagt 'm anders zelf op de checkout-pagina."""
    require_stripe()

    price_id, seats = get_team_price_id(tier, billing_period)

    line_item: dict = {
        "price": price_id,
        "quantity": 1,
    }
    if TAX_RATE_NL:
        line_item["tax_rates"] = [TAX_RATE_NL]

    metadata = {
        "product_family": "officeflow_team",
        "tier": tier,
        "seats": str(seats),
        "billing_period": billing_period,
        "team_name": (team_name or "").strip()[:100] or (email or "Team"),
    }
    if email:
        metadata["email"] = email

    session_params: dict = {
        "payment_method_types": ["card", "ideal", "bancontact"],
        "mode": "subscription",
        "line_items": [line_item],
        "success_url": TEAM_SUCCESS_URL,
        "cancel_url": CANCEL_URL,
        "allow_promotion_codes": True,
        "metadata": metadata,
        "subscription_data": {"metadata": metadata},
    }

    if email:
        session_params["customer_email"] = email
        session_params["client_reference_id"] = email

    try:
        session = stripe.checkout.Session.create(**session_params)
        return session

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")


# ============================================================
# SOLO CHECKOUT ENDPOINTS (onveranderd)
# ============================================================

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


# ============================================================
# TEAM CHECKOUT ENDPOINTS (nieuw)
# ============================================================

@router.post("/billing/create-team-checkout-session")
async def create_team_checkout_session(
    tier: str = "team_s",
    billing_period: str = "monthly",
    email: str | None = None,
    team_name: str | None = None,
):
    normalized_email = normalize_email(email) if email else None
    normalized_tier = normalize_team_tier(tier)
    normalized_period = normalize_billing_period(billing_period)

    session = create_team_stripe_checkout_session(
        tier=normalized_tier,
        billing_period=normalized_period,
        email=normalized_email,
        team_name=team_name,
    )

    return JSONResponse({"url": session.url})


@router.get("/billing/start-team-checkout")
async def start_team_checkout(
    tier: str = "team_s",
    billing_period: str = "monthly",
    email: str | None = Query(default=None),
    team_name: str | None = Query(default=None),
):
    normalized_email = normalize_email(email) if email else None
    normalized_tier = normalize_team_tier(tier)
    normalized_period = normalize_billing_period(billing_period)

    session = create_team_stripe_checkout_session(
        tier=normalized_tier,
        billing_period=normalized_period,
        email=normalized_email,
        team_name=team_name,
    )

    return RedirectResponse(url=session.url, status_code=303)
