"""
Team subscription helpers + webhook handlers voor OfficeFlow Teams.

Wordt geïmporteerd door main.py. Gebruikt de Supabase-helpers die al in
main.py bestaan (supabase_get, supabase_post, supabase_patch,
supabase_get_user_by_email, supabase_insert_user) — die worden hier
dynamisch opgehaald om circulaire imports te voorkomen.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote


# ---- Team tier config (moet matchen met billing.py) ----
TEAM_TIER_SEATS: dict[str, int] = {
    "team_s": 3,
    "team_pro": 10,
    "business": 25,
}

ALLOWED_TEAM_STATUSES = {"active", "trialing", "canceling"}


def _get_main():
    """Lazy-import van main.py om circulaire imports te voorkomen."""
    import app.main as main_module
    return main_module


def stripe_obj_get(obj: Any, key: str, default: Any = None) -> Any:
    """Stripe event objects kunnen dict of SDK-object zijn — pak veilig een attr."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# ============================================================
# SUPABASE HELPERS VOOR TEAMS
# ============================================================

async def supabase_insert_team(
    name: str,
    owner_user_id: str,
    tier: str,
    seats: int,
    billing_period: str,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
    stripe_price_id: str | None = None,
    access_allowed: bool = True,
    subscription_status: str | None = "active",
) -> dict[str, Any]:
    main = _get_main()
    payload = {
        "name": name,
        "owner_user_id": owner_user_id,
        "tier": tier,
        "seats": seats,
        "billing_period": billing_period,
        "stripe_customer_id": stripe_customer_id,
        "stripe_subscription_id": stripe_subscription_id,
        "stripe_price_id": stripe_price_id,
        "access_allowed": access_allowed,
        "subscription_status": subscription_status,
    }
    # supabase_post retourneert default "return=representation" → nieuwe rij komt terug
    result = await main.supabase_post(
        "/rest/v1/teams",
        payload,
    )
    if isinstance(result, list) and result:
        return result[0]
    if isinstance(result, dict):
        return result
    raise RuntimeError(f"Unexpected response from supabase_insert_team: {result!r}")


async def supabase_get_team_by_stripe_customer_id(customer_id: str) -> dict[str, Any] | None:
    main = _get_main()
    data = await main.supabase_get(
        f"/rest/v1/teams?stripe_customer_id=eq.{quote(customer_id, safe='')}&select=*"
    )
    return data[0] if isinstance(data, list) and data else None


async def supabase_get_team_by_subscription_id(subscription_id: str) -> dict[str, Any] | None:
    main = _get_main()
    data = await main.supabase_get(
        f"/rest/v1/teams?stripe_subscription_id=eq.{quote(subscription_id, safe='')}&select=*"
    )
    return data[0] if isinstance(data, list) and data else None


async def supabase_update_team_subscription(
    team_id: str,
    subscription_status: str | None,
    access_allowed: bool,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
    stripe_price_id: str | None = None,
    current_period_end: str | None = None,
) -> None:
    main = _get_main()
    payload: dict[str, Any] = {
        "subscription_status": subscription_status,
        "access_allowed": access_allowed,
    }
    if stripe_customer_id is not None:
        payload["stripe_customer_id"] = stripe_customer_id
    if stripe_subscription_id is not None:
        payload["stripe_subscription_id"] = stripe_subscription_id
    if stripe_price_id is not None:
        payload["stripe_price_id"] = stripe_price_id
    if current_period_end is not None:
        payload["current_period_end"] = current_period_end

    await main.supabase_patch(
        f"/rest/v1/teams?id=eq.{quote(team_id, safe='')}",
        payload,
    )


async def supabase_add_team_member(
    team_id: str,
    user_id: str,
    role: str = "member",
    invited_by: str | None = None,
    mark_joined: bool = False,
) -> None:
    main = _get_main()
    payload: dict[str, Any] = {
        "team_id": team_id,
        "user_id": user_id,
        "role": role,
    }
    if invited_by:
        payload["invited_by"] = invited_by
    if mark_joined:
        payload["joined_at"] = datetime.now(timezone.utc).isoformat()

    try:
        await main.supabase_post(
            "/rest/v1/team_members",
            payload,
        )
    except Exception as exc:
        # PK-violatie betekent: user is al lid — prima, silent no-op
        msg = str(exc).lower()
        if "duplicate" in msg or "23505" in msg:
            return
        raise


async def supabase_count_team_mailboxes(team_id: str) -> int:
    main = _get_main()
    return await main.supabase_get_count(
        f"/rest/v1/mailboxes?team_id=eq.{quote(team_id, safe='')}"
    )


# ============================================================
# WEBHOOK HANDLERS VOOR TEAM-SUBSCRIPTIONS
# ============================================================

def is_team_checkout(metadata: dict[str, Any] | None) -> bool:
    """Bepaalt of een Stripe checkout/session/subscription een team is."""
    if not metadata:
        return False
    return (metadata.get("product_family") or "").lower() == "officeflow_team"


async def handle_team_checkout_completed(data: dict[str, Any]) -> None:
    """Wordt aangeroepen door main.py's webhook bij checkout.session.completed
    met product_family='officeflow_team'. Maakt team + owner-membership aan."""
    main = _get_main()

    # Normaliseer metadata naar een gewone dict (Stripe stuurt soms StripeObject)
    raw_metadata = stripe_obj_get(data, "metadata") or {}
    metadata: dict[str, Any] = dict(raw_metadata) if raw_metadata else {}

    email = (
        stripe_obj_get(data, "customer_email")
        or stripe_obj_get(data, "client_reference_id")
        or metadata.get("email")
    )
    if not email:
        print("[teams-webhook] no email on team checkout — skipping")
        return

    customer_id = stripe_obj_get(data, "customer")
    subscription_id = stripe_obj_get(data, "subscription")

    tier = str(metadata.get("tier") or "team_s").lower()
    billing_period = str(metadata.get("billing_period") or "monthly").lower()
    team_name = str(metadata.get("team_name") or email)
    try:
        seats = TEAM_TIER_SEATS.get(tier) or int(metadata.get("seats") or 3)
    except (TypeError, ValueError):
        seats = 3

    # 1. Zorg dat de owner-user bestaat (net als bij solo flow)
    user = await main.supabase_get_user_by_email(email)
    if not user:
        user = await main.supabase_insert_user(email=email, full_name=None)

    # 2. Geef owner zelf ook toegang (voor als hij een inbox koppelt)
    await main.supabase_update_user_subscription(
        user_id=user["id"],
        subscription_status="active",
        access_allowed=True,
        stripe_customer_id=customer_id,
        stripe_subscription_id=subscription_id,
    )

    # 3. Bestaat er al een team voor deze subscription? (idempotent bij duplicates)
    existing = None
    if subscription_id:
        existing = await supabase_get_team_by_subscription_id(subscription_id)
    if existing:
        print(f"[teams-webhook] team already exists for subscription {subscription_id}, skipping insert")
        team = existing
    else:
        team = await supabase_insert_team(
            name=team_name,
            owner_user_id=user["id"],
            tier=tier,
            seats=seats,
            billing_period=billing_period,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            access_allowed=True,
            subscription_status="active",
        )
        print(f"[teams-webhook] created team {team['id']} ({tier}, {seats} seats) for {email}")

    # 4. Voeg owner toe als admin-member
    await supabase_add_team_member(
        team_id=team["id"],
        user_id=user["id"],
        role="admin",
        mark_joined=True,
    )


async def handle_team_subscription_updated(
    data: dict[str, Any],
    *,
    cancel_at_period_end: bool,
) -> bool:
    """Update team subscription status.
    Returns True als dit een team-subscription was (en is verwerkt),
    False als niet — dan kan main.py nog solo-logica proberen."""
    main = _get_main()

    raw_metadata = stripe_obj_get(data, "metadata") or {}
    metadata: dict[str, Any] = dict(raw_metadata) if raw_metadata else {}
    # Sommige webhooks hebben metadata op Subscription, andere niet —
    # probeer beide paden.
    if not is_team_checkout(metadata):
        # Fallback: check of dit subscription_id bij een team hoort
        subscription_id = stripe_obj_get(data, "id")
        if not subscription_id:
            return False
        team = await supabase_get_team_by_subscription_id(subscription_id)
        if not team:
            return False
    else:
        subscription_id = stripe_obj_get(data, "id")
        customer_id = stripe_obj_get(data, "customer")
        team = None
        if subscription_id:
            team = await supabase_get_team_by_subscription_id(subscription_id)
        if not team and customer_id:
            team = await supabase_get_team_by_stripe_customer_id(customer_id)
        if not team:
            print(f"[teams-webhook] team-metadata present but no team row found for sub {subscription_id}")
            return False

    status = stripe_obj_get(data, "status")

    if cancel_at_period_end and status in ALLOWED_TEAM_STATUSES:
        normalized_status = "canceling"
        access_allowed = True
    else:
        normalized_status = status
        access_allowed = status in ALLOWED_TEAM_STATUSES

    await supabase_update_team_subscription(
        team_id=team["id"],
        subscription_status=normalized_status,
        access_allowed=access_allowed,
    )
    print(f"[teams-webhook] team {team['id']} updated → {normalized_status}, access={access_allowed}")
    return True


async def handle_team_subscription_deleted(data: dict[str, Any]) -> bool:
    """Zet team op canceled. Returns True als team-subscription verwerkt."""
    main = _get_main()

    subscription_id = stripe_obj_get(data, "id")
    customer_id = stripe_obj_get(data, "customer")

    team = None
    if subscription_id:
        team = await supabase_get_team_by_subscription_id(subscription_id)
    if not team and customer_id:
        team = await supabase_get_team_by_stripe_customer_id(customer_id)
    if not team:
        return False

    await supabase_update_team_subscription(
        team_id=team["id"],
        subscription_status="canceled",
        access_allowed=False,
    )
    print(f"[teams-webhook] team {team['id']} canceled")
    return True
