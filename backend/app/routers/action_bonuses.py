"""CLAUD-96: Action Bonuses — Rule-based credit rewards for client lifecycle events."""

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/action-bonuses", tags=["action-bonuses"])

CREDIT_API_URL = "https://apicrm.cmtrading.com/SignalsCRM/crm-api/brokers/users/credit"
CREDIT_API_TOKEN = "699a3696-5869-44c9-aa31-1938f296a556"

VALID_ACTIONS = ("live_details", "submit_documents")

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ActionBonusRuleIn(BaseModel):
    action: str = Field(..., pattern="^(live_details|submit_documents)$")
    countries: list[str] | None = None       # None = all countries
    affiliates: list[str] | None = None      # None = all affiliates
    reward_amount: Decimal = Field(..., gt=0)
    isactive: bool = True


class ReorderRulesIn(BaseModel):
    action: str = Field(..., pattern="^(live_details|submit_documents)$")
    rule_ids: list[int] = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Helper: call credit API
# ---------------------------------------------------------------------------

async def _award_action_bonus(
    trading_account_id: str,
    amount: float,
    action: str,
) -> tuple[bool, str]:
    """Call the credit API. Returns (success, response_text)."""
    comment = f"Action Bonus: {action.replace('_', ' ').title()}"
    amount_cents = int(round(amount * 100))
    url = (
        f"{CREDIT_API_URL}"
        f"?brokerUserId={trading_account_id}"
        f"&amount={amount_cents}"
        f"&comment={comment}"
    )
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, headers={"x-crm-api-token": CREDIT_API_TOKEN})
            success = 200 <= resp.status_code < 300
            return success, resp.text[:2000]
    except Exception as e:
        return False, f"ERROR: {e}"


# ---------------------------------------------------------------------------
# Core processing (called from webhook)
# ---------------------------------------------------------------------------

async def process_action_bonus(
    event_type: str,
    accountid: str,
    trading_account_id: str,
    country: str | None,
    affiliate: str | None,
    db: AsyncSession,
) -> dict[str, Any]:
    """
    Process an action bonus for live_details or submit_documents events.
    Idempotent — each client can receive each action bonus at most once.
    """
    if event_type not in VALID_ACTIONS:
        return {"status": "ignored", "reason": f"unsupported action: {event_type}"}

    # Idempotency check
    existing = await db.execute(
        text(
            "SELECT 1 FROM action_bonus_log "
            "WHERE accountid = :acc AND action = :action LIMIT 1"
        ),
        {"acc": accountid, "action": event_type},
    )
    if existing.fetchone():
        logger.info("Action bonus already issued for accountid=%s action=%s — skipping", accountid, event_type)
        return {"status": "ok", "reason": "already rewarded"}

    # Fetch active rules for this action, ordered by priority ASC
    rules_result = await db.execute(
        text(
            "SELECT id, countries, affiliates, reward_amount "
            "FROM action_bonus_rules "
            "WHERE action = :action AND isactive = TRUE "
            "ORDER BY priority ASC"
        ),
        {"action": event_type},
    )
    rules = rules_result.fetchall()

    if not rules:
        return {"status": "ok", "reason": "no active rules"}

    # Find first matching rule
    matched_rule = None
    for rule in rules:
        rule_id, rule_countries, rule_affiliates, reward_amount = rule

        # countries filter (None = wildcard)
        if rule_countries:
            if not country:
                continue
            if country.upper() not in [c.upper() for c in rule_countries]:
                continue

        # affiliates filter (None = wildcard)
        if rule_affiliates:
            if not affiliate:
                continue
            if affiliate not in rule_affiliates:
                continue

        matched_rule = (rule_id, float(reward_amount))
        break

    if not matched_rule:
        logger.debug("No matching action bonus rule for accountid=%s action=%s country=%s affiliate=%s",
                     accountid, event_type, country, affiliate)
        return {"status": "ok", "reason": "no matching rule"}

    rule_id, reward_amount = matched_rule

    # Call credit API
    success, response_text = await _award_action_bonus(trading_account_id, reward_amount, event_type)

    # Log the result
    await db.execute(
        text(
            "INSERT INTO action_bonus_log "
            "(rule_id, accountid, trading_account_id, action, reward_amount, country, affiliate, credit_api_response, success) "
            "VALUES (:rule_id, :acc, :tid, :action, :reward, :country, :affiliate, :response, :success)"
        ),
        {
            "rule_id": rule_id,
            "acc": accountid,
            "tid": trading_account_id,
            "action": event_type,
            "reward": reward_amount,
            "country": country,
            "affiliate": affiliate,
            "response": response_text,
            "success": success,
        },
    )
    await db.commit()

    logger.info(
        "Action bonus: accountid=%s action=%s rule_id=%d reward=%.2f success=%s",
        accountid, event_type, rule_id, reward_amount, success,
    )
    return {"status": "ok", "rule_id": rule_id, "reward": reward_amount, "success": success}


# ---------------------------------------------------------------------------
# Admin CRUD endpoints
# ---------------------------------------------------------------------------

@router.get("/rules")
async def list_rules(
    action: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """List all action bonus rules, optionally filtered by action."""
    q = "SELECT id, action, countries, affiliates, reward_amount, reward_type, priority, isactive, created_at FROM action_bonus_rules"
    params: dict[str, Any] = {}
    if action:
        q += " WHERE action = :action"
        params["action"] = action
    q += " ORDER BY action, priority ASC"
    rows = await db.execute(text(q), params)
    return [
        {
            "id": r[0],
            "action": r[1],
            "countries": r[2],
            "affiliates": r[3],
            "reward_amount": float(r[4]),
            "reward_type": r[5],
            "priority": r[6],
            "isactive": r[7],
            "created_at": r[8].isoformat() if r[8] else None,
        }
        for r in rows.fetchall()
    ]


@router.post("/rules", status_code=201)
async def create_rule(
    body: ActionBonusRuleIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Create a new action bonus rule. Priority is set to max+1 for the action."""
    # Compute next priority
    max_prio = await db.execute(
        text("SELECT COALESCE(MAX(priority), -1) FROM action_bonus_rules WHERE action = :action"),
        {"action": body.action},
    )
    next_priority = (max_prio.fetchone()[0] or -1) + 1

    result = await db.execute(
        text(
            "INSERT INTO action_bonus_rules (action, countries, affiliates, reward_amount, priority, isactive) "
            "VALUES (:action, CAST(:countries AS jsonb), CAST(:affiliates AS jsonb), :reward, :priority, :isactive) "
            "RETURNING id"
        ),
        {
            "action": body.action,
            "countries": json.dumps(body.countries) if body.countries else None,
            "affiliates": json.dumps(body.affiliates) if body.affiliates else None,
            "reward": body.reward_amount,
            "priority": next_priority,
            "isactive": body.isactive,
        },
    )
    new_id = result.fetchone()[0]
    await db.commit()
    logger.info("Created action bonus rule id=%d action=%s", new_id, body.action)
    return {"status": "ok", "id": new_id}


@router.put("/rules/{rule_id}")
async def update_rule(
    rule_id: int,
    body: ActionBonusRuleIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Update an existing action bonus rule."""
    existing = await db.execute(
        text("SELECT 1 FROM action_bonus_rules WHERE id = :id"),
        {"id": rule_id},
    )
    if not existing.fetchone():
        raise HTTPException(404, f"Rule {rule_id} not found")

    await db.execute(
        text(
            "UPDATE action_bonus_rules SET "
            "action = :action, countries = CAST(:countries AS jsonb), affiliates = CAST(:affiliates AS jsonb), "
            "reward_amount = :reward, isactive = :isactive, updated_at = NOW() "
            "WHERE id = :id"
        ),
        {
            "action": body.action,
            "countries": json.dumps(body.countries) if body.countries else None,
            "affiliates": json.dumps(body.affiliates) if body.affiliates else None,
            "reward": body.reward_amount,
            "isactive": body.isactive,
            "id": rule_id,
        },
    )
    await db.commit()
    return {"status": "ok", "id": rule_id}


@router.patch("/rules/{rule_id}/toggle")
async def toggle_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Toggle isactive for a rule."""
    row = await db.execute(
        text("SELECT isactive FROM action_bonus_rules WHERE id = :id"),
        {"id": rule_id},
    )
    current = row.fetchone()
    if not current:
        raise HTTPException(404, f"Rule {rule_id} not found")

    new_status = not current[0]
    await db.execute(
        text("UPDATE action_bonus_rules SET isactive = :status, updated_at = NOW() WHERE id = :id"),
        {"status": new_status, "id": rule_id},
    )
    await db.commit()
    return {"status": "ok", "id": rule_id, "isactive": new_status}


@router.delete("/rules/{rule_id}")
async def delete_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Delete an action bonus rule."""
    result = await db.execute(
        text("DELETE FROM action_bonus_rules WHERE id = :id"),
        {"id": rule_id},
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, f"Rule {rule_id} not found")
    return {"status": "ok"}


@router.patch("/rules/reorder")
async def reorder_rules(
    body: ReorderRulesIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Bulk-update priority values for rules of the given action. rule_ids order = priority order."""
    for priority, rule_id in enumerate(body.rule_ids):
        await db.execute(
            text("UPDATE action_bonus_rules SET priority = :priority, updated_at = NOW() WHERE id = :id AND action = :action"),
            {"priority": priority, "id": rule_id, "action": body.action},
        )
    await db.commit()
    return {"status": "ok"}


@router.get("/log")
async def get_bonus_log(
    action: str | None = Query(None),
    country: str | None = Query(None),
    success: bool | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Paginated action bonus log with filters."""
    conditions = []
    params: dict[str, Any] = {}

    if action:
        conditions.append("l.action = :action")
        params["action"] = action
    if country:
        conditions.append("l.country ILIKE :country")
        params["country"] = f"%{country}%"
    if success is not None:
        conditions.append("l.success = :success")
        params["success"] = success
    if date_from:
        conditions.append("l.created_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("l.created_at < :date_to")
        params["date_to"] = date_to

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_row = await db.execute(
        text(f"SELECT COUNT(*) FROM action_bonus_log l {where}"),
        params,
    )
    total = count_row.fetchone()[0]

    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    rows = await db.execute(
        text(
            f"SELECT l.id, l.rule_id, l.accountid, l.trading_account_id, l.action, "
            f"l.reward_amount, l.country, l.affiliate, l.success, l.created_at "
            f"FROM action_bonus_log l {where} "
            f"ORDER BY l.created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    )

    items = [
        {
            "id": r[0],
            "rule_id": r[1],
            "accountid": r[2],
            "trading_account_id": r[3],
            "action": r[4],
            "reward_amount": float(r[5]),
            "country": r[6],
            "affiliate": r[7],
            "success": r[8],
            "created_at": r[9].isoformat() if r[9] else None,
        }
        for r in rows.fetchall()
    ]

    return {"total": total, "items": items}


@router.get("/campaigns")
async def get_campaigns(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Return distinct campaign_legacy_id values from vtiger_campaigns."""
    rows = await db.execute(
        text(
            "SELECT DISTINCT campaign_legacy_id, campaign_name "
            "FROM vtiger_campaigns "
            "WHERE campaign_legacy_id IS NOT NULL AND campaign_legacy_id != '' "
            "ORDER BY campaign_legacy_id"
        )
    )
    return [{"id": r[0], "name": r[1]} for r in rows.fetchall()]


@router.get("/countries")
async def get_countries(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Return distinct country values from the bonus log and a common hardcoded list."""
    # Try to get from existing client data
    try:
        rows = await db.execute(
            text(
                "SELECT DISTINCT country FROM action_bonus_log "
                "WHERE country IS NOT NULL ORDER BY country"
            )
        )
        from_log = [r[0] for r in rows.fetchall()]
    except Exception:
        from_log = []

    # Common countries hardcoded baseline
    common = [
        "Afghanistan", "Albania", "Algeria", "Angola", "Argentina", "Australia",
        "Austria", "Bahrain", "Bangladesh", "Belgium", "Bolivia", "Brazil",
        "Bulgaria", "Cambodia", "Canada", "Chile", "China", "Colombia",
        "Croatia", "Cyprus", "Czech Republic", "Denmark", "Ecuador", "Egypt",
        "Estonia", "Ethiopia", "Finland", "France", "Georgia", "Germany",
        "Ghana", "Greece", "Guatemala", "Hungary", "India", "Indonesia",
        "Iraq", "Ireland", "Israel", "Italy", "Japan", "Jordan", "Kazakhstan",
        "Kenya", "Kuwait", "Latvia", "Lebanon", "Libya", "Lithuania",
        "Malaysia", "Mexico", "Morocco", "Myanmar", "Netherlands", "New Zealand",
        "Nigeria", "Norway", "Oman", "Pakistan", "Peru", "Philippines",
        "Poland", "Portugal", "Qatar", "Romania", "Russia", "Saudi Arabia",
        "Serbia", "Singapore", "Slovakia", "Slovenia", "South Africa",
        "South Korea", "Spain", "Sri Lanka", "Sweden", "Switzerland",
        "Taiwan", "Tanzania", "Thailand", "Tunisia", "Turkey", "Uganda",
        "Ukraine", "United Arab Emirates", "United Kingdom", "United States",
        "Uruguay", "Uzbekistan", "Venezuela", "Vietnam", "Yemen", "Zimbabwe",
    ]

    # Merge, deduplicate, sort
    merged = sorted(set(common) | set(from_log))
    return merged
