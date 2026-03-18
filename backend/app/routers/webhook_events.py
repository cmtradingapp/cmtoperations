"""Webhook Events — generic event ingestion + action rules management.

Endpoints:
- POST   /api/webhooks/event                   — public generic webhook (no auth)
- GET    /api/webhook-events/log               — paginated event log (admin)
- GET    /api/webhook-events/actions           — list action rules (admin)
- POST   /api/webhook-events/actions           — create action rule (admin)
- PATCH  /api/webhook-events/actions/{id}      — update action rule (admin)
- DELETE /api/webhook-events/actions/{id}      — delete action rule (admin)
- GET    /api/webhook-events/stats             — event counts by name (admin)
"""

import json
import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.pg_database import get_db
from app.rbac import make_page_guard

logger = logging.getLogger(__name__)
router = APIRouter()

_require = make_page_guard("webhook-events")

ACTION_TYPES = ("log_only", "optimove", "chrome_plugin", "challenge", "bonus")

# Known event names (informational — the endpoint accepts anything)
KNOWN_EVENTS = ("open_trade",)


# ── Pydantic models ────────────────────────────────────────────────────────────

class GenericEventPayload(BaseModel):
    model_config = {"extra": "allow"}
    event: str
    customer: Any = None
    context: Any = None


class ActionRuleIn(BaseModel):
    event_name: str
    action_type: str  # log_only | optimove | challenge | bonus
    label: Optional[str] = None
    config: Optional[dict] = None  # action-specific config
    is_active: bool = True


class ActionRulePatch(BaseModel):
    action_type: Optional[str] = None
    label: Optional[str] = None
    config: Optional[dict] = None
    is_active: Optional[bool] = None


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _get_optimove_url(db: AsyncSession) -> str | None:
    result = await db.execute(
        text("SELECT base_url FROM integrations WHERE name = 'Optimove' AND is_active = TRUE LIMIT 1")
    )
    row = result.fetchone()
    return row[0] if row else None


async def _apply_optimove_action(
    config: dict,
    payload: GenericEventPayload,
    db: AsyncSession,
) -> str:
    """Forward the event to Optimove. Returns 'ok' or error description."""
    optimove_url = await _get_optimove_url(db)
    if not optimove_url:
        return "no_optimove_url"

    # Use configured optimove_event_name, or fall back to the raw event name
    optimove_event = config.get("optimove_event_name") or payload.event

    ctx = payload.context or {}
    if not isinstance(ctx, dict):
        ctx = {}

    body = {
        "tenant": config.get("tenant", 991),
        "event": optimove_event,
        "customer": str(payload.customer) if payload.customer is not None else None,
        "context": ctx,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                optimove_url,
                json=body,
                headers={"Content-Type": "application/json"},
            )
            return f"{resp.status_code}"
    except Exception as e:
        return f"error:{e}"


async def _get_chrome_plugin_url(db: AsyncSession) -> tuple[str | None, str | None]:
    """Return (push_url, push_secret) from integrations table."""
    result = await db.execute(
        text("SELECT base_url, api_key FROM integrations WHERE name = 'Chrome Plugin' AND is_active = TRUE LIMIT 1")
    )
    row = result.fetchone()
    return (row[0], row[1]) if row else (None, None)


async def _apply_chrome_plugin_action(
    config: dict,
    payload: GenericEventPayload,
) -> str:
    """Push event to the alertextension SSE server. Returns status string."""
    push_url = config.get("push_url")
    push_secret = config.get("push_secret")

    if not push_url:
        return "no_push_url"

    customer = str(payload.customer) if payload.customer is not None else None
    ctx = payload.context or {}
    if not isinstance(ctx, dict):
        ctx = {}

    body = {
        "event_type": payload.event,
        "customer": customer,
        "broadcast": config.get("broadcast", False),
        "agent_email": config.get("agent_email"),  # optional fixed target
        "data": ctx,
    }

    headers: dict = {"Content-Type": "application/json"}
    if push_secret:
        headers["X-Push-Secret"] = push_secret

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(push_url, json=body, headers=headers)
            return f"{resp.status_code}"
    except Exception as e:
        return f"error:{e}"


async def _store_event(
    db: AsyncSession,
    event_name: str,
    customer: str | None,
    payload_dict: dict,
    actions_applied: list,
) -> int:
    result = await db.execute(
        text(
            "INSERT INTO webhook_event_log (event_name, customer, payload, actions_applied, created_at) "
            "VALUES (:ev, :cust, CAST(:payload AS jsonb), CAST(:actions AS jsonb), NOW()) RETURNING id"
        ),
        {
            "ev": event_name,
            "cust": customer,
            "payload": json.dumps(payload_dict),
            "actions": json.dumps(actions_applied),
        },
    )
    row = result.fetchone()
    return row[0] if row else 0


# ── Webhook endpoint ───────────────────────────────────────────────────────────

@router.post("/webhooks/event")
async def receive_generic_event(
    payload: GenericEventPayload,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Accept any event, store it, and apply configured action rules."""
    event_name = payload.event.strip()
    customer = str(payload.customer) if payload.customer is not None else None
    payload_dict = payload.model_dump()

    # Load active action rules for this event
    rules_result = await db.execute(
        text(
            "SELECT id, action_type, config FROM webhook_event_actions "
            "WHERE event_name = :ev AND is_active = TRUE ORDER BY id"
        ),
        {"ev": event_name},
    )
    rules = rules_result.fetchall()

    actions_applied = []

    for rule_id, action_type, config in rules:
        cfg = config or {}
        result_str = "ok"

        if action_type == "log_only":
            result_str = "logged"

        elif action_type == "optimove":
            result_str = await _apply_optimove_action(cfg, payload, db)
            logger.info(
                "WEBHOOK: optimove action rule=%d event=%s customer=%s result=%s",
                rule_id, event_name, customer, result_str,
            )

        elif action_type == "chrome_plugin":
            result_str = await _apply_chrome_plugin_action(cfg, payload)
            logger.info(
                "WEBHOOK: chrome_plugin action rule=%d event=%s customer=%s result=%s",
                rule_id, event_name, customer, result_str,
            )

        elif action_type in ("challenge", "bonus"):
            # Placeholder — implement when needed
            result_str = "not_implemented"
            logger.info("WEBHOOK: %s action not yet implemented (rule=%d)", action_type, rule_id)

        actions_applied.append({"rule_id": rule_id, "action": action_type, "result": result_str})

    # Always store the event
    log_id = await _store_event(db, event_name, customer, payload_dict, actions_applied)
    await db.commit()

    logger.info("WEBHOOK: stored event=%s customer=%s log_id=%d actions=%d", event_name, customer, log_id, len(actions_applied))
    return {"received": True, "log_id": log_id, "actions_applied": len(actions_applied)}


# ── Admin: event log ───────────────────────────────────────────────────────────

@router.get("/webhook-events/log")
async def get_event_log(
    event_name: Optional[str] = Query(None),
    customer: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> dict:
    where = ["1=1"]
    params: dict = {}
    if event_name:
        where.append("event_name = :event_name")
        params["event_name"] = event_name
    if customer:
        where.append("customer = :customer")
        params["customer"] = customer

    where_sql = " AND ".join(where)
    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    total_result = await db.execute(
        text(f"SELECT COUNT(*) FROM webhook_event_log WHERE {where_sql}"), params
    )
    total = total_result.scalar() or 0

    rows_result = await db.execute(
        text(
            f"SELECT id, event_name, customer, payload, actions_applied, created_at "
            f"FROM webhook_event_log WHERE {where_sql} "
            f"ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    )
    rows = rows_result.fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": [
            {
                "id": r[0],
                "event_name": r[1],
                "customer": r[2],
                "payload": r[3],
                "actions_applied": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ],
    }


@router.get("/webhook-events/stats")
async def get_event_stats(
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> list:
    result = await db.execute(
        text(
            "SELECT event_name, COUNT(*) as total, "
            "MAX(created_at) as last_received "
            "FROM webhook_event_log "
            "GROUP BY event_name ORDER BY total DESC"
        )
    )
    rows = result.fetchall()
    return [
        {
            "event_name": r[0],
            "total": r[1],
            "last_received": r[2].isoformat() if r[2] else None,
        }
        for r in rows
    ]


# ── Admin: action rules ────────────────────────────────────────────────────────

@router.get("/webhook-events/actions")
async def list_action_rules(
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> list:
    result = await db.execute(
        text(
            "SELECT id, event_name, action_type, label, config, is_active, created_at, updated_at "
            "FROM webhook_event_actions ORDER BY event_name, id"
        )
    )
    rows = result.fetchall()
    return [
        {
            "id": r[0],
            "event_name": r[1],
            "action_type": r[2],
            "label": r[3],
            "config": r[4] or {},
            "is_active": r[5],
            "created_at": r[6].isoformat() if r[6] else None,
            "updated_at": r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]


@router.post("/webhook-events/actions", status_code=201)
async def create_action_rule(
    body: ActionRuleIn,
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> dict:
    if body.action_type not in ACTION_TYPES:
        raise HTTPException(status_code=400, detail=f"action_type must be one of {ACTION_TYPES}")

    result = await db.execute(
        text(
            "INSERT INTO webhook_event_actions (event_name, action_type, label, config, is_active, created_at, updated_at) "
            "VALUES (:ev, :at, :label, CAST(:cfg AS jsonb), :active, NOW(), NOW()) RETURNING id"
        ),
        {
            "ev": body.event_name,
            "at": body.action_type,
            "label": body.label,
            "cfg": json.dumps(body.config or {}),
            "active": body.is_active,
        },
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id": new_id, "created": True}


@router.patch("/webhook-events/actions/{action_id}")
async def update_action_rule(
    action_id: int,
    body: ActionRulePatch,
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> dict:
    # Build SET clause dynamically
    sets = ["updated_at = NOW()"]
    params: dict = {"id": action_id}

    if body.action_type is not None:
        if body.action_type not in ACTION_TYPES:
            raise HTTPException(status_code=400, detail=f"action_type must be one of {ACTION_TYPES}")
        sets.append("action_type = :at")
        params["at"] = body.action_type
    if body.label is not None:
        sets.append("label = :label")
        params["label"] = body.label
    if body.config is not None:
        sets.append("config = CAST(:cfg AS jsonb)")
        params["cfg"] = json.dumps(body.config)
    if body.is_active is not None:
        sets.append("is_active = :active")
        params["active"] = body.is_active

    await db.execute(
        text(f"UPDATE webhook_event_actions SET {', '.join(sets)} WHERE id = :id"),
        params,
    )
    await db.commit()
    return {"updated": True}


@router.delete("/webhook-events/actions/{action_id}")
async def delete_action_rule(
    action_id: int,
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require),
) -> dict:
    await db.execute(
        text("DELETE FROM webhook_event_actions WHERE id = :id"),
        {"id": action_id},
    )
    await db.commit()
    return {"deleted": True}
