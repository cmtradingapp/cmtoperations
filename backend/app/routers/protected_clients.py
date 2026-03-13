import asyncio
import json
import logging
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import execute_query, execute_write
from app.pg_database import get_db
from app.rbac import make_page_guard

router = APIRouter()
logger = logging.getLogger(__name__)

_require_protected_clients = make_page_guard("protected-clients")

# ── Optimove helpers ──────────────────────────────────────────────────────────

async def _get_optimove_url(db: AsyncSession) -> str | None:
    result = await db.execute(
        text("SELECT base_url FROM integrations WHERE name = 'Optimove' AND is_active = TRUE LIMIT 1")
    )
    row = result.fetchone()
    return row[0] if row else None


async def _fire_optimove_event(
    url: str, payload: dict, accountid: str, db: AsyncSession
) -> None:
    response_text = ""
    success = False
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
            response_text = resp.text[:2000]
            success = 200 <= resp.status_code < 300
    except Exception as e:
        response_text = f"ERROR: {e}"
    try:
        await db.execute(
            text(
                'INSERT INTO optimove_event_log '
                '("challengeId", accountid, event_name, payload, response, success, created_at) '
                "VALUES (:cid, :acc, :event, CAST(:payload AS jsonb), :resp, :ok, NOW())"
            ),
            {
                "cid": None,
                "acc": accountid,
                "event": "user_added_to_protected",
                "payload": json.dumps(payload),
                "resp": response_text,
                "ok": success,
            },
        )
        await db.commit()
    except Exception as e:
        logger.warning("Failed to log Optimove event: %s", e)


# ── Request model ─────────────────────────────────────────────────────────────

class AddProtectedRequest(BaseModel):
    accountid: str
    group: int  # 32, 33, or 34


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/protected-clients/add")
async def add_protected_client(
    req: AddProtectedRequest,
    db: AsyncSession = Depends(get_db),
    _user=Depends(_require_protected_clients),
) -> dict:
    if req.group not in (32, 33, 34):
        raise HTTPException(status_code=400, detail="Group must be 32, 33, or 34")

    # Step 1: Get mt4login and trading_account_id from MSSQL
    # Note: column is trading_acount_id (single 'c') — typo in production MSSQL
    vta_rows = await execute_query(
        "SELECT login, trading_acount_id FROM report.vtiger_trading_accounts WHERE vtigeraccountid = ?",
        (req.accountid,),
    )
    if not vta_rows:
        return {
            "status": "client_not_found",
            "message": "No trading account found for this Account ID",
        }

    vta = vta_rows[0]
    mt4login = str(vta.get("login") or "")
    trading_account_id = str(vta.get("trading_acount_id") or "")

    # Step 2: Check MSSQL accounts_protected_trades
    existing = await execute_query(
        "SELECT accountid, retention_promo_group FROM [dbo].[accounts_protected_trades] WHERE accountid = ?",
        (req.accountid,),
    )

    today = date.today().isoformat()

    if existing:
        current_group = existing[0].get("retention_promo_group")
        if current_group in (32, 33, 34):
            return {
                "status": "already_protected",
                "message": f"Client is already protected with group {current_group}",
                "current_group": current_group,
            }
        # Route B: old group (1/2/3) — delete and re-insert
        await execute_write(
            "DELETE FROM [dbo].[accounts_protected_trades] WHERE accountid = ?",
            (req.accountid,),
        )
        action = "updated"
    else:
        action = "added"

    # Step 3: Fetch promo group details for Optimove context
    promo_rows = await execute_query(
        "SELECT count_of_trades, max_amount_bonus FROM [dbo].[retention_promo_groups] WHERE id = ?",
        (req.group,),
    )
    promo = promo_rows[0] if promo_rows else {}
    number_of_trades = promo.get("count_of_trades", 0)
    max_bonus_amount = promo.get("max_amount_bonus", 0)

    # Step 4: INSERT new protected record
    await execute_write(
        """
        INSERT INTO [dbo].[accounts_protected_trades]
        (accountid, count_of_trades, cash_bonus_left, days_from_ftc, mt4login,
         trading_account_id, retention_promo_group, cash_bonus_left_old,
         cash_bonus_left_new, active, dateadded, CurrentNetDeposit)
        VALUES (?, 0, 0, 0, ?, ?, ?, 0, 0, 1, ?, 0)
        """,
        (req.accountid, mt4login, trading_account_id, req.group, today),
    )

    # Step 5: Fire Optimove event (fire-and-forget)
    optimove_url = await _get_optimove_url(db)
    if optimove_url:
        payload = {
            "tenant": 991,
            "event": "user_added_to_protected",
            "customer": req.accountid,
            "context": {
                "number_of_trades": number_of_trades,
                "max_bonus_amount": str(max_bonus_amount),
                "language": "EN",
            },
        }
        asyncio.create_task(_fire_optimove_event(optimove_url, payload, req.accountid, db))

    logger.info("[ProtectedClients] %s accountid=%s group=%d mt4login=%s", action, req.accountid, req.group, mt4login)

    return {
        "status": "success",
        "action": action,
        "accountid": req.accountid,
        "group": req.group,
        "mt4login": mt4login,
        "trading_account_id": trading_account_id,
    }
