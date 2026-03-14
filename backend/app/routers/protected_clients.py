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
        "SELECT accountid, retention_promo_group, active FROM [dbo].[accounts_protected_trades] WHERE accountid = ?",
        (req.accountid,),
    )

    today = date.today().isoformat()

    if existing:
        current_group = existing[0].get("retention_promo_group")
        is_active = existing[0].get("active")
        # Already protected with new group AND still active → skip
        if current_group in (32, 33, 34) and is_active:
            return {
                "status": "already_protected",
                "message": f"Client is already protected with group {current_group}",
                "current_group": current_group,
            }
        # Either old group (1/2/3) or inactive — delete and re-insert
        await execute_write(
            "DELETE FROM [dbo].[accounts_protected_trades] WHERE accountid = ?",
            (req.accountid,),
        )
        action = "reactivated" if not is_active else "updated"
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


@router.post("/protected-clients/reactivate-all")
async def reactivate_all_protected(
    _user=Depends(_require_protected_clients),
) -> dict:
    n = await execute_write(
        "UPDATE [dbo].[accounts_protected_trades] SET active = 1 WHERE retention_promo_group IN (32, 33, 34)",
        (),
    )
    return {"reactivated": n}


@router.get("/insured-clients/lookup/{accountid}")
async def lookup_insured_client(accountid: str) -> dict:
    """Public endpoint — no auth required. Returns insured status (groups 32/33/34) for a given accountid."""
    rows = await execute_query(
        """
        SELECT active, count_of_trades, retention_promo_group,
               cash_bonus_left_new, dateadded
        FROM [dbo].[accounts_protected_trades]
        WHERE accountid = ?
        """,
        (accountid,),
    )
    if not rows:
        return {"found": False}
    r = dict(rows[0])
    if hasattr(r.get("dateadded"), "isoformat"):
        r["dateadded"] = r["dateadded"].isoformat()
    return {"found": True, **r}


@router.get("/protected-clients/lookup/{accountid}")
async def lookup_protected_client(accountid: str) -> dict:
    """Public endpoint — no auth required. Returns protected status (groups 1-6) with calculated fields."""
    rows = await execute_query(
        """
        SELECT
            apt.accountid,
            apt.active,
            apt.retention_promo_group,
            apt.dateadded,
            apt.CurrentNetDeposit,
            apt.count_of_trades,
            apt.cash_bonus_left_new                                    AS CashReturned,
            apt.days_from_ftc,
            -- DaysLeftInGroup: group's days_from_ftd minus client's days_from_ftc
            (rpg.days_from_ftd - apt.days_from_ftc)                    AS DaysLeftInGroup,
            -- TradesLeft: group's trade target minus trades already done (floor 0)
            CASE WHEN (rpg.count_of_trades - apt.count_of_trades) < 0
                 THEN 0
                 ELSE (rpg.count_of_trades - apt.count_of_trades)
            END                                                         AS TradesLeft,
            -- AmountForNextGroup: nearest next group's min_net_deposit minus current net deposit
            (
                SELECT TOP 1 ng.min_net_deposit - apt.CurrentNetDeposit
                FROM [dbo].[retention_promo_groups] ng
                WHERE ng.min_net_deposit > rpg.min_net_deposit
                ORDER BY ng.min_net_deposit ASC
            )                                                           AS AmountForNextGroup
        FROM [dbo].[accounts_protected_trades_temp] apt
        JOIN [dbo].[retention_promo_groups] rpg ON rpg.id = apt.retention_promo_group
        WHERE apt.accountid = ?
        """,
        (accountid,),
    )
    if not rows:
        return {"found": False}
    r = dict(rows[0])
    if hasattr(r.get("dateadded"), "isoformat"):
        r["dateadded"] = r["dateadded"].isoformat()
    return {"found": True, **r}


async def expire_protected_clients() -> None:
    """Deactivate protected clients that have hit their group's limits.

    Runs every 15 minutes via APScheduler. Conditions (any one triggers deactivation):
    - count_of_trades reached the group limit (retention_promo_groups.count_of_trades)
    - cash_bonus_left_new reached the group max bonus (retention_promo_groups.max_amount_bonus)
    - dateadded + days_from_ftd days has passed
    """
    try:
        rowcount = await execute_write(
            """
            UPDATE apt
            SET apt.active = 0
            FROM [dbo].[accounts_protected_trades] apt
            JOIN [dbo].[retention_promo_groups] rpg ON rpg.id = apt.retention_promo_group
            WHERE apt.active = 1
              AND apt.retention_promo_group IN (32, 33, 34)
              AND (
                apt.count_of_trades >= rpg.count_of_trades
                OR apt.cash_bonus_left_new >= rpg.max_total_amount
                OR DATEDIFF(day, apt.dateadded, GETDATE()) >= rpg.days_from_ftd
              )
            """,
            (),
        )
        logger.info("[ProtectedClients] expire_protected_clients: deactivated %d row(s)", rowcount)
    except Exception as e:
        logger.error("[ProtectedClients] expire_protected_clients failed: %s", e)


@router.get("/protected-clients/list")
async def list_protected_clients(
    active: int | None = None,  # 1 = active only, 0 = inactive only, None = all
    _user=Depends(_require_protected_clients),
) -> list:
    where = "WHERE retention_promo_group IN (32, 33, 34)"
    params: tuple = ()
    if active is not None:
        where += " AND active = ?"
        params = (active,)
    rows = await execute_query(
        f"""
        SELECT accountid, count_of_trades, days_from_ftc, mt4login,
               trading_account_id, retention_promo_group, cash_bonus_left_new,
               active, dateadded
        FROM [dbo].[accounts_protected_trades]
        {where}
        ORDER BY dateadded DESC
        """,
        params,
    )
    # Serialize date objects to string
    result = []
    for row in rows:
        r = dict(row)
        if hasattr(r.get("dateadded"), "isoformat"):
            r["dateadded"] = r["dateadded"].isoformat()
        result.append(r)
    return result


@router.get("/protected-clients/list-legacy")
async def list_legacy_protected_clients(
    active: int | None = None,
    _user=Depends(_require_protected_clients),
) -> list:
    """Clients in retention promo groups 1–6 (legacy 'Clients in Protected')."""
    where = "WHERE retention_promo_group IN (1, 2, 3, 4, 5, 6)"
    params: tuple = ()
    if active is not None:
        where += " AND active = ?"
        params = (active,)
    rows = await execute_query(
        f"""
        SELECT accountid, count_of_trades, days_from_ftc, mt4login,
               trading_account_id, retention_promo_group, cash_bonus_left_new,
               active, dateadded, CurrentNetDeposit
        FROM [dbo].[accounts_protected_trades_temp]
        {where}
          AND dateadded IS NOT NULL
        ORDER BY dateadded DESC
        """,
        params,
    )
    result = []
    for row in rows:
        r = dict(row)
        if hasattr(r.get("dateadded"), "isoformat"):
            r["dateadded"] = r["dateadded"].isoformat()
        result.append(r)
    return result


@router.get("/protected-clients/groups")
async def list_protection_groups(
    _user=Depends(_require_protected_clients),
) -> list:
    rows = await execute_query(
        """
        SELECT TOP (1000) [id], [days_from_ftd], [max_amount_bonus], [max_total_amount],
               [count_of_trades], [min_net_deposit], [max_net_deposit], [is_promo],
               [promo_start_date], [promo_end_date], [promo_type],
               [max_percentage_bonus], [payback_method]
        FROM [dbo].[retention_promo_groups]
        """,
        (),
    )
    result = []
    for row in rows:
        r = dict(row)
        for key in ("promo_start_date", "promo_end_date"):
            if hasattr(r.get(key), "isoformat"):
                r[key] = r[key].isoformat()
        result.append(r)
    return result
