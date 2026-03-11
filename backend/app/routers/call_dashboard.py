import calendar
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user, get_jwt_payload, JWTPayload
from app.config import settings
from app.models.audit_log import AuditLog
from app.models.role import ALL_PAGES, Role
from app.pg_database import get_db
from app.rbac import get_client_scope_filter
from app.routers.crm import _crm_headers, _handle_crm_response
from app.routers.retention import _resolve_status_name, _APPROVED_STATUS_NAMES, _resolve_display_status
from app.services.internal_api import get_crm_data

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# CLAUD-36: Cache role permissions to avoid a DB hit on every call-dashboard
# request. Keyed by role name. TTL is 120 seconds (roles change rarely).
# ---------------------------------------------------------------------------
_ROLE_PERM_CACHE: dict[str, tuple[list, float]] = {}
_ROLE_PERM_TTL = 120  # seconds


async def require_call_dashboard_access(
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> JWTPayload:
    """Verify the user's role includes the 'retention-dial' page permission.

    Permissions are cached per role name for 120 seconds so the roles table
    is not queried on every single call-dashboard API request.
    """
    if jwt_payload.role == "admin":
        return jwt_payload

    now = time.monotonic()
    cached_entry = _ROLE_PERM_CACHE.get(jwt_payload.role)
    if cached_entry and cached_entry[1] > now:
        permissions = cached_entry[0]
    else:
        result = await db.execute(select(Role).where(Role.name == jwt_payload.role))
        role_obj = result.scalar_one_or_none()
        permissions = role_obj.permissions or [] if role_obj else []
        _ROLE_PERM_CACHE[jwt_payload.role] = (permissions, now + _ROLE_PERM_TTL)

    if "retention-dial" not in permissions:
        raise HTTPException(status_code=403, detail="Call Dashboard access denied for your role")
    return jwt_payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _log_audit(
    db: AsyncSession,
    agent_id: int,
    agent_username: str,
    client_account_id: str,
    action_type: str,
    action_value: str | None = None,
) -> None:
    """Insert a row into audit_log. Best-effort: logs a warning on failure."""
    try:
        entry = AuditLog(
            agent_id=agent_id,
            agent_username=agent_username,
            client_account_id=client_account_id,
            action_type=action_type,
            action_value=action_value,
        )
        db.add(entry)
        await db.flush()
        logger.info(
            "Audit logged: %s by %s (id=%d) on client %s",
            action_type, agent_username, agent_id, client_account_id,
        )
    except Exception as exc:
        logger.warning("Failed to write audit log: %s", exc)


async def _compute_lifecycle(row: dict, db: AsyncSession) -> list[dict]:
    """CLAUD-142: Compute lifecycle milestones dynamically from lifecycle_stages table."""
    stages_result = await db.execute(
        text(
            "SELECT id, name, key, metric_type, threshold "
            "FROM lifecycle_stages WHERE is_active = true ORDER BY display_order ASC"
        )
    )
    stages = stages_result.fetchall()
    lifecycle = []
    for stage_id, name, key, metric_type, threshold in stages:
        t = float(threshold)
        if metric_type == "ftd":
            reached = True  # presence in retention_mv implies FTD
        elif metric_type == "deposit":
            reached = (row.get("deposit_count") or 0) >= t
        elif metric_type == "position":
            reached = (row.get("trade_count") or 0) >= t
        elif metric_type == "volume":
            reached = (row.get("max_volume") or 0) >= t
        else:
            reached = False
        lifecycle.append({"id": stage_id, "name": name, "key": key, "reached": reached})
    return lifecycle


# ---------------------------------------------------------------------------
# GET /call-dashboard/queue
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/queue")
async def get_queue(
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return up to 50 clients for the agent call queue.

    Priority:
      1. Clients with pending callbacks (agent_id = current user, callback_time <= NOW()+5min)
         ordered by callback_time ASC
      2. All other clients from retention_mv ordered by score DESC, last_deposit_time ASC NULLS LAST
    """
    user_id = jwt_payload.user_id
    scope_filter, scope_params = get_client_scope_filter(
        jwt_payload.role, jwt_payload.vtiger_user_id, jwt_payload.vtiger_department,
        team=jwt_payload.team,
        app_department=jwt_payload.app_department,
    )

    try:
        result = await db.execute(
            text(f"""
                WITH callback_clients AS (
                    SELECT cc.accountid,
                           cc.callback_time,
                           TRUE AS has_callback
                    FROM client_callbacks cc
                    WHERE cc.agent_id = :uid
                      AND NOT cc.is_done
                      AND cc.callback_time <= NOW() + INTERVAL '5 minutes'
                    ORDER BY cc.callback_time ASC
                ),
                -- CLAUD-172: aggregate open PNL + leverage per vtiger account
                mt_data AS (
                    SELECT ta.vtigeraccountid AS accountid,
                           COALESCE(SUM(opc.pnl), 0) AS open_pnl,
                           COALESCE(
                               MAX(CASE WHEN COALESCE(du.equity, 0) > 0 THEN NULLIF(du.leverage, 0) END),
                               NULLIF(MAX(du.leverage), 0),
                               200
                           ) AS leverage
                    FROM vtiger_trading_accounts ta
                    LEFT JOIN dealio_users du ON du.login = ta.login
                    LEFT JOIN open_pnl_cache opc ON opc.login::bigint = ta.login
                    GROUP BY ta.vtigeraccountid
                ),
                queue AS (
                    SELECT m.accountid,
                           m.full_name,
                           COALESCE(cs.score, 0) AS score,
                           m.total_balance,
                           m.total_credit,
                           (uf.accountid IS NOT NULL) AS is_favorite,
                           (cb.accountid IS NOT NULL) AS has_callback,
                           cb.callback_time,
                           CASE
                               WHEN cb.accountid IS NOT NULL THEN 0
                               ELSE 1
                           END AS sort_group,
                           CASE
                               WHEN cb.accountid IS NOT NULL THEN cb.callback_time
                               ELSE NULL
                           END AS cb_sort,
                           COALESCE(cs.score, 0) AS score_sort,
                           m.last_deposit_time,
                           COALESCE(exp.exposure_usd, 0) AS exposure_usd,
                           COALESCE(mt.open_pnl, 0) AS open_pnl,
                           COALESCE(mt.leverage, 200) AS leverage
                    FROM retention_mv m
                    LEFT JOIN client_scores cs ON cs.accountid = m.accountid
                    LEFT JOIN user_favorites uf ON uf.accountid = m.accountid AND uf.user_id = :uid
                    LEFT JOIN callback_clients cb ON cb.accountid = m.accountid
                    LEFT JOIN account_exposure_cache exp ON exp.accountid = m.accountid
                    LEFT JOIN mt_data mt ON mt.accountid = m.accountid
                    WHERE m.client_qualification_date IS NOT NULL
                    {scope_filter}
                    ORDER BY sort_group ASC,
                             cb_sort ASC NULLS LAST,
                             score_sort DESC,
                             m.last_deposit_time ASC NULLS LAST
                    LIMIT 50
                )
                SELECT * FROM queue
            """),
            {"uid": user_id, **scope_params},
        )
        rows = result.mappings().all()

        out = []
        for r in rows:
            _exposure = float(r["exposure_usd"])
            _leverage = max(float(r["leverage"] or 200), 1)
            _live_eq = float(r["total_balance"] or 0) + float(r["total_credit"] or 0) + float(r["open_pnl"] or 0)
            _used_margin = _exposure / _leverage if _exposure > 0 else 0.0
            _margin_level_pct = round(_live_eq / _used_margin * 100, 1) if _used_margin > 0 else None
            out.append({
                "accountid": str(r["accountid"]),
                "full_name": r["full_name"] or "",
                "score": int(r["score"]),
                "total_balance": float(r["total_balance"] or 0),
                "is_favorite": bool(r["is_favorite"]),
                "has_callback": bool(r["has_callback"]),
                "callback_time": r["callback_time"].isoformat() if r["callback_time"] else None,
                "margin_level_pct": _margin_level_pct,
            })
        return out
    except Exception as e:
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        raise HTTPException(status_code=502, detail=f"Queue query failed: {e}")


# ---------------------------------------------------------------------------
# GET /call-dashboard/client/{account_id}
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/client/{account_id}")
async def get_client_detail(
    account_id: str,
    login: int | None = Query(None, description="CLAUD-173: filter to a specific MT login"),
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return full client detail for the active panel."""
    user_id = jwt_payload.user_id

    try:
        # CLAUD-173: Fetch all MT accounts for this client (for account selector chips)
        mt_accts_result = await db.execute(
            text("""
                SELECT ta.login,
                       COALESCE(du.balance, 0) AS balance,
                       COALESCE(du.equity, 0) AS equity,
                       COALESCE(du.leverage, 200) AS leverage
                FROM vtiger_trading_accounts ta
                LEFT JOIN dealio_users du ON du.login = ta.login
                WHERE ta.vtigeraccountid = :account_id
                ORDER BY COALESCE(du.equity, 0) DESC, ta.login
            """),
            {"account_id": account_id},
        )
        mt_accounts = [
            {"login": int(r["login"]), "balance": float(r["balance"]), "equity": float(r["equity"])}
            for r in mt_accts_result.mappings().all()
        ]

        # Main client data from retention_mv + joins
        mv_result = await db.execute(
            text("""
                SELECT m.accountid,
                       m.full_name,
                       m.client_qualification_date,
                       m.trade_count,
                       m.total_profit,
                       m.last_trade_date,
                       m.deposit_count,
                       m.total_deposit,
                       m.total_balance,
                       m.total_credit,
                       m.total_equity,
                       m.max_open_trade,
                       m.max_volume,
                       m.win_rate,
                       m.avg_trade_size,
                       m.assigned_to,
                       m.agent_name,
                       m.sales_client_potential,
                       m.birth_date,
                       m.last_deposit_time,
                       COALESCE(cs.score, 0) AS score,
                       (uf.accountid IS NOT NULL) AS is_favorite,
                       cct.card_type,
                       COALESCE(cas.is_active, FALSE) AS is_active,
                       m.last_deposit_time AS last_deposit_date,
                       lw.last_withdrawal_date,
                       COALESCE(aa.net_deposit, aa.total_deposit - aa.total_withdrawal) AS net_deposit_ever,
                       aa.country_iso AS country,
                       crs.status_label AS retention_status,
                       lc.last_communication_date
                FROM retention_mv m
                LEFT JOIN client_scores cs ON cs.accountid = m.accountid
                LEFT JOIN user_favorites uf ON uf.accountid = m.accountid AND uf.user_id = :uid
                LEFT JOIN client_card_type cct ON cct.accountid = m.accountid
                LEFT JOIN client_active_status cas ON cas.accountid = m.accountid
                LEFT JOIN ant_acc aa ON aa.accountid = m.accountid
                LEFT JOIN client_retention_status crs ON crs.accountid = m.accountid
                LEFT JOIN (
                    SELECT al.client_account_id,
                           MAX(al.timestamp) AS last_communication_date
                    FROM audit_log al
                    WHERE al.action_type IN ('status_change','note_added','call_initiated','whatsapp_opened')
                    GROUP BY al.client_account_id
                ) lc ON lc.client_account_id = m.accountid
                LEFT JOIN (
                    SELECT ta.vtigeraccountid AS accountid,
                           MAX(tx.modifiedtime) AS last_withdrawal_date
                    FROM vtiger_mttransactions tx
                    JOIN vtiger_trading_accounts ta ON ta.login = tx.login
                    WHERE tx.transactiontype IN ('Withdrawal', 'Withdraw')
                    GROUP BY ta.vtigeraccountid
                ) lw ON lw.accountid = m.accountid
                WHERE m.accountid = :account_id
                LIMIT 1
            """),
            {"account_id": account_id, "uid": user_id},
        )
        row = mv_result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Client not found in retention data.")

        # Pending callback for current agent
        cb_result = await db.execute(
            text("""
                SELECT id, callback_time, note
                FROM client_callbacks
                WHERE accountid = :account_id
                  AND agent_id = :uid
                  AND NOT is_done
                ORDER BY callback_time ASC
                LIMIT 1
            """),
            {"account_id": account_id, "uid": user_id},
        )
        cb_row = cb_result.mappings().first()

        # Per-symbol open positions from local trades_mt4
        # CLAUD-173: filter by specific login when selected
        _pos_login_clause = "AND t.login = :login_filter" if login else ""
        pos_result = await db.execute(
            text(f"""
                SELECT symbol,
                       SIGN(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END)) AS direction_sign,
                       COUNT(*) AS position_count,
                       ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END)) AS exposure_usd
                FROM trades_mt4 t
                JOIN vtiger_trading_accounts vta ON vta.login = t.login::bigint
                WHERE vta.vtigeraccountid = :account_id
                  {_pos_login_clause}
                  AND t.close_time < '1971-01-01'
                  AND t.cmd IN (0, 1)
                GROUP BY symbol
                HAVING ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END)) > 0
                ORDER BY exposure_usd DESC
            """),
            {"account_id": account_id, **({"login_filter": login} if login else {})},
        )
        positions = [
            {
                "symbol": str(p["symbol"]),
                "direction": "Buy" if (p["direction_sign"] or 0) >= 0 else "Sell",
                "position_count": int(p["position_count"] or 0),
                "exposure_usd": round(float(p["exposure_usd"] or 0), 2),
            }
            for p in pos_result.mappings().all()
        ]

        # Open PnL from cache (CLAUD-178: replica query removed — connection drops added latency)
        # CLAUD-173: filter by login if selected
        if login:
            pnl_result = await db.execute(
                text("SELECT COALESCE(pnl, 0) AS total_pnl FROM open_pnl_cache WHERE login = :login"),
                {"login": str(login)},
            )
        else:
            pnl_result = await db.execute(
                text("""
                    SELECT COALESCE(SUM(opc.pnl), 0) AS total_pnl
                    FROM open_pnl_cache opc
                    JOIN vtiger_trading_accounts vta ON vta.login::text = opc.login
                    WHERE vta.vtigeraccountid = :account_id
                """),
                {"account_id": account_id},
            )
        open_pnl = float(pnl_result.scalar() or 0)

        # CLAUD-173: If a specific login is selected, use dealio_users for balance/credit/equity/leverage
        if login:
            du_result = await db.execute(
                text("SELECT balance, credit, equity, leverage FROM dealio_users WHERE login = :login LIMIT 1"),
                {"login": login},
            )
            du_row = du_result.mappings().first()
            balance = float(du_row["balance"] or 0) if du_row else 0.0
            credit = float(du_row["credit"] or 0) if du_row else 0.0
            equity = float(du_row["equity"] or 0) if du_row else 0.0
            leverage = max(float(du_row["leverage"] or 200) if du_row else 200.0, 1)
        else:
            # Fetch leverage for this account (CLAUD-172)
            lev_result = await db.execute(
                text("""
                    SELECT COALESCE(
                        MAX(CASE WHEN COALESCE(du.equity, 0) > 0 THEN NULLIF(du.leverage, 0) END),
                        NULLIF(MAX(du.leverage), 0),
                        200
                    ) AS leverage
                    FROM vtiger_trading_accounts ta
                    LEFT JOIN dealio_users du ON du.login = ta.login
                    WHERE ta.vtigeraccountid = :account_id
                """),
                {"account_id": account_id},
            )
            leverage = max(float(lev_result.scalar() or 200), 1)
            balance = float(row["total_balance"] or 0)
            equity = float(row["total_equity"] or 0)
            credit = float(row["total_credit"] or 0)

        total_exposure_usd = sum(p["exposure_usd"] for p in positions)
        live_equity = balance + credit + open_pnl
        # CLAUD-53: Exposure % = (Live Equity / Exposure USD) × 100; None when Exposure USD = 0
        exposure_pct = round(live_equity / total_exposure_usd * 100, 2) if total_exposure_usd != 0 else None
        margin = round(balance - equity, 2)
        # CLAUD-172: Margin calculations
        used_margin = round(total_exposure_usd / leverage, 2) if total_exposure_usd > 0 else 0.0
        free_margin = round(live_equity - used_margin, 2)
        margin_level_pct = round(live_equity / used_margin * 100, 1) if used_margin > 0 else None

        lifecycle = await _compute_lifecycle(dict(row), db)

        # Fetch live retention status from CRM API
        async with httpx.AsyncClient() as http_client:
            crm_data = await get_crm_data(http_client, account_id)
        live_retention_status = _resolve_display_status(crm_data.retention_status_id)

        return {
            "accountid": str(row["accountid"]),
            "full_name": row["full_name"] or "",
            "client_qualification_date": row["client_qualification_date"].isoformat() if row["client_qualification_date"] else None,
            "trade_count": int(row["trade_count"] or 0),
            "total_profit": float(row["total_profit"] or 0),
            "last_trade_date": row["last_trade_date"].isoformat() if row["last_trade_date"] else None,
            "deposit_count": int(row["deposit_count"] or 0),
            "total_deposit": float(row["total_deposit"] or 0),
            "balance": balance,
            "credit": credit,
            "equity": equity,
            "open_pnl": round(open_pnl, 2),
            "live_equity": round(live_equity, 2),
            "margin": margin,
            "used_margin": used_margin,
            "free_margin": free_margin,
            "margin_level_pct": margin_level_pct,
            "max_open_trade": round(float(row["max_open_trade"]), 1) if row["max_open_trade"] is not None else None,
            "max_volume": round(float(row["max_volume"]), 1) if row["max_volume"] is not None else None,
            "turnover": round(float(row["max_volume"]) / abs(equity or 1), 1) if row["max_volume"] is not None and equity != 0 else None,
            "win_rate": round(float(row["win_rate"]), 1) if row["win_rate"] is not None else None,
            "avg_trade_size": round(float(row["avg_trade_size"]), 2) if row["avg_trade_size"] is not None else None,
            "assigned_to": row["assigned_to"],
            "agent_name": row["agent_name"] or None,
            "sales_client_potential": _resolve_status_name(row["sales_client_potential"]),
            "score": int(row["score"]),
            "is_favorite": bool(row["is_favorite"]),
            "is_active": bool(row["is_active"]),
            "card_type": row["card_type"],
            "exposure_usd": round(total_exposure_usd, 2),
            "exposure_pct": exposure_pct,
            "last_deposit_date": row["last_deposit_date"].isoformat() if row["last_deposit_date"] else None,
            "last_withdrawal_date": row["last_withdrawal_date"].isoformat() if row["last_withdrawal_date"] else None,
            "net_deposit_ever": round(float(row["net_deposit_ever"]), 2) if row["net_deposit_ever"] is not None else None,
            "country": row["country"] or None,
            "last_communication_date": row["last_communication_date"].isoformat() if row["last_communication_date"] else None,
            "retention_status": live_retention_status,
            "lifecycle": lifecycle,
            "positions": positions,
            "mt_accounts": mt_accounts,
            "callback": {
                "id": cb_row["id"],
                "callback_time": cb_row["callback_time"].isoformat() if cb_row["callback_time"] else None,
                "note": cb_row["note"],
            } if cb_row else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error("Client detail query failed for %s: %s\n%s", account_id, e, traceback.format_exc())
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        raise HTTPException(status_code=502, detail=f"Client detail query failed: {e}")


# ---------------------------------------------------------------------------
# POST /call-dashboard/save
# ---------------------------------------------------------------------------

class SavePayload(BaseModel):
    account_id: str
    status_key: int | None = None
    note: str = ""
    talk_seconds: int = 0
    callback_preset: str | None = None
    callback_custom_utc: str | None = None


_CALLBACK_PRESETS = {
    "15min": timedelta(minutes=15),
    "1hour": timedelta(hours=1),
    "2hours": timedelta(hours=2),
    "tomorrow": timedelta(days=1),
}


@router.post("/call-dashboard/save")
async def save_call(
    body: SavePayload,
    request: Request,
    _perm: JWTPayload = Depends(require_call_dashboard_access),
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Atomic save: note, audit, callback. Status update is optional (CLAUD-130: status shown read-only)."""
    http_client = request.app.state.http_client
    # 1. PUT status to CRM only if a status_key was provided
    if body.status_key is not None:
        crm_url = f"{settings.crm_api_base_url}/crm-api/retention"
        params: dict[str, Any] = {
            "userId": body.account_id,
            "retentionStatus": body.status_key,
        }
        try:
            response = await http_client.put(crm_url, params=params, headers=_crm_headers())
            _handle_crm_response(response, f"call_dashboard_save_status({body.account_id})")
        except HTTPException:
            raise
        except httpx.TimeoutException:
            raise HTTPException(status_code=503, detail="CRM service timed out. Please try again later.")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")

    # 2. POST note to CRM if non-empty
    if body.note and body.note.strip():
        note_url = f"{settings.crm_api_base_url}/crm-api/user-note"
        note_params: dict[str, Any] = {
            "userId": body.account_id,
            "note": body.note.strip(),
        }
        try:
            response = await http_client.post(note_url, params=note_params, headers=_crm_headers())
            _handle_crm_response(response, f"call_dashboard_save_note({body.account_id})")
            # CLAUD-166: log note_added so it appears in call history
            await _log_audit(db, user.id, user.username, body.account_id, "note_added", body.note.strip())
        except HTTPException:
            raise
        except httpx.TimeoutException:
            raise HTTPException(status_code=503, detail="CRM service timed out while adding note.")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach CRM API for note: {e}")

    # 3. Insert audit_log for talk_time
    await _log_audit(
        db, user.id, user.username, body.account_id,
        "talk_time", f"{body.talk_seconds}s",
    )

    # Audit the status change only if a status was set
    if body.status_key is not None:
        await _log_audit(
            db, user.id, user.username, body.account_id,
            "status_change", str(body.status_key),
        )

    # 4. Mark prior pending callbacks for this account+agent as done
    await db.execute(
        text("""
            UPDATE client_callbacks
            SET is_done = TRUE
            WHERE accountid = :account_id
              AND agent_id = :uid
              AND NOT is_done
        """),
        {"account_id": body.account_id, "uid": user.id},
    )

    # 5. If callback: compute callback_time and INSERT
    if body.callback_preset or body.callback_custom_utc:
        if body.callback_custom_utc:
            try:
                callback_time = datetime.fromisoformat(body.callback_custom_utc.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid callback_custom_utc format.")
        elif body.callback_preset in _CALLBACK_PRESETS:
            callback_time = datetime.now(timezone.utc) + _CALLBACK_PRESETS[body.callback_preset]
        else:
            raise HTTPException(status_code=400, detail=f"Invalid callback_preset: {body.callback_preset}")

        await db.execute(
            text("""
                INSERT INTO client_callbacks (accountid, agent_id, callback_time, note)
                VALUES (:account_id, :uid, :cb_time, :note)
            """),
            {
                "account_id": body.account_id,
                "uid": user.id,
                "cb_time": callback_time,
                "note": body.note.strip() if body.note else None,
            },
        )

    await db.commit()
    return {"success": True}


# ---------------------------------------------------------------------------
# GET /call-dashboard/callbacks
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/callbacks")
async def get_callbacks(
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return pending callbacks for the current agent."""
    user_id = jwt_payload.user_id

    result = await db.execute(
        text("""
            SELECT cc.id,
                   cc.accountid,
                   cc.callback_time,
                   cc.note,
                   cc.created_at,
                   m.full_name
            FROM client_callbacks cc
            LEFT JOIN retention_mv m ON m.accountid = cc.accountid
            WHERE cc.agent_id = :uid
              AND NOT cc.is_done
            ORDER BY cc.callback_time ASC
        """),
        {"uid": user_id},
    )
    rows = result.mappings().all()

    return [
        {
            "id": r["id"],
            "accountid": str(r["accountid"]),
            "callback_time": r["callback_time"].isoformat() if r["callback_time"] else None,
            "note": r["note"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "full_name": r["full_name"] or "",
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# PATCH /call-dashboard/callbacks/{callback_id}/done
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# GET /call-dashboard/performance
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/performance")
async def get_performance(
    period: str = Query("daily", regex="^(daily|monthly)$"),
    _perm: JWTPayload = Depends(require_call_dashboard_access),
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return 12 KPIs for the current agent, filtered by daily or monthly period.

    CLAUD-106: Full performance strip with cache.
    """
    try:
        today = date.today()
        now_utc = datetime.now(timezone.utc)

        # Determine period boundaries
        if period == "daily":
            period_start_sql = "CURRENT_DATE"
            period_date = today
        else:
            period_start_sql = "date_trunc('month', NOW())"
            period_date = today.replace(day=1)

        # --- Check cache first ---
        cache_row = (await db.execute(
            text("""
                SELECT net_deposit, depositors, traders, volume, contacted,
                       calls_made, talk_time_secs, target, callbacks_set,
                       run_rate, contact_rate, avg_call_secs, computed_at
                FROM agent_performance_cache
                WHERE agent_id = :agent_id AND period = :period AND period_date = :period_date
                  AND computed_at > NOW() - INTERVAL '60 seconds'
            """),
            {"agent_id": str(user.id), "period": period, "period_date": period_date},
        )).fetchone()

        if cache_row:
            return {
                "net_deposit": float(cache_row[0]) if cache_row[0] is not None else None,
                "depositors": int(cache_row[1] or 0),
                "traders": int(cache_row[2] or 0),
                "volume": float(cache_row[3]) if cache_row[3] is not None else None,
                "contacted": int(cache_row[4] or 0),
                "calls_made": int(cache_row[5] or 0),
                "talk_time_secs": int(cache_row[6] or 0),
                "target": int(cache_row[7]) if cache_row[7] is not None else None,
                "callbacks_set": int(cache_row[8] or 0),
                "run_rate": float(cache_row[9]) if cache_row[9] is not None else None,
                "contact_rate": float(cache_row[10]) if cache_row[10] is not None else None,
                "avg_call_secs": int(cache_row[11] or 0),
                "computed_at": cache_row[12].isoformat() if cache_row[12] else None,
                "period": period,
            }

        # --- Query 1: vtiger-scoped financial metrics (net deposit, depositors, traders, volume, target) ---
        vtiger_metrics_sql = text(f"""
            WITH agent AS (
                SELECT id::text AS vtiger_id
                FROM vtiger_users
                WHERE LOWER(email) = LOWER(:email)
                LIMIT 1
            ),
            txn_agg AS (
                SELECT
                    COALESCE(SUM(CASE WHEN vmt.transactiontype = 'Deposit' THEN vmt.usdamount ELSE 0 END), 0)
                    - COALESCE(SUM(CASE WHEN vmt.transactiontype = 'Withdrawal' THEN ABS(vmt.usdamount) ELSE 0 END), 0) AS net_deposit,
                    COUNT(DISTINCT CASE WHEN vmt.transactiontype = 'Deposit' THEN vta.vtigeraccountid END) AS depositors
                FROM vtiger_mttransactions vmt
                JOIN vtiger_trading_accounts vta ON vta.login = vmt.login
                JOIN retention_mv rm ON rm.accountid = vta.vtigeraccountid
                JOIN agent ON rm.assigned_to = agent.vtiger_id
                WHERE vmt.transactionapproval = 'Approved'
                  AND vmt.transactiontype IN ('Deposit', 'Withdrawal')
                  AND vmt.confirmation_time >= {period_start_sql}
            ),
            trade_agg AS (
                SELECT
                    COALESCE(SUM(t.notional_value), 0) AS volume,
                    COUNT(DISTINCT vta.vtigeraccountid) AS traders
                FROM trades_mt4 t
                JOIN vtiger_trading_accounts vta ON vta.login = t.login::bigint
                JOIN retention_mv rm ON rm.accountid = vta.vtigeraccountid
                JOIN agent ON rm.assigned_to = agent.vtiger_id
                WHERE t.open_time >= {period_start_sql}
                  AND t.cmd IN (0, 1)
            ),
            tgt AS (
                SELECT at.net AS target_net
                FROM agent_targets at
                JOIN vtiger_users vu ON vu.id::int = at.agent_id
                WHERE LOWER(vu.email) = LOWER(:email)
                  AND at.month_date = date_trunc('month', CURRENT_DATE)::date
                LIMIT 1
            )
            SELECT
                (SELECT vtiger_id FROM agent) AS vtiger_id,
                (SELECT net_deposit FROM txn_agg) AS net_deposit,
                (SELECT depositors FROM txn_agg) AS depositors,
                (SELECT volume FROM trade_agg) AS volume,
                (SELECT traders FROM trade_agg) AS traders,
                (SELECT target_net FROM tgt) AS target
        """)
        v_row = (await db.execute(vtiger_metrics_sql, {"email": user.email})).fetchone()

        net_deposit = float(v_row[1] or 0) if v_row else 0.0
        depositors = int(v_row[2] or 0) if v_row else 0
        volume = float(v_row[3] or 0) if v_row else 0.0
        traders = int(v_row[4] or 0) if v_row else 0
        target = int(v_row[5]) if v_row and v_row[5] is not None else None

        # --- Query 2: audit_log + callbacks stats (single round-trip) ---
        audit_sql = text(f"""
            WITH audit_stats AS (
                SELECT
                    COUNT(DISTINCT client_account_id) FILTER (
                        WHERE action_type IN ('call_initiated', 'whatsapp_opened')
                    ) AS contacted,
                    COUNT(*) FILTER (
                        WHERE action_type = 'call_initiated'
                    ) AS calls_made,
                    COALESCE(SUM(
                        CASE WHEN action_type = 'talk_time'
                        THEN CAST(REPLACE(action_value, 's', '') AS INTEGER)
                        ELSE 0 END
                    ), 0) AS talk_time_secs
                FROM audit_log
                WHERE agent_id = :uid
                  AND timestamp >= {period_start_sql}
            ),
            cb_stats AS (
                SELECT COUNT(*) AS callbacks_set
                FROM client_callbacks
                WHERE agent_id = :uid
                  AND created_at >= {period_start_sql}
            )
            SELECT
                (SELECT contacted FROM audit_stats),
                (SELECT calls_made FROM audit_stats),
                (SELECT talk_time_secs FROM audit_stats),
                (SELECT callbacks_set FROM cb_stats)
        """)
        a_row = (await db.execute(audit_sql, {"uid": user.id})).fetchone()
        contacted = int(a_row[0] or 0) if a_row else 0
        calls_made = int(a_row[1] or 0) if a_row else 0
        talk_time_secs = int(a_row[2] or 0) if a_row else 0
        callbacks_set = int(a_row[3] or 0) if a_row else 0

        # --- Derived metrics ---
        # Contact rate: contacted / calls_made * 100
        contact_rate: float | None = round(contacted / calls_made * 100, 1) if calls_made > 0 else None

        # Avg call duration: talk_time_secs / contacted
        avg_call_secs = round(talk_time_secs / contacted) if contacted > 0 else 0

        # Run rate: (contacted so far / elapsed_fraction) / target * 100
        run_rate: float | None = None
        if target and target > 0:
            if period == "monthly":
                day_of_month = today.day
                days_in_month = calendar.monthrange(today.year, today.month)[1]
                elapsed_fraction = day_of_month / days_in_month
            else:
                # daily: elapsed fraction of working day (8h shift: 08:00 - 16:00 IL)
                # Use simple hours elapsed / 8
                hours_elapsed = max(1, min(8, (now_utc.hour + 2) - 8))  # rough IL offset
                elapsed_fraction = hours_elapsed / 8
            projected = contacted / elapsed_fraction if elapsed_fraction > 0 else contacted
            run_rate = round(projected / target * 100, 1)

        result = {
            "net_deposit": round(net_deposit, 2),
            "depositors": depositors,
            "traders": traders,
            "volume": round(volume, 2),
            "contacted": contacted,
            "calls_made": calls_made,
            "talk_time_secs": talk_time_secs,
            "target": target,
            "callbacks_set": callbacks_set,
            "run_rate": run_rate,
            "contact_rate": contact_rate,
            "avg_call_secs": avg_call_secs,
            "computed_at": now_utc.isoformat(),
            "period": period,
        }

        # --- Upsert cache ---
        try:
            await db.execute(
                text("""
                    INSERT INTO agent_performance_cache
                        (agent_id, period, period_date, net_deposit, depositors, traders,
                         volume, contacted, calls_made, talk_time_secs, target,
                         callbacks_set, run_rate, contact_rate, avg_call_secs, computed_at)
                    VALUES
                        (:agent_id, :period, :period_date, :net_deposit, :depositors, :traders,
                         :volume, :contacted, :calls_made, :talk_time_secs, :target,
                         :callbacks_set, :run_rate, :contact_rate, :avg_call_secs, NOW())
                    ON CONFLICT (agent_id, period, period_date)
                    DO UPDATE SET
                        net_deposit = EXCLUDED.net_deposit,
                        depositors = EXCLUDED.depositors,
                        traders = EXCLUDED.traders,
                        volume = EXCLUDED.volume,
                        contacted = EXCLUDED.contacted,
                        calls_made = EXCLUDED.calls_made,
                        talk_time_secs = EXCLUDED.talk_time_secs,
                        target = EXCLUDED.target,
                        callbacks_set = EXCLUDED.callbacks_set,
                        run_rate = EXCLUDED.run_rate,
                        contact_rate = EXCLUDED.contact_rate,
                        avg_call_secs = EXCLUDED.avg_call_secs,
                        computed_at = NOW()
                """),
                {
                    "agent_id": str(user.id),
                    "period": period,
                    "period_date": period_date,
                    "net_deposit": round(net_deposit, 2),
                    "depositors": depositors,
                    "traders": traders,
                    "volume": round(volume, 2),
                    "contacted": contacted,
                    "calls_made": calls_made,
                    "talk_time_secs": talk_time_secs,
                    "target": target,
                    "callbacks_set": callbacks_set,
                    "run_rate": run_rate,
                    "contact_rate": contact_rate,
                    "avg_call_secs": avg_call_secs,
                },
            )
            await db.commit()
        except Exception as cache_err:
            logger.warning("Performance cache upsert failed (non-fatal): %s", cache_err)

        return result
    except Exception as e:
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        logger.error("Performance endpoint error: %s", e)
        raise HTTPException(status_code=502, detail=f"Performance query failed: {e}")


# ---------------------------------------------------------------------------
# CLAUD-155: GET /call-dashboard/client/{account_id}/open-positions
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/client/{account_id}/open-positions")
async def get_open_positions(
    account_id: str,
    response: Response,
    login: int | None = Query(None, description="CLAUD-173: filter to a specific MT login"),
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return all open positions with full MT4 detail for a client.

    Queries the Dealio replica for live data (volume, open_price, sl, tp, swap,
    commission). Falls back to local trades_mt4 when replica is unavailable.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    from app.replica_database import _ReplicaSession

    # CLAUD-173: if a specific login is selected, use it directly; otherwise resolve all logins
    if login:
        logins = [login]
    else:
        login_result = await db.execute(
            text("SELECT login FROM vtiger_trading_accounts WHERE vtigeraccountid = :account_id LIMIT 5"),
            {"account_id": account_id},
        )
        logins = [int(r[0]) for r in login_result.fetchall()]
    if not logins:
        return []

    # Try Dealio replica for full per-trade detail
    if _ReplicaSession is not None:
        try:
            async with _ReplicaSession() as replica:
                # CLAUD-170: fetch positions with symbol contract_size and profitcurrency
                pos_result = await replica.execute(
                    text("""
                        SELECT t.ticket,
                               t.symbol,
                               t.cmd,
                               COALESCE(t.volume, 0)                    AS volume,
                               COALESCE(t.open_price, 0)                AS open_price,
                               t.open_time,
                               COALESCE(t.computed_profit, t.profit, 0) AS pnl,
                               COALESCE(t.notional_value, 0)            AS notional_value,
                               COALESCE(s.contractsize, 100000)         AS contract_size,
                               COALESCE(s.profitcurrency, 'USD')        AS profit_currency
                        FROM dealio.trades_mt4 t
                        LEFT JOIN dealio.symbols s
                               ON s.symbol = t.symbol AND s.sourcename = 'Live'
                        WHERE t.login = ANY(:logins)
                          AND t.close_time < '1971-01-01'
                          AND t.cmd IN (0, 1)
                        ORDER BY t.open_time DESC
                    """),
                    {"logins": logins},
                )
                rows = pos_result.mappings().all()

                # Fetch live prices for all trade symbols + FX conversion pairs
                trade_symbols = list({str(r["symbol"]) for r in rows if r["symbol"]})
                profit_ccys = list({str(r["profit_currency"]) for r in rows if r["profit_currency"] != "USD"})
                fx_pairs = [c + "USD" for c in profit_ccys] + ["USD" + c for c in profit_ccys]
                all_symbols = list(set(trade_symbols + fx_pairs))
                live_prices: dict[str, float] = {}
                if all_symbols:
                    try:
                        px_result = await replica.execute(
                            text("SELECT symbol, bid FROM dealio.live_ticks WHERE symbol = ANY(:symbols)"),
                            {"symbols": all_symbols},
                        )
                        live_prices = {str(r[0]): float(r[1]) for r in px_result.fetchall() if r[1] is not None}
                    except Exception:
                        pass  # fallback to notional_value if prices unavailable

                def _usd_rate(profit_ccy: str) -> float | None:
                    """Return the multiplier to convert 1 unit of profit_ccy → USD."""
                    if profit_ccy == "USD":
                        return 1.0
                    pair_direct = profit_ccy + "USD"   # e.g. EURUSD, GBPUSD
                    pair_inverse = "USD" + profit_ccy  # e.g. USDJPY, USDCAD
                    if pair_direct in live_prices:
                        return live_prices[pair_direct]
                    if pair_inverse in live_prices and live_prices[pair_inverse] != 0:
                        return 1.0 / live_prices[pair_inverse]
                    return None

                result_list = []
                for r in rows:
                    volume = float(r["volume"] or 0)        # already in lots (CLAUD-170 fix)
                    cs = float(r["contract_size"] or 100000)
                    sym = str(r["symbol"] or "")
                    profit_ccy = str(r["profit_currency"] or "USD")
                    current_px = live_prices.get(sym)

                    if current_px is not None:
                        usd_rate = _usd_rate(profit_ccy)
                        if usd_rate is not None:
                            exposure_usd = cs * volume * current_px * usd_rate
                        else:
                            exposure_usd = float(r["notional_value"] or 0)
                    else:
                        exposure_usd = float(r["notional_value"] or 0)

                    result_list.append({
                        "ticket": int(r["ticket"]),
                        "symbol": sym,
                        "side": "Long" if int(r["cmd"]) == 0 else "Short",
                        "net_lot": round(volume, 4),
                        "contract_size": round(cs, 0),
                        "open_price": round(float(r["open_price"] or 0), 5),
                        "open_time": r["open_time"].strftime("%d/%m/%Y %H:%M") if r["open_time"] else None,
                        "exposure": round(exposure_usd, 2),
                        "pnl": round(float(r["pnl"] or 0), 2),
                        "sl": 0,
                        "tp": 0,
                        "swap": 0,
                        "commission": 0,
                    })
                return result_list
        except Exception as e:
            logger.warning("CLAUD-155: replica open positions failed, using local fallback: %s", e)

    # Fallback: local trades_mt4 (CLAUD-170: volume column added for correct lot display)
    try:
        result = await db.execute(
            text("""
                SELECT t.ticket,
                       t.symbol,
                       t.cmd,
                       COALESCE(t.volume, 0)          AS volume,
                       COALESCE(t.open_price, 0)      AS open_price,
                       COALESCE(t.notional_value, 0)  AS exposure,
                       COALESCE(t.profit, 0)          AS pnl,
                       t.open_time
                FROM trades_mt4 t
                JOIN vtiger_trading_accounts vta ON vta.login = t.login::bigint
                WHERE vta.vtigeraccountid = :account_id
                  AND t.close_time < '1971-01-01'
                  AND t.cmd IN (0, 1)
                ORDER BY t.open_time DESC
            """),
            {"account_id": account_id},
        )
        rows = result.mappings().all()
        return [
            {
                "ticket": int(r["ticket"]),
                "symbol": str(r["symbol"] or ""),
                "side": "Long" if int(r["cmd"]) == 0 else "Short",
                "net_lot": round(float(r["volume"] or 0), 4),
                "contract_size": None,
                "open_price": round(float(r["open_price"] or 0), 5),
                "open_time": r["open_time"].strftime("%d/%m/%Y %H:%M") if r["open_time"] else None,
                "exposure": round(float(r["exposure"] or 0), 2),
                "pnl": round(float(r["pnl"] or 0), 2),
                "sl": 0,
                "tp": 0,
                "swap": 0,
                "commission": 0,
            }
            for r in rows
        ]
    except Exception as e:
        logger.error("CLAUD-155: local open positions fallback failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Open positions query failed: {e}")


# ---------------------------------------------------------------------------
# CLAUD-161/170: GET /call-dashboard/live-prices?symbols=XAUUSD,EURUSD
# Returns current bid price per symbol from Dealio replica (live_ticks table).
# Returns {} if Dealio replica is unavailable.
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/live-prices")
async def get_live_prices(
    symbols: str,
    response: Response,
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
) -> dict:
    """Return current bid prices for a comma-separated list of symbols.

    Queries dealio.live_ticks (bid column). Returns an empty dict if the
    Dealio replica is unreachable.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    from app.replica_database import _ReplicaSession

    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list or _ReplicaSession is None:
        return {}

    try:
        async with _ReplicaSession() as replica:
            result = await replica.execute(
                text("""
                    SELECT symbol, bid
                    FROM dealio.live_ticks
                    WHERE symbol = ANY(:symbols)
                """),
                {"symbols": symbol_list},
            )
            rows = result.fetchall()
            return {str(r[0]).upper(): round(float(r[1]), 5) for r in rows if r[1] is not None}
    except Exception as e:
        logger.warning("CLAUD-170: live prices query failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# GET /call-dashboard/client/{account_id}/closed-positions
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/client/{account_id}/closed-positions")
async def get_closed_positions(
    account_id: str,
    login: int | None = Query(None, description="CLAUD-173: filter to a specific MT login"),
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return up to 100 most recent closed positions for a client."""
    try:
        _login_clause = "AND t.login = :login_filter" if login else ""
        result = await db.execute(
            text(f"""
                SELECT t.symbol,
                       CASE WHEN t.cmd = 0 THEN 'Long' ELSE 'Short' END AS direction,
                       t.notional_value / 100.0 AS net_lot,
                       t.notional_value AS exposure,
                       t.profit AS pnl,
                       t.open_time AS entry_time,
                       t.close_time
                FROM trades_mt4 t
                JOIN vtiger_trading_accounts vta ON vta.login = t.login::bigint
                WHERE vta.vtigeraccountid = :account_id
                  {_login_clause}
                  AND t.close_time > '1971-01-01'
                  AND t.cmd IN (0, 1)
                ORDER BY t.close_time DESC
                LIMIT 100
            """),
            {"account_id": account_id, **({"login_filter": login} if login else {})},
        )
        rows = result.mappings().all()
        return [
            {
                "symbol": str(r["symbol"]),
                "direction": r["direction"],
                "net_lot": round(float(r["net_lot"] or 0), 2),
                "exposure": round(float(r["exposure"] or 0), 2),
                "pnl": round(float(r["pnl"] or 0), 2),
                "entry_time": r["entry_time"].isoformat() if r["entry_time"] else None,
                "close_time": r["close_time"].isoformat() if r["close_time"] else None,
            }
            for r in rows
        ]
    except Exception as e:
        logger.error("Closed positions query failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Closed positions query failed: {e}")


# ---------------------------------------------------------------------------
# GET /call-dashboard/client/{account_id}/transactions
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/client/{account_id}/transactions")
async def get_transactions(
    account_id: str,
    login: int | None = Query(None, description="CLAUD-173: filter to a specific MT login"),
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return up to 100 most recent transactions for a client."""
    try:
        _login_clause = "AND vta.login = :login_filter" if login else ""
        result = await db.execute(
            text(f"""
                SELECT vmt.mttransactionsid,
                       vmt.transactiontype,
                       vmt.amount,
                       vmt.usdamount,
                       vmt.payment_method,
                       vmt.transactionapproval,
                       vmt.confirmation_time,
                       vmt.creditcardlast
                FROM vtiger_mttransactions vmt
                JOIN vtiger_trading_accounts vta ON vta.login = vmt.login
                WHERE vta.vtigeraccountid = :account_id
                  {_login_clause}
                  AND LOWER(vmt.transactionapproval) IN ('approved', 'success', 'completed', 'successful')
                ORDER BY vmt.confirmation_time DESC
                LIMIT 100
            """),
            {"account_id": account_id, **({"login_filter": login} if login else {})},
        )
        rows = result.mappings().all()
        return [
            {
                "id": r["mttransactionsid"],
                "type": r["transactiontype"],
                "amount": round(float(r["amount"] or 0), 2),
                "usd_amount": round(float(r["usdamount"] or 0), 2),
                "method": r["payment_method"],
                "status": r["transactionapproval"],
                "date": r["confirmation_time"].isoformat() if r["confirmation_time"] else None,
                "card": r["creditcardlast"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error("Transactions query failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Transactions query failed: {e}")


# ---------------------------------------------------------------------------
# GET /call-dashboard/client/{account_id}/call-history
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/client/{account_id}/call-history")
async def get_client_call_history(
    account_id: str,
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return last 20 call interactions for a client (CLAUD-164: agent name, notes, split date/time)."""
    try:
        result = await db.execute(
            text("""
                SELECT al.id, al.agent_username, al.action_type, al.action_value, al.timestamp,
                       COALESCE(NULLIF(TRIM(COALESCE(vu.first_name, '') || ' ' || COALESCE(vu.last_name, '')), ''), al.agent_username) AS agent_name
                FROM audit_log al
                LEFT JOIN users u ON LOWER(u.username) = LOWER(al.agent_username)
                LEFT JOIN vtiger_users vu ON LOWER(vu.email) = LOWER(u.email)
                WHERE al.client_account_id = :account_id
                  AND al.action_type IN ('talk_time', 'status_change', 'note_added')
                ORDER BY al.timestamp DESC
                LIMIT 500
            """),
            {"account_id": account_id},
        )
        rows = list(result.mappings().all())

        status_label_map = {
            "0": "New", "1": "CallBack", "2": "Invalid", "3": "No Answer",
            "4": "Reassign - Has Potential", "6": "Not Interested", "9": "Deposited With Me",
            "17": "Recycle", "19": "Potential", "20": "Appointment", "21": "High Potential",
            "23": "Call Again", "28": "Reassigned", "34": "Terminated/Complain/Legal",
            "35": "Remove From my Portfolio", "36": "Daily Trading with me", "37": "A+ Client",
        }

        talk_rows = [r for r in rows if r["action_type"] == "talk_time"]
        status_rows = [r for r in rows if r["action_type"] == "status_change"]
        note_rows = [r for r in rows if r["action_type"] == "note_added"]
        used_status_ids: set[int] = set()
        used_note_ids: set[int] = set()
        calls = []

        for tt in talk_rows:
            # Match status_change within 60s of call
            matched_status = None
            for sc in status_rows:
                if sc["id"] in used_status_ids:
                    continue
                if sc["agent_username"] != tt["agent_username"]:
                    continue
                if abs((sc["timestamp"] - tt["timestamp"]).total_seconds()) <= 60:
                    matched_status = sc
                    used_status_ids.add(sc["id"])
                    break

            # Match note_added within 600s of call (extended window for pre-save notes)
            matched_note = None
            for nr in note_rows:
                if nr["id"] in used_note_ids:
                    continue
                if nr["agent_username"] != tt["agent_username"]:
                    continue
                if abs((nr["timestamp"] - tt["timestamp"]).total_seconds()) <= 600:
                    matched_note = nr
                    used_note_ids.add(nr["id"])
                    break

            duration_sec = None
            val = tt["action_value"]
            if val and val.endswith("s"):
                try:
                    duration_sec = int(val[:-1])
                except ValueError:
                    pass

            status_key = matched_status["action_value"] if matched_status else None
            calls.append({
                "timestamp": tt["timestamp"].isoformat(),
                "agent": tt["agent_name"],
                "status_key": status_key,
                "status_label": status_label_map.get(status_key, status_key) if status_key else None,
                "duration_sec": duration_sec,
                "note": matched_note["action_value"] if matched_note else None,
            })
            if len(calls) >= 20:
                break

        # Also include standalone note_added entries not matched to any talk_time
        # (e.g. notes added via Retention Page modal, or notes outside the time window)
        for nr in note_rows:
            if nr["id"] in used_note_ids:
                continue
            calls.append({
                "timestamp": nr["timestamp"].isoformat(),
                "agent": nr["agent_name"],
                "status_key": None,
                "status_label": None,
                "duration_sec": None,
                "note": nr["action_value"],
            })

        # Re-sort by timestamp descending after adding standalone notes
        calls.sort(key=lambda c: c["timestamp"], reverse=True)

        return calls[:20]
    except Exception as e:
        logger.error("Call history query failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Call history query failed: {e}")


@router.patch("/call-dashboard/callbacks/{callback_id}/done")
async def mark_callback_done(
    callback_id: int,
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Mark a callback as done (only if owned by current user)."""
    user_id = jwt_payload.user_id

    result = await db.execute(
        text("""
            UPDATE client_callbacks
            SET is_done = TRUE
            WHERE id = :cb_id AND agent_id = :uid AND NOT is_done
            RETURNING id
        """),
        {"cb_id": callback_id, "uid": user_id},
    )
    row = result.fetchone()
    await db.commit()

    if not row:
        raise HTTPException(status_code=404, detail="Callback not found or already completed.")

    return {"success": True}


# ---------------------------------------------------------------------------
# GET /call-dashboard/sidebar-stats
# ---------------------------------------------------------------------------

@router.get("/call-dashboard/sidebar-stats")
async def get_sidebar_stats(
    jwt_payload: JWTPayload = Depends(require_call_dashboard_access),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return ACTIVE client count (scoped to agent) and pending TASKS count.

    CLAUD-113: Replace hardcoded sidebar values with agent-scoped live data.
    - active_count: clients assigned to this agent that have is_active=TRUE
    - tasks_count: pending callbacks for this agent (not done)
    """
    user_id = jwt_payload.user_id

    active_count = 0
    if jwt_payload.vtiger_user_id is not None:
        row = (await db.execute(
            text("""
                SELECT COUNT(*)
                FROM retention_mv m
                JOIN client_active_status cas ON cas.accountid = m.accountid
                WHERE m.assigned_to = :vtiger_uid
                  AND cas.is_active = TRUE
            """),
            {"vtiger_uid": str(jwt_payload.vtiger_user_id)},
        )).scalar()
        active_count = int(row or 0)

    tasks_row = (await db.execute(
        text("""
            SELECT COUNT(*)
            FROM client_callbacks
            WHERE agent_id = :uid AND NOT is_done
        """),
        {"uid": user_id},
    )).scalar()
    tasks_count = int(tasks_row or 0)

    return {"active_count": active_count, "tasks_count": tasks_count}
