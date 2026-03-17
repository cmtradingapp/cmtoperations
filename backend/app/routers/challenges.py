"""CLAUD-89/90/91: Challenges Module — Daily Trade Challenges with Credit Rewards,
Webhook Integration & Optimove Event Notifications.

Endpoints:
- POST   /api/challenges                       — create challenge group (admin)
- GET    /api/challenges                       — list all challenge groups (admin)
- PATCH  /api/challenges/{group_name}/toggle   — toggle active status (admin)
- DELETE /api/challenges/{group_name}          — delete challenge group (admin)
- GET    /api/challenges/progress              — client progress (admin) [CLAUD-90]
- GET    /api/challenges/events                — Optimove event log (admin) [CLAUD-91]
- POST   /api/webhooks/trade-event             — public webhook for trade events
"""

import asyncio
import json
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.database import execute_query as mssql_query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.pg_database import get_db
from app.rbac import make_page_guard

_require_challenges = make_page_guard("challenges")

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# CLAUD-91: Optimove event helpers
# ---------------------------------------------------------------------------


async def _get_optimove_url(db: AsyncSession) -> str | None:
    """Look up the Optimove base_url from the integrations table."""
    row = await db.execute(
        text("SELECT base_url FROM integrations WHERE name = 'Optimove' AND is_active = TRUE LIMIT 1")
    )
    result = row.fetchone()
    return result[0] if result else None


async def _fire_optimove_event(
    url: str,
    payload: dict,
    challenge_id: int | None,
    accountid: str | None,
    event_name: str,
    db: AsyncSession,
) -> None:
    """POST event to Optimove and log result. Never raises."""
    response_text = ""
    success = False
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response_text = resp.text[:2000]
            success = 200 <= resp.status_code < 300
            logger.info(
                "CLAUD-91: Optimove %s for accountid=%s -> HTTP %d",
                event_name, accountid, resp.status_code,
            )
    except Exception as e:
        response_text = f"ERROR: {e}"
        logger.warning("CLAUD-91: Optimove %s failed for accountid=%s: %s", event_name, accountid, e)

    try:
        await db.execute(
            text(
                'INSERT INTO optimove_event_log ("challengeId", accountid, event_name, payload, response, success, created_at) '
                "VALUES (:cid, :acc, :event, CAST(:payload AS jsonb), :resp, :ok, :now)"
            ),
            {
                "cid": challenge_id,
                "acc": accountid,
                "event": event_name,
                "payload": json.dumps(payload),
                "resp": response_text,
                "ok": success,
                "now": datetime.utcnow(),
            },
        )
    except Exception as e:
        logger.error("CLAUD-91: Failed to log Optimove event: %s", e)


# ---------------------------------------------------------------------------
# Credit API configuration
# ---------------------------------------------------------------------------
CREDIT_API_URL = "https://apicrm.cmtrading.com/SignalsCRM/crm-api/brokers/users/credit"
CREDIT_API_TOKEN = "699a3696-5869-44c9-aa31-1938f296a556"

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class ChallengeTierIn(BaseModel):
    name: str = Field(..., max_length=100)
    targetvalue: Decimal = Field(..., gt=0)
    rewardamount: Decimal = Field(..., gt=0)
    symbol: str | None = None  # instrument challenge: target trading symbol for this tier


class ChallengeGroupIn(BaseModel):
    group_name: str = Field(..., max_length=100)
    type: str = Field(..., pattern="^(trade|volume|streak|pnl|diversity|instrument)$")
    tiers: list[ChallengeTierIn] = Field(..., min_length=1)
    audience_criteria: dict[str, Any] | None = None
    timeperiod: str = Field("daily", pattern="^(daily|weekly)$")   # CLAUD-94
    valid_until: datetime | None = None                             # CLAUD-95: flash expiry
    reward_multiplier: Decimal = Field(Decimal("1.00"), ge=Decimal("0.01"))  # CLAUD-95
    expires_on: date | None = None  # Challenge expiration date


class ChallengeTierOut(BaseModel):
    challengeId: int
    name: str
    targetvalue: Decimal
    rewardamount: Decimal
    tier_rank: int


class ChallengeGroupOut(BaseModel):
    group_name: str
    type: str
    isactive: int
    tiers: list[ChallengeTierOut]
    InsertDate: datetime


class TradeEventContext(BaseModel):
    model_config = {"extra": "ignore"}
    email: str | None = None
    language: str | None = None
    country: str | None = None
    account_number: str | None = None
    volume: Any = None
    trading_volume: Any = None
    profit: Any = None
    symbol: str | None = None  # CLAUD-94: trading instrument symbol
    affiliate: str | None = None


class TradeEventPayload(BaseModel):
    model_config = {"extra": "ignore"}
    tenant: Any = None
    event: str | None = None
    customer: Any = None
    context: TradeEventContext | None = None


# CLAUD-94: Symbol asset class mapping model
ASSET_CLASSES = ("forex", "commodity", "index", "crypto", "stock")


class SymbolMappingIn(BaseModel):
    symbol: str = Field(..., max_length=20)
    asset_class: str = Field(..., pattern="^(forex|commodity|index|crypto|stock)$")


# ---------------------------------------------------------------------------
# CLAUD-181: Challenges Dashboard — analytics overview endpoint
# ---------------------------------------------------------------------------


@router.get("/challenges/dashboard")
async def challenges_dashboard(
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Return aggregated analytics for the Challenges Dashboard tab."""

    from decimal import Decimal as D

    def _row_to_dict(row) -> dict:
        return dict(row._mapping) if row else {}

    def _serial(v):
        """Make values JSON-serialisable."""
        if isinstance(v, (D, Decimal)):
            return float(v)
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    def _clean(d: dict) -> dict:
        return {k: _serial(v) for k, v in d.items()}

    # 1. Snapshot (today's live data)
    snap_row = await db.execute(text("""
        SELECT
            (SELECT COUNT(DISTINCT group_name) FROM challenges WHERE isactive = 1)
                AS active_challenges,
            (SELECT COUNT(DISTINCT accountid)
             FROM challenge_client_progress
             WHERE status = 'In Progress' AND date = CURRENT_DATE AND accountid IS NOT NULL)
                AS clients_in_progress_today,
            (SELECT COUNT(DISTINCT accountid)
             FROM challenge_client_progress
             WHERE status = 'Completed' AND date = CURRENT_DATE AND accountid IS NOT NULL)
                AS completions_today,
            (SELECT COALESCE(SUM(reward_amount), 0)
             FROM challenge_credit_log
             WHERE created_at::date = CURRENT_DATE)
                AS usd_paid_today,
            (SELECT CASE WHEN COUNT(*) > 0
                THEN ROUND(100.0 * COUNT(*) FILTER (WHERE api_response NOT LIKE 'ERROR%%') / COUNT(*), 2)
                ELSE 100.0 END
             FROM challenge_credit_log)
                AS credit_api_success_rate_pct
    """))
    snapshot = _clean(_row_to_dict(snap_row.fetchone()))

    # 2. Funnel (all-time from credit_log, since progress is purged after 7d)
    funnel_row = await db.execute(text("""
        SELECT
            (SELECT COUNT(DISTINCT accountid)
             FROM challenge_client_progress
             WHERE status IN ('In Progress','Completed','Cancelled') AND accountid IS NOT NULL)
                AS started_7d,
            (SELECT COUNT(DISTINCT accountid)
             FROM challenge_client_progress
             WHERE status = 'Completed' AND accountid IS NOT NULL)
                AS completed_7d,
            (SELECT COALESCE(SUM(reward_amount), 0)
             FROM challenge_credit_log)
                AS total_usd_paid_alltime
    """))
    funnel = _clean(_row_to_dict(funnel_row.fetchone()))
    started = funnel.get("started_7d", 0) or 0
    completed = funnel.get("completed_7d", 0) or 0
    funnel["completion_rate_pct"] = round(completed / started * 100, 2) if started > 0 else 0

    # 3. Daily trend (last 7 days)
    trend_rows = await db.execute(text("""
        WITH dp AS (
            SELECT date,
                COUNT(DISTINCT accountid)
                    FILTER (WHERE status IN ('In Progress','Completed','Cancelled'))
                    AS started,
                COUNT(DISTINCT accountid)
                    FILTER (WHERE status = 'Completed')
                    AS completed
            FROM challenge_client_progress
            WHERE accountid IS NOT NULL
            GROUP BY date
        ),
        dc AS (
            SELECT created_at::date AS payout_date,
                COALESCE(SUM(reward_amount), 0) AS usd_paid
            FROM challenge_credit_log
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY created_at::date
        )
        SELECT COALESCE(dp.date, dc.payout_date) AS day,
            COALESCE(dp.started, 0) AS started,
            COALESCE(dp.completed, 0) AS completed,
            ROUND(COALESCE(dc.usd_paid, 0)::numeric, 2) AS usd_paid
        FROM dp
        FULL OUTER JOIN dc ON dp.date = dc.payout_date
        ORDER BY day DESC
        LIMIT 7
    """))
    daily_trend = [_clean(_row_to_dict(r)) for r in trend_rows.fetchall()]

    # 4. Type distribution
    type_rows = await db.execute(text("""
        SELECT type, COUNT(DISTINCT group_name) AS count
        FROM challenges WHERE isactive = 1
        GROUP BY type ORDER BY count DESC
    """))
    type_distribution = [_clean(_row_to_dict(r)) for r in type_rows.fetchall()]

    # 5. Payout by group
    payout_rows = await db.execute(text("""
        SELECT
            COALESCE(ccl.group_name, c.group_name) AS group_name,
            ROUND(SUM(ccl.reward_amount)::numeric, 2) AS total_paid
        FROM challenge_credit_log ccl
        LEFT JOIN challenges c ON c."challengeId" = ccl."challengeId"
        WHERE COALESCE(ccl.group_name, c.group_name) IS NOT NULL
        GROUP BY COALESCE(ccl.group_name, c.group_name)
        ORDER BY total_paid DESC
    """))
    payout_by_group = [_clean(_row_to_dict(r)) for r in payout_rows.fetchall()]

    # 6. Optimove health
    opt_rows = await db.execute(text("""
        SELECT event_name,
            COUNT(*) FILTER (WHERE success = TRUE) AS success_count,
            COUNT(*) FILTER (WHERE success = FALSE) AS failure_count,
            COUNT(*) AS total,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(100.0 * COUNT(*) FILTER (WHERE success = TRUE) / COUNT(*), 2)
                ELSE 0 END AS success_rate_pct
        FROM optimove_event_log
        GROUP BY event_name ORDER BY total DESC
    """))
    optimove_health = [_clean(_row_to_dict(r)) for r in opt_rows.fetchall()]

    # 7. Per-challenge breakdown
    pc_rows = await db.execute(text("""
        WITH group_def AS (
            SELECT DISTINCT ON (group_name) group_name, type, timeperiod,
                MAX(isactive) OVER (PARTITION BY group_name) AS is_active,
                reward_multiplier
            FROM challenges ORDER BY group_name, "InsertDate" DESC
        ),
        progress_stats AS (
            SELECT c.group_name,
                COUNT(DISTINCT cp.accountid)
                    FILTER (WHERE cp.status IN ('In Progress','Completed','Cancelled'))
                    AS started,
                COUNT(DISTINCT cp.accountid)
                    FILTER (WHERE cp.status = 'Completed')
                    AS completed
            FROM challenge_client_progress cp
            JOIN challenges c ON c."challengeId" = cp."challengeId"
            WHERE cp.accountid IS NOT NULL
            GROUP BY c.group_name
        ),
        credit_stats AS (
            SELECT COALESCE(ccl.group_name, c.group_name) AS group_name,
                COUNT(*) AS payout_count,
                ROUND(SUM(ccl.reward_amount)::numeric, 2) AS total_paid
            FROM challenge_credit_log ccl
            LEFT JOIN challenges c ON c."challengeId" = ccl."challengeId"
            WHERE COALESCE(ccl.group_name, c.group_name) IS NOT NULL
            GROUP BY COALESCE(ccl.group_name, c.group_name)
        )
        SELECT gd.group_name, gd.type, gd.timeperiod, gd.is_active, gd.reward_multiplier,
            COALESCE(ps.started, 0) AS clients_started,
            COALESCE(ps.completed, 0) AS clients_completed,
            CASE WHEN COALESCE(ps.started, 0) > 0
                THEN ROUND(100.0 * COALESCE(ps.completed, 0) / ps.started, 2)
                ELSE 0 END AS completion_rate_pct,
            COALESCE(cs.payout_count, 0) AS payout_count,
            COALESCE(cs.total_paid, 0) AS total_usd_paid
        FROM group_def gd
        LEFT JOIN progress_stats ps ON ps.group_name = gd.group_name
        LEFT JOIN credit_stats cs ON cs.group_name = gd.group_name
        ORDER BY gd.group_name
    """))
    per_challenge = [_clean(_row_to_dict(r)) for r in pc_rows.fetchall()]

    # 8. Top earners
    te_rows = await db.execute(text("""
        SELECT COALESCE(accountid, trading_account_id) AS client_id,
            COUNT(*) AS total_payouts,
            ROUND(SUM(reward_amount)::numeric, 2) AS total_usd_earned,
            COUNT(DISTINCT group_name) AS groups_participated,
            MIN(created_at) AS first_reward,
            MAX(created_at) AS last_reward
        FROM challenge_credit_log
        GROUP BY COALESCE(accountid, trading_account_id)
        ORDER BY total_usd_earned DESC LIMIT 20
    """))
    top_earners = [_clean(_row_to_dict(r)) for r in te_rows.fetchall()]

    # 9. Streak leaderboard
    sk_rows = await db.execute(text("""
        SELECT accountid, group_name, current_streak, last_trade_date,
            last_rewarded_tier,
            ROUND(total_reward::numeric, 2) AS total_streak_reward
        FROM challenge_client_streaks
        WHERE current_streak > 0
        ORDER BY current_streak DESC LIMIT 20
    """))
    streak_leaderboard = [_clean(_row_to_dict(r)) for r in sk_rows.fetchall()]

    # 10. Asset class diversity
    div_rows = await db.execute(text("""
        SELECT group_name, asset_class,
            COUNT(DISTINCT accountid) AS unique_clients,
            week_start
        FROM challenge_client_instruments
        GROUP BY group_name, asset_class, week_start
        ORDER BY week_start DESC, group_name, unique_clients DESC
    """))
    diversity = [_clean(_row_to_dict(r)) for r in div_rows.fetchall()]

    return {
        "snapshot": snapshot,
        "funnel_7d": funnel,
        "daily_trend": daily_trend,
        "type_distribution": type_distribution,
        "payout_by_group": payout_by_group,
        "optimove_health": optimove_health,
        "per_challenge": per_challenge,
        "top_earners": top_earners,
        "streak_leaderboard": streak_leaderboard,
        "diversity": diversity,
    }


# ---------------------------------------------------------------------------
# Admin CRUD endpoints
# ---------------------------------------------------------------------------


@router.post("/challenges", status_code=201)
async def create_challenge_group(
    body: ChallengeGroupIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Create a new challenge group with multiple tiers."""
    # Check if group_name already exists
    existing = await db.execute(
        text("SELECT 1 FROM challenges WHERE group_name = :gn LIMIT 1"),
        {"gn": body.group_name},
    )
    if existing.fetchone():
        raise HTTPException(400, f"Challenge group '{body.group_name}' already exists")

    audience_json = json.dumps(body.audience_criteria) if body.audience_criteria else None

    for idx, tier in enumerate(body.tiers):
        await db.execute(
            text(
                'INSERT INTO challenges (name, type, targetvalue, timeperiod, isactive, '
                'rewardtype, rewardamount, "InsertDate", group_name, audience_criteria, '
                'valid_until, reward_multiplier, expires_on, symbol) '
                "VALUES (:name, :type, :target, :period, 1, 'credit', :reward, :now, :gn, "
                "CAST(:aud AS jsonb), :valid_until, :reward_mult, :expires_on, :symbol)"
            ),
            {
                "name": tier.name,
                "type": body.type,
                "target": tier.targetvalue,
                "reward": tier.rewardamount,
                "now": datetime.utcnow(),
                "gn": body.group_name,
                "aud": audience_json,
                "period": body.timeperiod,
                "valid_until": body.valid_until,
                "reward_mult": body.reward_multiplier,
                "expires_on": body.expires_on,
                "symbol": tier.symbol,
            },
        )
    await db.commit()
    logger.info("CLAUD-89: Created challenge group '%s' with %d tiers", body.group_name, len(body.tiers))
    return {"status": "ok", "group_name": body.group_name, "tiers": len(body.tiers)}


@router.get("/challenges")
async def list_challenge_groups(
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """List all challenge groups with their tiers."""
    rows = await db.execute(
        text(
            'SELECT "challengeId", name, type, targetvalue, isactive, rewardamount, '
            '"InsertDate", group_name, audience_criteria, timeperiod, valid_until, reward_multiplier, expires_on, symbol '
            "FROM challenges ORDER BY group_name, targetvalue"
        )
    )
    all_rows = rows.fetchall()

    groups: dict[str, dict] = {}
    for r in all_rows:
        gn = r[7]  # group_name
        if gn not in groups:
            groups[gn] = {
                "group_name": gn,
                "type": r[2],
                "isactive": r[4],
                "InsertDate": r[6].isoformat() if r[6] else None,
                "audience_criteria": r[8],
                "timeperiod": r[9] or "daily",
                "valid_until": r[10].isoformat() if r[10] else None,
                "reward_multiplier": float(r[11]) if r[11] is not None else 1.0,
                "expires_on": r[12].isoformat() if r[12] else None,
                "tiers": [],
            }
        groups[gn]["tiers"].append({
            "challengeId": r[0],
            "name": r[1],
            "targetvalue": float(r[3]),
            "rewardamount": float(r[5]),
            "symbol": r[13],  # instrument type
        })

    # Sort tiers by targetvalue and assign tier_rank
    for g in groups.values():
        g["tiers"].sort(key=lambda t: (t["targetvalue"], t["challengeId"]))
        for idx, tier in enumerate(g["tiers"]):
            tier["tier_rank"] = idx + 1

    return list(groups.values())


@router.patch("/challenges/{group_name}/toggle")
async def toggle_challenge_group(
    group_name: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Toggle isactive for all tiers in a challenge group."""
    # Get current state
    row = await db.execute(
        text("SELECT isactive FROM challenges WHERE group_name = :gn LIMIT 1"),
        {"gn": group_name},
    )
    current = row.fetchone()
    if not current:
        raise HTTPException(404, f"Challenge group '{group_name}' not found")

    new_status = 0 if current[0] == 1 else 1
    await db.execute(
        text("UPDATE challenges SET isactive = :status WHERE group_name = :gn"),
        {"status": new_status, "gn": group_name},
    )
    cancelled_count = 0
    if new_status == 0:
        # CLAUD-109: Cancel all In Progress client records for this group
        result = await db.execute(
            text(
                "UPDATE challenge_client_progress SET status = 'Cancelled' "
                'WHERE status = \'In Progress\' AND "challengeId" IN ('
                '    SELECT "challengeId" FROM challenges WHERE group_name = :gn'
                ")"
            ),
            {"gn": group_name},
        )
        cancelled_count = result.rowcount
        logger.info(
            "CLAUD-109: Cancelled %d in-progress client records for disabled group '%s'",
            cancelled_count, group_name,
        )
    await db.commit()
    logger.info("CLAUD-89: Toggled challenge group '%s' to isactive=%d", group_name, new_status)
    return {"status": "ok", "group_name": group_name, "isactive": new_status, "cancelled_clients": cancelled_count}


@router.delete("/challenges/{group_name}")
async def delete_challenge_group(
    group_name: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Delete all rows for a challenge group."""
    result = await db.execute(
        text("DELETE FROM challenges WHERE group_name = :gn"),
        {"gn": group_name},
    )
    if result.rowcount == 0:
        raise HTTPException(404, f"Challenge group '{group_name}' not found")

    # Also clean up progress records for this group's challenges
    await db.execute(
        text(
            'DELETE FROM challenge_client_progress WHERE "challengeId" NOT IN '
            '(SELECT "challengeId" FROM challenges)'
        )
    )
    await db.commit()
    logger.info("CLAUD-89: Deleted challenge group '%s'", group_name)
    return {"status": "ok", "group_name": group_name}


# ---------------------------------------------------------------------------
# CLAUD-94: Symbol Asset Class Mapping endpoints (admin)
# ---------------------------------------------------------------------------


@router.get("/challenges/symbols")
async def list_symbol_mappings(
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """List all symbol -> asset class mappings."""
    rows = await db.execute(
        text("SELECT symbol, asset_class FROM symbol_asset_class ORDER BY symbol")
    )
    return [{"symbol": r[0], "asset_class": r[1]} for r in rows.fetchall()]


@router.post("/challenges/symbols", status_code=201)
async def upsert_symbol_mapping(
    body: SymbolMappingIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Create or update a symbol -> asset class mapping."""
    sym = body.symbol.upper()
    await db.execute(
        text(
            "INSERT INTO symbol_asset_class (symbol, asset_class) VALUES (:sym, :cls) "
            "ON CONFLICT (symbol) DO UPDATE SET asset_class = :cls"
        ),
        {"sym": sym, "cls": body.asset_class},
    )
    await db.commit()
    logger.info("CLAUD-94: Symbol mapping upserted: %s -> %s", sym, body.asset_class)
    return {"status": "ok", "symbol": sym, "asset_class": body.asset_class}


@router.delete("/challenges/symbols/{symbol}")
async def delete_symbol_mapping(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Delete a symbol -> asset class mapping."""
    result = await db.execute(
        text("DELETE FROM symbol_asset_class WHERE symbol = :sym"),
        {"sym": symbol.upper()},
    )
    if result.rowcount == 0:
        raise HTTPException(404, f"Symbol '{symbol.upper()}' not found")
    await db.commit()
    return {"status": "ok", "symbol": symbol.upper()}


# ---------------------------------------------------------------------------
# CLAUD-90: Client Progress endpoint (admin)
# ---------------------------------------------------------------------------


@router.get("/challenges/progress")
async def get_challenge_progress(
    date_filter: Optional[str] = Query(None, alias="date", description="Date filter YYYY-MM-DD, default today"),
    group_name: Optional[str] = Query(None, description="Filter by challenge group name"),
    accountid: Optional[str] = Query(None, description="Filter by accountid"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Page size"),
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Return paginated client challenge progress records."""
    from datetime import date as date_type
    try:
        target_date = date_type.fromisoformat(date_filter) if date_filter else date_type.today()
    except ValueError:
        raise HTTPException(400, "Invalid date format, expected YYYY-MM-DD")

    # Build WHERE clause dynamically
    where_clauses = ["cp.date = :target_date"]
    params: dict[str, Any] = {"target_date": target_date}

    if group_name:
        where_clauses.append("c.group_name = :group_name")
        params["group_name"] = group_name

    if accountid:
        where_clauses.append("cp.accountid = :accountid")
        params["accountid"] = accountid

    where_sql = " AND ".join(where_clauses)

    # Count total
    count_sql = (
        "SELECT COUNT(*) FROM challenge_client_progress cp "
        'JOIN challenges c ON c."challengeId" = cp."challengeId" '
        f"WHERE {where_sql}"
    )
    count_row = await db.execute(text(count_sql), params)
    total = count_row.scalar() or 0

    # Fetch page
    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    data_sql = (
        "SELECT cp.accountid, c.group_name, c.type AS challenge_type, "
        "cp.progress_value, "
        "(SELECT COUNT(*) FROM challenges c2 WHERE c2.group_name = c.group_name AND c2.isactive = 1) AS total_tiers, "
        "cp.last_rewarded_tier, cp.total_reward, cp.status, cp.date "
        "FROM challenge_client_progress cp "
        'JOIN challenges c ON c."challengeId" = cp."challengeId" '
        f"WHERE {where_sql} "
        "ORDER BY cp.date DESC, c.group_name, cp.accountid "
        "LIMIT :limit OFFSET :offset"
    )
    rows = await db.execute(text(data_sql), params)
    items = []
    for r in rows.fetchall():
        items.append({
            "accountid": r[0] or "",
            "group_name": r[1],
            "challenge_type": r[2],
            "progress_value": float(r[3]) if r[3] is not None else 0,
            "total_tiers": r[4] or 0,
            "last_rewarded_tier": r[5] or 0,
            "total_reward": float(r[6]) if r[6] is not None else 0,
            "status": r[7] or "Open",
            "date": str(r[8]) if r[8] else target_date.isoformat(),
        })

    return {"total": total, "items": items}


# ---------------------------------------------------------------------------
# CLAUD-91: Optimove Events Log endpoint (admin)
# ---------------------------------------------------------------------------


@router.get("/challenges/events")
async def get_optimove_events(
    date_filter: Optional[str] = Query(None, alias="date", description="Date filter YYYY-MM-DD, default today"),
    event_name: Optional[str] = Query(None, description="Filter by event name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_challenges),
):
    """Return paginated optimove_event_log rows with challenge group name."""
    from datetime import date as date_type
    try:
        target_date = date_type.fromisoformat(date_filter) if date_filter else date_type.today()
    except ValueError:
        raise HTTPException(400, "Invalid date format, expected YYYY-MM-DD")

    where_clauses = ["oel.created_at::date = :target_date"]
    params: dict[str, Any] = {"target_date": target_date}

    if event_name:
        where_clauses.append("oel.event_name = :event_name")
        params["event_name"] = event_name

    where_sql = " AND ".join(where_clauses)

    # Count total
    count_sql = (
        "SELECT COUNT(*) FROM optimove_event_log oel "
        f"WHERE {where_sql}"
    )
    count_row = await db.execute(text(count_sql), params)
    total = count_row.scalar() or 0

    # Fetch page
    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    data_sql = (
        'SELECT oel.id, oel."challengeId", c.group_name, oel.accountid, '
        "oel.event_name, oel.payload, oel.response, oel.success, oel.created_at "
        "FROM optimove_event_log oel "
        'LEFT JOIN challenges c ON c."challengeId" = oel."challengeId" '
        f"WHERE {where_sql} "
        "ORDER BY oel.created_at DESC "
        "LIMIT :limit OFFSET :offset"
    )
    rows = await db.execute(text(data_sql), params)
    items = []
    for r in rows.fetchall():
        items.append({
            "id": r[0],
            "challengeId": r[1],
            "group_name": r[2] or "",
            "accountid": r[3] or "",
            "event_name": r[4],
            "payload": r[5],
            "response": r[6] or "",
            "success": bool(r[7]),
            "created_at": r[8].isoformat() if r[8] else None,
        })

    return {"total": total, "items": items}


# ---------------------------------------------------------------------------
# Webhook endpoint (public -- no auth)
# ---------------------------------------------------------------------------


def _safe_float(v: Any) -> float:
    """Parse a value to float, returning 0.0 if it can't be parsed."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


@router.post("/webhooks/trade-event")
async def trade_event_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Receive open_trade events and process challenge progress."""
    raw_body = await request.body()
    logger.info("WEBHOOK: raw body: %s", raw_body.decode("utf-8", errors="replace")[:2000])
    try:
        raw_json = await request.json()
    except Exception as e:
        logger.error("WEBHOOK: failed to parse JSON: %s | body: %s", e, raw_body.decode("utf-8", errors="replace")[:500])
        return {"status": "error", "reason": f"invalid JSON: {e}"}
    payload = TradeEventPayload(**{k: raw_json.get(k) for k in ("tenant", "event", "customer", "context")})
    logger.info("WEBHOOK: received payload: %s", payload.model_dump())

    # Always log to webhook_event_log regardless of challenge activity
    import json as _json
    from sqlalchemy import text as _text
    from app.pg_database import AsyncSessionLocal as _AsyncSessionLocal
    try:
        async with _AsyncSessionLocal() as _log_sess:
            await _log_sess.execute(
                _text(
                    "INSERT INTO webhook_event_log (event_name, customer, payload, actions_applied) "
                    "VALUES (:ev, :cust, CAST(:payload AS jsonb), '[]'::jsonb)"
                ),
                {
                    "ev": payload.event or "unknown",
                    "cust": str(payload.customer) if payload.customer is not None else None,
                    "payload": _json.dumps(raw_json),
                },
            )
            await _log_sess.commit()
            logger.info("WEBHOOK: event logged to webhook_event_log")
    except Exception as _log_err:
        logger.warning("WEBHOOK: failed to store event log: %s", _log_err)

    _ACTION_BONUS_EVENTS = ("live_details", "submit_documents")
    if not payload.event or payload.event not in ("open_trade", "close_trade") + _ACTION_BONUS_EVENTS:
        return {"status": "ignored", "reason": "not a supported trade event"}

    ctx = payload.context
    if not ctx or not ctx.account_number:
        # Action bonus events may arrive without account_number — handle them via accountid only
        if payload.event in _ACTION_BONUS_EVENTS:
            accountid_ab = str(payload.customer) if payload.customer is not None else None
            if not accountid_ab:
                return {"status": "ignored", "reason": "missing customer (accountid) in payload"}
            from app.routers.action_bonuses import process_action_bonus
            country_ab = ctx.country if ctx else None
            affiliate_ab = ctx.affiliate if ctx else None
            return await process_action_bonus(
                event_type=payload.event,
                accountid=accountid_ab,
                trading_account_id=accountid_ab,  # no MT4 lookup available without account_number
                country=country_ab,
                affiliate=affiliate_ab,
                db=db,
            )
        return {"status": "ignored", "reason": "missing context or account_number"}

    # CLAUD-90 Fix 1: payload.customer = accountid (vtiger CRM ID)
    # ctx.account_number = MT4 login -> look up trading_acount_id
    accountid = str(payload.customer) if payload.customer is not None else None
    if not accountid:
        return {"status": "ignored", "reason": "missing customer (accountid) in payload"}

    account_number = ctx.account_number  # MT4 login
    event_type = payload.event  # "open_trade" or "close_trade"
    trade_volume = _safe_float(ctx.volume) or _safe_float(ctx.trading_volume)
    trade_profit = _safe_float(ctx.profit)

    # CLAUD-93: For close_trade, skip if profit is non-positive
    if event_type == "close_trade" and trade_profit <= 0:
        logger.info(
            "CLAUD-93: close_trade ignored for accountid=%s — profit=%.2f is non-positive",
            accountid, trade_profit,
        )
        return {"status": "ignored", "reason": "zero or negative profit"}
    today = date.today()

    logger.info(
        "CLAUD-90: Processing trade event for accountid=%s, MT4 login=%s (volume=%.2f)",
        accountid, account_number, trade_volume,
    )

    # CLAUD-90: Look up trading_acount_id from MSSQL report.vtiger_trading_accounts
    try:
        account_number_int = int(account_number)
    except (ValueError, TypeError):
        logger.warning("CLAUD-90: account_number '%s' is not a valid integer MT4 login", account_number)
        return {"status": "ignored", "reason": "invalid account_number"}

    try:
        rows = await mssql_query(
            "SELECT trading_acount_id FROM report.vtiger_trading_accounts WHERE login = ?",
            (account_number_int,),
        )
    except Exception as e:
        logger.warning("CLAUD-90: MSSQL lookup failed for MT4 login %s: %s", account_number, e)
        return {"status": "ignored", "reason": "trading account lookup unavailable"}

    if not rows:
        logger.warning("CLAUD-90: MT4 login %s not found in report.vtiger_trading_accounts (MSSQL)", account_number)
        return {"status": "ignored", "reason": "trading account not found"}

    trading_acount_id = str(rows[0]["trading_acount_id"])

    # CLAUD-96: Action bonus events (live_details, submit_documents)
    if event_type in ("live_details", "submit_documents"):
        from app.routers.action_bonuses import process_action_bonus
        country = ctx.country if ctx else None
        affiliate = ctx.affiliate if ctx else None
        result = await process_action_bonus(
            event_type=event_type,
            accountid=accountid,
            trading_account_id=trading_acount_id,
            country=country,
            affiliate=affiliate,
            db=db,
        )
        return result

    # CLAUD-91: Fetch Optimove URL once (before processing groups)
    optimove_url = await _get_optimove_url(db)

    # Fetch all active challenges
    active_challenges = await db.execute(
        text(
            'SELECT "challengeId", name, type, targetvalue, rewardamount, group_name, audience_criteria, timeperiod, valid_until, reward_multiplier, expires_on, symbol '
            'FROM challenges WHERE isactive = 1 ORDER BY group_name, targetvalue, "challengeId"'
        )
    )
    challenges = active_challenges.fetchall()

    if not challenges:
        return {"status": "no_credit", "reason": "no active challenges found"}

    # CLAUD-93: Filter challenges by event type compatibility
    # open_trade drives: trade, volume, streak, diversity, instrument
    # close_trade drives: pnl only
    challenges = [
        ch for ch in challenges
        if (event_type == "open_trade" and ch[2] in ("trade", "volume", "streak", "diversity", "instrument"))
        or (event_type == "close_trade" and ch[2] == "pnl")
    ]

    if not challenges:
        return {"status": "no_credit", "reason": "no active challenges found for this event type"}

    # CLAUD-90 Fix 1: Filter challenges by audience criteria using accountid (not account_number)
    matching_challenges = []
    for ch in challenges:
        aud = ch[6]  # audience_criteria (JSONB -- auto-parsed by asyncpg)
        if aud is None or (isinstance(aud, dict) and aud.get("all_clients", False)):
            matching_challenges.append(ch)
            continue

        if isinstance(aud, str):
            try:
                aud = json.loads(aud)
            except (json.JSONDecodeError, TypeError):
                matching_challenges.append(ch)
                continue

        if not isinstance(aud, dict):
            matching_challenges.append(ch)
            continue

        # Check audience filters
        matches = True

        if aud.get("countries") and ctx.country:
            if ctx.country.upper() not in [c.upper() for c in aud["countries"]]:
                matches = False

        if aud.get("languages") and ctx.language:
            if ctx.language.lower() not in [lang.lower() for lang in aud["languages"]]:
                matches = False

        # account_ids whitelist (CSV upload audience)
        if aud.get("account_ids") and isinstance(aud["account_ids"], list):
            account_ids_set = set(str(x) for x in aud["account_ids"])
            if accountid not in account_ids_set:
                matches = False

        # balance_min / balance_max -- would need a balance lookup; skip for now
        # (webhook context doesn't carry balance; future enhancement)

        if matches:
            matching_challenges.append(ch)

    if not matching_challenges:
        return {"status": "no_credit", "reason": "no active challenges found for this client"}

    # Group matching challenges by group_name for tier processing
    groups: dict[str, list] = {}
    for ch in matching_challenges:
        gn = ch[5]  # group_name
        if gn not in groups:
            groups[gn] = []
        groups[gn].append(ch)

    rewards_given = 0

    for group_name, tiers in groups.items():
        # Sort tiers by targetvalue ascending to assign tier_rank
        tiers.sort(key=lambda t: (t[3], t[0]))  # t[3] = targetvalue, t[0] = challengeId (insertion order tiebreaker)
        total_tiers_in_group = len(tiers)

        # Determine challenge type from the first tier
        challenge_type = tiers[0][2]  # type
        challenge_timeperiod = tiers[0][7] if len(tiers[0]) > 7 else "daily"  # timeperiod

        # Upsert progress -- use the first tier's challengeId as anchor for the group.
        anchor_id = tiers[0][0]

        # CLAUD-95: Check flash challenge expiry
        valid_until = tiers[0][8] if len(tiers[0]) > 8 else None
        if valid_until and datetime.utcnow() > valid_until:
            logger.info("CLAUD-95: Skipping expired flash challenge group '%s'", group_name)
            continue

        reward_multiplier_val = float(tiers[0][9]) if len(tiers[0]) > 9 and tiers[0][9] is not None else 1.0

        expires_on_val = tiers[0][10] if len(tiers[0]) > 10 else None
        # Check challenge expiration date
        if expires_on_val and today > expires_on_val:
            logger.info("Challenge group '%s' expired on %s — skipping", group_name, expires_on_val)
            continue

        # CLAUD-94: For weekly challenges, use week_start as progress_date
        if challenge_timeperiod == "weekly":
            progress_date = today - timedelta(days=today.weekday())  # Monday
        else:
            progress_date = today

        # ---- CLAUD-92: Streak challenge type ---------------------------------
        if challenge_type == "streak":
            yesterday = today - timedelta(days=1)

            # Get persisted streak state for this group + accountid
            streak_row = await db.execute(
                text(
                    "SELECT current_streak, last_trade_date, last_rewarded_tier "
                    "FROM challenge_client_streaks WHERE group_name = :gn AND accountid = :acc"
                ),
                {"gn": group_name, "acc": accountid},
            )
            streak = streak_row.fetchone()

            if streak and streak[1] == today:
                # Already counted today — read state, no update needed
                new_streak = streak[0]
                last_rewarded = streak[2] or 0
                is_fresh_insert = False
            else:
                # Compute new streak value
                if streak and streak[1] == yesterday:
                    new_streak = streak[0] + 1
                else:
                    new_streak = 1
                last_rewarded = (streak[2] or 0) if streak else 0

                # Check if first trade today (for Optimove, before upsert)
                existing_row = await db.execute(
                    text(
                        'SELECT 1 FROM challenge_client_progress '
                        'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                    ),
                    {"cid": anchor_id, "acc": account_number, "pd": progress_date},
                )
                is_fresh_insert = existing_row.fetchone() is None

                # Persist streak state (only update current_streak + last_trade_date; preserve last_rewarded_tier)
                await db.execute(
                    text(
                        "INSERT INTO challenge_client_streaks "
                        "(group_name, accountid, current_streak, last_trade_date, last_rewarded_tier, total_reward) "
                        "VALUES (:gn, :acc, :streak, :today, 0, 0) "
                        "ON CONFLICT (group_name, accountid) "
                        "DO UPDATE SET current_streak = :streak, last_trade_date = :today"
                    ),
                    {"gn": group_name, "acc": accountid, "streak": new_streak, "today": today},
                )

                # Upsert daily progress record (SET progress_value = streak, not additive)
                await db.execute(
                    text(
                        'INSERT INTO challenge_client_progress '
                        '("challengeId", trading_account_id, progress_value, last_rewarded_tier, '
                        'date, accountid, status, total_reward) '
                        "VALUES (:cid, :acc, :streak, 0, :pd, :accountid, 'In Progress', 0) "
                        'ON CONFLICT ("challengeId", trading_account_id, date) '
                        "DO UPDATE SET progress_value = :streak, "
                        "status = CASE WHEN challenge_client_progress.status = 'Open' THEN 'In Progress' "
                        "ELSE challenge_client_progress.status END"
                    ),
                    {"cid": anchor_id, "acc": account_number, "streak": new_streak,
                     "pd": progress_date, "accountid": accountid},
                )

            current_progress = float(new_streak)

        # ---- CLAUD-94: Diversity challenge type (weekly, by asset class) ------
        elif challenge_type == "diversity":
            if not ctx.symbol:
                logger.warning("CLAUD-94: diversity challenge '%s' skipped — no symbol in payload", group_name)
                continue

            # Look up asset class for symbol
            sym_row = await db.execute(
                text("SELECT asset_class FROM symbol_asset_class WHERE symbol = :sym"),
                {"sym": ctx.symbol.upper()},
            )
            sym_result = sym_row.fetchone()
            if not sym_result:
                logger.warning("CLAUD-94: symbol '%s' not found in symbol_asset_class — skipping diversity", ctx.symbol)
                continue

            asset_class = sym_result[0]

            # Check if first trade for this client/group/week (for Optimove)
            existing_row = await db.execute(
                text(
                    'SELECT 1 FROM challenge_client_progress '
                    'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                ),
                {"cid": anchor_id, "acc": account_number, "pd": progress_date},
            )
            is_fresh_insert = existing_row.fetchone() is None

            # Insert instrument record (idempotent ON CONFLICT DO NOTHING)
            await db.execute(
                text(
                    "INSERT INTO challenge_client_instruments "
                    "(group_name, accountid, asset_class, week_start) "
                    "VALUES (:gn, :acc, :cls, :week) "
                    "ON CONFLICT (group_name, accountid, asset_class, week_start) DO NOTHING"
                ),
                {"gn": group_name, "acc": accountid, "cls": asset_class, "week": progress_date},
            )

            # Count distinct asset classes this client has traded this week for this group
            count_row = await db.execute(
                text(
                    "SELECT COUNT(DISTINCT asset_class) "
                    "FROM challenge_client_instruments "
                    "WHERE group_name = :gn AND accountid = :acc AND week_start = :week"
                ),
                {"gn": group_name, "acc": accountid, "week": progress_date},
            )
            distinct_count = count_row.scalar() or 0

            # Upsert progress record (SET, not additive)
            await db.execute(
                text(
                    'INSERT INTO challenge_client_progress '
                    '("challengeId", trading_account_id, progress_value, last_rewarded_tier, '
                    'date, accountid, status, total_reward) '
                    "VALUES (:cid, :acc, :cnt, 0, :pd, :accountid, 'In Progress', 0) "
                    'ON CONFLICT ("challengeId", trading_account_id, date) '
                    "DO UPDATE SET progress_value = :cnt, "
                    "status = CASE WHEN challenge_client_progress.status = 'Open' THEN 'In Progress' "
                    "ELSE challenge_client_progress.status END"
                ),
                {"cid": anchor_id, "acc": account_number, "cnt": distinct_count,
                 "pd": progress_date, "accountid": accountid},
            )

            # Read last_rewarded_tier
            prog_row = await db.execute(
                text(
                    'SELECT progress_value, last_rewarded_tier FROM challenge_client_progress '
                    'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                ),
                {"cid": anchor_id, "acc": account_number, "pd": progress_date},
            )
            progress = prog_row.fetchone()
            if not progress:
                continue

            current_progress = float(progress[0])
            last_rewarded = progress[1] or 0

        # ---- Instrument challenge type (per-symbol, independent tiers) --------
        elif challenge_type == "instrument":
            if not ctx.symbol:
                logger.debug("Instrument challenge '%s': no symbol in event context — skipping", group_name)
                continue

            trade_symbol = ctx.symbol.upper().strip()

            for tier in tiers:
                tier_challenge_id = tier[0]
                tier_symbol = tier[11] if len(tier) > 11 else None  # symbol column
                if not tier_symbol or tier_symbol.upper().strip() != trade_symbol:
                    continue

                # Idempotency: check if this tier was already rewarded today
                already = await db.execute(
                    text(
                        'SELECT 1 FROM challenge_credit_log '
                        'WHERE group_name = :gn AND accountid = :acc AND "challengeId" = :cid '
                        "AND DATE(created_at AT TIME ZONE 'UTC') = :today"
                    ),
                    {"gn": group_name, "acc": accountid, "cid": tier_challenge_id, "today": today},
                )
                if already.fetchone():
                    logger.debug(
                        "Instrument tier %d already rewarded today for accountid=%s",
                        tier_challenge_id, accountid,
                    )
                    continue

                # Award credit via the credit API
                effective_reward = float(tier[4]) * reward_multiplier_val
                await _award_credit(
                    trading_acount_id,
                    effective_reward,
                    tier_challenge_id,
                    db,
                )
                rewards_given += 1

                # Log the credit with group_name and accountid for idempotency queries
                await db.execute(
                    text(
                        'INSERT INTO challenge_credit_log '
                        '(group_name, "challengeId", accountid, trading_account_id, reward_amount) '
                        "VALUES (:gn, :cid, :acc, :tid, :reward)"
                    ),
                    {
                        "gn": group_name,
                        "cid": tier_challenge_id,
                        "acc": accountid,
                        "tid": trading_acount_id,
                        "reward": effective_reward,
                    },
                )

                # Fire Optimove event
                if optimove_url:
                    asyncio.create_task(_fire_optimove_event(
                        optimove_url,
                        {
                            "customerId": accountid,
                            "challengeGroup": group_name,
                            "challengeType": "instrument",
                            "symbol": tier_symbol,
                            "reward": effective_reward,
                        },
                        tier_challenge_id,
                        accountid,
                        "instrument_reward",
                        db,
                    ))

            # Update progress: count distinct symbols rewarded today from credit_log
            rewarded_symbols = await db.execute(
                text(
                    'SELECT COUNT(*) FROM challenge_credit_log '
                    'WHERE group_name = :gn AND accountid = :acc '
                    "AND DATE(created_at AT TIME ZONE 'UTC') = :today"
                ),
                {"gn": group_name, "acc": accountid, "today": today},
            )
            prog_val = rewarded_symbols.fetchone()[0] or 0

            # Sum total rewards from credit_log today
            total_rewarded = await db.execute(
                text(
                    'SELECT COALESCE(SUM(reward_amount), 0) FROM challenge_credit_log '
                    'WHERE group_name = :gn AND accountid = :acc '
                    "AND DATE(created_at AT TIME ZONE 'UTC') = :today"
                ),
                {"gn": group_name, "acc": accountid, "today": today},
            )
            total_reward_val = float(total_rewarded.fetchone()[0] or 0)

            prog_status = (
                "Completed" if prog_val >= total_tiers_in_group
                else ("In Progress" if prog_val > 0 else "Open")
            )

            await db.execute(
                text(
                    'INSERT INTO challenge_client_progress '
                    '("challengeId", accountid, trading_account_id, group_name, challenge_type, '
                    'progress_value, date, total_tiers, last_rewarded_tier, total_reward, status) '
                    'VALUES (:cid, :acc, :tid, :gn, :ctype, :pval, :pd, :ttiers, :lrt, :tr, :status) '
                    'ON CONFLICT ("challengeId", trading_account_id, date) DO UPDATE SET '
                    'progress_value = EXCLUDED.progress_value, '
                    'last_rewarded_tier = EXCLUDED.last_rewarded_tier, '
                    'total_reward = EXCLUDED.total_reward, '
                    'status = EXCLUDED.status'
                ),
                {
                    "cid": anchor_id,
                    "acc": accountid,
                    "tid": account_number,
                    "gn": group_name,
                    "ctype": "instrument",
                    "pval": prog_val,
                    "pd": progress_date,
                    "ttiers": total_tiers_in_group,
                    "lrt": prog_val,
                    "tr": total_reward_val,
                    "status": prog_status,
                },
            )
            continue  # instrument type manages its own flow — skip the shared tier-loop below

        # ---- Trade / Volume / PnL challenge types ----------------------------
        else:
            if challenge_type == "pnl":
                increment = trade_profit  # Always positive (filtered earlier)
            elif challenge_type == "trade":
                increment = 1
            else:  # volume
                increment = trade_volume

            # CLAUD-91: Check if progress record already exists (to detect fresh insert for Optimove)
            existing_row = await db.execute(
                text(
                    'SELECT 1 FROM challenge_client_progress '
                    'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                ),
                {"cid": anchor_id, "acc": account_number, "pd": progress_date},
            )
            is_fresh_insert = existing_row.fetchone() is None

            await db.execute(
                text(
                    'INSERT INTO challenge_client_progress '
                    '("challengeId", trading_account_id, progress_value, last_rewarded_tier, date, accountid, status, total_reward) '
                    "VALUES (:cid, :acc, :inc, 0, :pd, :accountid, 'In Progress', 0) "
                    'ON CONFLICT ("challengeId", trading_account_id, date) '
                    "DO UPDATE SET progress_value = challenge_client_progress.progress_value + :inc, "
                    "status = CASE WHEN challenge_client_progress.status = 'Open' THEN 'In Progress' "
                    "ELSE challenge_client_progress.status END"
                ),
                {"cid": anchor_id, "acc": account_number, "inc": increment, "pd": progress_date, "accountid": accountid},
            )

            # Read current progress
            prog_row = await db.execute(
                text(
                    'SELECT progress_value, last_rewarded_tier FROM challenge_client_progress '
                    'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                ),
                {"cid": anchor_id, "acc": account_number, "pd": progress_date},
            )
            progress = prog_row.fetchone()
            if not progress:
                continue

            current_progress = float(progress[0])
            last_rewarded = progress[1] or 0

        # CLAUD-91: Fire challenge_started + challenge_started_live ONCE per client per group per day
        if is_fresh_insert and optimove_url:
            first_tier = tiers[0]
            started_payload = {
                "tenant": 991,
                "event": "challenge_started",
                "customer": int(payload.customer),
                "context": {
                    "email": ctx.email or "",
                    "challenge_type": challenge_type,
                    "challenge_goal": str(float(first_tier[3])),
                    "challenge_period": challenge_timeperiod or "daily",
                },
            }
            await _fire_optimove_event(
                optimove_url, started_payload, anchor_id, accountid, "challenge_started", db
            )

            started_live_payload = {
                "tenant": 991,
                "event": "challenge_started_live",
                "customer": int(ctx.account_number),
                "context": {
                    "email": ctx.email or "",
                    "challenge_type": challenge_type,
                    "challenge_goal": str(float(first_tier[3])),
                    "challenge_period": challenge_timeperiod or "daily",
                },
            }
            await _fire_optimove_event(
                optimove_url, started_live_payload, anchor_id, accountid, "challenge_started_live", db
            )

        # Check each tier threshold
        for tier_rank, tier in enumerate(tiers, start=1):
            target = float(tier[3])  # targetvalue
            reward = float(tier[4])  # rewardamount

            if current_progress >= target and tier_rank > last_rewarded:
                # CLAUD-90: use trading_acount_id (from vtiger lookup) as brokerUserId
                # CLAUD-95: Apply reward_multiplier for flash challenges
                effective_reward = reward * reward_multiplier_val
                credit_success = await _award_credit(trading_acount_id, effective_reward, tier[0], db)
                if credit_success:
                    rewards_given += 1

                new_status = "'Completed'" if tier_rank == total_tiers_in_group else "challenge_client_progress.status"

                # CLAUD-92: For streak, also update streak table's last_rewarded_tier
                if challenge_type == "streak":
                    await db.execute(
                        text(
                            "UPDATE challenge_client_streaks "
                            "SET last_rewarded_tier = :tier, total_reward = total_reward + :reward "
                            "WHERE group_name = :gn AND accountid = :acc"
                        ),
                        {"tier": tier_rank, "reward": effective_reward, "gn": group_name, "acc": accountid},
                    )

                await db.execute(
                    text(
                        f'UPDATE challenge_client_progress SET last_rewarded_tier = :tier, '
                        f'total_reward = challenge_client_progress.total_reward + :reward, '
                        f'status = {new_status} '
                        f'WHERE "challengeId" = :cid AND trading_account_id = :acc AND date = :pd'
                    ),
                    {"tier": tier_rank, "reward": effective_reward, "cid": anchor_id, "acc": account_number, "pd": progress_date},
                )
                last_rewarded = tier_rank

                # CLAUD-91/92/94/95: Fire challenge_completed for EACH tier completion
                if optimove_url:
                    completed_context: dict = {
                        "email": ctx.email or "",
                        "challenge_type": challenge_type,
                        "challenge_goal": str(float(tier[3])),
                        "challenge_period": challenge_timeperiod or "daily",
                        "amount_received": effective_reward,
                    }
                    # CLAUD-92: Add streak_day for streak type
                    if challenge_type == "streak":
                        completed_context["streak_day"] = int(current_progress)
                    # CLAUD-94: Add asset_classes for diversity
                    if challenge_type == "diversity":
                        completed_context["asset_classes_traded"] = int(current_progress)
                    # CLAUD-95: Add flash info
                    if valid_until:
                        completed_context["is_flash"] = True
                        completed_context["reward_multiplier"] = reward_multiplier_val
                    completed_payload_body = {
                        "tenant": 991,
                        "event": "challenge_completed",
                        "customer": int(payload.customer),
                        "context": completed_context,
                    }
                    await _fire_optimove_event(
                        optimove_url, completed_payload_body, tier[0], accountid, "challenge_completed", db
                    )

    await db.commit()
    logger.info(
        "CLAUD-90: Trade event processed for accountid=%s, MT4=%s -- %d rewards given",
        accountid, account_number, rewards_given,
    )
    if rewards_given == 0:
        return {"status": "no_credit", "reason": "no credit given — all tiers already completed or no qualifying tiers", "accountid": accountid, "account": account_number, "rewards_given": 0}
    return {"status": "ok", "accountid": accountid, "account": account_number, "rewards_given": rewards_given}


async def _award_credit(
    trading_acount_id: str,
    reward_amount: float,
    challenge_id: int,
    db: AsyncSession,
) -> bool:
    """Call the Credit API and log the result."""
    # Amount is in cents (multiply by 100)
    amount_cents = int(reward_amount * 100)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                CREDIT_API_URL,
                params={
                    "brokerUserId": trading_acount_id,
                    "amount": amount_cents,
                    "comment": "Challenge Bonus",
                },
                headers={"x-crm-api-token": CREDIT_API_TOKEN},
            )
            api_response = resp.text
            logger.info(
                "CLAUD-90: Credit API response for brokerUserId=%s, amount %d cents: %s",
                trading_acount_id, amount_cents, api_response[:200],
            )
    except Exception as e:
        api_response = f"ERROR: {e}"
        logger.error("CLAUD-90: Credit API call failed for brokerUserId=%s: %s", trading_acount_id, e)

    # Log to audit table
    await db.execute(
        text(
            'INSERT INTO challenge_credit_log ("challengeId", trading_account_id, reward_amount, api_response, created_at) '
            "VALUES (:cid, :acc, :reward, :resp, :now)"
        ),
        {
            "cid": challenge_id,
            "acc": trading_acount_id,
            "reward": reward_amount,
            "resp": api_response,
            "now": datetime.utcnow(),
        },
    )

    return "ERROR" not in api_response


# ---------------------------------------------------------------------------
# Daily reset scheduler function
# ---------------------------------------------------------------------------


async def reset_daily_challenges() -> None:
    """Reset daily challenges: deactivate current rows, re-insert as fresh, clean old progress."""
    from app.pg_database import AsyncSessionLocal

    logger.info("CLAUD-90: Running daily challenge reset")
    try:
        async with AsyncSessionLocal() as db:
            # CLAUD-90 Fix 4: Cancel all Open/In Progress progress records for today
            await db.execute(text(
                "UPDATE challenge_client_progress SET status = 'Cancelled' "
                "WHERE date = CURRENT_DATE AND status IN ('Open', 'In Progress')"
            ))

            # Deactivate groups that have passed their expires_on date
            await db.execute(text(
                "UPDATE challenges SET isactive = 0 "
                "WHERE expires_on IS NOT NULL AND expires_on < CURRENT_DATE AND isactive = 1"
            ))

            # Get all active challenge groups (CLAUD-94: include timeperiod to skip weekly)
            rows = await db.execute(
                text(
                    'SELECT DISTINCT group_name, type, audience_criteria, timeperiod '
                    "FROM challenges WHERE isactive = 1"
                )
            )
            active_groups = rows.fetchall()

            for group_row in active_groups:
                gn = group_row[0]
                gn_timeperiod = group_row[3] if len(group_row) > 3 else "daily"

                # CLAUD-94: Skip weekly challenges during daily reset
                if gn_timeperiod == "weekly":
                    continue

                # Get current tiers for this group
                tier_rows = await db.execute(
                    text(
                        "SELECT name, type, targetvalue, rewardtype, rewardamount, audience_criteria, "
                        "valid_until, reward_multiplier, expires_on, symbol "
                        "FROM challenges WHERE group_name = :gn AND isactive = 1 "
                        "ORDER BY targetvalue"
                    ),
                    {"gn": gn},
                )
                tiers = tier_rows.fetchall()

                # Deactivate current rows
                await db.execute(
                    text("UPDATE challenges SET isactive = 0 WHERE group_name = :gn AND isactive = 1"),
                    {"gn": gn},
                )

                # Re-insert as new active rows
                now = datetime.utcnow()
                for t in tiers:
                    await db.execute(
                        text(
                            'INSERT INTO challenges (name, type, targetvalue, timeperiod, isactive, '
                            'rewardtype, rewardamount, "InsertDate", group_name, audience_criteria, '
                            'valid_until, reward_multiplier, expires_on, symbol) '
                            "VALUES (:name, :type, :target, :period, 1, :rtype, :reward, :now, :gn, "
                            "CAST(:aud AS jsonb), :valid_until, :reward_mult, :expires_on, :symbol)"
                        ),
                        {
                            "name": t[0],
                            "type": t[1],
                            "target": t[2],
                            "rtype": t[3],
                            "reward": t[4],
                            "now": now,
                            "gn": gn,
                            "aud": json.dumps(t[5]) if t[5] else None,
                            "period": "daily",
                            "valid_until": t[6],
                            "reward_mult": t[7] or Decimal("1.00"),
                            "expires_on": t[8],
                            "symbol": t[9],
                        },
                    )

            # CLAUD-92: Break streaks where client missed yesterday (didn't trade on previous day)
            await db.execute(text(
                "UPDATE challenge_client_streaks SET current_streak = 0 "
                "WHERE last_trade_date < CURRENT_DATE - INTERVAL '1 day'"
            ))

            # Delete progress records older than 7 days
            await db.execute(
                text(
                    "DELETE FROM challenge_client_progress WHERE date < CURRENT_DATE - INTERVAL '7 days'"
                )
            )

            await db.commit()
            logger.info(
                "CLAUD-90: Daily challenge reset complete -- %d groups refreshed",
                len(active_groups),
            )
    except Exception as e:
        logger.error("CLAUD-90: Daily challenge reset failed: %s", e)


async def reset_weekly_challenges() -> None:
    """CLAUD-94: Reset weekly diversity challenges every Sunday at 22:00 UTC (midnight GMT+2)."""
    from app.pg_database import AsyncSessionLocal

    logger.info("CLAUD-94: Running weekly challenge reset (Sunday)")
    try:
        async with AsyncSessionLocal() as db:
            # Cancel current week's progress for weekly challenges
            await db.execute(text("""
                UPDATE challenge_client_progress SET status = 'Cancelled'
                WHERE status IN ('Open', 'In Progress')
                AND "challengeId" IN (
                    SELECT "challengeId" FROM challenges WHERE timeperiod = 'weekly' AND isactive = 1
                )
            """))

            # Get all active weekly challenge groups
            rows = await db.execute(
                text(
                    'SELECT DISTINCT group_name, type, audience_criteria '
                    "FROM challenges WHERE isactive = 1 AND timeperiod = 'weekly'"
                )
            )
            active_groups = rows.fetchall()

            for group_row in active_groups:
                gn = group_row[0]

                tier_rows = await db.execute(
                    text(
                        "SELECT name, type, targetvalue, rewardtype, rewardamount, "
                        "audience_criteria, valid_until, reward_multiplier, expires_on, symbol "
                        "FROM challenges WHERE group_name = :gn AND isactive = 1 "
                        "ORDER BY targetvalue"
                    ),
                    {"gn": gn},
                )
                tiers = tier_rows.fetchall()

                await db.execute(
                    text("UPDATE challenges SET isactive = 0 WHERE group_name = :gn AND isactive = 1"),
                    {"gn": gn},
                )

                now = datetime.utcnow()
                for t in tiers:
                    await db.execute(
                        text(
                            'INSERT INTO challenges (name, type, targetvalue, timeperiod, isactive, '
                            'rewardtype, rewardamount, "InsertDate", group_name, audience_criteria, '
                            'valid_until, reward_multiplier, expires_on, symbol) '
                            "VALUES (:name, :type, :target, 'weekly', 1, :rtype, :reward, :now, :gn, "
                            "CAST(:aud AS jsonb), :valid_until, :reward_mult, :expires_on, :symbol)"
                        ),
                        {
                            "name": t[0],
                            "type": t[1],
                            "target": t[2],
                            "rtype": t[3],
                            "reward": t[4],
                            "now": now,
                            "gn": gn,
                            "aud": json.dumps(t[5]) if t[5] else None,
                            "valid_until": t[6],
                            "reward_mult": t[7] or Decimal("1.00"),
                            "expires_on": t[8],
                            "symbol": t[9],
                        },
                    )

            # Clean up old instrument records (older than 4 weeks)
            await db.execute(text(
                "DELETE FROM challenge_client_instruments "
                "WHERE week_start < CURRENT_DATE - INTERVAL '28 days'"
            ))

            await db.commit()
            logger.info(
                "CLAUD-94: Weekly challenge reset complete -- %d groups refreshed",
                len(active_groups),
            )
    except Exception as e:
        logger.error("CLAUD-94: Weekly challenge reset failed: %s", e)


async def expire_flash_challenges() -> None:
    """CLAUD-95: Deactivate flash challenges that have passed their valid_until timestamp."""
    from app.pg_database import AsyncSessionLocal

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(text(
                "UPDATE challenges SET isactive = 0 "
                "WHERE valid_until IS NOT NULL AND valid_until < NOW() AND isactive = 1"
            ))
            if result.rowcount:
                logger.info("CLAUD-95: Deactivated %d expired flash challenge(s)", result.rowcount)
            await db.commit()
    except Exception as e:
        logger.error("CLAUD-95: Flash expiry job failed: %s", e)
