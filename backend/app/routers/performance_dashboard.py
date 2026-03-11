"""CLAUD-75: Performance Dashboard API — Retention Portfolio Scoped.

Provides a summary endpoint with agent targets, KPIs (deposits, withdrawals,
unique depositors, open volume, exposure, unique traders), run-rate
projections, and portfolio overview (equity, scores, status breakdown, tasks).

ALL metrics are scoped to the retention portfolio (retention_mv) only.
Even admin / retention_manager see only clients present in retention_mv.

Permission scoping (within retention portfolio):
  - admin / retention_manager: see all retention portfolio data
  - team_leader: see data for agents in their department
  - agent: see only their own assigned clients
"""

import hashlib
import logging
import time
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user, get_jwt_payload, JWTPayload
from app.models.user import User
from app.pg_database import get_db
from app.rbac import make_page_guard
from app.routers.retention import _resolve_status_name

logger = logging.getLogger(__name__)

router = APIRouter()

# CLAUD-124: Server-side page guard
_require_perf_dashboard = make_page_guard("performance-dashboard")

# ---------------------------------------------------------------------------
# CLAUD-36: Server-side result cache for /performance-dashboard/summary.
# The dashboard auto-refreshes every 60 seconds. With 70 agents that is
# 70 requests/min each firing 8 sequential queries = 560 DB round-trips/min.
# Cache results for 30 seconds keyed by scope (role + vtiger_id/dept).
# Admin/retention_manager share one cache key; each agent gets their own key.
# ---------------------------------------------------------------------------
_PERF_CACHE: dict[str, tuple[dict, float]] = {}
_PERF_CACHE_TTL = 30  # seconds
_PERF_CACHE_MAX = 200  # max entries before eviction pass


def _perf_cache_key(jwt_payload: JWTPayload) -> str:
    """Build a cache key that captures the full data scope of the request."""
    raw = f"{jwt_payload.role}|{jwt_payload.vtiger_user_id}|{jwt_payload.vtiger_department}|{jwt_payload.team}|{jwt_payload.app_department}"
    return hashlib.md5(raw.encode()).hexdigest()  # noqa: S324


# ---------------------------------------------------------------------------
# Approved status names for the status breakdown chart.
# ---------------------------------------------------------------------------

APPROVED_STATUSES = {
    "New", "Interested", "Potential", "Active", "Not Interested",
    "DNC", "Appointment", "Depositor", "FTD", "Low Potential", "Inactive",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _safe_query(
    db: AsyncSession,
    sql: str,
    label: str,
    params: dict | None = None,
    *,
    fetch_all: bool = False,
):
    """Execute a query with rollback-on-error so one bad query
    does not poison the transaction for subsequent queries."""
    try:
        result = await db.execute(text(sql), params or {})
        return result.fetchall() if fetch_all else result.fetchone()
    except Exception as exc:
        logger.warning("PerfDashboard query [%s] failed: %s", label, exc)
        try:
            await db.rollback()
        except Exception:
            pass
        return [] if fetch_all else None


def _build_scope_filter(
    jwt_payload: JWTPayload,
) -> tuple[str, dict]:
    """Return a SQL WHERE fragment scoped to the user's role.

    All queries JOIN through retention_mv (alias 'm') so every metric
    is automatically limited to the retention portfolio.
    """
    if jwt_payload.role in ("admin", "crm_manager", "retention_manager"):
        return "", {}
    elif jwt_payload.role == "team_leader":
        if jwt_payload.team:
            # CLAUD-180: team-based scoping
            return (
                "AND m.assigned_to IN ("
                "  SELECT vu.id FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.team = :rbac_team AND u.role = 'agent'"
                ")",
                {"rbac_team": jwt_payload.team},
            )
        elif jwt_payload.vtiger_department:
            return (
                "AND m.assigned_to IN (SELECT id FROM vtiger_users WHERE department = :rbac_dept)",
                {"rbac_dept": jwt_payload.vtiger_department},
            )
        elif jwt_payload.app_department:
            # CLAUD-180 fix: vtiger_department empty — use users.department fallback
            return (
                "AND m.assigned_to IN ("
                "  SELECT vu.id FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.department = :rbac_app_dept AND u.role = 'agent'"
                ")",
                {"rbac_app_dept": jwt_payload.app_department},
            )
        return "AND 1=0", {}
    elif jwt_payload.role == "agent" and jwt_payload.vtiger_user_id is not None:
        return (
            "AND m.assigned_to = :rbac_vtiger_uid",
            {"rbac_vtiger_uid": str(jwt_payload.vtiger_user_id)},
        )
    # Fallback: block all data
    return "AND 1=0", {}


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.get("/performance-dashboard/summary")
async def get_performance_summary(
    jwt_payload: JWTPayload = Depends(_require_perf_dashboard),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Return performance dashboard data: targets, actuals, run rate, portfolio.

    CLAUD-36: Results are cached for 30s per scope to prevent 70 agents
    auto-refreshing every 60s from generating 560 DB queries/min.
    """
    # --- Cache check ---
    now = time.monotonic()
    cache_key = _perf_cache_key(jwt_payload)
    cached = _PERF_CACHE.get(cache_key)
    if cached and cached[1] > now:
        return cached[0]

    mv_scope, mv_params = _build_scope_filter(jwt_payload)

    # ===================================================================
    # 1. TARGET — sum of agent_targets for current month
    # ===================================================================
    # For agents, filter by their vtiger_user_id; for managers/admin, sum all.
    target_sql = """
        SELECT COALESCE(SUM(net), 0)
        FROM agent_targets
        WHERE month_date >= DATE_TRUNC('month', CURRENT_DATE)::date
          AND month_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::date
    """
    target_params: dict = {}
    if jwt_payload.role == "agent" and jwt_payload.vtiger_user_id is not None:
        target_sql += " AND agent_id = :target_agent_id"
        target_params["target_agent_id"] = jwt_payload.vtiger_user_id
    elif jwt_payload.role == "team_leader":
        if jwt_payload.team:
            # CLAUD-180: scope targets to team agents
            target_sql += (
                " AND agent_email IN ("
                "  SELECT vu.email FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.team = :target_team AND u.role = 'agent'"
                ")"
            )
            target_params["target_team"] = jwt_payload.team
        elif jwt_payload.vtiger_department:
            target_sql += (
                " AND agent_email IN ("
                "  SELECT email FROM vtiger_users WHERE department = :target_dept"
                ")"
            )
            target_params["target_dept"] = jwt_payload.vtiger_department
        elif jwt_payload.app_department:
            # CLAUD-180 fix: vtiger_department empty — use users.department fallback
            target_sql += (
                " AND agent_email IN ("
                "  SELECT vu.email FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.department = :target_app_dept AND u.role = 'agent'"
                ")"
            )
            target_params["target_app_dept"] = jwt_payload.app_department

    row = await _safe_query(db, target_sql, "target_net", target_params)
    target_net = round(float(row[0]), 2) if row and row[0] else 0.0

    # ===================================================================
    # 2. ACTUALS -- deposits, withdrawals, unique depositors (current month)
    #    Scoped to retention portfolio via JOIN retention_mv
    # ===================================================================
    deposits_sql = f"""
        SELECT COALESCE(SUM(CASE WHEN mtt.transactiontype = 'Deposit' THEN mtt.usdamount ELSE 0 END), 0) AS deposits,
               COALESCE(SUM(CASE WHEN mtt.transactiontype IN ('Withdrawal', 'Withdraw') THEN mtt.usdamount ELSE 0 END), 0) AS withdrawals,
               COUNT(DISTINCT CASE WHEN mtt.transactiontype = 'Deposit' THEN mtt.login END) AS unique_depositors
        FROM vtiger_mttransactions mtt
        JOIN vtiger_trading_accounts vta ON vta.login = mtt.login
        JOIN retention_mv m ON m.accountid = vta.vtigeraccountid
        WHERE mtt.transactionapproval = 'Approved'
          AND mtt.confirmation_time >= DATE_TRUNC('month', CURRENT_DATE)
          AND (mtt.payment_method IS NULL OR mtt.payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
          {mv_scope}
    """
    row = await _safe_query(db, deposits_sql, "actuals_deposits", mv_params)
    deposits = round(float(row[0]), 2) if row and row[0] else 0.0
    withdrawals = round(float(row[1]), 2) if row and row[1] else 0.0
    unique_depositors = int(row[2]) if row and row[2] else 0
    net_deposit = round(deposits - withdrawals, 2)

    # ===================================================================
    # 3. ACTUALS -- open volume, exposure, unique traders (current month)
    #    Scoped to retention portfolio via JOIN retention_mv
    # ===================================================================
    open_volume_sql = f"""
        SELECT COALESCE(SUM(t.notional_value), 0),
               COUNT(DISTINCT t.login)
        FROM trades_mt4 t
        JOIN vtiger_trading_accounts vta ON vta.login = t.login
        JOIN retention_mv m ON m.accountid = vta.vtigeraccountid
        WHERE t.open_time >= DATE_TRUNC('month', CURRENT_DATE)
          AND t.cmd IN (0, 1)
          {mv_scope}
    """
    row = await _safe_query(db, open_volume_sql, "open_volume", mv_params)
    open_volume = round(float(row[0]), 2) if row and row[0] else 0.0
    unique_traders = int(row[1]) if row and row[1] else 0

    # CLAUD-77: Total exposure from pre-computed account_exposure_cache
    exposure_sql = f"""
        SELECT COALESCE(SUM(aec.exposure_usd), 0)
        FROM account_exposure_cache aec
        JOIN retention_mv m ON m.accountid = aec.accountid
        WHERE 1=1 {mv_scope}
    """
    row = await _safe_query(db, exposure_sql, "total_exposure", mv_params)
    total_exposure = round(float(row[0]), 2) if row and row[0] else 0.0

    # ===================================================================
    # 4. RUN RATE projection
    # ===================================================================
    run_rate_sql = """
        SELECT EXTRACT(day FROM CURRENT_DATE - DATE_TRUNC('month', CURRENT_DATE)) + 1 AS days_elapsed,
               EXTRACT(day FROM DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day') AS days_in_month
    """
    row = await _safe_query(db, run_rate_sql, "run_rate")
    days_elapsed = int(row[0]) if row and row[0] else 1
    days_in_month = int(row[1]) if row and row[1] else 30

    run_rate_net = round(net_deposit / max(days_elapsed, 1) * days_in_month, 2) if days_elapsed > 0 else 0.0

    # ===================================================================
    # 5. PORTFOLIO — equity, exposure, avg score, distributions
    # ===================================================================
    # Total live equity from retention_mv
    equity_sql = f"""
        SELECT COALESCE(SUM(ABS(m.total_balance + m.total_credit)), 0)
        FROM retention_mv m
        WHERE 1=1 {mv_scope}
    """
    row = await _safe_query(db, equity_sql, "total_live_equity", mv_params)
    total_live_equity = round(float(row[0]), 2) if row and row[0] else 0.0

    # Average score from client_scores joined with retention_mv scope
    avg_score_sql = f"""
        SELECT ROUND(AVG(cs.score)::numeric, 1)
        FROM client_scores cs
        JOIN retention_mv m ON m.accountid = cs.accountid
        WHERE 1=1 {mv_scope}
    """
    row = await _safe_query(db, avg_score_sql, "avg_score", mv_params)
    avg_score = float(row[0]) if row and row[0] else 0.0

    # Score distribution in buckets (0-25, 26-50, 51-75, 76-100)
    score_dist_sql = f"""
        SELECT
            COUNT(*) FILTER (WHERE cs.score BETWEEN 0 AND 25) AS b_0_25,
            COUNT(*) FILTER (WHERE cs.score BETWEEN 26 AND 50) AS b_26_50,
            COUNT(*) FILTER (WHERE cs.score BETWEEN 51 AND 75) AS b_51_75,
            COUNT(*) FILTER (WHERE cs.score BETWEEN 76 AND 100) AS b_76_100
        FROM client_scores cs
        JOIN retention_mv m ON m.accountid = cs.accountid
        WHERE 1=1 {mv_scope}
    """
    row = await _safe_query(db, score_dist_sql, "score_dist", mv_params)
    score_distribution = {
        "0-25": int(row[0]) if row and row[0] else 0,
        "26-50": int(row[1]) if row and row[1] else 0,
        "51-75": int(row[2]) if row and row[2] else 0,
        "76-100": int(row[3]) if row and row[3] else 0,
    } if row else {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}

    # Retention status breakdown (sales_client_potential)
    # Resolve numeric IDs to names, then filter to approved statuses only
    status_sql = f"""
        SELECT m.sales_client_potential AS status, COUNT(*) AS cnt
        FROM retention_mv m
        WHERE m.sales_client_potential IS NOT NULL
          AND TRIM(m.sales_client_potential) != ''
          {mv_scope}
        GROUP BY m.sales_client_potential
        ORDER BY cnt DESC
    """
    rows = await _safe_query(db, status_sql, "status_breakdown", mv_params, fetch_all=True)
    status_breakdown: Dict[str, int] = {}
    for r in (rows or []):
        raw = str(r[0]).strip()
        name = _resolve_status_name(raw) or raw
        if name not in APPROVED_STATUSES:
            continue
        # Multiple raw IDs may resolve to the same name -- accumulate
        status_breakdown[name] = status_breakdown.get(name, 0) + int(r[1])

    # Task summary: count of clients per retention task
    task_sql = f"""
        SELECT rt.name, COUNT(DISTINCT cta.accountid) AS cnt
        FROM client_task_assignments cta
        JOIN retention_tasks rt ON rt.id = cta.task_id
        JOIN retention_mv m ON m.accountid = cta.accountid
        WHERE 1=1 {mv_scope}
        GROUP BY rt.name
        ORDER BY cnt DESC
    """
    rows = await _safe_query(db, task_sql, "task_summary", mv_params, fetch_all=True)
    task_summary: Dict[str, int] = {}
    for r in (rows or []):
        task_summary[str(r[0])] = int(r[1])

    # ===================================================================
    # CLAUD-107: Scope metadata for frontend role-aware rendering
    # ===================================================================
    if jwt_payload.role in ("admin", "crm_manager", "retention_manager"):
        scope_type = "all"
    elif jwt_payload.role == "team_leader":
        scope_type = "team"
    else:
        scope_type = "own"

    # ===================================================================
    # ASSEMBLE RESPONSE
    # ===================================================================
    result = {
        "scope": {
            "role": jwt_payload.role,
            "scope_type": scope_type,
            "username": current_user.username,
        },
        "target": {
            "net": target_net,
        },
        "actuals": {
            "net_deposit": net_deposit,
            "deposits": deposits,
            "withdrawals": withdrawals,
            "unique_depositors": unique_depositors,
            "open_volume": open_volume,
            "total_exposure": total_exposure,
            "unique_traders": unique_traders,
        },
        "run_rate": {
            "net_deposit": run_rate_net,
            "days_elapsed": days_elapsed,
            "days_in_month": days_in_month,
        },
        "portfolio": {
            "total_live_equity": total_live_equity,
            "total_exposure_usd": total_exposure,
            "avg_score": avg_score,
            "score_distribution": score_distribution,
            "status_breakdown": status_breakdown,
            "task_summary": task_summary,
        },
    }

    # Store in cache; evict expired entries if cache is large
    _PERF_CACHE[cache_key] = (result, time.monotonic() + _PERF_CACHE_TTL)
    if len(_PERF_CACHE) > _PERF_CACHE_MAX:
        _expired = [k for k, v in _PERF_CACHE.items() if v[1] <= time.monotonic()]
        for k in _expired:
            del _PERF_CACHE[k]

    return result
