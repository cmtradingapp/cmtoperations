"""CLAUD-156: Agent Activity endpoint — per-agent daily KPI summary for managers."""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_jwt_payload, JWTPayload
from app.pg_database import get_db
from app.rbac import make_page_guard

logger = logging.getLogger(__name__)

router = APIRouter()

_require_agent_activity = make_page_guard("agent_activity")


def _fmt_duration(secs: int) -> str:
    h = secs // 3600
    m = (secs % 3600) // 60
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m"


@router.get("/agent-activity")
async def get_agent_activity(
    jwt_payload: JWTPayload = Depends(_require_agent_activity),
    db: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Return today's activity summary for all agents visible to the caller.

    Role scoping (server-enforced):
      admin / retention_manager → all active non-admin users
      team_leader               → users in same department
      agent                     → self only
    """
    today = date.today()

    # ------------------------------------------------------------------
    # 1. Fetch visible users
    # ------------------------------------------------------------------
    if jwt_payload.role in ("admin", "retention_manager"):
        scope_where = "AND u.role != 'admin'"
        scope_params: dict = {}
    elif jwt_payload.role == "team_leader":
        if jwt_payload.team:
            # CLAUD-180: scope to agents in same team
            scope_where = "AND u.team = :team AND u.role = 'agent'"
            scope_params = {"team": jwt_payload.team}
        elif jwt_payload.vtiger_department:
            scope_where = "AND vu.department = :dept AND u.role != 'admin'"
            scope_params = {"dept": jwt_payload.vtiger_department}
        elif jwt_payload.app_department:
            # CLAUD-180 fix: vtiger_department empty — use users.department fallback
            scope_where = "AND u.department = :app_dept AND u.role = 'agent'"
            scope_params = {"app_dept": jwt_payload.app_department}
        else:
            return []
    elif jwt_payload.role == "agent" and jwt_payload.vtiger_user_id is not None:
        scope_where = "AND vu.id = :vtiger_id"
        scope_params = {"vtiger_id": str(jwt_payload.vtiger_user_id)}
    else:
        return []

    users_sql = text(f"""
        SELECT u.id,
               COALESCE(NULLIF(TRIM(vu.first_name || ' ' || vu.last_name), ''), u.username) AS full_name,
               u.email,
               COALESCE(vu.department, '') AS department,
               COALESCE(vu.id, '') AS vtiger_id
        FROM users u
        LEFT JOIN vtiger_users vu ON LOWER(vu.email) = LOWER(u.email)
        WHERE u.is_active = true
          {scope_where}
        ORDER BY full_name
    """)
    user_rows = (await db.execute(users_sql, scope_params)).fetchall()
    if not user_rows:
        return []

    user_ids = [str(r[0]) for r in user_rows]
    vtiger_ids = [r[4] for r in user_rows if r[4]]

    # ------------------------------------------------------------------
    # 2. Bulk-fetch today's performance from cache
    # ------------------------------------------------------------------
    try:
        kpi_rows = (await db.execute(
            text("""
                SELECT agent_id,
                       net_deposit, depositors, traders, volume,
                       contacted, calls_made, talk_time_secs, target,
                       callbacks_set, run_rate, contact_rate, avg_call_secs,
                       computed_at
                FROM agent_performance_cache
                WHERE agent_id = ANY(:ids)
                  AND period = 'daily'
                  AND period_date = :today
            """),
            {"ids": user_ids, "today": today},
        )).fetchall()
    except Exception as exc:
        logger.warning("agent-activity: performance cache query failed: %s", exc)
        kpi_rows = []

    kpi_by_agent: dict = {r[0]: r for r in kpi_rows}

    # ------------------------------------------------------------------
    # 3. Bulk-fetch active portfolio client counts per vtiger_id
    # ------------------------------------------------------------------
    portfolio_by_vtiger: dict = {}
    if vtiger_ids:
        try:
            port_rows = (await db.execute(
                text("""
                    SELECT assigned_to, COUNT(*) AS cnt
                    FROM retention_mv
                    WHERE assigned_to = ANY(:ids)
                      AND client_qualification_date IS NOT NULL
                    GROUP BY assigned_to
                """),
                {"ids": vtiger_ids},
            )).fetchall()
            for pr in port_rows:
                portfolio_by_vtiger[pr[0]] = int(pr[1])
        except Exception as exc:
            logger.warning("agent-activity: portfolio count query failed: %s", exc)

    # ------------------------------------------------------------------
    # 4. Bulk-fetch pending task counts per vtiger_id
    # ------------------------------------------------------------------
    tasks_by_vtiger: dict = {}
    if vtiger_ids:
        try:
            task_rows = (await db.execute(
                text("""
                    SELECT assigned_to, task_type, COUNT(*) AS cnt
                    FROM retention_tasks
                    WHERE assigned_to = ANY(:ids)
                      AND status != 'done'
                    GROUP BY assigned_to, task_type
                """),
                {"ids": vtiger_ids},
            )).fetchall()
            for tr in task_rows:
                bucket = tasks_by_vtiger.setdefault(tr[0], {})
                bucket[tr[1]] = int(tr[2])
        except Exception as exc:
            logger.warning("agent-activity: tasks query failed: %s", exc)

    # ------------------------------------------------------------------
    # 5. Assemble response
    # ------------------------------------------------------------------
    result = []
    now_utc = datetime.now(timezone.utc)

    for uid, full_name, email, department, vtiger_id in user_rows:
        kpi = kpi_by_agent.get(str(uid))
        tasks = tasks_by_vtiger.get(vtiger_id, {})
        portfolio_clients = portfolio_by_vtiger.get(vtiger_id, 0)

        # Derive online status from computed_at freshness
        status = "offline"
        last_seen = None
        shift_elapsed = None

        if kpi:
            computed_at = kpi[13]
            if computed_at:
                if not computed_at.tzinfo:
                    computed_at = computed_at.replace(tzinfo=timezone.utc)
                age_secs = (now_utc - computed_at).total_seconds()
                if age_secs < 1800:  # <30 min → on_call
                    status = "on_call"
                    last_seen = "Active now"
                else:
                    status = "available"
                    mins = int(age_secs / 60)
                    last_seen = f"{mins}m ago"

                # Shift elapsed = talk_time_secs as a proxy (total call time today)
                talk_secs = int(kpi[7] or 0)
                if talk_secs > 0:
                    shift_elapsed = _fmt_duration(talk_secs)

        # Build initials
        parts = full_name.split()
        if len(parts) >= 2:
            initials = (parts[0][0] + parts[-1][0]).upper()
        elif parts:
            initials = parts[0][:2].upper()
        else:
            initials = "?"

        result.append({
            "id": uid,
            "name": full_name,
            "initials": initials,
            "team": department,
            "status": status,
            "lastSeen": last_seen,
            "shiftElapsed": shift_elapsed,
            "kpi": {
                "contacted": int(kpi[5] or 0) if kpi else 0,
                "traders": int(kpi[3] or 0) if kpi else 0,
                "depositors": int(kpi[2] or 0) if kpi else 0,
                "netDeposit": float(kpi[1] or 0) if kpi else 0,
                "volume": float(kpi[4] or 0) if kpi else 0,
                "callsMade": int(kpi[6] or 0) if kpi else 0,
                "talkTimeSecs": int(kpi[7] or 0) if kpi else 0,
                "target": int(kpi[8]) if kpi and kpi[8] is not None else None,
                "callbacksSet": int(kpi[9] or 0) if kpi else 0,
                "runRate": float(kpi[10]) if kpi and kpi[10] is not None else None,
                "contactRate": float(kpi[11]) if kpi and kpi[11] is not None else None,
                "avgCallSecs": int(kpi[12] or 0) if kpi else 0,
            },
            "tasks": tasks,
            "portfolioClients": portfolio_clients,
        })

    # CLAUD-160: exclude agents with no online status and no activity today
    result = [
        a for a in result
        if a["status"] != "offline"
        or a["kpi"]["callsMade"] > 0
        or a["kpi"]["contacted"] > 0
        or a["kpi"]["netDeposit"] > 0
        or sum(a["tasks"].values()) > 0
    ]

    # Offline agents sorted to bottom
    STATUS_ORDER = {"on_call": 0, "available": 1, "offline": 2}
    result.sort(key=lambda a: (STATUS_ORDER.get(a["status"], 3), a["name"]))

    return result
