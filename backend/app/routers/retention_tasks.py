import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.retention_task import RetentionTask
from app.models.role import Role
from app.pg_database import get_db
from app.rbac import make_page_guard

router = APIRouter()

# CLAUD-124: Server-side page guard for retention-tasks page
_require_retention_tasks = make_page_guard("retention-tasks")
# CLAUD-176: Agents have 'retention' permission but not 'retention-tasks' — use
# weaker guard for read-only endpoints (task type list used by filter dropdown)
_require_retention = make_page_guard("retention")

# ---------------------------------------------------------------------------
# Column / operator maps
# ---------------------------------------------------------------------------
# CLAUD-72: Expanded to include ALL retention grid fields so every column
# visible in the Retention Manager is also available as a task criterion.
# ---------------------------------------------------------------------------

_TASK_COL_SQL: Dict[str, str] = {
    # --- Financial ---
    "balance":              "m.total_balance",
    "credit":               "m.total_credit",
    "equity":               "m.total_equity",
    "live_equity":          "(m.total_balance + m.total_credit)",
    "margin":               "(m.total_balance - m.total_equity)",
    "total_profit":         "m.total_profit",
    "total_deposit":        "m.total_deposit",
    "net_deposit":          "(SELECT aa.net_deposit FROM ant_acc aa WHERE aa.accountid = m.accountid) / 100.0",
    "total_withdrawal":     "(SELECT aa.total_withdrawal FROM ant_acc aa WHERE aa.accountid = m.accountid) / 100.0",
    "turnover":             "CASE WHEN (m.total_balance + m.total_credit) != 0 THEN m.max_volume / (m.total_balance + m.total_credit) ELSE 0 END",
    "exposure_usd":         "COALESCE(aec.exposure_usd, 0)",
    "exposure_pct":         "COALESCE(aec.exposure_pct, 0)",
    "open_pnl":             "(SELECT COALESCE(SUM(opc.pnl), 0) FROM open_pnl_cache opc JOIN vtiger_trading_accounts vta ON vta.login = opc.login WHERE vta.vtigeraccountid = m.accountid)",
    # --- Trading Activity ---
    "trade_count":          "m.trade_count",
    "max_open_trade":       "m.max_open_trade",
    "max_volume":           "m.max_volume",
    "max_volume_usd":       "m.max_volume",
    "open_volume":          "m.max_volume",
    "avg_trade_size":       "CASE WHEN COALESCE(m.trade_count, 0) > 0 THEN m.max_volume / m.trade_count ELSE 0 END",
    "win_rate":             "m.win_rate",
    "days_from_last_trade": "(CURRENT_DATE - m.last_trade_date::date)",
    "open_positions":       "(SELECT COUNT(*) FROM open_pnl_cache opc JOIN vtiger_trading_accounts vta ON vta.login = opc.login WHERE vta.vtigeraccountid = m.accountid AND opc.pnl IS NOT NULL)",
    "unique_symbols":       "(SELECT COUNT(DISTINCT t.symbol) FROM trades_mt4 t JOIN vtiger_trading_accounts vta ON vta.login = t.login WHERE vta.vtigeraccountid = m.accountid AND t.cmd IN (0, 1) AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN ('inactivity','zeroingusd','spread')))",
    # --- Engagement ---
    "days_in_retention":    "(CURRENT_DATE - m.client_qualification_date)",
    "deposit_count":        "m.deposit_count",
    "withdrawal_count":     "(SELECT COUNT(mtt.mttransactionsid) FROM vtiger_mttransactions mtt JOIN vtiger_trading_accounts vta ON vta.login = mtt.login WHERE vta.vtigeraccountid = m.accountid AND mtt.transactionapproval = 'Approved' AND mtt.transactiontype = 'Withdrawal')",
    "sales_potential":      "NULLIF(TRIM(m.sales_client_potential), '')::numeric",
    "days_since_last_communication": "EXTRACT(day FROM NOW() - (SELECT MAX(al.timestamp) FROM audit_log al WHERE al.client_account_id = m.accountid AND al.action_type IN ('status_change','note_added','call_initiated','whatsapp_opened')))::numeric",
    # --- Profile ---
    "age":                  "EXTRACT(year FROM AGE(m.birth_date))::numeric",
    "score":                "(SELECT cs.score FROM client_scores cs WHERE cs.accountid = m.accountid)",
    "card_type":            "(SELECT cct.card_type FROM client_card_type cct WHERE cct.accountid = m.accountid LIMIT 1)",
    "accountid":            "m.accountid",
    "full_name":            "m.full_name",
    "sales_client_potential": "m.sales_client_potential",
    "assigned_to":          "m.assigned_to",
    "agent_name":           "m.agent_name",
    "country":              "(SELECT aa.country_iso FROM ant_acc aa WHERE aa.accountid = m.accountid)",
    "desk":                 "(SELECT vu.department FROM vtiger_users vu WHERE vu.id::text = m.assigned_to LIMIT 1)",
    "is_favorite":          "CASE WHEN EXISTS (SELECT 1 FROM user_favorites uf WHERE uf.accountid = m.accountid) THEN 1 ELSE 0 END",
    "task_type":            "(SELECT string_agg(rt.name, ',') FROM client_task_assignments cta JOIN retention_tasks rt ON rt.id = cta.task_id WHERE cta.accountid = m.accountid)",
    # --- Date fields (numeric: days since) ---
    "ftd_date":             "(CURRENT_DATE - (SELECT aa.first_deposit_date::date FROM ant_acc aa WHERE aa.accountid = m.accountid))",
    "reg_date":             "(CURRENT_DATE - m.client_qualification_date)",
}

_TEXT_FIELDS = {
    "card_type", "accountid", "full_name", "sales_client_potential",
    "assigned_to", "agent_name", "country", "desk", "task_type",
}

_OP_MAP: Dict[str, str] = {
    "eq":       "=",
    "gt":       ">",
    "lt":       "<",
    "gte":      ">=",
    "lte":      "<=",
    "contains": "ILIKE",
}

_MV_ACTIVE = (
    "COALESCE(m.last_trade_date > CURRENT_DATE - make_interval(days => 35)"
    " OR m.last_deposit_time > CURRENT_DATE - make_interval(days => 35), false)"
)

_MV_ACTIVE_FTD = (
    f"(m.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_MV_ACTIVE})"
)


# ---------------------------------------------------------------------------
# WHERE-clause builder
# ---------------------------------------------------------------------------

def _build_task_where(
    conditions: List[Dict[str, Any]],
) -> Tuple[List[str], Dict[str, Any]]:
    where_list: List[str] = ["m.client_qualification_date IS NOT NULL"]
    params: Dict[str, Any] = {}

    for i, cond in enumerate(conditions):
        column = cond.get("column", "")
        op = cond.get("op", "eq")
        value = cond.get("value", "")

        # Skip conditions with no value (prevents SQL type errors in UNION ALL)
        if column not in ("active", "active_ftd") and str(value).strip() == "":
            continue

        if column == "active":
            if value == "true":
                where_list.append(f"({_MV_ACTIVE})")
            else:
                where_list.append(f"NOT ({_MV_ACTIVE})")
            continue

        if column == "active_ftd":
            if value == "true":
                where_list.append(f"({_MV_ACTIVE_FTD})")
            else:
                where_list.append(f"NOT ({_MV_ACTIVE_FTD})")
            continue

        sql_op = _OP_MAP.get(op, "=")

        if column == "days_from_last_trade":
            try:
                cast_value: Any = int(value)
            except (ValueError, TypeError):
                cast_value = value
            params[f"cond_{i}"] = cast_value
            where_list.append(
                f"m.last_trade_date IS NOT NULL"
                f" AND (CURRENT_DATE - m.last_trade_date::date) {sql_op} :cond_{i}"
            )
            continue

        sql_expr = _TASK_COL_SQL.get(column)
        if sql_expr is None:
            continue

        if op == "contains":
            cast_value = f"%{value}%"
        elif column in _TEXT_FIELDS:
            cast_value = value
        else:
            try:
                cast_value = float(value)
            except (ValueError, TypeError):
                cast_value = value

        params[f"cond_{i}"] = cast_value
        where_list.append(f"{sql_expr} {sql_op} :cond_{i}")

    return where_list, params


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ConditionIn(BaseModel):
    column: str
    op: str
    value: str


VALID_COLORS = {"red", "orange", "yellow", "green", "blue", "purple", "pink", "grey"}


class TaskCreate(BaseModel):
    name: str
    conditions: List[ConditionIn]
    color: Optional[str] = "grey"


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    conditions: Optional[List[ConditionIn]] = None
    color: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _trigger_task_assignments() -> None:
    """Fire-and-forget: recompute client_task_assignments after a task change."""
    from app.routers.etl import rebuild_task_assignments
    await rebuild_task_assignments()


def _task_out(task: RetentionTask) -> Dict[str, Any]:
    return {
        "id": task.id,
        "name": task.name,
        "conditions": json.loads(task.conditions),
        "color": task.color or "grey",
        "created_at": task.created_at.isoformat() if task.created_at else None,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/retention/tasks")
async def list_tasks(
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
    _g: Any = Depends(_require_retention),  # CLAUD-176: agents need this for filter dropdown
) -> List[Dict[str, Any]]:
    result = await db.execute(
        select(RetentionTask).order_by(RetentionTask.created_at)
    )
    tasks = result.scalars().all()
    return [_task_out(t) for t in tasks]


@router.post("/retention/tasks", status_code=201)
async def create_task(
    body: TaskCreate,
    db: AsyncSession = Depends(get_db),
    current_user: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    # CLAUD-108: Only users whose role includes 'retention-tasks' page permission may create tasks.
    role_obj = await db.get(Role, current_user.role)
    role_permissions: List[str] = (role_obj.permissions or []) if role_obj else []
    if "retention-tasks" not in role_permissions:
        raise HTTPException(status_code=403, detail="Your role does not have permission to manage retention tasks")
    color = (body.color or "grey").lower()
    if color not in VALID_COLORS:
        color = "grey"
    task = RetentionTask(
        name=body.name,
        conditions=json.dumps([c.model_dump() for c in body.conditions]),
        color=color,
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    asyncio.create_task(_trigger_task_assignments())
    return _task_out(task)


@router.put("/retention/tasks/{task_id}")
async def update_task(
    task_id: int,
    body: TaskUpdate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if body.name is not None:
        task.name = body.name
    if body.conditions is not None:
        task.conditions = json.dumps([c.model_dump() for c in body.conditions])
    if body.color is not None:
        color = body.color.lower()
        if color in VALID_COLORS:
            task.color = color
    await db.commit()
    await db.refresh(task)
    asyncio.create_task(_trigger_task_assignments())
    return _task_out(task)


@router.delete("/retention/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> None:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await db.delete(task)
    await db.commit()
    asyncio.create_task(_trigger_task_assignments())


@router.get("/retention/clients/{accountid}/tasks")
async def get_client_tasks(
    accountid: str,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Return all retention tasks currently assigned to a given client.

    Uses the pre-computed client_task_assignments lookup table.
    Returns: id, task_type (name), color, due_date (null), status ('active'), note (null).
    """
    result = await db.execute(
        text(
            "SELECT rt.id, rt.name, rt.color"
            " FROM client_task_assignments cta"
            " JOIN retention_tasks rt ON rt.id = cta.task_id"
            " WHERE cta.accountid = :accountid"
            " ORDER BY rt.name"
        ),
        {"accountid": accountid},
    )
    rows = result.fetchall()
    return [
        {
            "id": row.id,
            "task_type": row.name,
            "color": row.color or "grey",
            "due_date": None,
            "status": "active",
            "note": None,
        }
        for row in rows
    ]


@router.get("/retention/tasks/{task_id}/clients")
async def get_task_clients(
    task_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    task = await db.get(RetentionTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        conditions: List[Dict[str, Any]] = json.loads(task.conditions)
        where_list, params = _build_task_where(conditions)
        where_clause = " AND ".join(where_list)

        # Count
        count_sql = text(f"SELECT COUNT(*) FROM retention_mv m LEFT JOIN account_exposure_cache aec ON aec.accountid = m.accountid WHERE {where_clause}")
        count_result = await db.execute(count_sql, params)
        total: int = count_result.scalar() or 0

        # Paginated rows
        offset = (page - 1) * page_size
        data_params = {**params, "limit": page_size, "offset": offset}
        data_sql = text(
            f"SELECT"
            f"  m.accountid,"
            f"  m.total_balance    AS balance,"
            f"  m.total_credit     AS credit,"
            f"  m.total_equity     AS equity,"
            f"  m.trade_count,"
            f"  m.total_profit,"
            f"  m.last_trade_date,"
            f"  m.assigned_to,"
            f"  COALESCE("
            f"    m.last_trade_date > CURRENT_DATE - make_interval(days => 35)"
            f"    OR m.last_deposit_time > CURRENT_DATE - make_interval(days => 35),"
            f"    false"
            f"  ) AS active"
            f" FROM retention_mv m"
            f" LEFT JOIN account_exposure_cache aec ON aec.accountid = m.accountid"
            f" WHERE {where_clause}"
            f" ORDER BY m.accountid"
            f" LIMIT :limit OFFSET :offset"
        )
        data_result = await db.execute(data_sql, data_params)
        rows = data_result.fetchall()

        # Collect assigned_to IDs to resolve agent names
        agent_ids = list({r.assigned_to for r in rows if r.assigned_to})
        agent_map: Dict[str, str] = {}
        if agent_ids:
            users_result = await db.execute(
                text(
                    "SELECT id, TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) AS full_name"
                    " FROM vtiger_users WHERE id = ANY(:ids)"
                ),
                {"ids": agent_ids},
            )
            for ur in users_result.fetchall():
                agent_map[str(ur.id)] = ur.full_name or ur.id

        clients = []
        for r in rows:
            clients.append(
                {
                    "accountid": r.accountid,
                    "balance": r.balance,
                    "credit": r.credit,
                    "equity": r.equity,
                    "trade_count": r.trade_count,
                    "total_profit": r.total_profit,
                    "last_trade_date": (
                        r.last_trade_date.isoformat() if r.last_trade_date else None
                    ),
                    "active": bool(r.active),
                    "agent_name": agent_map.get(str(r.assigned_to)) if r.assigned_to else None,
                }
            )

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": clients,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Query failed: {exc}") from exc
