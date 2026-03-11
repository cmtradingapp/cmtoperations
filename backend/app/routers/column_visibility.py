"""CLAUD-85: Per-role column visibility for the Retention Grid.

Endpoints:
- GET  /retention/column-visibility          — caller's role visibility
- GET  /admin/column-visibility/{role_name}  — admin: any role
- PUT  /admin/column-visibility/{role_name}  — admin: upsert columns
- POST /admin/column-visibility/{role_name}/reset — admin: reset to defaults
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_jwt_payload, require_admin, JWTPayload
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

# Default visibility matrix — column_key: {role: visible}
DEFAULT_VISIBILITY: dict[str, dict[str, bool]] = {
    # ── IDENTITY ────────────────────────────────────────────────────────────
    "client_name":           {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "client_id":             {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "score":                 {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "age":                   {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "client_potential":      {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},   # CLAUD-116
    "client_segment":        {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},   # CLAUD-116
    "sales_client_potential": {"admin": True, "retention_manager": True,  "team_leader": True,  "agent": True},   # CLAUD-138
    # ── FINANCIALS ──────────────────────────────────────────────────────────
    "balance":               {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "equity":                {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "live_equity":           {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "credit":                {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "exposure_usd":          {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "exposure_pct":          {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "open_pnl":              {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "closed_pnl":            {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},  # CLAUD-121
    "net_deposit_ever":      {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},  # CLAUD-121
    "total_deposit":         {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "deposit_count":         {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    # ── TRADING ─────────────────────────────────────────────────────────────
    "trade_count":           {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "turnover":              {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "max_volume":            {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "max_open_trade":        {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "avg_trade_size":        {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "last_trade_date":       {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "days_in_retention":     {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "days_from_last_trade":  {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    # ── ACTIVITY ────────────────────────────────────────────────────────────
    "retention_status":      {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "last_contact":          {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "assigned_to":           {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "task_type":             {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
    "registration_date":     {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "card_type":             {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},
    "last_deposit_date":     {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},   # CLAUD-121
    "last_withdrawal_date":  {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": False},  # CLAUD-121
    "favorites":             {"admin": True,  "retention_manager": True,  "team_leader": True,  "agent": True},
}

ALL_ROLES = ["admin", "retention_manager", "team_leader", "agent"]


class ColumnVisibilityUpdate(BaseModel):
    columns: dict[str, bool]


# ── Self-service: caller's own visibility ─────────────────────────────────

@router.get("/retention/column-visibility")
async def get_my_column_visibility(
    jwt: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
):
    """Return column visibility config for the caller's role."""
    role = jwt.role
    rows = await db.execute(
        text("SELECT column_key, is_visible FROM role_column_visibility WHERE role_name = :role"),
        {"role": role},
    )
    columns = {r[0]: r[1] for r in rows.fetchall()}

    # If no rows found (role not in DB), fall back to defaults
    if not columns:
        for col_key, roles in DEFAULT_VISIBILITY.items():
            columns[col_key] = roles.get(role, True)

    return {"role": role, "columns": columns}


# ── Admin endpoints ───────────────────────────────────────────────────────

@router.get("/admin/column-visibility/{role_name}")
async def admin_get_column_visibility(
    role_name: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Admin: get column visibility for a specific role."""
    rows = await db.execute(
        text("SELECT column_key, is_visible FROM role_column_visibility WHERE role_name = :role"),
        {"role": role_name},
    )
    columns = {r[0]: r[1] for r in rows.fetchall()}

    # Fall back to defaults if nothing stored
    if not columns:
        for col_key, roles in DEFAULT_VISIBILITY.items():
            columns[col_key] = roles.get(role_name, True)

    return {"role_name": role_name, "columns": columns}


@router.put("/admin/column-visibility/{role_name}")
async def admin_update_column_visibility(
    role_name: str,
    body: ColumnVisibilityUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Admin: upsert column visibility for a specific role."""
    now = datetime.utcnow()
    for col_key, is_visible in body.columns.items():
        await db.execute(
            text(
                "INSERT INTO role_column_visibility (role_name, column_key, is_visible, updated_at) "
                "VALUES (:role, :col, :vis, :now) "
                "ON CONFLICT (role_name, column_key) "
                "DO UPDATE SET is_visible = :vis, updated_at = :now"
            ),
            {"role": role_name, "col": col_key, "vis": is_visible, "now": now},
        )
    await db.commit()
    logger.info("CLAUD-85: Updated column visibility for role '%s': %s", role_name, body.columns)
    return {"status": "ok", "role_name": role_name, "columns": body.columns}


@router.post("/admin/column-visibility/{role_name}/reset")
async def admin_reset_column_visibility(
    role_name: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Admin: reset column visibility to defaults for a specific role."""
    # Delete existing rows for this role
    await db.execute(
        text("DELETE FROM role_column_visibility WHERE role_name = :role"),
        {"role": role_name},
    )
    # Re-insert defaults
    now = datetime.utcnow()
    for col_key, roles in DEFAULT_VISIBILITY.items():
        is_visible = roles.get(role_name, True)
        await db.execute(
            text(
                "INSERT INTO role_column_visibility (role_name, column_key, is_visible, updated_at) "
                "VALUES (:role, :col, :vis, :now)"
            ),
            {"role": role_name, "col": col_key, "vis": is_visible, "now": now},
        )
    await db.commit()
    logger.info("CLAUD-85: Reset column visibility to defaults for role '%s'", role_name)

    # Return the reset config
    defaults = {col_key: roles.get(role_name, True) for col_key, roles in DEFAULT_VISIBILITY.items()}
    return {"status": "ok", "role_name": role_name, "columns": defaults}
