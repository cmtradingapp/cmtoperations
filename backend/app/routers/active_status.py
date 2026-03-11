"""CLAUD-86: Active Client Auto-Classification — API endpoints.

Provides:
  GET  /retention/active-count                       — cached active client count
  POST /retention/clients/{accountid}/active-override — manual override (admin/agent)
"""

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.user import User
from app.pg_database import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# GET /retention/active-count
# ---------------------------------------------------------------------------

@router.get("/retention/active-count")
async def get_active_count(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
) -> dict:
    """Return the cached active client count from system_metrics."""
    row = (await db.execute(
        text("SELECT value, computed_at FROM system_metrics WHERE key = 'active_client_count'")
    )).fetchone()

    if row is None:
        return {"count": 0, "computed_at": None}

    return {
        "count": int(row[0]) if row[0] is not None else 0,
        "computed_at": row[1].isoformat() if row[1] else None,
    }


# ---------------------------------------------------------------------------
# POST /retention/clients/{accountid}/active-override
# ---------------------------------------------------------------------------

class ActiveOverrideRequest(BaseModel):
    is_active: bool
    override_days: int = Field(default=7, ge=1, le=365)


@router.post("/retention/clients/{accountid}/active-override")
async def set_active_override(
    accountid: str,
    body: ActiveOverrideRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Manually override the active status for a client.

    Requires agent or admin role. The override expires after `override_days`.
    """
    if current_user.role not in ("admin", "agent", "team-leader"):
        raise HTTPException(status_code=403, detail="Agent or admin access required")

    now = datetime.utcnow()
    expires_at = now + timedelta(days=body.override_days)

    await db.execute(
        text(
            "INSERT INTO client_active_status "
            "(accountid, is_active, is_manual_override, override_by, override_at, override_expires_at, computed_at) "
            "VALUES (:accountid, :is_active, TRUE, :override_by, :override_at, :override_expires_at, :computed_at) "
            "ON CONFLICT (accountid) DO UPDATE SET "
            "  is_active = EXCLUDED.is_active, "
            "  is_manual_override = TRUE, "
            "  override_by = EXCLUDED.override_by, "
            "  override_at = EXCLUDED.override_at, "
            "  override_expires_at = EXCLUDED.override_expires_at"
        ),
        {
            "accountid": accountid,
            "is_active": body.is_active,
            "override_by": current_user.id,
            "override_at": now,
            "override_expires_at": expires_at,
            "computed_at": now,
        },
    )
    await db.commit()

    logger.info(
        "CLAUD-86: active override set for %s -> is_active=%s by user %d, expires %s",
        accountid, body.is_active, current_user.id, expires_at.isoformat(),
    )

    return {
        "accountid": accountid,
        "is_active": body.is_active,
        "is_manual_override": True,
        "override_by": current_user.id,
        "override_expires_at": expires_at.isoformat(),
    }
