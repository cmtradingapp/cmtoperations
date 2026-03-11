"""CRM Permissions API (CLAUD-16 RBAC).

GET  /api/permissions/my       -- current user's permission map
GET  /api/permissions/          -- admin: list all crm_permissions rows
PUT  /api/permissions/{id}     -- admin: toggle enabled
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user, get_jwt_payload, JWTPayload, require_admin
from app.models.crm_permission import CrmPermission
from app.models.user import User
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/permissions/my")
async def my_permissions(
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return the current user's action permission map based on their role."""
    result = await db.execute(
        select(CrmPermission).where(CrmPermission.role == jwt_payload.role)
    )
    rows = result.scalars().all()
    return {row.action: row.enabled for row in rows}


@router.get("/permissions/")
async def list_permissions(
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Admin-only: list all crm_permissions rows."""
    result = await db.execute(
        select(CrmPermission).order_by(CrmPermission.role, CrmPermission.action)
    )
    rows = result.scalars().all()
    return [
        {"id": row.id, "role": row.role, "action": row.action, "enabled": row.enabled}
        for row in rows
    ]


class TogglePermissionRequest(BaseModel):
    enabled: bool


@router.put("/permissions/{permission_id}")
async def toggle_permission(
    permission_id: int,
    body: TogglePermissionRequest,
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Admin-only: toggle a permission's enabled state."""
    perm = await db.get(CrmPermission, permission_id)
    if not perm:
        raise HTTPException(status_code=404, detail="Permission not found")
    perm.enabled = body.enabled
    await db.commit()
    await db.refresh(perm)
    logger.info("Permission %d (%s/%s) set to enabled=%s", perm.id, perm.role, perm.action, perm.enabled)
    return {"id": perm.id, "role": perm.role, "action": perm.action, "enabled": perm.enabled}
