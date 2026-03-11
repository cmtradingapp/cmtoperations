"""RBAC helpers for CRM role-based access control (CLAUD-16)."""

import time
from typing import Callable

from fastapi import Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def get_client_scope_filter(
    role: str,
    vtiger_user_id: int | None,
    vtiger_department: str | None,
    team: str | None = None,
    app_department: str | None = None,
) -> tuple[str, dict]:
    """Return a SQL WHERE fragment and params dict to inject into the retention query.

    Uses parameterised queries to prevent SQL injection.

    CLAUD-124 data visibility hierarchy:
      admin            — all clients
      retention_manager — all clients on their desk (department-scoped), or all if no dept set
      team_leader      — CLAUD-180: if team set → team-scoped; else desk-scoped
                         app_department fallback: users.department when vtiger_department is empty
      agent            — only their own assigned clients
    """
    if role == "admin":
        return "", {}
    elif role == "retention_manager":
        if vtiger_department:
            # CLAUD-124: scope to agents on the same desk
            return (
                "AND m.assigned_to IN (SELECT id FROM vtiger_users WHERE department = :rbac_dept)",
                {"rbac_dept": vtiger_department},
            )
        # No department set — fallback to unrestricted (transition period)
        return "", {}
    elif role == "team_leader":
        if team:
            # CLAUD-180: scope to agents in the same team (via users table → vtiger bridge)
            return (
                "AND m.assigned_to IN ("
                "  SELECT vu.id FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.team = :rbac_team AND u.role = 'agent'"
                ")",
                {"rbac_team": team},
            )
        elif vtiger_department:
            # Fallback: desk-level scoping via vtiger department
            return (
                "AND m.assigned_to IN (SELECT id FROM vtiger_users WHERE department = :rbac_dept)",
                {"rbac_dept": vtiger_department},
            )
        elif app_department:
            # CLAUD-180 fix: vtiger_department is empty for some team leaders —
            # use users.department (app_department) as fallback via email bridge
            return (
                "AND m.assigned_to IN ("
                "  SELECT vu.id FROM vtiger_users vu"
                "  JOIN users u ON LOWER(u.email) = LOWER(vu.email)"
                "  WHERE u.department = :rbac_app_dept AND u.role = 'agent'"
                ")",
                {"rbac_app_dept": app_department},
            )
        return "AND 1=0", {}
    elif role == "agent" and vtiger_user_id is not None:
        return (
            "AND m.assigned_to = :rbac_vtiger_uid",
            {"rbac_vtiger_uid": str(vtiger_user_id)},
        )
    # Fallback: unknown role or missing identity — block all data
    return "AND 1=0", {}


# ---------------------------------------------------------------------------
# CLAUD-124: Generic page-level permission guard factory.
#
# Usage: create a module-level guard for a page key, then use it as a
# FastAPI dependency on each endpoint that belongs to that page.
#
#   _require_retention = make_page_guard("retention")
#
#   @router.get("/retention/clients")
#   async def get_clients(jwt_payload = Depends(_require_retention), ...):
#       ...
# ---------------------------------------------------------------------------

_PAGE_PERM_CACHE: dict[str, tuple[list, float]] = {}
_PAGE_PERM_TTL = 120  # seconds — roles change rarely


def make_page_guard(page_key: str) -> Callable:
    """Return a FastAPI dependency that enforces page-level permission for *page_key*.

    - admin role: always allowed (no DB hit)
    - other roles: permissions loaded from roles table (cached 120 s)
    - missing permission: raises HTTP 403
    Returns the decoded JWTPayload so callers can use it directly.
    """
    from app.auth_deps import get_jwt_payload, JWTPayload
    from app.models.role import Role
    from app.pg_database import get_db
    from sqlalchemy import select

    async def _guard(
        jwt_payload: JWTPayload = Depends(get_jwt_payload),
        db: AsyncSession = Depends(get_db),
    ) -> JWTPayload:
        if jwt_payload.role == "admin":
            return jwt_payload

        now = time.monotonic()
        cached = _PAGE_PERM_CACHE.get(jwt_payload.role)
        if cached and cached[1] > now:
            permissions = cached[0]
        else:
            result = await db.execute(select(Role).where(Role.name == jwt_payload.role))
            role_obj = result.scalar_one_or_none()
            permissions = role_obj.permissions or [] if role_obj else []
            _PAGE_PERM_CACHE[jwt_payload.role] = (permissions, now + _PAGE_PERM_TTL)

        if page_key not in permissions:
            raise HTTPException(
                status_code=403,
                detail=f"Your role does not have access to this page ({page_key}).",
            )
        return jwt_payload

    # Give the inner function a unique name so FastAPI can distinguish
    # dependencies created for different page keys
    _guard.__name__ = f"require_{page_key.replace('-', '_')}_access"
    return _guard


def invalidate_page_perm_cache(role_name: str) -> None:
    """Evict a role from the page-permission cache (call after role update)."""
    _PAGE_PERM_CACHE.pop(role_name, None)


async def can(role: str, action: str, db: AsyncSession) -> bool:
    """Check whether the given role has permission for the given action."""
    result = await db.execute(
        text("SELECT enabled FROM crm_permissions WHERE role = :role AND action = :action"),
        {"role": role, "action": action},
    )
    row = result.fetchone()
    return bool(row and row[0])
