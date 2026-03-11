import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.models.role import ALL_PAGES, Role
from app.models.user import User
from app.pg_database import get_db
from app.routers.auth import _resolve_vtiger_identity
from app.seed import hash_password

logger = logging.getLogger(__name__)

router = APIRouter()

# Default password assigned to all vtiger-synced users
_VTIGER_USER_DEFAULT_PASSWORD = "Hdtkfvi1234567"


@router.post("/admin/sync-vtiger-users")
async def sync_vtiger_users_to_local(
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> dict:
    """Sync active Retention users from local vtiger_users mirror → local users table.

    vtiger_users is populated hourly by the ETL job from the MSSQL replica.
    Idempotent: existing users (matched by email) are updated rather than duplicated.
    All synced users are assigned role='agent'.
    Returns a summary of created/skipped/total counts.
    """
    # 1. Fetch active Retention users from the local vtiger_users mirror table
    result = await db.execute(
        text(
            "SELECT id, user_name, first_name, last_name, email, department, office"
            " FROM vtiger_users"
            " WHERE status = 'Active' AND fax = 'Retention'"
        )
    )
    rows = result.fetchall()

    created = 0
    skipped = 0
    default_hashed = hash_password(_VTIGER_USER_DEFAULT_PASSWORD)

    for row in rows:
        vtiger_id, user_name, first_name, last_name, email, department, office = (
            row[0], row[1], row[2], row[3], row[4], row[5], row[6]
        )

        # Email is used as the login username — skip users without one
        if not email:
            logger.warning("sync-vtiger-users: skipping row with null email (vtiger_id=%s, user_name=%s)", vtiger_id, user_name)
            skipped += 1
            continue

        # Check for existing user by email (primary dedup key = username)
        existing_user: User | None = None
        email_result = await db.execute(select(User).where(User.email == email))
        existing_user = email_result.scalar_one_or_none()

        if existing_user is None:
            username_result = await db.execute(select(User).where(User.username == email))
            existing_user = username_result.scalar_one_or_none()

        if existing_user is not None:
            # Update existing — keep their password, update role to agent, sync fields
            existing_user.role = "agent"
            existing_user.is_active = True
            existing_user.email = email
            if existing_user.username != email:
                existing_user.username = email
            existing_user.office = office
            existing_user.department = department
            skipped += 1
        else:
            # Create new user — username = email
            new_user = User(
                username=email,
                email=email,
                hashed_password=default_hashed,
                role="agent",
                is_active=True,
                office=office,
                department=department,
            )
            db.add(new_user)
            created += 1

    await db.commit()

    total = created + skipped
    logger.info(
        "sync-vtiger-users: created=%d, skipped/updated=%d, total=%d",
        created, skipped, total,
    )
    return {"created": created, "skipped": skipped, "total": total}


@router.post("/admin/login-as/{user_id}")
async def login_as_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(require_admin),
) -> dict:
    """Generate a JWT for a target user (admin impersonation).

    The returned token is identical in structure to a regular login token,
    plus an 'impersonated_by' field in the payload for audit purposes.
    The frontend swaps its stored token for the returned one.
    """
    # Fetch target user
    result = await db.execute(select(User).where(User.id == user_id))
    target_user: User | None = result.scalar_one_or_none()

    if target_user is None:
        raise HTTPException(status_code=404, detail="User not found")

    if not target_user.is_active:
        raise HTTPException(status_code=403, detail="Target user account is disabled")

    # Resolve vtiger CRM identity for the target user
    vtiger = await _resolve_vtiger_identity(target_user.email, db)

    # Build JWT payload — create_token handles the standard fields;
    # we add impersonated_by by re-encoding with the extra field appended.
    from datetime import datetime, timedelta, timezone
    from jose import jwt
    from app.config import settings

    expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expire_hours)
    payload: dict = {
        "sub": str(target_user.id),
        "role": target_user.role,
        "exp": expire,
        "impersonated_by": current_admin.id,
    }
    if vtiger["vtiger_user_id"] is not None:
        payload["vtiger_user_id"] = vtiger["vtiger_user_id"]
    if vtiger["vtiger_office"] is not None:
        payload["vtiger_office"] = vtiger["vtiger_office"]
    if vtiger["vtiger_department"] is not None:
        payload["vtiger_department"] = vtiger["vtiger_department"]
    if target_user.team is not None:
        payload["team"] = target_user.team
    if target_user.department:
        payload["app_department"] = target_user.department

    token = jwt.encode(payload, settings.jwt_secret, algorithm="HS256")

    # Fetch permissions for the target user's role
    if target_user.role == "admin":
        permissions = list(ALL_PAGES)
    else:
        role_result = await db.execute(select(Role).where(Role.name == target_user.role))
        role_obj = role_result.scalar_one_or_none()
        permissions = role_obj.permissions if role_obj else []

    logger.info(
        "login-as: admin user_id=%d (%s) impersonating user_id=%d (%s) role=%s vtiger_user_id=%s",
        current_admin.id, current_admin.username,
        target_user.id, target_user.username,
        target_user.role, vtiger["vtiger_user_id"],
    )

    return {
        "access_token": token,
        "token_type": "bearer",
        "username": target_user.username,
        "role": target_user.role,
        "permissions": permissions,
        "vtiger_user_id": vtiger["vtiger_user_id"],
    }
