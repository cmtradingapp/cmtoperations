import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from jose import jwt
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.config import settings
from app.models.role import ALL_PAGES, Role
from app.models.user import User
from app.pg_database import get_db

from app.seed import verify_password

logger = logging.getLogger(__name__)

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


def create_token(
    user_id: int,
    role: str,
    vtiger_user_id: int | None = None,
    vtiger_office: str | None = None,
    vtiger_department: str | None = None,
    team: str | None = None,
    app_department: str | None = None,
) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expire_hours)
    payload: dict = {"sub": str(user_id), "role": role, "exp": expire}
    if vtiger_user_id is not None:
        payload["vtiger_user_id"] = vtiger_user_id
    if vtiger_office is not None:
        payload["vtiger_office"] = vtiger_office
    if vtiger_department is not None:
        payload["vtiger_department"] = vtiger_department
    if team:
        payload["team"] = team
    if app_department:
        payload["app_department"] = app_department
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


async def _resolve_vtiger_identity(email: str | None, db: AsyncSession) -> dict:
    """Look up the CRM identity from the local vtiger_users table by email.

    Returns a dict with vtiger_user_id, vtiger_office, vtiger_department (all nullable).
    """
    result: dict = {"vtiger_user_id": None, "vtiger_office": None, "vtiger_department": None}
    if not email:
        return result
    try:
        row = await db.execute(
            text("SELECT id, office, department FROM vtiger_users WHERE LOWER(email) = LOWER(:email) LIMIT 1"),
            {"email": email},
        )
        vtiger_row = row.fetchone()
        if vtiger_row:
            try:
                result["vtiger_user_id"] = int(vtiger_row[0])
            except (ValueError, TypeError):
                result["vtiger_user_id"] = None
            result["vtiger_office"] = vtiger_row[1]
            result["vtiger_department"] = vtiger_row[2]
    except Exception as e:
        logger.warning("Could not resolve vtiger identity for %s: %s", email, e)
    return result


@router.post("/auth/login")
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == body.username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is disabled")

    # Fetch permissions from role -- admin always gets all pages
    if user.role == "admin":
        permissions = list(ALL_PAGES)
    else:
        role_result = await db.execute(select(Role).where(Role.name == user.role))
        role_obj = role_result.scalar_one_or_none()
        permissions = role_obj.permissions if role_obj else []

    # Resolve vtiger CRM identity via email bridge (CLAUD-16 RBAC)
    vtiger = await _resolve_vtiger_identity(user.email, db)

    token = create_token(
        user.id,
        user.role,
        vtiger_user_id=vtiger["vtiger_user_id"],
        vtiger_office=vtiger["vtiger_office"],
        vtiger_department=vtiger["vtiger_department"],
        team=user.team,
        app_department=user.department,
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "username": user.username,
        "role": user.role,
        "permissions": permissions,
        "vtiger_user_id": vtiger["vtiger_user_id"],
        "vtiger_office": vtiger["vtiger_office"],
        "vtiger_department": vtiger["vtiger_department"],
    }


@router.get("/auth/me")
async def me(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    vtiger = await _resolve_vtiger_identity(current_user.email, db)

    # Always fetch permissions fresh from DB so the frontend can refresh
    # stale permissions stored in localStorage (CLAUD-67)
    if current_user.role == "admin":
        permissions = list(ALL_PAGES)
    else:
        role_result = await db.execute(select(Role).where(Role.name == current_user.role))
        role_obj = role_result.scalar_one_or_none()
        permissions = role_obj.permissions if role_obj else []

    return {
        "id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "role": current_user.role,
        "is_active": current_user.is_active,
        "permissions": permissions,
        "vtiger_user_id": vtiger["vtiger_user_id"],
        "vtiger_office": vtiger["vtiger_office"],
        "vtiger_department": vtiger["vtiger_department"],
    }
