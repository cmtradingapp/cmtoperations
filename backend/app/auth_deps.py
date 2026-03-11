import time
from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User
from app.pg_database import get_db

security = HTTPBearer()

# ---------------------------------------------------------------------------
# CLAUD-36: Lightweight cached user proxy.
#
# Without caching, get_current_user hits the DB on EVERY request to verify
# the user still exists and is_active. With 70 concurrent agents making 5+
# API calls per page load, that is 350+ redundant SELECT FROM users queries
# per navigation. The JWT is already cryptographically signed and verified
# by decode_jwt, so the DB check is for deactivation only — a 60s lag is
# acceptable for this use-case.
#
# We use a dataclass proxy rather than reconstructing an ORM object, which
# avoids SQLAlchemy instrumentation issues with detached instances.
# ---------------------------------------------------------------------------

@dataclass
class _CachedUser:
    """Lightweight proxy that exposes the same attributes as User for all
    downstream consumers that receive a User from get_current_user."""
    id: int
    username: str
    email: Optional[str]
    role: str
    is_active: bool


_USER_CACHE: dict[int, tuple[_CachedUser, float]] = {}
_USER_CACHE_TTL = 60  # seconds
_USER_CACHE_MAX = 500  # maximum entries before an eviction pass


def _user_cache_get(user_id: int) -> Optional[_CachedUser]:
    entry = _USER_CACHE.get(user_id)
    if entry and entry[1] > time.monotonic():
        return entry[0]
    return None


def _user_cache_put(user_id: int, user: User) -> None:
    now = time.monotonic()
    proxy = _CachedUser(
        id=user.id,
        username=user.username,
        email=user.email,
        role=user.role,
        is_active=user.is_active,
    )
    _USER_CACHE[user_id] = (proxy, now + _USER_CACHE_TTL)
    if len(_USER_CACHE) > _USER_CACHE_MAX:
        # Evict expired entries to prevent unbounded growth
        expired = [k for k, v in _USER_CACHE.items() if v[1] <= now]
        for k in expired:
            del _USER_CACHE[k]


def _user_cache_invalidate(user_id: int) -> None:
    """Remove a user from the cache (call after password/role/status change)."""
    _USER_CACHE.pop(user_id, None)


@dataclass
class JWTPayload:
    """Decoded JWT payload with vtiger CRM identity fields (CLAUD-16 RBAC)."""
    user_id: int
    role: str
    vtiger_user_id: int | None = None
    vtiger_office: str | None = None
    vtiger_department: str | None = None
    team: str | None = None        # CLAUD-180: team-level scoping for team_leader
    app_department: str | None = None  # CLAUD-180: users.department fallback for team_leader


def decode_jwt(token: str) -> JWTPayload:
    """Decode and validate JWT, returning a structured payload."""
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        user_id = int(payload["sub"])
        role = payload.get("role", "agent")
        vtiger_user_id = payload.get("vtiger_user_id")
        vtiger_office = payload.get("vtiger_office")
        vtiger_department = payload.get("vtiger_department")
        team = payload.get("team")
        app_department = payload.get("app_department")
        return JWTPayload(
            user_id=user_id,
            role=role,
            vtiger_user_id=vtiger_user_id,
            vtiger_office=vtiger_office,
            vtiger_department=vtiger_department,
            team=team,
            app_department=app_department,
        )
    except (JWTError, KeyError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_jwt_payload(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> JWTPayload:
    """FastAPI dependency that returns the decoded JWT payload."""
    return decode_jwt(credentials.credentials)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    payload = decode_jwt(credentials.credentials)

    # --- Cache check: skip the DB hit for recently validated users ---
    cached = _user_cache_get(payload.user_id)
    if cached is not None:
        return cached  # type: ignore[return-value]  # _CachedUser is duck-type compatible

    result = await db.execute(select(User).where(User.id == payload.user_id))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    _user_cache_put(payload.user_id, user)
    return user


async def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
