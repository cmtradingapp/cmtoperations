import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.user import User
from app.models.user_preferences import UserPreferences
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ColumnOrderResponse(BaseModel):
    column_order: Optional[List[str]]


class ColumnOrderBody(BaseModel):
    column_order: Optional[List[str]]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/preferences/columns", response_model=ColumnOrderResponse)
async def get_column_order(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    result = await db.execute(
        select(UserPreferences).where(UserPreferences.username == current_user.username)
    )
    prefs = result.scalar_one_or_none()
    if prefs is None:
        return {"column_order": None}
    return {"column_order": prefs.retention_column_order}


@router.put("/preferences/columns", response_model=ColumnOrderResponse)
async def put_column_order(
    body: ColumnOrderBody,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    result = await db.execute(
        select(UserPreferences).where(UserPreferences.username == current_user.username)
    )
    prefs = result.scalar_one_or_none()
    if prefs is None:
        prefs = UserPreferences(
            username=current_user.username,
            retention_column_order=body.column_order,
        )
        db.add(prefs)
    else:
        prefs.retention_column_order = body.column_order

    await db.commit()
    await db.refresh(prefs)
    logger.info("Updated retention_column_order for user %s", current_user.username)
    return {"column_order": prefs.retention_column_order}
