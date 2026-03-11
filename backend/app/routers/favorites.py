from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_jwt_payload, JWTPayload
from app.pg_database import get_db

router = APIRouter()


@router.post("/retention/favorites/{account_id}")
async def toggle_favorite(
    account_id: str,
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Toggle a client as a favorite for the current user.

    Returns ``{"is_favorite": true}`` after adding, ``{"is_favorite": false}``
    after removing.
    """
    user_id = jwt_payload.user_id
    result = await db.execute(
        text("SELECT 1 FROM user_favorites WHERE user_id = :uid AND accountid = :aid"),
        {"uid": user_id, "aid": account_id},
    )
    already_favorited = result.fetchone() is not None

    if already_favorited:
        await db.execute(
            text("DELETE FROM user_favorites WHERE user_id = :uid AND accountid = :aid"),
            {"uid": user_id, "aid": account_id},
        )
        await db.commit()
        return {"is_favorite": False}

    await db.execute(
        text(
            "INSERT INTO user_favorites (user_id, accountid) VALUES (:uid, :aid)"
            " ON CONFLICT (user_id, accountid) DO NOTHING"
        ),
        {"uid": user_id, "aid": account_id},
    )
    await db.commit()
    return {"is_favorite": True}
