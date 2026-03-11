import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_jwt_payload, JWTPayload
from app.pg_database import get_db

router = APIRouter()


class SavedSearchPayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=40)
    filters: dict = {}
    column_order: list = []
    column_visibility: dict = {}
    col_filters: dict = {}
    sort_field: str | None = None
    sort_direction: str | None = None
    status_filter: str | None = None


@router.get("/retention/saved-searches")
async def list_saved_searches(
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    result = await db.execute(
        text("""
            SELECT id::text, name, filters, column_order, column_visibility,
                   col_filters, sort_field, sort_direction, status_filter,
                   created_at, updated_at
            FROM saved_searches
            WHERE user_id = :uid
            ORDER BY updated_at DESC
        """),
        {"uid": jwt_payload.user_id},
    )
    rows = result.mappings().all()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "filters": r["filters"] or {},
            "column_order": r["column_order"] or [],
            "column_visibility": r["column_visibility"] or {},
            "col_filters": r["col_filters"] or {},
            "sort_field": r["sort_field"],
            "sort_direction": r["sort_direction"],
            "status_filter": r["status_filter"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        }
        for r in rows
    ]


@router.post("/retention/saved-searches")
async def create_saved_search(
    body: SavedSearchPayload,
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> dict:
    # Check for duplicate name
    existing = await db.execute(
        text("SELECT id::text FROM saved_searches WHERE user_id = :uid AND name = :name"),
        {"uid": jwt_payload.user_id, "name": body.name},
    )
    if existing.fetchone():
        raise HTTPException(status_code=409, detail="A search with this name already exists.")

    result = await db.execute(
        text("""
            INSERT INTO saved_searches (user_id, name, filters, column_order, column_visibility,
                                        col_filters, sort_field, sort_direction, status_filter)
            VALUES (:uid, :name, :filters, :col_order, :col_vis, :col_filters,
                    :sort_field, :sort_dir, :status_filter)
            RETURNING id::text, created_at, updated_at
        """),
        {
            "uid": jwt_payload.user_id,
            "name": body.name,
            "filters": json.dumps(body.filters),
            "col_order": json.dumps(body.column_order),
            "col_vis": json.dumps(body.column_visibility),
            "col_filters": json.dumps(body.col_filters),
            "sort_field": body.sort_field,
            "sort_dir": body.sort_direction,
            "status_filter": body.status_filter,
        },
    )
    await db.commit()
    row = result.mappings().first()
    return {"id": row["id"], "name": body.name, "created_at": row["created_at"].isoformat()}


@router.put("/retention/saved-searches/{search_id}")
async def update_saved_search(
    search_id: str,
    body: SavedSearchPayload,
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> dict:
    # Verify ownership
    existing = await db.execute(
        text("SELECT id FROM saved_searches WHERE id = :sid AND user_id = :uid"),
        {"sid": search_id, "uid": jwt_payload.user_id},
    )
    if not existing.fetchone():
        raise HTTPException(status_code=404, detail="Saved search not found.")

    # Check name uniqueness (excluding current record)
    dup = await db.execute(
        text("SELECT id FROM saved_searches WHERE user_id = :uid AND name = :name AND id != :sid"),
        {"uid": jwt_payload.user_id, "name": body.name, "sid": search_id},
    )
    if dup.fetchone():
        raise HTTPException(status_code=409, detail="A search with this name already exists.")

    await db.execute(
        text("""
            UPDATE saved_searches SET
                name = :name,
                filters = :filters,
                column_order = :col_order,
                column_visibility = :col_vis,
                col_filters = :col_filters,
                sort_field = :sort_field,
                sort_direction = :sort_dir,
                status_filter = :status_filter,
                updated_at = NOW()
            WHERE id = :sid AND user_id = :uid
        """),
        {
            "sid": search_id,
            "uid": jwt_payload.user_id,
            "name": body.name,
            "filters": json.dumps(body.filters),
            "col_order": json.dumps(body.column_order),
            "col_vis": json.dumps(body.column_visibility),
            "col_filters": json.dumps(body.col_filters),
            "sort_field": body.sort_field,
            "sort_dir": body.sort_direction,
            "status_filter": body.status_filter,
        },
    )
    await db.commit()
    return {"id": search_id, "name": body.name}


@router.delete("/retention/saved-searches/{search_id}")
async def delete_saved_search(
    search_id: str,
    jwt_payload: JWTPayload = Depends(get_jwt_payload),
    db: AsyncSession = Depends(get_db),
) -> dict:
    result = await db.execute(
        text("DELETE FROM saved_searches WHERE id = :sid AND user_id = :uid RETURNING id"),
        {"sid": search_id, "uid": jwt_payload.user_id},
    )
    await db.commit()
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Saved search not found.")
    return {"deleted": search_id}
