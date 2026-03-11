"""CLAUD-97: Lifecycle Stages — Admin CRUD for configurable client lifecycle milestones."""

import logging
import re
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lifecycle", tags=["lifecycle"])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class StageIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    metric_type: str = Field(..., pattern="^(ftd|deposit|position|volume|custom)$")
    threshold: float = Field(..., gt=0)


class StageUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    metric_type: str = Field(..., pattern="^(ftd|deposit|position|volume|custom)$")
    threshold: float = Field(..., gt=0)


class ReorderIn(BaseModel):
    order: list[int] = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_key(name: str) -> str:
    """Derive a unique key from the stage name: lowercase, spaces to underscores, strip specials."""
    key = name.strip().lower()
    key = re.sub(r"[^a-z0-9_\s]", "", key)
    key = re.sub(r"\s+", "_", key)
    return key


def _row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "name": row[1],
        "key": row[2],
        "metric_type": row[3],
        "threshold": float(row[4]),
        "display_order": row[5],
        "is_active": row[6],
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stages")
async def list_stages(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """List all lifecycle stages ordered by display_order."""
    result = await db.execute(
        text(
            "SELECT id, name, key, metric_type, threshold, display_order, is_active, created_at, updated_at "
            "FROM lifecycle_stages ORDER BY display_order ASC"
        )
    )
    return [_row_to_dict(r) for r in result.fetchall()]


@router.post("/stages", status_code=201)
async def create_stage(
    body: StageIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Create a new lifecycle stage. Key is auto-derived from name."""
    key = _make_key(body.name)

    # Check key uniqueness
    existing = await db.execute(
        text("SELECT 1 FROM lifecycle_stages WHERE key = :key"),
        {"key": key},
    )
    if existing.fetchone():
        raise HTTPException(400, f"A stage with key '{key}' already exists. Choose a different name.")

    # Next display_order
    max_order = await db.execute(
        text("SELECT COALESCE(MAX(display_order), 0) FROM lifecycle_stages")
    )
    next_order = max_order.fetchone()[0] + 1

    result = await db.execute(
        text(
            "INSERT INTO lifecycle_stages (name, key, metric_type, threshold, display_order) "
            "VALUES (:name, :key, :metric_type, :threshold, :display_order) "
            "RETURNING id, name, key, metric_type, threshold, display_order, is_active, created_at, updated_at"
        ),
        {
            "name": body.name,
            "key": key,
            "metric_type": body.metric_type,
            "threshold": body.threshold,
            "display_order": next_order,
        },
    )
    row = result.fetchone()
    await db.commit()
    logger.info("Created lifecycle stage id=%d name=%s key=%s", row[0], body.name, key)
    return _row_to_dict(row)


@router.put("/stages/{stage_id}")
async def update_stage(
    stage_id: int,
    body: StageUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Update an existing lifecycle stage (name, metric_type, threshold)."""
    existing = await db.execute(
        text("SELECT key FROM lifecycle_stages WHERE id = :id"),
        {"id": stage_id},
    )
    old = existing.fetchone()
    if not old:
        raise HTTPException(404, f"Stage {stage_id} not found")

    new_key = _make_key(body.name)

    # If key changed, check uniqueness
    if new_key != old[0]:
        dup = await db.execute(
            text("SELECT 1 FROM lifecycle_stages WHERE key = :key AND id != :id"),
            {"key": new_key, "id": stage_id},
        )
        if dup.fetchone():
            raise HTTPException(400, f"A stage with key '{new_key}' already exists.")

    result = await db.execute(
        text(
            "UPDATE lifecycle_stages SET name = :name, key = :key, metric_type = :metric_type, "
            "threshold = :threshold, updated_at = NOW() "
            "WHERE id = :id "
            "RETURNING id, name, key, metric_type, threshold, display_order, is_active, created_at, updated_at"
        ),
        {
            "name": body.name,
            "key": new_key,
            "metric_type": body.metric_type,
            "threshold": body.threshold,
            "id": stage_id,
        },
    )
    row = result.fetchone()
    await db.commit()
    return _row_to_dict(row)


@router.delete("/stages/{stage_id}")
async def delete_stage(
    stage_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Delete a lifecycle stage. Rejects if only 1 stage remains."""
    count_row = await db.execute(text("SELECT COUNT(*) FROM lifecycle_stages"))
    total = count_row.fetchone()[0]
    if total <= 1:
        raise HTTPException(400, "Cannot delete the last remaining stage.")

    result = await db.execute(
        text("DELETE FROM lifecycle_stages WHERE id = :id"),
        {"id": stage_id},
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, f"Stage {stage_id} not found")
    return {"status": "ok"}


@router.patch("/stages/{stage_id}/toggle")
async def toggle_stage(
    stage_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Toggle is_active for a stage."""
    row = await db.execute(
        text("SELECT is_active FROM lifecycle_stages WHERE id = :id"),
        {"id": stage_id},
    )
    current = row.fetchone()
    if not current:
        raise HTTPException(404, f"Stage {stage_id} not found")

    new_status = not current[0]
    await db.execute(
        text("UPDATE lifecycle_stages SET is_active = :status, updated_at = NOW() WHERE id = :id"),
        {"status": new_status, "id": stage_id},
    )
    await db.commit()
    return {"status": "ok", "id": stage_id, "is_active": new_status}


@router.patch("/stages/reorder")
async def reorder_stages(
    body: ReorderIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Bulk-update display_order. Body: {"order": [id1, id2, ...]} where position = new display_order."""
    for display_order, stage_id in enumerate(body.order, start=1):
        await db.execute(
            text("UPDATE lifecycle_stages SET display_order = :order, updated_at = NOW() WHERE id = :id"),
            {"order": display_order, "id": stage_id},
        )
    await db.commit()
    return {"status": "ok"}
