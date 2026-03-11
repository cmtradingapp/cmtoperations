import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.models.audit_log import AuditLog
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

VALID_ACTION_TYPES = {"status_change", "note_added", "call_initiated", "whatsapp_opened"}


@router.get("/admin/audit-log")
async def list_audit_log(
    agent_username: Optional[str] = Query(None, description="Filter by agent username"),
    client_account_id: Optional[str] = Query(None, description="Filter by client account ID"),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    date_from: Optional[datetime] = Query(None, description="Filter from date (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="Filter to date (ISO 8601)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Return paginated audit log entries with optional filters. Admin-only."""
    query = select(AuditLog)
    count_query = select(func.count(AuditLog.id))

    # Apply filters
    if agent_username:
        query = query.where(AuditLog.agent_username == agent_username)
        count_query = count_query.where(AuditLog.agent_username == agent_username)
    if client_account_id:
        query = query.where(AuditLog.client_account_id == client_account_id)
        count_query = count_query.where(AuditLog.client_account_id == client_account_id)
    if action_type:
        query = query.where(AuditLog.action_type == action_type)
        count_query = count_query.where(AuditLog.action_type == action_type)
    if date_from:
        query = query.where(AuditLog.timestamp >= date_from)
        count_query = count_query.where(AuditLog.timestamp >= date_from)
    if date_to:
        query = query.where(AuditLog.timestamp <= date_to)
        count_query = count_query.where(AuditLog.timestamp <= date_to)

    # Total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginate (newest first)
    offset = (page - 1) * page_size
    query = query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(page_size)

    result = await db.execute(query)
    entries = result.scalars().all()

    return {
        "items": [
            {
                "id": e.id,
                "agent_id": e.agent_id,
                "agent_username": e.agent_username,
                "client_account_id": e.client_account_id,
                "action_type": e.action_type,
                "action_value": e.action_value,
                "timestamp": e.timestamp.isoformat(),
            }
            for e in entries
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }


