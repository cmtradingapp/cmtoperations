import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.config import settings
from app.models.integration import Integration
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


class IntegrationRequest(BaseModel):
    name: str
    base_url: str
    auth_key: Optional[str] = None
    description: Optional[str] = None
    is_active: bool = True


def _mask_key(key: str | None) -> str | None:
    """Mask an auth key, showing only the first 4 and last 4 characters."""
    if not key:
        return None
    if len(key) <= 8:
        return "*" * len(key)
    return key[:4] + "*" * (len(key) - 8) + key[-4:]


def _serialize(integration: Integration, reveal_key: bool = False) -> dict:
    return {
        "id": integration.id,
        "name": integration.name,
        "base_url": integration.base_url,
        "auth_key": integration.auth_key if reveal_key else _mask_key(integration.auth_key),
        "description": integration.description,
        "is_active": integration.is_active,
        "created_at": integration.created_at.isoformat(),
    }


@router.get("/admin/integrations")
async def list_integrations(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """List all integrations plus system connection info."""
    result = await db.execute(select(Integration).order_by(Integration.created_at))
    integrations = result.scalars().all()

    # Database connection info (no passwords exposed)
    db_info = {
        "postgres": {
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "database": settings.postgres_db,
            "user": settings.postgres_user,
            "status": "connected",
        },
        "mssql": {
            "host": settings.mssql_server,
            "database": settings.mssql_database,
            "user": settings.mssql_username,
            "status": "configured" if settings.mssql_server != "localhost" else "local",
        },
    }

    # Add replica info if configured
    if settings.replica_db_host:
        db_info["replica"] = {
            "host": settings.replica_db_host,
            "port": settings.replica_db_port,
            "database": settings.replica_db_name,
            "user": settings.replica_db_user,
            "status": "configured",
        }

    return {
        "integrations": [_serialize(i) for i in integrations],
        "databases": db_info,
    }


@router.get("/admin/integrations/{integration_id}")
async def get_integration(
    integration_id: int,
    reveal_key: bool = False,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Get a single integration by ID. Pass ?reveal_key=true to unmask the auth key."""
    result = await db.execute(select(Integration).where(Integration.id == integration_id))
    integration = result.scalar_one_or_none()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    return _serialize(integration, reveal_key=reveal_key)


@router.post("/admin/integrations", status_code=201)
async def create_integration(
    body: IntegrationRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Create a new integration."""
    integration = Integration(
        name=body.name,
        base_url=body.base_url,
        auth_key=body.auth_key,
        description=body.description,
        is_active=body.is_active,
    )
    db.add(integration)
    await db.commit()
    await db.refresh(integration)
    logger.info("Integration created: %s (id=%d)", integration.name, integration.id)
    return _serialize(integration)


@router.put("/admin/integrations/{integration_id}")
async def update_integration(
    integration_id: int,
    body: IntegrationRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Update an existing integration."""
    result = await db.execute(select(Integration).where(Integration.id == integration_id))
    integration = result.scalar_one_or_none()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    integration.name = body.name
    integration.base_url = body.base_url
    if body.auth_key is not None:
        integration.auth_key = body.auth_key
    integration.description = body.description
    integration.is_active = body.is_active
    await db.commit()
    await db.refresh(integration)
    logger.info("Integration updated: %s (id=%d)", integration.name, integration.id)
    return _serialize(integration)


@router.delete("/admin/integrations/{integration_id}", status_code=204)
async def delete_integration(
    integration_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Delete an integration."""
    result = await db.execute(select(Integration).where(Integration.id == integration_id))
    integration = result.scalar_one_or_none()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    await db.delete(integration)
    await db.commit()
    logger.info("Integration deleted: id=%d", integration_id)
