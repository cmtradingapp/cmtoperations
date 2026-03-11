import logging
from dataclasses import dataclass
from typing import Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class CRMClientData:
    phone: Optional[str]
    first_name: Optional[str]
    email: Optional[str]
    retention_status_id: Optional[str] = None
    retention_rep: Optional[str] = None
    retention_status_display: Optional[str] = None


async def get_crm_data(client: httpx.AsyncClient, client_id: str) -> CRMClientData:
    """Fetch phone, first name and email for a client from the CRM API."""
    if settings.mock_mode:
        return CRMClientData(phone=None, first_name=None, email=None, retention_status_id=None)

    try:
        response = await client.get(
            f"{settings.crm_api_base_url}//crm-api/user",
            params={"id": client_id},
            headers={"x-crm-api-token": settings.crm_api_token},
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json()
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict):
            return CRMClientData(phone=None, first_name=None, email=None)
        raw_status = result.get("retentionStatus")
        crm_data = CRMClientData(
            phone=result.get("fullTelephone") or None,
            first_name=result.get("firstName") or None,
            email=result.get("email") or None,
            retention_status_id=str(raw_status) if raw_status is not None else None,
            retention_rep=result.get("retentionRepFullName") or None,
            retention_status_display=result.get("retentionStatusDisplay") or None,
        )
        logger.info(
            "CRM data | client=%s phone=%s first_name=%r email=%r",
            client_id, crm_data.phone, crm_data.first_name, crm_data.email,
        )
        return crm_data
    except Exception as e:
        logger.warning("CRM API failed for client %s: %s", client_id, e)
        return CRMClientData(phone=None, first_name=None, email=None)


# Kept for backward compatibility with client_service.py enrichment
async def get_phone_number(client: httpx.AsyncClient, client_id: str) -> Optional[str]:
    return (await get_crm_data(client, client_id)).phone
