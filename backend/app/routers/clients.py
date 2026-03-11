import asyncio
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.schemas.client import ClientDetail, FilterParams
from app.services.client_service import get_filtered_clients
from app.services.internal_api import get_crm_data

router = APIRouter()


@router.get("/clients", response_model=List[ClientDetail])
async def list_clients(
    request: Request,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    sales_status: Optional[int] = None,
    region: Optional[str] = None,
    custom_field: Optional[str] = None,
    sales_client_potential: Optional[int] = None,
    sales_client_potential_op: Optional[str] = None,
    language: Optional[str] = None,
    live: Optional[str] = None,
    ftd: Optional[str] = None,
) -> List[ClientDetail]:
    filters = FilterParams(
        date_from=date_from,
        date_to=date_to,
        sales_status=sales_status,
        region=region,
        custom_field=custom_field,
        sales_client_potential=sales_client_potential,
        sales_client_potential_op=sales_client_potential_op,
        language=language,
        live=live,
        ftd=ftd,
    )
    http_client = request.app.state.http_client
    return await get_filtered_clients(http_client, filters)


class LookupItem(BaseModel):
    id: str


class LookupRequest(BaseModel):
    clients: List[LookupItem]


@router.post("/clients/lookup")
async def lookup_clients(request: Request, body: LookupRequest):
    http_client = request.app.state.http_client

    async def enrich(item: LookupItem) -> dict:
        crm = await get_crm_data(http_client, item.id)
        return {
            "id": item.id,
            "first_name": crm.first_name,
            "email": crm.email,
            "phone": crm.phone,
            "retention_rep": crm.retention_rep,
            "retention_status_display": crm.retention_status_display,
            "error": None if crm.phone else "Phone number not found in CRM",
        }

    results = await asyncio.gather(*[enrich(c) for c in body.clients])
    return list(results)
