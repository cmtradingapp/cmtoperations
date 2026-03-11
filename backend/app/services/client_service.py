import asyncio
import logging
from typing import List, Optional

import httpx

from app import database
from app.config import settings
from app.schemas.client import ClientDetail, FilterParams
from app.services.internal_api import get_phone_number
from app.services.mock_data import filter_mock_clients

logger = logging.getLogger(__name__)

_RESULTS_LIMIT = 500
_ENRICH_SEMAPHORE = asyncio.Semaphore(10)


async def get_filtered_clients(
    http_client: httpx.AsyncClient, filters: FilterParams
) -> List[ClientDetail]:
    if settings.mock_mode:
        return filter_mock_clients(filters)

    clients = await _query_mssql(filters)
    if not clients:
        return []

    await _enrich_phones(http_client, clients)
    return clients


async def _enrich_phones(
    http_client: httpx.AsyncClient, clients: List[ClientDetail]
) -> None:
    """Fetch phone numbers concurrently and mutate each ClientDetail in place."""
    async def _fetch_one(c: ClientDetail) -> None:
        async with _ENRICH_SEMAPHORE:
            c.phone_number = await get_phone_number(http_client, c.client_id)

    await asyncio.gather(*[_fetch_one(c) for c in clients])


async def _query_mssql(filters: FilterParams) -> List[ClientDetail]:
    conditions: list[str] = []
    params: list = []

    if filters.date_from:
        conditions.append("CAST(a.createdtime AS DATE) >= ?")
        params.append(str(filters.date_from))

    if filters.date_to:
        conditions.append("CAST(a.createdtime AS DATE) <= ?")
        params.append(str(filters.date_to))

    if filters.sales_status is not None:
        conditions.append("a.sales_status = ?")
        params.append(filters.sales_status)

    if filters.region:
        conditions.append("a.country_iso = ?")
        params.append(filters.region)

    if filters.sales_client_potential is not None:
        _OP_MAP = {"eq": "=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
        op = _OP_MAP.get(filters.sales_client_potential_op or "eq", "=")
        conditions.append(f"a.sales_client_potential {op} ?")
        params.append(filters.sales_client_potential)

    if filters.language:
        conditions.append("a.customer_language = ?")
        params.append(filters.language)

    if filters.live == "yes":
        conditions.append("a.birth_date IS NOT NULL")
    elif filters.live == "no":
        conditions.append("a.birth_date IS NULL")

    if filters.ftd == "yes":
        conditions.append("a.client_qualification_date IS NOT NULL")
    elif filters.ftd == "no":
        conditions.append("a.client_qualification_date IS NULL")

    if filters.custom_field:
        conditions.append("(a.full_name LIKE ? OR a.email LIKE ?)")
        params.append(f"%{filters.custom_field}%")
        params.append(f"%{filters.custom_field}%")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    query = (
        f"SELECT TOP {_RESULTS_LIMIT} "
        "CAST(a.accountid AS NVARCHAR) AS client_id, "
        "a.full_name AS name, "
        "ISNULL(ss.value, CAST(a.sales_status AS NVARCHAR)) AS status, "
        "a.country_iso AS region, "
        "CONVERT(NVARCHAR(10), a.createdtime, 23) AS created_at, "
        "a.email, "
        "a.sales_client_potential, "
        "a.customer_language AS language "
        "FROM report.ant_acc a "
        "LEFT JOIN report.ant_sales_status ss ON a.sales_status = ss.status_key "
        f"WHERE {where_clause}"
    )

    rows = await database.execute_query(query, tuple(params))
    return [
        ClientDetail(
            client_id=str(row["client_id"] or ""),
            name=str(row["name"] or ""),
            status=str(row["status"] or ""),
            region=row.get("region") or None,
            created_at=row.get("created_at"),
            phone_number=None,
            email=row.get("email") or None,
            sales_client_potential=row.get("sales_client_potential"),
            language=row.get("language") or None,
        )
        for row in rows
    ]
