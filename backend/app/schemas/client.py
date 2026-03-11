from datetime import date
from typing import Optional

from pydantic import BaseModel


class FilterParams(BaseModel):
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    sales_status: Optional[int] = None
    region: Optional[str] = None
    custom_field: Optional[str] = None
    sales_client_potential: Optional[int] = None
    sales_client_potential_op: Optional[str] = None  # eq, gt, gte, lt, lte
    language: Optional[str] = None
    live: Optional[str] = None  # "yes" = birth_date IS NOT NULL, "no" = birth_date IS NULL
    ftd: Optional[str] = None   # "yes" = client_qualification_date IS NOT NULL, "no" = IS NULL


class ClientSummary(BaseModel):
    client_id: str
    name: str
    status: str
    region: Optional[str] = None
    created_at: Optional[str] = None


class ClientDetail(BaseModel):
    client_id: str
    name: str
    status: str
    region: Optional[str] = None
    created_at: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    account_manager: Optional[str] = None
    sales_client_potential: Optional[int] = None
    language: Optional[str] = None
