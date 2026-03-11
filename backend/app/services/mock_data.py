"""
Mock data used when MOCK_MODE=true.
Provides 15 realistic fake clients covering all filter dimensions
so every filter combination returns visible results in the UI.
"""

from typing import Dict, List, Optional

from app.schemas.client import ClientDetail, FilterParams

MOCK_CLIENTS: List[ClientDetail] = [
    ClientDetail(client_id="C-001", name="Acme Corporation",     status="active",   region="northeast", created_at="2024-03-15", phone_number="+15551234001", email="contact@acme.com",      account_manager="John Smith"),
    ClientDetail(client_id="C-002", name="Globex Industries",    status="active",   region="southeast", created_at="2024-05-20", phone_number="+15551234002", email="info@globex.com",       account_manager="Sarah Johnson"),
    ClientDetail(client_id="C-003", name="Initech Solutions",    status="inactive", region="midwest",   created_at="2023-11-08", phone_number="+15551234003", email="support@initech.com",   account_manager="Mike Davis"),
    ClientDetail(client_id="C-004", name="Umbrella Corp",        status="pending",  region="west",      created_at="2024-08-01", phone_number="+15551234004", email="admin@umbrella.com",    account_manager="Lisa Chen"),
    ClientDetail(client_id="C-005", name="Stark Industries",     status="active",   region="northeast", created_at="2024-01-10", phone_number="+15551234005", email="info@stark.com",        account_manager="John Smith"),
    ClientDetail(client_id="C-006", name="Wayne Enterprises",    status="active",   region="midwest",   created_at="2024-02-28", phone_number="+15551234006", email="contact@wayne.com",     account_manager="Sarah Johnson"),
    ClientDetail(client_id="C-007", name="Oscorp Technologies",  status="inactive", region="northeast", created_at="2023-09-14", phone_number="+15551234007", email="tech@oscorp.com",       account_manager="Mike Davis"),
    ClientDetail(client_id="C-008", name="Massive Dynamic",      status="active",   region="west",      created_at="2024-06-07", phone_number="+15551234008", email="info@massive.com",      account_manager="Lisa Chen"),
    ClientDetail(client_id="C-009", name="Nakatomi Trading",     status="pending",  region="west",      created_at="2024-09-22", phone_number="+15551234009", email="trade@nakatomi.com",    account_manager="John Smith"),
    ClientDetail(client_id="C-010", name="Cyberdyne Systems",    status="active",   region="southeast", created_at="2024-04-17", phone_number="+15551234010", email="contact@cyberdyne.com", account_manager="Sarah Johnson"),
    ClientDetail(client_id="C-011", name="Soylent Corp",         status="inactive", region="midwest",   created_at="2023-12-03", phone_number="+15551234011", email="info@soylent.com",      account_manager="Mike Davis"),
    ClientDetail(client_id="C-012", name="Rekall Inc",           status="active",   region="west",      created_at="2024-07-11", phone_number="+15551234012", email="recall@rekall.com",     account_manager="Lisa Chen"),
    ClientDetail(client_id="C-013", name="Tyrell Corporation",   status="active",   region="southeast", created_at="2024-03-29", phone_number="+15551234013", email="nexus@tyrell.com",      account_manager="John Smith"),
    ClientDetail(client_id="C-014", name="Weyland-Yutani Corp",  status="pending",  region="northeast", created_at="2024-10-05", phone_number="+15551234014", email="bio@weyland.com",       account_manager="Sarah Johnson"),
    ClientDetail(client_id="C-015", name="Pied Piper LLC",       status="active",   region="west",      created_at="2024-05-01", phone_number="+15551234015", email="hello@piedpiper.com",   account_manager="Mike Davis"),
]

# Fast lookup by client_id
_INDEX: Dict[str, ClientDetail] = {c.client_id: c for c in MOCK_CLIENTS}


def get_mock_client(client_id: str) -> Optional[ClientDetail]:
    return _INDEX.get(client_id)


def filter_mock_clients(filters: FilterParams) -> List[ClientDetail]:
    results = MOCK_CLIENTS[:]

    if filters.status:
        results = [c for c in results if c.status == filters.status.value]

    if filters.region:
        results = [c for c in results if c.region and c.region.lower() == filters.region.lower()]

    if filters.date_from:
        results = [c for c in results if c.created_at and c.created_at >= str(filters.date_from)]

    if filters.date_to:
        results = [c for c in results if c.created_at and c.created_at <= str(filters.date_to)]

    if filters.custom_field:
        needle = filters.custom_field.lower()
        results = [
            c for c in results
            if needle in c.name.lower()
            or needle in (c.account_manager or "").lower()
            or needle in (c.email or "").lower()
        ]

    return results
