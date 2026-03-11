import logging

from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.crm_permission import CrmPermission
from app.models.integration import Integration
from app.models.role import ALL_PAGES, Role
from app.models.user import User

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def seed_admin(session: AsyncSession) -> None:
    # Seed admin role — always sync permissions to current ALL_PAGES
    result = await session.execute(select(Role).where(Role.name == "admin"))
    admin_role = result.scalar_one_or_none()
    if not admin_role:
        admin_role = Role(name="admin", permissions=list(ALL_PAGES))
        session.add(admin_role)
        await session.flush()
        logger.info("Admin role created")
    elif sorted(admin_role.permissions) != sorted(ALL_PAGES):
        admin_role.permissions = list(ALL_PAGES)
        logger.info("Admin role permissions synced to ALL_PAGES: %s", ALL_PAGES)

    # Clean stale permissions from all non-admin roles (remove pages no longer in ALL_PAGES)
    all_roles_result = await session.execute(select(Role).where(Role.name != "admin"))
    valid_pages = set(ALL_PAGES)
    for role in all_roles_result.scalars().all():
        cleaned = [p for p in role.permissions if p in valid_pages]
        if len(cleaned) != len(role.permissions):
            removed = set(role.permissions) - valid_pages
            role.permissions = cleaned
            logger.info("Cleaned stale permissions %s from role '%s'", removed, role.name)

    # Seed admin user
    result = await session.execute(select(User).where(User.username == "admin"))
    if not result.scalar_one_or_none():
        admin = User(
            username="admin",
            email="admin@backoffice.local",
            hashed_password=hash_password("Hdtkfvi12345"),
            role="admin",
            is_active=True,
        )
        session.add(admin)
        logger.info("Admin user created")

    # Seed CRM integration if not already present
    result = await session.execute(select(Integration).where(Integration.name == "CRM API"))
    if not result.scalar_one_or_none():
        from app.config import settings as _settings
        crm_integration = Integration(
            name="CRM API",
            base_url=_settings.crm_api_base_url + "/crm-api/",
            auth_key=_settings.crm_api_token or None,
            description="CMTrading CRM API — retention status, user notes, user lookup",
            is_active=True,
        )
        session.add(crm_integration)
        logger.info("CRM integration seeded")

    # Seed SquareTalk integration if not already present
    result = await session.execute(select(Integration).where(Integration.name == "SquareTalk"))
    if not result.scalar_one_or_none():
        squaretalk_integration = Integration(
            name="SquareTalk",
            base_url="https://cmtrading.squaretalk.com/Integration",
            auth_key=None,
            description="SquareTalk telephony — click-to-call via agent extensions",
            is_active=True,
        )
        session.add(squaretalk_integration)
        logger.info("SquareTalk integration seeded")

    # Seed CRM permissions (RBAC) — idempotent: skip rows that already exist
    _default_permissions = [
        ("admin", "view_clients", True),
        ("admin", "edit_client_status", True),
        ("admin", "make_call", True),
        ("admin", "send_note", True),
        ("admin", "send_whatsapp", True),
        ("retention_manager", "view_clients", True),
        ("retention_manager", "edit_client_status", True),
        ("retention_manager", "make_call", True),
        ("retention_manager", "send_note", True),
        ("retention_manager", "send_whatsapp", True),
        ("team_leader", "view_clients", True),
        ("team_leader", "edit_client_status", True),
        ("team_leader", "make_call", True),
        ("team_leader", "send_note", True),
        ("team_leader", "send_whatsapp", True),
        ("agent", "view_clients", True),
        ("agent", "edit_client_status", False),
        ("agent", "make_call", True),
        ("agent", "send_note", True),
        ("agent", "send_whatsapp", True),
        ("admin", "export_data", True),
        ("retention_manager", "export_data", True),
        ("team_leader", "export_data", False),
        ("agent", "export_data", False),
    ]
    for _role, _action, _enabled in _default_permissions:
        exists = await session.execute(
            select(CrmPermission).where(
                CrmPermission.role == _role,
                CrmPermission.action == _action,
            )
        )
        if not exists.scalar_one_or_none():
            session.add(CrmPermission(role=_role, action=_action, enabled=_enabled))
            logger.info("Seeded crm_permission: %s / %s = %s", _role, _action, _enabled)

    await session.commit()
