from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


def _build_url() -> str:
    return (
        f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        f"?ssl=disable"
    )


engine = create_async_engine(
    _build_url(),
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_pg() -> None:
    from app.models import ant_acc, audit_log, call_mapping, calling_agent, client_score, crm_permission, dealio_users, etl_sync_log, integration, role, scoring_rule, trades_mt4, user, user_preferences, vtiger_mttransactions, vtiger_trading_accounts  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # CLAUD-161: add open_price column if it doesn't exist yet
        # Lock timeout prevents hanging when REFRESH MATERIALIZED VIEW holds a lock.
        try:
            await conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            await conn.execute(
                text("ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS open_price NUMERIC(18,5)")
            )
        except Exception as _e:
            pass  # Column already exists or lock timeout — safe to skip


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
