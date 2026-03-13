import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.history_db import init_history_db
from app.pg_database import AsyncSessionLocal, init_pg
from app.replica_database import init_replica
from app.routers import calls, clients, filters
from app.routers.call_mappings import router as call_mappings_router
from app.routers.etl import daily_full_sync_all, incremental_sync_ant_acc, incremental_sync_dealio_users, incremental_sync_mtt, incremental_sync_trades, incremental_sync_vta, hourly_sync_vtiger_users, hourly_sync_vtiger_campaigns, hourly_sync_extensions, refresh_retention_mv, rebuild_retention_mv, sync_open_pnl_background, sync_card_types_background, sync_agent_targets, sync_account_exposure_cache, compute_active_status, router as etl_router
from app.routers.proline_etl import sync_proline_data, router as proline_router
from app.routers.retention import router as retention_router
from app.routers.retention_tasks import router as retention_tasks_router
from app.routers.client_scoring import router as client_scoring_router
from app.routers.crm import router as crm_router
from app.routers.auth import router as auth_router
from app.routers.roles_admin import router as roles_router
from app.routers.users_admin import router as users_router
from app.routers.integrations_admin import router as integrations_router
from app.routers.audit_log_admin import router as audit_log_router
from app.routers.preferences import router as preferences_router
from app.routers.favorites import router as favorites_router
from app.routers.dashboard import router as dashboard_router
from app.routers.permissions import router as permissions_router
from app.routers.admin import router as admin_router
from app.routers.call_dashboard import router as call_dashboard_router
from app.routers.elena_ai import router as elena_ai_router
from app.routers.performance_dashboard import router as performance_dashboard_router
from app.routers.active_status import router as active_status_router
from app.routers.column_visibility import router as column_visibility_router
from app.routers.password_reset import router as password_reset_router
from app.routers.challenges import router as challenges_router, reset_daily_challenges, reset_weekly_challenges, expire_flash_challenges
from app.routers.action_bonuses import router as action_bonuses_router
from app.routers.lifecycle import router as lifecycle_router
from app.routers.saved_searches import router as saved_searches_router
from app.routers.sendgrid_admin import router as sendgrid_router
from app.routers.batch_calls import router as batch_calls_router
from app.routers.agent_activity import router as agent_activity_router
from app.routers.protected_clients import router as protected_clients_router
from app.seed import seed_admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_history_db()
    await init_pg()
    init_replica()
    from sqlalchemy import text as _text
    # Lock timeout for all DDL migrations� prevents infinite hang if autovacuum or
    # another session holds a lock. try/except on each block so a timeout skips
    # that migration (columns already exist from previous runs) rather than crashing.
    _LT = "SET LOCAL lock_timeout = '10s'"

    # Add office/department to users table BEFORE seed_admin uses the ORM on users
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE users ADD COLUMN IF NOT EXISTS office VARCHAR(128)"))
            await session.execute(_text("ALTER TABLE users ADD COLUMN IF NOT EXISTS department VARCHAR(128)"))
            await session.execute(_text("ALTER TABLE users ADD COLUMN IF NOT EXISTS team VARCHAR(128)"))
            await session.commit()
        logger.info("users office/department columns migration applied")
    except Exception as _e:
        logger.warning("users office/department migration skipped: %s", _e)

    async with AsyncSessionLocal() as session:
        await seed_admin(session)
    # Mark any stale "running" jobs left over from a previous crash/restart
    async with AsyncSessionLocal() as session:
        await session.execute(
            _text("UPDATE etl_sync_log SET status='error', error_message='Interrupted by restart' WHERE status='running'")
        )
        await session.commit()
    # Add new ant_acc columns if not yet present (safe ADD COLUMN IF NOT EXISTS)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS is_test_account SMALLINT"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS sales_client_potential VARCHAR(100)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS birth_date DATE"))
            await session.commit()
        logger.info("ant_acc column migrations applied")
    except Exception as _e:
        logger.warning("ant_acc column migrations skipped (lock or already done): %s", _e)
    # Create performance indexes if missing (covers existing deployments)
    async with AsyncSessionLocal() as session:
        # Covering index for retention query: filters on (login, cmd, symbol), reads notional_value, open_time, close_time
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_trades_mt4_login_cmd_cov ON trades_mt4 (login, cmd) INCLUDE (symbol, notional_value, close_time, open_time)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_trades_mt4_close_time ON trades_mt4 (close_time)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_ant_acc_qual_date ON ant_acc (client_qualification_date)"
        ))
        # Covering index for deposits_agg join in retention query
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_mtt_login_approval_type ON vtiger_mttransactions (login, transactionapproval, transactiontype) INCLUDE (usdamount, confirmation_time, payment_method)"
        ))
        # Index for qualifying_logins join (vtigeraccountid lookup)
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_vta_vtigeraccountid ON vtiger_trading_accounts (vtigeraccountid)"
        ))
        await session.commit()
    logger.info("Performance indexes created/verified")
    # CLAUD-35: Additional indexes for 70-agent load readiness
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            # RBAC scope filter: agents filter by assigned_to on every request
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_retention_mv_assigned_to ON retention_mv (assigned_to)"
            ))
            # Base WHERE clause always filters on client_qualification_date
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_retention_mv_qual_date ON retention_mv (client_qualification_date)"
            ))
            # Team-leader RBAC subquery filters vtiger_users by department
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_vtiger_users_department ON vtiger_users (department)"
            ))
            # Open PNL lookup joins vtiger_trading_accounts on login
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_vta_login ON vtiger_trading_accounts (login)"
            ))
            await session.commit()
        logger.info("CLAUD-35 load-readiness indexes created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35 load-readiness indexes skipped (lock or MV not yet created): %s", _e)
    # Migrate: ensure retention_extra_columns table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS retention_extra_columns ("
            "id SERIAL PRIMARY KEY, "
            "display_name VARCHAR(128) NOT NULL, "
            "source_table VARCHAR(64) NOT NULL, "
            "source_column VARCHAR(128) NOT NULL, "
            "agg_fn VARCHAR(16) NOT NULL DEFAULT 'SUM', "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("retention_extra_columns migration applied")
    # Migrate: ensure retention_tasks table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS retention_tasks ("
            "id SERIAL PRIMARY KEY, "
            "name VARCHAR(255) NOT NULL, "
            "conditions TEXT NOT NULL DEFAULT '[]', "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("retention_tasks table migration applied")
    # Migrate: add color column to retention_tasks if not present
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "ALTER TABLE retention_tasks ADD COLUMN IF NOT EXISTS color VARCHAR(20) NOT NULL DEFAULT 'grey'"
            ))
            await session.commit()
        logger.info("retention_tasks.color column migration applied")
    except Exception as _e:
        logger.warning("retention_tasks.color migration skipped (lock or already done): %s", _e)
    # Migrate: ensure scoring_rules table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS scoring_rules ("
            "id SERIAL PRIMARY KEY, "
            "field VARCHAR(64) NOT NULL, "
            "operator VARCHAR(8) NOT NULL, "
            "value VARCHAR(64) NOT NULL, "
            "score INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("scoring_rules table migration applied")
    # Migrate: ensure client_scores table exists (CLAUD-24 hotfix)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS client_scores (
                accountid VARCHAR PRIMARY KEY,
                score INTEGER NOT NULL DEFAULT 0,
                computed_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.commit()
    logger.info("client_scores table migration applied")
    # Widen sync_type column if still VARCHAR(20) — dealio_users_incremental is 24 chars
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE etl_sync_log ALTER COLUMN sync_type TYPE VARCHAR(50)"))
            await session.commit()
        logger.info("etl_sync_log.sync_type column widened to VARCHAR(50)")
    except Exception as _e:
        logger.warning("etl_sync_log.sync_type migration skipped (lock or already done): %s", _e)
    # Add equity column to dealio_users if missing
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE dealio_users ADD COLUMN IF NOT EXISTS equity FLOAT"))
            await session.commit()
        logger.info("dealio_users.equity column migration applied")
    except Exception as _e:
        logger.warning("dealio_users.equity migration skipped (lock or already done): %s", _e)
    # Add assigned_to column to ant_acc if missing
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS assigned_to VARCHAR(50)"))
            await session.commit()
        logger.info("ant_acc.assigned_to column migration applied")
    except Exception as _e:
        logger.warning("ant_acc.assigned_to migration skipped (lock or already done): %s", _e)
    # Add full_name / firstname / lastname columns to ant_acc if missing
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS full_name VARCHAR(400)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS firstname VARCHAR(255)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS lastname VARCHAR(255)"))
            await session.commit()
        logger.info("ant_acc.full_name/firstname/lastname column migrations applied")
    except Exception as _e:
        logger.warning("ant_acc.full_name migrations skipped (lock or already done): %s", _e)
    # Add extended ant_acc columns from MSSQL source (email, country, language, status, financials)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS email VARCHAR(255)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS country_iso VARCHAR(10)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS customer_language VARCHAR(50)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS accountstatus VARCHAR(255)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS regulation VARCHAR(255)"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS createdtime TIMESTAMP"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS first_deposit_date TIMESTAMP"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS ftd_amount INTEGER"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS total_deposit INTEGER"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS total_withdrawal INTEGER"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS net_deposit INTEGER"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS funded SMALLINT"))
            await session.execute(_text("ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS original_affiliate VARCHAR(255)"))
            await session.commit()
        logger.info("ant_acc extended columns migration applied")
    except Exception as _e:
        logger.warning("ant_acc extended columns migration skipped (lock or already done): %s", _e)
    # Ensure vtiger_users table exists with correct schema (never drop — data is wiped on restart otherwise)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE TABLE IF NOT EXISTS vtiger_users ("
                "id VARCHAR(50) PRIMARY KEY, "
                "user_name TEXT, first_name TEXT, last_name TEXT, "
                "email TEXT, phone TEXT, department TEXT, status TEXT, "
                "office TEXT, position TEXT, fax TEXT)"
            ))
            await session.commit()
        logger.info("vtiger_users table ensured with correct schema")
    except Exception as _e:
        logger.warning("vtiger_users recreate skipped (lock or already done): %s", _e)
    # Ensure vtiger_campaigns table exists (never drop — use CREATE IF NOT EXISTS)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE TABLE IF NOT EXISTS vtiger_campaigns ("
                "crmid VARCHAR(50) PRIMARY KEY, "
                "campaign_id TEXT, campaign_name TEXT, "
                "campaign_legacy_id TEXT, campaign_channel TEXT, campaign_sub_channel TEXT)"
            ))
            await session.commit()
        logger.info("vtiger_campaigns table ensured with correct schema")
    except Exception as _e:
        logger.warning("vtiger_campaigns migration skipped (already done): %s", _e)
    # Migrate: ensure extensions table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS extensions ("
            "id SERIAL PRIMARY KEY, "
            "name TEXT, extension VARCHAR(50) UNIQUE, "
            "user_name TEXT, agent_name TEXT, manager TEXT, "
            "position TEXT, office TEXT, email TEXT, manager_email TEXT, "
            "synced_at TIMESTAMPTZ DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("extensions table migration applied")
    # Migrate: ensure integrations table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS integrations ("
            "id SERIAL PRIMARY KEY, "
            "name VARCHAR(255) NOT NULL, "
            "base_url VARCHAR(500) NOT NULL, "
            "auth_key VARCHAR(500), "
            "description TEXT, "
            "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("integrations table migration applied")
    # Migrate: ensure audit_log table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS audit_log ("
            "id SERIAL PRIMARY KEY, "
            "agent_id INTEGER NOT NULL, "
            "agent_username VARCHAR(64) NOT NULL, "
            "client_account_id VARCHAR(64) NOT NULL, "
            "action_type VARCHAR(32) NOT NULL, "
            "action_value TEXT, "
            "timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_agent_username ON audit_log (agent_username)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_client_account_id ON audit_log (client_account_id)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_action_type ON audit_log (action_type)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_audit_log_timestamp ON audit_log (timestamp)"
        ))
        await session.commit()
    logger.info("audit_log table migration applied")
    # Migrate: ensure user_preferences table exists (CLAUD-25)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                id SERIAL PRIMARY KEY,
                username VARCHAR NOT NULL UNIQUE,
                retention_column_order JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_user_preferences_username ON user_preferences (username)"
        ))
        await session.commit()
    logger.info("user_preferences table migration applied")
    # Migrate: ensure client_task_assignments lookup table exists (perf: replaces per-page N-queries)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS client_task_assignments ("
            "accountid TEXT, "
            "task_id INTEGER, "
            "PRIMARY KEY (accountid, task_id))"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS idx_cta_accountid ON client_task_assignments (accountid)"
        ))
        await session.commit()
    logger.info("client_task_assignments table migration applied")
    # Migrate: ensure open_pnl_cache table exists (local mirror of dealio.positions aggregated by login)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS open_pnl_cache ("
            "login TEXT PRIMARY KEY, "
            "pnl NUMERIC NOT NULL DEFAULT 0, "
            "updated_at TIMESTAMPTZ DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("open_pnl_cache table migration applied")
    # Migrate: add exposure_usd column to open_pnl_cache if missing
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "ALTER TABLE open_pnl_cache ADD COLUMN IF NOT EXISTS exposure_usd NUMERIC NOT NULL DEFAULT 0"
        ))
        await session.commit()
    logger.info("open_pnl_cache exposure_usd column migration applied")
    # CLAUD-77: Migrate: ensure account_exposure_cache table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS account_exposure_cache ("
            "accountid TEXT PRIMARY KEY, "
            "exposure_usd NUMERIC NOT NULL DEFAULT 0, "
            "exposure_pct NUMERIC, "
            "updated_at TIMESTAMPTZ DEFAULT NOW())"
        ))
        await session.commit()
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_aec_accountid ON account_exposure_cache (accountid)"
            ))
            await session.commit()
    except Exception as _e:
        logger.warning("CLAUD-77: ix_aec_accountid index skipped (lock or already done): %s", _e)
    logger.info("CLAUD-77: account_exposure_cache table migration applied")
    # Migrate: ensure user_favorites table exists (CLAUD-50)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS user_favorites ("
            "id SERIAL PRIMARY KEY, "
            "user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, "
            "accountid VARCHAR(100) NOT NULL, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
            "UNIQUE (user_id, accountid))"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_user_favorites_user_id ON user_favorites (user_id)"
        ))
        await session.commit()
    logger.info("user_favorites table migration applied")
    # Migrate: ensure agent_targets table exists (CLAUD-56 performance bar)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS agent_targets (
                id         SERIAL PRIMARY KEY,
                agent_id   INTEGER NOT NULL,
                month_date DATE NOT NULL,
                net        INTEGER NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (agent_id, month_date)
            )
        """))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_agent_targets_month ON agent_targets (month_date)"
        ))
        await session.commit()
    logger.info("agent_targets table migration applied")
    # Migrate: add agent_email column + widen net to NUMERIC (CLAUD-70 performance dashboard)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE agent_targets ADD COLUMN IF NOT EXISTS agent_email TEXT"))
            await session.execute(_text("ALTER TABLE agent_targets ALTER COLUMN net TYPE NUMERIC(18,2)"))
            await session.commit()
        logger.info("CLAUD-70: agent_targets schema upgrade applied (agent_email + numeric net)")
    except Exception as _e:
        logger.warning("CLAUD-70: agent_targets schema upgrade skipped: %s", _e)
    # Migrate: add creditcardlast column to vtiger_mttransactions if missing
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("ALTER TABLE vtiger_mttransactions ADD COLUMN IF NOT EXISTS creditcardlast VARCHAR(30)"))
            await session.commit()
        logger.info("vtiger_mttransactions.creditcardlast column migration applied")
    except Exception as _e:
        logger.warning("vtiger_mttransactions.creditcardlast migration skipped (lock or already done): %s", _e)
    # Migrate: ensure iin_cache table exists (IIN/BIN → card product name)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS iin_cache ("
            "iin VARCHAR(10) PRIMARY KEY, "
            "card_type VARCHAR(255), "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("iin_cache table migration applied")
    # Migrate: ensure client_card_type table exists (per-client card product name)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS client_card_type ("
            "accountid VARCHAR(100) PRIMARY KEY, "
            "card_type VARCHAR(255), "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.commit()
    logger.info("client_card_type table migration applied")
    # Migrate: ensure proline_data table exists (Proline affiliate data sync)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS proline_data ("
            "account_id VARCHAR(50) PRIMARY KEY, "
            "affiliate_id VARCHAR(100), "
            "performance_commission NUMERIC(18,4), "
            "qualified_ftd_date TIMESTAMPTZ, "
            "synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_proline_data_affiliate_id ON proline_data (affiliate_id)"
        ))
        await session.commit()
    logger.info("proline_data table migration applied")
    # Migrate: ensure crm_permissions table exists (CLAUD-16 RBAC)
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS crm_permissions ("
            "id SERIAL PRIMARY KEY, "
            "role VARCHAR(32) NOT NULL, "
            "action VARCHAR(64) NOT NULL, "
            "enabled BOOLEAN NOT NULL DEFAULT TRUE)"
        ))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_crm_permissions_role ON crm_permissions (role)"
        ))
        await session.commit()
    logger.info("crm_permissions table migration applied")
    # Migrate: ensure client_callbacks table exists (CLAUD-51 Call Dashboard)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS client_callbacks (
                id            SERIAL PRIMARY KEY,
                accountid     VARCHAR(100) NOT NULL,
                agent_id      INTEGER REFERENCES users(id) ON DELETE SET NULL,
                callback_time TIMESTAMPTZ NOT NULL,
                note          TEXT,
                is_done       BOOLEAN NOT NULL DEFAULT FALSE,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.commit()
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_callbacks_agent_time ON client_callbacks (agent_id, callback_time) WHERE NOT is_done"
            ))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_callbacks_accountid ON client_callbacks (accountid)"
            ))
            await session.commit()
    except Exception as _e:
        logger.warning("client_callbacks indexes skipped (lock or already done): %s", _e)
    logger.info("client_callbacks table migration applied")
    # CLAUD-79: Migrate — ensure password_resets table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS password_resets (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash VARCHAR(64) NOT NULL UNIQUE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMP NOT NULL,
                used_at    TIMESTAMP
            )
        """))
        await session.commit()
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_pr_user_created ON password_resets(user_id, created_at)"
            ))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_pr_token_hash ON password_resets(token_hash)"
            ))
            await session.commit()
    except Exception as _e:
        logger.warning("CLAUD-79: password_resets indexes skipped (lock or already done): %s", _e)
    logger.info("CLAUD-79: password_resets table migration applied")
    # CLAUD-86: Ensure client_active_status table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS client_active_status (
                accountid           VARCHAR PRIMARY KEY,
                is_active           BOOLEAN NOT NULL DEFAULT FALSE,
                cond_open_positions BOOLEAN NOT NULL DEFAULT FALSE,
                cond_recent_trade   BOOLEAN NOT NULL DEFAULT FALSE,
                cond_recent_deposit BOOLEAN NOT NULL DEFAULT FALSE,
                computed_at         TIMESTAMP,
                is_manual_override  BOOLEAN NOT NULL DEFAULT FALSE,
                override_by         INTEGER,
                override_at         TIMESTAMP,
                override_expires_at TIMESTAMP
            )
        """))
        await session.commit()
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_cas_is_active ON client_active_status(is_active)"
            ))
            await session.commit()
    except Exception as _e:
        logger.warning("CLAUD-86: idx_cas_is_active index skipped (lock or already done): %s", _e)
    logger.info("CLAUD-86: client_active_status table migration applied")
    # CLAUD-86: Ensure system_metrics table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS system_metrics (
                key         VARCHAR PRIMARY KEY,
                value       VARCHAR,
                computed_at TIMESTAMP
            )
        """))
        await session.commit()
    logger.info("CLAUD-86: system_metrics table migration applied")
    # CLAUD-114: Local retention status override table (stores status set via PUT /retention-status)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS client_retention_status (
                accountid    VARCHAR(100) PRIMARY KEY,
                status_key   INTEGER NOT NULL,
                status_label VARCHAR(255) NOT NULL,
                updated_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await session.commit()
    logger.info("CLAUD-114: client_retention_status table migration applied")
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS retention_status INTEGER"
            ))
            await session.commit()
        logger.info("ant_acc: retention_status column migration applied")
    except Exception as _e:
        logger.warning("ant_acc: retention_status column migration skipped (lock or already done): %s", _e)

    # CLAUD-167: Legacy ID (customer_id from MSSQL report.ant_acc)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "ALTER TABLE ant_acc ADD COLUMN IF NOT EXISTS customer_id VARCHAR(100)"
            ))
            await session.commit()
        logger.info("ant_acc: customer_id column migration applied")
    except Exception as _e:
        logger.warning("ant_acc: customer_id migration skipped: %s", _e)

    # CLAUD-170: Add volume (lot size) to trades_mt4 for correct lot display
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "ALTER TABLE trades_mt4 ADD COLUMN IF NOT EXISTS volume NUMERIC(18,4)"
            ))
            await session.commit()
        logger.info("CLAUD-170: trades_mt4.volume column migration applied")
    except Exception as _e:
        logger.warning("CLAUD-170: trades_mt4.volume migration skipped: %s", _e)

    # CLAUD-116: Sales client potential & segmentation profile table
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS client_sales_profile (
                account_id      VARCHAR(100) PRIMARY KEY,
                client_potential VARCHAR(50),
                client_segment   VARCHAR(50),
                updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await session.commit()
    logger.info("CLAUD-116: client_sales_profile table migration applied")

    # CLAUD-122: Saved searches table
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS saved_searches (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id         INTEGER NOT NULL,
                name            VARCHAR(40) NOT NULL,
                filters         JSONB NOT NULL DEFAULT '{}',
                column_order    JSONB NOT NULL DEFAULT '[]',
                column_visibility JSONB NOT NULL DEFAULT '{}',
                col_filters     JSONB NOT NULL DEFAULT '{}',
                sort_field      VARCHAR(100),
                sort_direction  VARCHAR(4),
                status_filter   VARCHAR(50),
                created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (user_id, name)
            )
        """))
        await session.commit()
    logger.info("CLAUD-122: saved_searches table migration applied")

    # CLAUD-87: DB indexes for retention_mv and related tables
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_retention_mv_status ON retention_mv(sales_client_potential)"
            ))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_retention_mv_status_score ON retention_mv(sales_client_potential, accountid)"
            ))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_client_scores_accountid ON client_scores(accountid)"
            ))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS idx_aec_accountid ON account_exposure_cache(accountid)"
            ))
            await session.commit()
        logger.info("CLAUD-87: retention_mv and related indexes created/verified")
    except Exception as _e:
        logger.warning("CLAUD-87: index creation skipped (lock or MV not yet created): %s", _e)
    # Migrate: add 'retention-dial' page permission to all existing roles (CLAUD-63)
    try:
        from app.models.role import Role as _Role
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select as _sel
            all_roles = (await session.execute(_sel(_Role))).scalars().all()
            for _r in all_roles:
                if "retention-dial" not in (_r.permissions or []):
                    _r.permissions = list(_r.permissions or []) + ["retention-dial"]
                    logger.info("CLAUD-63: added 'retention-dial' permission to role '%s'", _r.name)
            await session.commit()
        logger.info("CLAUD-63: retention-dial permission migration applied")
    except Exception as _e:
        logger.warning("CLAUD-63: retention-dial migration skipped: %s", _e)
    # Migrate: add 'performance-dashboard' page permission to all existing roles (CLAUD-70)
    try:
        from app.models.role import Role as _Role70
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select as _sel70
            all_roles70 = (await session.execute(_sel70(_Role70))).scalars().all()
            for _r70 in all_roles70:
                if "performance-dashboard" not in (_r70.permissions or []):
                    _r70.permissions = list(_r70.permissions or []) + ["performance-dashboard"]
                    logger.info("CLAUD-70: added 'performance-dashboard' permission to role '%s'", _r70.name)
            await session.commit()
        logger.info("CLAUD-70: performance-dashboard permission migration applied")
    except Exception as _e:
        logger.warning("CLAUD-70: performance-dashboard migration skipped: %s", _e)
    # Migrate: add 'elena-ai-upload' page permission to all existing roles (CLAUD-69)
    try:
        from app.models.role import Role as _Role69
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select as _sel69
            all_roles69 = (await session.execute(_sel69(_Role69))).scalars().all()
            for _r69 in all_roles69:
                if "elena-ai-upload" not in (_r69.permissions or []):
                    _r69.permissions = list(_r69.permissions or []) + ["elena-ai-upload"]
                    logger.info("CLAUD-69: added 'elena-ai-upload' permission to role '%s'", _r69.name)
            await session.commit()
        logger.info("CLAUD-69: elena-ai-upload permission migration applied")
    except Exception as _e:
        logger.warning("CLAUD-69: elena-ai-upload migration skipped: %s", _e)
    # Migrate: add 'retention_grid_export' permission to privileged roles (CLAUD-120)
    try:
        from app.models.role import Role as _Role120
        _EXPORT_ROLES = {"admin", "cro", "team_leader", "retention_manager"}
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select as _sel120
            all_roles120 = (await session.execute(_sel120(_Role120))).scalars().all()
            for _r120 in all_roles120:
                has_export = "retention_grid_export" in (_r120.permissions or [])
                should_have = _r120.name in _EXPORT_ROLES
                if should_have and not has_export:
                    _r120.permissions = list(_r120.permissions or []) + ["retention_grid_export"]
                    logger.info("CLAUD-120: added 'retention_grid_export' to role '%s'", _r120.name)
            await session.commit()
        logger.info("CLAUD-120: retention_grid_export permission migration applied")
    except Exception as _e:
        logger.warning("CLAUD-120: retention_grid_export migration skipped: %s", _e)
    # Migrate: add 'agent_activity' permission to privileged roles (CLAUD-156)
    try:
        from app.models.role import Role as _Role156
        _ACTIVITY_ROLES = {"admin", "retention_manager", "team_leader", "agent"}
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select as _sel156
            all_roles156 = (await session.execute(_sel156(_Role156))).scalars().all()
            for _r156 in all_roles156:
                has_perm = "agent_activity" in (_r156.permissions or [])
                should_have = _r156.name in _ACTIVITY_ROLES
                if should_have and not has_perm:
                    _r156.permissions = list(_r156.permissions or []) + ["agent_activity"]
                    from sqlalchemy.orm.attributes import flag_modified as _fm156
                    _fm156(_r156, "permissions")
                    logger.info("CLAUD-156: added 'agent_activity' to role '%s'", _r156.name)
            await session.commit()
        logger.info("CLAUD-156: agent_activity permission migration applied")
    except Exception as _e:
        logger.warning("CLAUD-156: agent_activity migration skipped: %s", _e)

    # CLAUD-85: Migrate — ensure role_column_visibility table exists + seed defaults
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS role_column_visibility ("
            "role_name VARCHAR NOT NULL, "
            "column_key VARCHAR NOT NULL, "
            "is_visible BOOLEAN NOT NULL DEFAULT TRUE, "
            "updated_at TIMESTAMP DEFAULT NOW(), "
            "PRIMARY KEY (role_name, column_key))"
        ))
        await session.commit()
    logger.info("CLAUD-85: role_column_visibility table migration applied")
    # Seed default visibility rows (insert if not exists)
    try:
        from app.routers.column_visibility import DEFAULT_VISIBILITY, ALL_ROLES
        async with AsyncSessionLocal() as session:
            for col_key, roles_map in DEFAULT_VISIBILITY.items():
                for role_name in ALL_ROLES:
                    is_visible = roles_map.get(role_name, True)
                    await session.execute(_text(
                        "INSERT INTO role_column_visibility (role_name, column_key, is_visible) "
                        "VALUES (:role, :col, :vis) "
                        "ON CONFLICT (role_name, column_key) DO NOTHING"
                    ), {"role": role_name, "col": col_key, "vis": is_visible})
            await session.commit()
            logger.info("CLAUD-85: role_column_visibility default rows seeded")
    except Exception as _e:
        logger.warning("CLAUD-85: role_column_visibility seeding skipped: %s", _e)

    # CLAUD-89: Ensure challenges table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS challenges (
                "challengeId" SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                type VARCHAR(20) NOT NULL CHECK (type IN ('trade', 'volume')),
                targetvalue DECIMAL(10,2) NOT NULL,
                timeperiod VARCHAR(20) NOT NULL DEFAULT 'daily',
                isactive SMALLINT NOT NULL DEFAULT 1,
                rewardtype VARCHAR(20) NOT NULL DEFAULT 'credit',
                rewardamount DECIMAL(10,2) NOT NULL,
                "InsertDate" TIMESTAMP NOT NULL DEFAULT NOW(),
                group_name VARCHAR(100),
                audience_criteria JSONB
            )
        """))
        await session.commit()
    logger.info("CLAUD-89: challenges table migration applied")
    # CLAUD-89: Ensure challenge_client_progress table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS challenge_client_progress (
                id SERIAL PRIMARY KEY,
                "challengeId" INT NOT NULL,
                trading_account_id VARCHAR(50) NOT NULL,
                progress_value DECIMAL(10,2) DEFAULT 0,
                last_rewarded_tier INT DEFAULT 0,
                date DATE NOT NULL,
                UNIQUE ("challengeId", trading_account_id, date)
            )
        """))
        await session.commit()
    logger.info("CLAUD-89: challenge_client_progress table migration applied")
    # CLAUD-90: add accountid, status, total_reward columns to challenge_client_progress
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("ALTER TABLE challenge_client_progress ADD COLUMN IF NOT EXISTS accountid VARCHAR(100)"))
            await session.execute(_text("ALTER TABLE challenge_client_progress ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'Open'"))
            await session.execute(_text("ALTER TABLE challenge_client_progress ADD COLUMN IF NOT EXISTS total_reward DECIMAL(10,2) NOT NULL DEFAULT 0"))
            await session.commit()
        logger.info("CLAUD-90: challenge_client_progress columns migration applied")
    except Exception as _e:
        logger.warning("CLAUD-90: challenge_client_progress migration skipped: %s", _e)
    # CLAUD-89: Ensure challenge_credit_log table exists (audit trail)
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS challenge_credit_log (
                id SERIAL PRIMARY KEY,
                "challengeId" INT NOT NULL,
                trading_account_id VARCHAR(50) NOT NULL,
                reward_amount DECIMAL(10,2) NOT NULL,
                api_response TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_ccl_account ON challenge_credit_log (trading_account_id)'
        ))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_ccp_account_date ON challenge_client_progress (trading_account_id, date)'
        ))
        await session.commit()
    logger.info("CLAUD-89: challenge_credit_log table migration applied")
    # CLAUD-91: Ensure optimove_event_log table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS optimove_event_log (
                id SERIAL PRIMARY KEY,
                "challengeId" INTEGER,
                accountid VARCHAR(100),
                event_name VARCHAR(100) NOT NULL,
                payload JSONB,
                response TEXT,
                success BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_oel_created ON optimove_event_log (created_at DESC)'
        ))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_oel_accountid ON optimove_event_log (accountid)'
        ))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_oel_event ON optimove_event_log (event_name)'
        ))
        await session.commit()
    logger.info("CLAUD-91: optimove_event_log table migration applied")

    # CLAUD-92: Extend challenges.type CHECK constraint to include 'streak'
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.check_constraints
                    WHERE constraint_name = 'challenges_type_check'
                    AND constraint_catalog = current_database()
                ) THEN
                    ALTER TABLE challenges DROP CONSTRAINT IF EXISTS challenges_type_check;
                    ALTER TABLE challenges ADD CONSTRAINT challenges_type_check
                        CHECK (type IN ('trade', 'volume', 'streak', 'pnl'));
                END IF;
            END $$;
        """))
        await session.commit()
    logger.info("CLAUD-92: challenges.type CHECK constraint extended to include streak")

    # CLAUD-92: Ensure challenge_client_streaks table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS challenge_client_streaks (
                id SERIAL PRIMARY KEY,
                group_name VARCHAR(100) NOT NULL,
                accountid VARCHAR(100) NOT NULL,
                current_streak INTEGER DEFAULT 0,
                last_trade_date DATE,
                last_rewarded_tier INTEGER DEFAULT 0,
                total_reward DECIMAL(10,2) DEFAULT 0,
                UNIQUE (group_name, accountid)
            )
        """))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_ccs_group_acc ON challenge_client_streaks (group_name, accountid)'
        ))
        await session.commit()
    logger.info("CLAUD-92: challenge_client_streaks table migration applied")

    # CLAUD-94/95: Extend challenges.type CHECK constraint to include 'diversity'
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.check_constraints
                    WHERE constraint_name = 'challenges_type_check'
                    AND constraint_catalog = current_database()
                ) THEN
                    ALTER TABLE challenges DROP CONSTRAINT IF EXISTS challenges_type_check;
                    ALTER TABLE challenges ADD CONSTRAINT challenges_type_check
                        CHECK (type IN ('trade', 'volume', 'streak', 'pnl', 'diversity', 'instrument'));
                END IF;
            END $$;
        """))
        await session.commit()
    logger.info("CLAUD-94: challenges.type CHECK constraint extended to include diversity and instrument")

    # CLAUD-95: Add valid_until and reward_multiplier columns to challenges table
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE challenges ADD COLUMN IF NOT EXISTS valid_until TIMESTAMP NULL"
            ))
            await session.execute(_text(
                "ALTER TABLE challenges ADD COLUMN IF NOT EXISTS reward_multiplier DECIMAL(4,2) NOT NULL DEFAULT 1.00"
            ))
            await session.commit()
        logger.info("CLAUD-95: challenges valid_until + reward_multiplier columns added")
    except Exception as _e:
        logger.warning("CLAUD-95: challenges column migration skipped: %s", _e)

    # CSV audience + expires_on: Add expires_on column to challenges table
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE challenges ADD COLUMN IF NOT EXISTS expires_on DATE NULL"
            ))
            await session.commit()
        logger.info("challenges.expires_on column added")
    except Exception as _e:
        logger.warning("challenges.expires_on migration skipped: %s", _e)

    # instrument challenge: Add symbol column to challenges table
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE challenges ADD COLUMN IF NOT EXISTS symbol VARCHAR(20) NULL"
            ))
            await session.commit()
        logger.info("challenges.symbol column added")
    except Exception as _e:
        logger.warning("challenges.symbol migration skipped: %s", _e)

    # instrument challenge: Add group_name and accountid columns to challenge_credit_log
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE challenge_credit_log ADD COLUMN IF NOT EXISTS group_name VARCHAR(100) NULL"
            ))
            await session.execute(_text(
                "ALTER TABLE challenge_credit_log ADD COLUMN IF NOT EXISTS accountid VARCHAR(100) NULL"
            ))
            await session.commit()
        logger.info("challenge_credit_log.group_name + accountid columns added")
    except Exception as _e:
        logger.warning("challenge_credit_log column migration skipped: %s", _e)

    # CLAUD-94: Ensure challenge_client_instruments table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS challenge_client_instruments (
                id SERIAL PRIMARY KEY,
                group_name VARCHAR(100) NOT NULL,
                accountid VARCHAR(100) NOT NULL,
                asset_class VARCHAR(50) NOT NULL,
                week_start DATE NOT NULL,
                UNIQUE (group_name, accountid, asset_class, week_start)
            )
        """))
        await session.execute(_text(
            'CREATE INDEX IF NOT EXISTS ix_cci_group_acc_week ON challenge_client_instruments (group_name, accountid, week_start)'
        ))
        await session.commit()
    logger.info("CLAUD-94: challenge_client_instruments table migration applied")

    # CLAUD-94: Ensure symbol_asset_class table exists and seed common symbols
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS symbol_asset_class (
                symbol VARCHAR(20) PRIMARY KEY,
                asset_class VARCHAR(20) NOT NULL CHECK (asset_class IN ('forex', 'commodity', 'index', 'crypto', 'stock'))
            )
        """))
        # Seed common symbols (ON CONFLICT DO NOTHING = idempotent)
        seed_symbols = [
            ("EURUSD", "forex"), ("GBPUSD", "forex"), ("USDJPY", "forex"),
            ("AUDUSD", "forex"), ("USDCHF", "forex"), ("EURGBP", "forex"),
            ("USDCAD", "forex"), ("NZDUSD", "forex"), ("EURJPY", "forex"),
            ("GBPJPY", "forex"), ("XAUUSD", "commodity"), ("XAGUSD", "commodity"),
            ("USOIL", "commodity"), ("UKOIL", "commodity"), ("XPDUSD", "commodity"),
            ("US30", "index"), ("US500", "index"), ("NAS100", "index"),
            ("GER40", "index"), ("UK100", "index"), ("JPN225", "index"),
            ("BTCUSD", "crypto"), ("ETHUSD", "crypto"), ("LTCUSD", "crypto"),
            ("XRPUSD", "crypto"), ("AAPL", "stock"), ("TSLA", "stock"),
            ("AMZN", "stock"), ("GOOGL", "stock"), ("MSFT", "stock"),
        ]
        for sym, cls in seed_symbols:
            await session.execute(
                _text(
                    "INSERT INTO symbol_asset_class (symbol, asset_class) VALUES (:sym, :cls) "
                    "ON CONFLICT (symbol) DO NOTHING"
                ),
                {"sym": sym, "cls": cls},
            )
        await session.commit()
    logger.info("CLAUD-94: symbol_asset_class table created and seeded")

    # CLAUD-96: Action Bonuses tables
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS action_bonus_rules (
                    id SERIAL PRIMARY KEY,
                    action VARCHAR(20) NOT NULL CHECK (action IN ('live_details', 'submit_documents')),
                    countries JSONB NULL,
                    affiliates JSONB NULL,
                    reward_amount DECIMAL(10,2) NOT NULL,
                    reward_type VARCHAR(20) NOT NULL DEFAULT 'credit',
                    priority INT NOT NULL DEFAULT 0,
                    isactive BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS action_bonus_log (
                    id SERIAL PRIMARY KEY,
                    rule_id INT NOT NULL,
                    accountid VARCHAR(100) NOT NULL,
                    trading_account_id VARCHAR(50) NOT NULL,
                    action VARCHAR(20) NOT NULL,
                    reward_amount DECIMAL(10,2) NOT NULL,
                    country VARCHAR(100),
                    affiliate VARCHAR(100),
                    credit_api_response TEXT,
                    success BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (accountid, action)
                )
            """))
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_abl_action_created ON action_bonus_log (action, created_at DESC)"
            ))
            await session.commit()
        logger.info("CLAUD-96: action_bonus_rules and action_bonus_log tables created")
    except Exception as _e:
        logger.warning("CLAUD-96: action bonus tables migration skipped: %s", _e)

    # CLAUD-97: Lifecycle stages tables
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS lifecycle_stages (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(64) NOT NULL,
                    key VARCHAR(64) NOT NULL UNIQUE,
                    metric_type VARCHAR(20) NOT NULL CHECK (metric_type IN ('ftd','deposit','position','volume','custom')),
                    threshold DECIMAL(15,2) NOT NULL,
                    display_order INT NOT NULL DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS client_lifecycle_progress (
                    client_id VARCHAR(50) NOT NULL,
                    stage_id INT NOT NULL REFERENCES lifecycle_stages(id) ON DELETE CASCADE,
                    achieved_at TIMESTAMP,
                    PRIMARY KEY (client_id, stage_id)
                )
            """))
            await session.commit()
        logger.info("CLAUD-97: lifecycle_stages + client_lifecycle_progress tables created")
    except Exception as _e:
        logger.warning("CLAUD-97: lifecycle tables migration skipped: %s", _e)
    # CLAUD-97: Seed default lifecycle stages (only if table is empty)
    try:
        async with AsyncSessionLocal() as session:
            count_row = await session.execute(_text("SELECT COUNT(*) FROM lifecycle_stages"))
            if count_row.fetchone()[0] == 0:
                await session.execute(_text("""
                    INSERT INTO lifecycle_stages (name, key, metric_type, threshold, display_order, is_active)
                    VALUES
                        ('FTD',     'ftd',      'ftd',      1,         1, true),
                        ('1st Pos', '1st_pos',  'position', 1,         2, true),
                        ('2x Dep',  '2x_dep',   'deposit',  2,         3, true),
                        ('1K Vol',  '1k_vol',   'volume',   1000,      4, true),
                        ('3x Dep',  '3x_dep',   'deposit',  3,         5, true),
                        ('50 Pos',  '50_pos',   'position', 50,        6, true),
                        ('1M Vol',  '1m_vol',   'volume',   1000000,   7, true)
                """))
                await session.commit()
                logger.info("CLAUD-97: default lifecycle stages seeded")
            else:
                logger.info("CLAUD-97: lifecycle_stages already populated — skipping seed")
    except Exception as _e:
        logger.warning("CLAUD-97: lifecycle stages seeding skipped: %s", _e)

    # CLAUD-105: Add value_min and value_max columns to scoring_rules (between operator)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE scoring_rules ADD COLUMN IF NOT EXISTS value_min DECIMAL(15,4) NULL"
            ))
            await session.commit()
        logger.info("CLAUD-105: scoring_rules.value_min column added")
    except Exception as _e:
        logger.warning("CLAUD-105: scoring_rules.value_min migration skipped: %s", _e)
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "ALTER TABLE scoring_rules ADD COLUMN IF NOT EXISTS value_max DECIMAL(15,4) NULL"
            ))
            await session.commit()
        logger.info("CLAUD-105: scoring_rules.value_max column added")
    except Exception as _e:
        logger.warning("CLAUD-105: scoring_rules.value_max migration skipped: %s", _e)

    # CLAUD-35 (extended): Critical composite index on audit_log for last-contact join.
    # The _LAST_COMM_JOIN in retention.py aggregates: WHERE action_type IN (...) GROUP BY client_account_id.
    # A composite (client_account_id, action_type) covering index turns a full table scan into
    # an index-only scan — critical when 70 agents each load a 50-row page simultaneously.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_audit_log_account_action "
                "ON audit_log (client_account_id, action_type)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_audit_log_account_action composite index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_audit_log_account_action skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index for call-dashboard/performance — filters audit_log by
    # (agent_id, action_type, timestamp) on every performance card load.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_audit_log_agent_action_ts "
                "ON audit_log (agent_id, action_type, timestamp)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_audit_log_agent_action_ts index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_audit_log_agent_action_ts skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Reverse composite on user_favorites for the retention JOIN.
    # The JOIN is: ON uf.accountid = m.accountid AND uf.user_id = :uid
    # Planner can use either direction; this ensures an index-only path from accountid side.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_user_favorites_accountid_user "
                "ON user_favorites (accountid, user_id)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_user_favorites_accountid_user index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_user_favorites_accountid_user skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on client_task_assignments(task_id) for the EXISTS subquery
    # that filters by task name: JOIN retention_tasks rt ON rt.id = cta.task_id.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_cta_task_id ON client_task_assignments (task_id)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_cta_task_id index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_cta_task_id skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on challenge_client_progress(accountid) — queried in
    # GET /api/challenges/progress filtered by accountid, and in webhook lookup.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_ccp_accountid ON challenge_client_progress (accountid)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_ccp_accountid index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_ccp_accountid skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on challenge_client_progress("challengeId") — webhook
    # looks up progress records by challengeId per batch.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                'CREATE INDEX IF NOT EXISTS ix_ccp_challenge_id ON challenge_client_progress ("challengeId")'
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_ccp_challenge_id index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_ccp_challenge_id skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Composite index on challenge_credit_log("challengeId", trading_account_id)
    # — webhook checks for duplicate credits per challenge + account.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                'CREATE INDEX IF NOT EXISTS ix_ccl_challenge_account '
                'ON challenge_credit_log ("challengeId", trading_account_id)'
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_ccl_challenge_account index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_ccl_challenge_account skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Partial composite index on client_callbacks(accountid, agent_id)
    # for the save_call UPDATE and get_client_detail query:
    # WHERE accountid = :aid AND agent_id = :uid AND NOT is_done
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_callbacks_account_agent "
                "ON client_callbacks (accountid, agent_id) WHERE NOT is_done"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_callbacks_account_agent partial index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_callbacks_account_agent skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on trades_mt4(login, close_time) for the open-positions
    # query in call_dashboard: WHERE login=... AND close_time < '1971-01-01' AND cmd IN (0,1).
    # The existing ix_trades_mt4_login_cmd_cov covers (login, cmd) but not close_time filtering;
    # this partial index covers the open-positions lookup path exclusively.
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_trades_mt4_login_open "
                "ON trades_mt4 (login, cmd) WHERE close_time < '1971-01-01'::timestamp"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_trades_mt4_login_open partial index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_trades_mt4_login_open skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on vtiger_mttransactions(confirmation_time) for the
    # performance dashboard MTD queries filtering by date_trunc('month', NOW()).
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_mtt_confirmation_time "
                "ON vtiger_mttransactions (confirmation_time)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_mtt_confirmation_time index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_mtt_confirmation_time skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on trades_mt4(open_time) for the performance dashboard
    # traders_mtd and volume_mtd queries filtering by date_trunc('month', NOW()).
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_trades_mt4_open_time ON trades_mt4 (open_time)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_trades_mt4_open_time index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_trades_mt4_open_time skipped (lock or already done): %s", _e)

    # CLAUD-35 (extended): Index on agent_targets(agent_id, month_date) — performance dashboard
    # looks up current month target: WHERE ... AND month_date = date_trunc('month', CURRENT_DATE).
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(
                "CREATE INDEX IF NOT EXISTS ix_agent_targets_agent_month "
                "ON agent_targets (agent_id, month_date)"
            ))
            await session.commit()
        logger.info("CLAUD-35: ix_agent_targets_agent_month index created/verified")
    except Exception as _e:
        logger.warning("CLAUD-35: ix_agent_targets_agent_month skipped (lock or already done): %s", _e)

    # CLAUD-106: agent_performance_cache table for call dashboard KPI caching
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS agent_performance_cache (
                    agent_id VARCHAR(64) NOT NULL,
                    period VARCHAR(10) NOT NULL,
                    period_date DATE NOT NULL,
                    net_deposit DECIMAL(15,2),
                    depositors INT,
                    traders INT,
                    volume DECIMAL(15,2),
                    contacted INT,
                    calls_made INT,
                    talk_time_secs INT,
                    target INT,
                    callbacks_set INT,
                    run_rate DECIMAL(5,2),
                    contact_rate DECIMAL(5,2),
                    avg_call_secs INT,
                    computed_at TIMESTAMPTZ,
                    PRIMARY KEY (agent_id, period, period_date)
                )
            """))
            await session.commit()
        logger.info("CLAUD-106: agent_performance_cache table migration applied")
    except Exception as _e:
        logger.warning("CLAUD-106: agent_performance_cache migration skipped: %s", _e)

    # Start retention_mv initialisation in the background so the server becomes
    # ready immediately.  On subsequent restarts (MV already populated) this
    # runs a fast CONCURRENT refresh; on first boot it does the full rebuild.
    async def _startup_mv_init() -> None:
        try:
            from app.pg_database import AsyncSessionLocal as _ASL, engine as _eng
            from sqlalchemy import text as _t
            async with _ASL() as _s:
                row = await _s.execute(_t(
                    "SELECT ispopulated FROM pg_matviews WHERE matviewname = 'retention_mv'"
                ))
                mv_row = row.fetchone()

            if mv_row is not None:
                # MV already exists — just refresh without dropping
                logger.info("retention_mv exists — running background refresh (no rebuild)")
                await refresh_retention_mv()
                logger.info("retention_mv background refresh complete")
            else:
                # First boot or MV was dropped — full rebuild required
                logger.info("retention_mv missing — running full background rebuild")
                await rebuild_retention_mv()
                logger.info("retention_mv background rebuild complete")
        except Exception as _mv_err:
            logger.error("retention_mv background init failed: %s", _mv_err)

    asyncio.create_task(_startup_mv_init())
    logger.info("retention_mv initialisation started in background — server is ready")
    # Tune PostgreSQL — must run outside a transaction (AUTOCOMMIT)
    try:
        from app.pg_database import engine as _pg_engine
        async with _pg_engine.connect() as _conn:
            await _conn.execution_options(isolation_level="AUTOCOMMIT")
            await _conn.execute(_text("ALTER SYSTEM SET work_mem = '256MB'"))
            await _conn.execute(_text("ALTER SYSTEM SET effective_cache_size = '9GB'"))
            await _conn.execute(_text("ALTER SYSTEM SET shared_buffers = '3GB'"))
            await _conn.execute(_text("ALTER SYSTEM SET maintenance_work_mem = '512MB'"))
            await _conn.execute(_text("SELECT pg_reload_conf()"))
        logger.info("PostgreSQL system settings tuned")
    except Exception as pg_tune_err:
        logger.warning("Could not apply PostgreSQL system settings (need superuser): %s", pg_tune_err)
    # Migrate: ensure app_settings key-value table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text(
            "CREATE TABLE IF NOT EXISTS app_settings ("
            "key VARCHAR(128) PRIMARY KEY, "
            "value TEXT NOT NULL)"
        ))
        await session.commit()
    logger.info("app_settings table migration applied")
    # CLAUD-153: Migrate — ensure batch_jobs table exists
    async with AsyncSessionLocal() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS batch_jobs (
                id                SERIAL PRIMARY KEY,
                job_id            VARCHAR(64) NOT NULL UNIQUE,
                status            VARCHAR(20) NOT NULL DEFAULT 'queued',
                created_by        INT NOT NULL REFERENCES users(id),
                agent_id          VARCHAR(255),
                agent_phone_number_id VARCHAR(255),
                call_provider     VARCHAR(20) NOT NULL DEFAULT 'twilio',
                clients_json      TEXT NOT NULL DEFAULT '[]',
                total_records     INT NOT NULL DEFAULT 0,
                processed_records INT NOT NULL DEFAULT 0,
                failed_records    INT NOT NULL DEFAULT 0,
                error_message     TEXT,
                started_at        TIMESTAMPTZ,
                completed_at      TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(_text(
            "CREATE INDEX IF NOT EXISTS ix_batch_jobs_created_by ON batch_jobs (created_by, created_at DESC)"
        ))
        await session.commit()
    logger.info("CLAUD-153: batch_jobs table migration applied")
    # CLAUD-157: Add concurrency column to batch_jobs
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text(_LT))
            await session.execute(_text(
                "ALTER TABLE batch_jobs ADD COLUMN IF NOT EXISTS concurrency INT NOT NULL DEFAULT 1"
            ))
            await session.commit()
        logger.info("CLAUD-157: batch_jobs.concurrency column migration applied")
    except Exception as _e:
        logger.warning("CLAUD-157: batch_jobs.concurrency migration skipped: %s", _e)
    # CLAUD-156: Seed agent_activity permission for retention_manager, team_leader, agent roles
    try:
        from sqlalchemy import select as _select
        from app.models.role import Role as _Role
        async with AsyncSessionLocal() as session:
            for _rname in ("retention_manager", "team_leader", "agent"):
                _res = await session.execute(_select(_Role).where(_Role.name == _rname))
                _role = _res.scalar_one_or_none()
                if _role and "agent_activity" not in (_role.permissions or []):
                    _role.permissions = list(_role.permissions or []) + ["agent_activity"]
                    logger.info("CLAUD-156: seeded agent_activity permission for role '%s'", _rname)
            await session.commit()
    except Exception as _e:
        logger.warning("CLAUD-156: agent_activity seed skipped: %s", _e)
    # Elena AI campaign configs table
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("""
                CREATE TABLE IF NOT EXISTS elena_ai_campaign_configs (
                    id SERIAL PRIMARY KEY,
                    campaign_id VARCHAR(100) NOT NULL UNIQUE,
                    label VARCHAR(200),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            await session.commit()
        logger.info("elena_ai_campaign_configs table migration applied")
    except Exception as _e:
        logger.warning("elena_ai_campaign_configs migration skipped: %s", _e)

    app.state.http_client = httpx.AsyncClient(timeout=30.0)
    logger.info("Shared HTTP client initialised")
    # CLAUD-153: Resume any batch jobs that were running when the server last restarted
    try:
        import json as _json
        from app.routers.batch_calls import BatchClientItem as _BatchClientItem, start_batch_task as _start_batch_task
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(
                _text(
                    "UPDATE batch_jobs SET status='interrupted', error_message='Server restarted' "
                    "WHERE status IN ('queued', 'running') "
                    "RETURNING job_id, clients_json, agent_id, agent_phone_number_id, call_provider, processed_records, concurrency"
                )
            )).fetchall()
            await session.commit()
        for _row in rows:
            _all_clients = [_BatchClientItem(**c) for c in _json.loads(_row.clients_json or "[]")]
            _remaining = _all_clients[_row.processed_records:]
            if _remaining:
                _start_batch_task(
                    _row.job_id,
                    _remaining,
                    _row.agent_id,
                    _row.agent_phone_number_id,
                    _row.call_provider or "twilio",
                    app.state.http_client,
                    initial_processed=_row.processed_records,
                    concurrency=getattr(_row, "concurrency", 1) or 1,
                )
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        _text("UPDATE batch_jobs SET status='running' WHERE job_id=:jid"),
                        {"jid": _row.job_id},
                    )
                    await session.commit()
                logger.info("CLAUD-153: Resumed batch_job %s with %d remaining clients", _row.job_id, len(_remaining))
    except Exception as _e:
        logger.warning("CLAUD-153: Failed to recover interrupted batch jobs: %s", _e)
    from app.replica_database import _ReplicaSession
    # Stagger startup times so all ETL jobs don't hammer the DB simultaneously.
    # Incremental syncs start 2 min apart; heavy hourly syncs start after 10 min;
    # MV refresh waits 15 min so ETL burst is fully settled before first refresh.
    _t = datetime.now(timezone.utc)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        incremental_sync_ant_acc,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=2),
    )
    scheduler.add_job(
        incremental_sync_vta,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=4),
    )
    scheduler.add_job(
        incremental_sync_mtt,
        "interval",
        minutes=30,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=6),
    )
    if _ReplicaSession is not None:
        scheduler.add_job(
            incremental_sync_trades,
            "interval",
            minutes=30,
            args=[AsyncSessionLocal, _ReplicaSession],
            next_run_time=_t + timedelta(minutes=8),
        )
        scheduler.add_job(
            incremental_sync_dealio_users,
            "interval",
            minutes=30,
            args=[AsyncSessionLocal, _ReplicaSession],
            next_run_time=_t + timedelta(minutes=10),
        )
    scheduler.add_job(
        hourly_sync_vtiger_users,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=12),
    )
    scheduler.add_job(
        hourly_sync_vtiger_campaigns,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=14),
    )
    scheduler.add_job(
        hourly_sync_extensions,
        "interval",
        hours=1,
        args=[AsyncSessionLocal],
        next_run_time=_t + timedelta(minutes=16),
    )
    scheduler.add_job(
        daily_full_sync_all,
        "cron",
        hour=0,
        minute=0,
    )
    scheduler.add_job(
        sync_proline_data,
        "cron",
        hour=2,
        minute=0,
        args=[AsyncSessionLocal],
    )
    # MV refresh: start 20 min after startup so ETL burst is fully settled.
    # Interval kept at 5 min (was 3) to avoid overlapping long refreshes.
    scheduler.add_job(
        refresh_retention_mv,
        "interval",
        minutes=5,
        next_run_time=_t + timedelta(minutes=20),
    )
    # open_pnl_cache sync: always run — exposure uses local trades_mt4 (no replica needed)
    scheduler.add_job(
        sync_open_pnl_background,
        "interval",
        minutes=3,
        next_run_time=_t + timedelta(minutes=3),
    )
    # CLAUD-81: account_exposure_cache sync — now reads trades_mt4 directly (no dependency on open_pnl_cache).
    # Run 30s after startup so the retention grid is populated immediately.
    scheduler.add_job(
        sync_account_exposure_cache,
        "interval",
        minutes=3,
        next_run_time=_t + timedelta(seconds=30),
    )
    # CLAUD-86: Active client classification — every 15 min.
    # Start 2 min after exposure cache so conditions have fresh data.
    scheduler.add_job(
        compute_active_status,
        "interval",
        minutes=15,
        next_run_time=_t + timedelta(minutes=2, seconds=30),
    )
    # card_type sync: hourly — looks up new IINs via iinlist API, updates client_card_type
    scheduler.add_job(
        sync_card_types_background,
        "interval",
        hours=1,
        next_run_time=_t + timedelta(minutes=18),
    )
    # agent_targets daily sync (CLAUD-56): run once after startup, then daily at 01:00
    scheduler.add_job(
        sync_agent_targets,
        "cron",
        hour=1,
        minute=30,
        args=[AsyncSessionLocal],
    )
    scheduler.add_job(
        sync_agent_targets,
        "date",
        run_date=_t + timedelta(minutes=22),
        args=[AsyncSessionLocal],
    )
    # CLAUD-89: Daily challenge reset — midnight GMT+2 = 22:00 UTC
    scheduler.add_job(
        reset_daily_challenges,
        "cron",
        hour=22,
        minute=0,
        id="reset_daily_challenges",
    )
    # CLAUD-94: Weekly diversity challenge reset — runs Sunday 22:00 UTC (midnight GMT+2)
    scheduler.add_job(
        reset_weekly_challenges,
        "cron",
        day_of_week="sun",
        hour=22,
        minute=5,
    )
    # CLAUD-95: Flash challenge expiry check — runs every 15 minutes
    scheduler.add_job(
        expire_flash_challenges,
        "interval",
        minutes=15,
        next_run_time=_t + timedelta(minutes=5),
    )
    scheduler.start()
    logger.info("ETL scheduler started — incremental sync every 30 min, vtiger/extensions hourly full refresh, daily full sync at midnight")

    yield

    scheduler.shutdown(wait=False)
    await app.state.http_client.aclose()
    logger.info("Shared HTTP client closed")


app = FastAPI(title="Client Call Manager API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api")
app.include_router(clients.router, prefix="/api")
app.include_router(calls.router, prefix="/api")
app.include_router(filters.router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.include_router(roles_router, prefix="/api")
app.include_router(call_mappings_router, prefix="/api")
app.include_router(etl_router, prefix="/api")
app.include_router(retention_router, prefix="/api")
app.include_router(retention_tasks_router, prefix="/api")
app.include_router(client_scoring_router, prefix="/api")
app.include_router(crm_router, prefix="/api")
app.include_router(integrations_router, prefix="/api")
app.include_router(audit_log_router, prefix="/api")
app.include_router(preferences_router, prefix="/api")
app.include_router(favorites_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(proline_router, prefix="/api")
app.include_router(permissions_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(call_dashboard_router, prefix="/api")
app.include_router(elena_ai_router, prefix="/api")
app.include_router(performance_dashboard_router, prefix="/api")
app.include_router(active_status_router, prefix="/api")
app.include_router(column_visibility_router, prefix="/api")
app.include_router(password_reset_router, prefix="/api")
app.include_router(challenges_router, prefix="/api")
app.include_router(action_bonuses_router, prefix="/api")
app.include_router(lifecycle_router, prefix="/api")
app.include_router(saved_searches_router, prefix="/api")
app.include_router(sendgrid_router, prefix="/api")
app.include_router(batch_calls_router, prefix="/api")
app.include_router(agent_activity_router, prefix="/api")
app.include_router(protected_clients_router, prefix="/api")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/api/health/time")
async def health_time() -> dict:
    from datetime import datetime, timedelta, timezone, timezone
    from app.database import execute_query
    result = {"server_utc": datetime.now(timezone.utc).isoformat()}
    try:
        rows = await execute_query("SELECT GETUTCDATE() AS mssql_utc, GETDATE() AS mssql_local", ())
        if rows:
            result["mssql_utc"] = rows[0]["mssql_utc"].isoformat() if rows[0]["mssql_utc"] else None
            result["mssql_local"] = rows[0]["mssql_local"].isoformat() if rows[0]["mssql_local"] else None
            if rows[0]["mssql_utc"]:
                diff = datetime.now(timezone.utc).replace(tzinfo=None) - rows[0]["mssql_utc"]
                result["server_ahead_of_mssql_utc_seconds"] = round(diff.total_seconds())
    except Exception as e:
        result["mssql_error"] = str(e)
    return result


@app.get("/api/health/replica")
async def health_replica() -> dict:
    import asyncio
    from app.replica_database import get_replica_engine
    engine = get_replica_engine()
    if engine is None:
        return {"status": "not_configured", "detail": "REPLICA_DB_HOST is not set"}
    try:
        from sqlalchemy import text
        result = {}
        async def _check():
            async with engine.connect() as conn:
                row = (await conn.execute(text(
                    "SELECT NOW() AS server_now, NOW() AT TIME ZONE 'UTC' AS server_utc, current_setting('TimeZone') AS tz"
                ))).first()
                result["server_now"] = row[0].isoformat() if row[0] else None
                result["server_utc"] = row[1].isoformat() if row[1] else None
                result["server_timezone"] = row[2]
        await asyncio.wait_for(_check(), timeout=5.0)
        return {"status": "ok", "host": settings.replica_db_host, "port": settings.replica_db_port, "db": settings.replica_db_name, **result}
    except asyncio.TimeoutError:
        return {"status": "error", "detail": "Connection timed out — IP may not be whitelisted"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
