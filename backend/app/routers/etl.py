import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth_deps import get_current_user, require_admin
from app.database import execute_query
from app.models.etl_sync_log import EtlSyncLog
from app.pg_database import AsyncSessionLocal, engine, get_db
from app.replica_database import get_replica_db

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# CLAUD-36: In-memory cache for sync-status endpoint (30s TTL).
# Prevents 70 users polling every 10s from hammering the DB with 20+ COUNT
# queries per call.
# ---------------------------------------------------------------------------
_sync_status_cache: dict | None = None
_sync_status_cache_expires: float = 0.0
_SYNC_STATUS_TTL = 30  # seconds

_TRADES_BATCH_SIZE = 100_000
_ANT_ACC_BATCH_SIZE = 100_000

_TRADES_INSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified, open_price, volume)"
    " VALUES (:ticket, :login, :cmd, :profit, :computed_profit, :notional_value, :close_time, :open_time, :symbol, :last_modified, :open_price, :volume)"
)
_TRADES_UPSERT = (
    "INSERT INTO trades_mt4 (ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified, open_price, volume)"
    " VALUES (:ticket, :login, :cmd, :profit, :computed_profit, :notional_value, :close_time, :open_time, :symbol, :last_modified, :open_price, :volume)"
    " ON CONFLICT (ticket) DO UPDATE SET login = EXCLUDED.login, cmd = EXCLUDED.cmd,"
    " profit = EXCLUDED.profit, computed_profit = EXCLUDED.computed_profit, notional_value = EXCLUDED.notional_value,"
    " close_time = EXCLUDED.close_time, open_time = EXCLUDED.open_time, symbol = EXCLUDED.symbol,"
    " last_modified = EXCLUDED.last_modified, open_price = EXCLUDED.open_price, volume = EXCLUDED.volume"
)

_ANT_ACC_SELECT = (
    "SELECT accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential,"
    " birth_date, assigned_to, first_name, last_name, full_name,"
    " email, country_iso, customer_language, accountstatus, regulation,"
    " createdtime, first_deposit_date, ftd_amount, total_deposit, total_withdrawal,"
    " net_deposit, funded, original_affiliate, retention_status, customer_id"
    " FROM report.ant_acc"
)

_ANT_ACC_UPSERT = (
    "INSERT INTO ant_acc (accountid, client_qualification_date, modifiedtime, is_test_account, sales_client_potential,"
    " birth_date, assigned_to, firstname, lastname, full_name,"
    " email, country_iso, customer_language, accountstatus, regulation,"
    " createdtime, first_deposit_date, ftd_amount, total_deposit, total_withdrawal,"
    " net_deposit, funded, original_affiliate, retention_status, customer_id)"
    " VALUES (:accountid, :client_qualification_date, :modifiedtime, :is_test_account, :sales_client_potential,"
    " :birth_date, :assigned_to, :firstname, :lastname, :full_name,"
    " :email, :country_iso, :customer_language, :accountstatus, :regulation,"
    " :createdtime, :first_deposit_date, :ftd_amount, :total_deposit, :total_withdrawal,"
    " :net_deposit, :funded, :original_affiliate, :retention_status, :customer_id)"
    " ON CONFLICT (accountid) DO UPDATE SET"
    " client_qualification_date = EXCLUDED.client_qualification_date,"
    " modifiedtime = EXCLUDED.modifiedtime,"
    " is_test_account = EXCLUDED.is_test_account,"
    " sales_client_potential = EXCLUDED.sales_client_potential,"
    " birth_date = EXCLUDED.birth_date,"
    " assigned_to = EXCLUDED.assigned_to,"
    " firstname = EXCLUDED.firstname,"
    " lastname = EXCLUDED.lastname,"
    " full_name = EXCLUDED.full_name,"
    " email = EXCLUDED.email,"
    " country_iso = EXCLUDED.country_iso,"
    " customer_language = EXCLUDED.customer_language,"
    " accountstatus = EXCLUDED.accountstatus,"
    " regulation = EXCLUDED.regulation,"
    " createdtime = EXCLUDED.createdtime,"
    " first_deposit_date = EXCLUDED.first_deposit_date,"
    " ftd_amount = EXCLUDED.ftd_amount,"
    " total_deposit = EXCLUDED.total_deposit,"
    " total_withdrawal = EXCLUDED.total_withdrawal,"
    " net_deposit = EXCLUDED.net_deposit,"
    " funded = EXCLUDED.funded,"
    " original_affiliate = EXCLUDED.original_affiliate,"
    " retention_status = EXCLUDED.retention_status,"
    " customer_id = EXCLUDED.customer_id"
)

_ant_acc_map = lambda r: {  # noqa: E731
    "accountid": str(r["accountid"]),
    "client_qualification_date": r["client_qualification_date"].date() if hasattr(r["client_qualification_date"], "date") else r["client_qualification_date"],
    "modifiedtime": r["modifiedtime"],
    "is_test_account": int(r["is_test_account"]) if r["is_test_account"] is not None else None,
    "sales_client_potential": str(r["sales_client_potential"]) if r["sales_client_potential"] is not None else None,
    "birth_date": r["birth_date"].date() if hasattr(r["birth_date"], "date") else r["birth_date"],
    "assigned_to": str(r["assigned_to"]) if r["assigned_to"] is not None else None,
    "firstname": str(r["first_name"]).replace("\x00", "").strip() if r["first_name"] is not None else None,
    "lastname": str(r["last_name"]).replace("\x00", "").strip() if r["last_name"] is not None else None,
    "full_name": str(r["full_name"]).replace("\x00", "").strip() if r["full_name"] is not None else None,
    "email": str(r["email"]).strip() if r["email"] is not None else None,
    "country_iso": str(r["country_iso"]).strip() if r["country_iso"] is not None else None,
    "customer_language": str(r["customer_language"]).strip() if r["customer_language"] is not None else None,
    "accountstatus": str(r["accountstatus"]).strip() if r["accountstatus"] is not None else None,
    "regulation": str(r["regulation"]).strip() if r["regulation"] is not None else None,
    "createdtime": r["createdtime"],
    "first_deposit_date": r["first_deposit_date"],
    "ftd_amount": int(r["ftd_amount"]) if r["ftd_amount"] is not None else None,
    "total_deposit": int(r["total_deposit"]) if r["total_deposit"] is not None else None,
    "total_withdrawal": int(r["total_withdrawal"]) if r["total_withdrawal"] is not None else None,
    "net_deposit": int(r["net_deposit"]) if r["net_deposit"] is not None else None,
    "funded": int(r["funded"]) if r["funded"] is not None else None,
    "original_affiliate": str(r["original_affiliate"]).strip() if r["original_affiliate"] is not None else None,
    "retention_status": int(r["retention_status"]) if r["retention_status"] is not None else None,
    "customer_id": str(r["customer_id"]).strip() if r["customer_id"] is not None else None,  # CLAUD-167
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _update_log(log_id: int, status: str, rows_synced: int | None = None, error: str | None = None) -> None:
    async with AsyncSessionLocal() as db:
        log = await db.get(EtlSyncLog, log_id)
        if log:
            log.status = status
            log.rows_synced = rows_synced
            log.error_message = error
            log.completed_at = datetime.now(timezone.utc)
            await db.commit()


async def _is_running(prefix: str) -> bool:
    """Return True if any sync for this table prefix is already running."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT 1 FROM etl_sync_log WHERE sync_type LIKE :prefix AND status = 'running' LIMIT 1"),
            {"prefix": f"{prefix}%"},
        )
        return result.first() is not None


# ---------------------------------------------------------------------------
# Trades (dealio.trades_mt4) — full sync
# ---------------------------------------------------------------------------

async def _run_full_sync_trades(log_id: int) -> None:
    from app.replica_database import _ReplicaSession

    if _ReplicaSession is None:
        await _update_log(log_id, "error", error="Replica database not configured")
        return

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE trades_mt4"))
            await db.commit()

        total = 0
        cursor = 0

        while True:
            rows = None
            for attempt in range(5):
                try:
                    async with _ReplicaSession() as replica_db:
                        result = await replica_db.execute(
                            text(
                                "SELECT ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified, COALESCE(open_price, 0) AS open_price, COALESCE(volume, 0) AS volume FROM dealio.trades_mt4"
                                " WHERE ticket > :cursor ORDER BY ticket LIMIT :limit"
                            ),
                            {"cursor": cursor, "limit": _TRADES_BATCH_SIZE},
                        )
                        rows = result.fetchall()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = 2 ** attempt
                    logger.warning("ETL trades full: attempt %d failed (%s), retrying in %ds", attempt + 1, e, wait)
                    await asyncio.sleep(wait)

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_TRADES_INSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "computed_profit": r[4], "notional_value": r[5], "close_time": r[6], "open_time": r[7], "symbol": r[8], "last_modified": r[9], "open_price": r[10], "volume": r[11]} for r in rows])
                await db.commit()

            total += len(rows)
            cursor = rows[-1][0]
            logger.info("ETL trades full: %d rows (cursor=%d)", total, cursor)

            if len(rows) < _TRADES_BATCH_SIZE:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL trades full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL trades full sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# Trades — incremental sync (called by scheduler)
# ---------------------------------------------------------------------------

async def incremental_sync_trades(
    session_factory: async_sessionmaker,
    replica_session_factory: async_sessionmaker,
    lookback_hours: int = 3,
) -> None:
    if await _is_running("trades"):
        logger.info("ETL trades: skipping scheduled run — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="trades_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        # Replica stores last_modified as timestamp without time zone — strip tz
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).replace(tzinfo=None)
        total = 0
        offset = 0

        while True:
            rows = None
            for attempt in range(3):
                try:
                    async with replica_session_factory() as replica_db:
                        result = await replica_db.execute(
                            text(
                                "SELECT ticket, login, cmd, profit, computed_profit, notional_value, close_time, open_time, symbol, last_modified, COALESCE(open_price, 0) AS open_price, COALESCE(volume, 0) AS volume FROM dealio.trades_mt4"
                                " WHERE last_modified > :cutoff ORDER BY last_modified, ticket LIMIT :limit OFFSET :offset"
                            ),
                            {"cutoff": cutoff, "limit": _TRADES_BATCH_SIZE, "offset": offset},
                        )
                        rows = result.fetchall()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logger.warning("ETL trades: connection error on attempt %d, retrying: %s", attempt + 1, e)
                    await asyncio.sleep(2)

            if not rows:
                break

            async with session_factory() as db:
                await db.execute(text(_TRADES_UPSERT), [{"ticket": r[0], "login": r[1], "cmd": r[2], "profit": r[3], "computed_profit": r[4], "notional_value": r[5], "close_time": r[6], "open_time": r[7], "symbol": r[8], "last_modified": r[9], "open_price": r[10], "volume": r[11]} for r in rows])
                await db.commit()

            total += len(rows)
            offset += len(rows)

            if len(rows) < _TRADES_BATCH_SIZE:
                break

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if total:
            logger.info("ETL trades incremental: %d new/updated rows", total)

    except Exception as e:
        logger.error("ETL trades incremental failed: %s", e)
        if log_id:
            try:
                async with session_factory() as db:
                    log = await db.get(EtlSyncLog, log_id)
                    if log:
                        log.status = "error"
                        log.error_message = str(e)
                        log.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Ant Acc (report.ant_acc) — full sync
# ---------------------------------------------------------------------------

_ANT_ACC_TEST_FILTER = "ISNULL(is_test_account, 0) = 0"


async def _run_full_sync_ant_acc(log_id: int) -> None:
    await _mssql_full_sync(
        log_id, "ant_acc_full",
        f"{_ANT_ACC_SELECT} WHERE {_ANT_ACC_TEST_FILTER}",
        "ant_acc", _ANT_ACC_UPSERT, _ant_acc_map,
    )


# ---------------------------------------------------------------------------
# Ant Acc — incremental sync (called by scheduler)
# ---------------------------------------------------------------------------

async def incremental_sync_ant_acc(session_factory: async_sessionmaker) -> None:
    if await _is_running("ant_acc"):
        logger.info("ETL ant_acc: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "ant_acc_incremental", "ant_acc", _ANT_ACC_SELECT, _ANT_ACC_UPSERT, _ant_acc_map, extra_where=_ANT_ACC_TEST_FILTER)


# ---------------------------------------------------------------------------
# Shared helper for MSSQL-sourced full + incremental syncs
# ---------------------------------------------------------------------------

async def _mssql_full_sync(
    log_id: int,
    sync_type: str,
    select_sql: str,
    local_table: str,
    upsert_sql: str,
    row_mapper,
    batch_size: int = 100_000,
    lazy_truncate: bool = False,
    skip_truncate: bool = False,
) -> None:
    """Full sync from MSSQL to a local table.

    When lazy_truncate=True the TRUNCATE is deferred until the first batch
    is successfully fetched from MSSQL.  This ensures the local table stays
    populated if MSSQL is temporarily unavailable (no truncate-then-fail).

    When skip_truncate=True no TRUNCATE is ever issued — pure upsert mode.
    Use this for tables like vtiger_users where a momentary empty state would
    corrupt the retention_mv (agent_name goes blank for up to 1 hour).
    """
    try:
        if not lazy_truncate and not skip_truncate:
            async with AsyncSessionLocal() as db:
                await db.execute(text(f"TRUNCATE TABLE {local_table}"))
                await db.commit()

        total = 0
        offset = 0
        truncated = (lazy_truncate is False) or skip_truncate  # skip if already done or not needed
        while True:
            rows = await execute_query(
                f"{select_sql} ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                (offset, batch_size),
            )
            if not rows:
                break
            # Lazy truncate: only wipe existing data once we know MSSQL responded
            if not truncated:
                async with AsyncSessionLocal() as db:
                    await db.execute(text(f"TRUNCATE TABLE {local_table}"))
                    await db.commit()
                truncated = True
            async with AsyncSessionLocal() as db:
                await db.execute(text(upsert_sql), [row_mapper(r) for r in rows])
                await db.commit()
            total += len(rows)
            offset += batch_size
            logger.info("ETL %s full: %d rows so far", local_table, total)
            if len(rows) < batch_size:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL %s full sync complete: %d rows", local_table, total)
    except Exception as e:
        logger.error("ETL %s full sync failed: %s", local_table, e)
        await _update_log(log_id, "error", error=str(e))


async def _mssql_incremental_sync(
    session_factory: async_sessionmaker,
    sync_type: str,
    local_table: str,
    mssql_select: str,
    upsert_sql: str,
    row_mapper,
    timestamp_col: str = "modifiedtime",
    lookback_hours: int = 3,
    window_minutes: int | None = None,
    extra_where: str = "",
) -> None:
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type=sync_type, status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        extra = f" AND {extra_where}" if extra_where else ""

        rows = await execute_query(
            f"{mssql_select} WHERE {timestamp_col} > ?{extra} ORDER BY {timestamp_col}",
            (cutoff,),
        )
        if rows:
            async with session_factory() as db:
                await db.execute(text(upsert_sql), [row_mapper(r) for r in rows])
                await db.commit()

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = len(rows)
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()
        if rows:
            logger.info("ETL %s incremental: %d rows updated", local_table, len(rows))

    except Exception as e:
        logger.error("ETL %s incremental failed: %s", local_table, e)
        if log_id:
            try:
                async with session_factory() as db:
                    log = await db.get(EtlSyncLog, log_id)
                    if log:
                        log.status = "error"
                        log.error_message = str(e)
                        log.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# vtiger_trading_accounts
# ---------------------------------------------------------------------------

_VTA_SELECT = "SELECT login, vtigeraccountid, balance, credit, last_update AS modifiedtime FROM report.vtiger_trading_accounts"
_VTA_UPSERT = (
    "INSERT INTO vtiger_trading_accounts (login, vtigeraccountid, balance, credit, modifiedtime)"
    " VALUES (:login, :vtigeraccountid, :balance, :credit, :modifiedtime)"
    " ON CONFLICT (login) DO UPDATE SET"
    " vtigeraccountid = EXCLUDED.vtigeraccountid, balance = EXCLUDED.balance,"
    " credit = EXCLUDED.credit, modifiedtime = EXCLUDED.modifiedtime"
)
_vta_map = lambda r: {"login": r["login"], "vtigeraccountid": str(r["vtigeraccountid"]) if r["vtigeraccountid"] else None, "balance": r["balance"], "credit": r["credit"], "modifiedtime": r["modifiedtime"]}  # noqa: E731


async def _run_full_sync_vta(log_id: int) -> None:
    await _mssql_full_sync(log_id, "vta_full", _VTA_SELECT, "vtiger_trading_accounts", _VTA_UPSERT, _vta_map)


async def incremental_sync_vta(session_factory: async_sessionmaker) -> None:
    if await _is_running("vta"):
        logger.info("ETL vta: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "vta_incremental", "vtiger_trading_accounts", _VTA_SELECT, _VTA_UPSERT, _vta_map, timestamp_col="last_update", lookback_hours=3)


# ---------------------------------------------------------------------------
# vtiger_mttransactions
# ---------------------------------------------------------------------------

_MTT_SELECT = (
    "SELECT mttransactionsid, login, amount, transactiontype,"
    " transactionapproval, confirmation_time, payment_method, usdamount, modifiedtime, creditcardlast"
    " FROM report.vtiger_mttransactions"
)
_MTT_UPSERT = (
    "INSERT INTO vtiger_mttransactions"
    " (mttransactionsid, login, amount, transactiontype, transactionapproval, confirmation_time, payment_method, usdamount, modifiedtime, creditcardlast)"
    " VALUES (:mttransactionsid, :login, :amount, :transactiontype, :transactionapproval, :confirmation_time, :payment_method, :usdamount, :modifiedtime, :creditcardlast)"
    " ON CONFLICT (mttransactionsid) DO UPDATE SET"
    " login = EXCLUDED.login, amount = EXCLUDED.amount, transactiontype = EXCLUDED.transactiontype,"
    " transactionapproval = EXCLUDED.transactionapproval, confirmation_time = EXCLUDED.confirmation_time,"
    " payment_method = EXCLUDED.payment_method, usdamount = EXCLUDED.usdamount, modifiedtime = EXCLUDED.modifiedtime,"
    " creditcardlast = EXCLUDED.creditcardlast"
)
_mtt_map = lambda r: {  # noqa: E731
    "mttransactionsid": r["mttransactionsid"], "login": r["login"], "amount": r["amount"],
    "transactiontype": r["transactiontype"], "transactionapproval": r["transactionapproval"],
    "confirmation_time": r["confirmation_time"], "payment_method": r["payment_method"],
    "usdamount": r["usdamount"], "modifiedtime": r["modifiedtime"],
    "creditcardlast": r.get("creditcardlast"),
}


async def _run_full_sync_mtt(log_id: int) -> None:
    await _mssql_full_sync(log_id, "mtt_full", _MTT_SELECT, "vtiger_mttransactions", _MTT_UPSERT, _mtt_map)


async def incremental_sync_mtt(session_factory: async_sessionmaker) -> None:
    if await _is_running("mtt"):
        logger.info("ETL mtt: skipping scheduled run — sync already in progress")
        return
    await _mssql_incremental_sync(session_factory, "mtt_incremental", "vtiger_mttransactions", _MTT_SELECT, _MTT_UPSERT, _mtt_map, lookback_hours=3)


# ---------------------------------------------------------------------------
# dealio.users — full + incremental sync
# ---------------------------------------------------------------------------

_DEALIO_USERS_SELECT = (
    "SELECT login, lastupdate, sourceid, sourcename, sourcetype, groupname, groupcurrency,"
    " userid, actualuserid, regdate, lastdate, agentaccount, lastip::text AS lastip,"
    " balance, prevmonthbalance, prevbalance, prevequity, credit, name, country, city,"
    " state, zipcode, address, phone, email, compbalance, compprevbalance,"
    " compprevmonthbalance, compprevequity, compcredit, conversionratio, book,"
    " isenabled, status, prevmonthequity, compprevmonthequity, comment, color,"
    " leverage, condition, calculationcurrency, calculationcurrencydigits"
    " FROM dealio.users"
)

_DEALIO_USERS_UPSERT = (
    "INSERT INTO dealio_users"
    " (login, lastupdate, sourceid, sourcename, sourcetype, groupname, groupcurrency,"
    " userid, actualuserid, regdate, lastdate, agentaccount, lastip,"
    " balance, prevmonthbalance, prevbalance, prevequity, credit, name, country, city,"
    " state, zipcode, address, phone, email, compbalance, compprevbalance,"
    " compprevmonthbalance, compprevequity, compcredit, conversionratio, book,"
    " isenabled, status, prevmonthequity, compprevmonthequity, comment, color,"
    " leverage, condition, calculationcurrency, calculationcurrencydigits)"
    " VALUES"
    " (:login, :lastupdate, :sourceid, :sourcename, :sourcetype, :groupname, :groupcurrency,"
    " :userid, :actualuserid, :regdate, :lastdate, :agentaccount, :lastip,"
    " :balance, :prevmonthbalance, :prevbalance, :prevequity, :credit, :name, :country, :city,"
    " :state, :zipcode, :address, :phone, :email, :compbalance, :compprevbalance,"
    " :compprevmonthbalance, :compprevequity, :compcredit, :conversionratio, :book,"
    " :isenabled, :status, :prevmonthequity, :compprevmonthequity, :comment, :color,"
    " :leverage, :condition, :calculationcurrency, :calculationcurrencydigits)"
    " ON CONFLICT (login) DO UPDATE SET"
    " lastupdate = EXCLUDED.lastupdate, sourceid = EXCLUDED.sourceid, sourcename = EXCLUDED.sourcename,"
    " sourcetype = EXCLUDED.sourcetype, groupname = EXCLUDED.groupname, groupcurrency = EXCLUDED.groupcurrency,"
    " userid = EXCLUDED.userid, actualuserid = EXCLUDED.actualuserid, regdate = EXCLUDED.regdate,"
    " lastdate = EXCLUDED.lastdate, agentaccount = EXCLUDED.agentaccount, lastip = EXCLUDED.lastip,"
    " balance = EXCLUDED.balance, prevmonthbalance = EXCLUDED.prevmonthbalance, prevbalance = EXCLUDED.prevbalance,"
    " prevequity = EXCLUDED.prevequity, credit = EXCLUDED.credit, name = EXCLUDED.name,"
    " country = EXCLUDED.country, city = EXCLUDED.city, state = EXCLUDED.state,"
    " zipcode = EXCLUDED.zipcode, address = EXCLUDED.address, phone = EXCLUDED.phone,"
    " email = EXCLUDED.email, compbalance = EXCLUDED.compbalance, compprevbalance = EXCLUDED.compprevbalance,"
    " compprevmonthbalance = EXCLUDED.compprevmonthbalance, compprevequity = EXCLUDED.compprevequity,"
    " compcredit = EXCLUDED.compcredit, conversionratio = EXCLUDED.conversionratio, book = EXCLUDED.book,"
    " isenabled = EXCLUDED.isenabled, status = EXCLUDED.status, prevmonthequity = EXCLUDED.prevmonthequity,"
    " compprevmonthequity = EXCLUDED.compprevmonthequity, comment = EXCLUDED.comment, color = EXCLUDED.color,"
    " leverage = EXCLUDED.leverage, condition = EXCLUDED.condition, calculationcurrency = EXCLUDED.calculationcurrency,"
    " calculationcurrencydigits = EXCLUDED.calculationcurrencydigits"
)

_dealio_users_map = lambda r: {  # noqa: E731
    "login": r["login"], "lastupdate": r["lastupdate"], "sourceid": r["sourceid"],
    "sourcename": r["sourcename"], "sourcetype": r["sourcetype"], "groupname": r["groupname"],
    "groupcurrency": r["groupcurrency"], "userid": r["userid"], "actualuserid": r["actualuserid"],
    "regdate": r["regdate"], "lastdate": r["lastdate"], "agentaccount": r["agentaccount"],
    "lastip": r["lastip"], "balance": r["balance"], "prevmonthbalance": r["prevmonthbalance"],
    "prevbalance": r["prevbalance"], "prevequity": r["prevequity"], "credit": r["credit"],
    "name": r["name"], "country": r["country"], "city": r["city"], "state": r["state"],
    "zipcode": r["zipcode"], "address": r["address"], "phone": r["phone"], "email": r["email"],
    "compbalance": r["compbalance"], "compprevbalance": r["compprevbalance"],
    "compprevmonthbalance": r["compprevmonthbalance"], "compprevequity": r["compprevequity"],
    "compcredit": r["compcredit"], "conversionratio": r["conversionratio"], "book": r["book"],
    "isenabled": r["isenabled"], "status": r["status"], "prevmonthequity": r["prevmonthequity"],
    "compprevmonthequity": r["compprevmonthequity"], "comment": r["comment"], "color": r["color"],
    "leverage": r["leverage"], "condition": r["condition"], "calculationcurrency": r["calculationcurrency"],
    "calculationcurrencydigits": r["calculationcurrencydigits"],
}

_DEALIO_USERS_BATCH = 50_000


async def _run_full_sync_dealio_users(log_id: int) -> None:
    from app.replica_database import _ReplicaSession

    if _ReplicaSession is None:
        await _update_log(log_id, "error", error="Replica database not configured")
        return

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE dealio_users"))
            await db.commit()

        total = 0
        cursor = 0

        while True:
            rows = None
            for attempt in range(5):
                try:
                    async with _ReplicaSession() as replica_db:
                        result = await replica_db.execute(
                            text(f"{_DEALIO_USERS_SELECT} WHERE login > :cursor ORDER BY login LIMIT :limit"),
                            {"cursor": cursor, "limit": _DEALIO_USERS_BATCH},
                        )
                        rows = result.mappings().fetchall()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = 2 ** attempt
                    logger.warning("ETL dealio_users full: attempt %d failed (%s), retrying in %ds", attempt + 1, e, wait)
                    await asyncio.sleep(wait)

            if not rows:
                break

            async with AsyncSessionLocal() as db:
                await db.execute(text(_DEALIO_USERS_UPSERT), [_dealio_users_map(r) for r in rows])
                await db.commit()

            total += len(rows)
            cursor = rows[-1]["login"]
            logger.info("ETL dealio_users full: %d rows (cursor=%d)", total, cursor)

            if len(rows) < _DEALIO_USERS_BATCH:
                break

        await _update_log(log_id, "completed", rows_synced=total)
        logger.info("ETL dealio_users full sync complete: %d rows", total)

    except Exception as e:
        logger.error("ETL dealio_users full sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


async def incremental_sync_dealio_users(
    session_factory: async_sessionmaker,
    replica_session_factory: async_sessionmaker,
    lookback_hours: int = 3,
) -> None:
    logger.info("ETL dealio_users: incremental_sync_dealio_users called")
    is_running = await _is_running("dealio_users")
    logger.info("ETL dealio_users: _is_running=%s", is_running)
    if is_running:
        logger.info("ETL dealio_users: skipping scheduled run — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="dealio_users_incremental", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        # Strip tzinfo so the cutoff matches replica's timestamp without time zone,
        # same approach as trades incremental (avoids type mismatch on some replicas)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).replace(tzinfo=None)
        total = 0
        offset = 0

        while True:
            rows = None
            for attempt in range(3):
                try:
                    async with replica_session_factory() as replica_db:
                        result = await replica_db.execute(
                            text(
                                f"{_DEALIO_USERS_SELECT}"
                                " WHERE lastupdate > :cutoff ORDER BY lastupdate, login LIMIT :limit OFFSET :offset"
                            ),
                            {"cutoff": cutoff, "limit": _DEALIO_USERS_BATCH, "offset": offset},
                        )
                        rows = result.mappings().fetchall()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logger.warning("ETL dealio_users: connection error on attempt %d, retrying: %s", attempt + 1, e)
                    await asyncio.sleep(2)

            if not rows:
                break

            async with session_factory() as db:
                await db.execute(text(_DEALIO_USERS_UPSERT), [_dealio_users_map(r) for r in rows])
                await db.commit()

            total += len(rows)
            offset += len(rows)

            if len(rows) < _DEALIO_USERS_BATCH:
                break

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = total
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        if total:
            logger.info("ETL dealio_users incremental: %d new/updated rows", total)

    except Exception as e:
        logger.error("ETL dealio_users incremental failed: %s", e)
        if log_id:
            try:
                async with session_factory() as db:
                    log = await db.get(EtlSyncLog, log_id)
                    if log:
                        log.status = "error"
                        log.error_message = str(e)
                        log.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Daily midnight full sync — all tables
# ---------------------------------------------------------------------------

async def _create_log(sync_type: str) -> int:
    async with AsyncSessionLocal() as db:
        log = EtlSyncLog(sync_type=sync_type, status="running")
        db.add(log)
        await db.commit()
        await db.refresh(log)
        return log.id


async def daily_full_sync_all() -> None:
    logger.info("Daily full sync starting")
    from app.replica_database import _ReplicaSession

    # trades — 24h incremental (safe: no truncate, upserts only)
    if _ReplicaSession is not None:
        if not await _is_running("trades"):
            await incremental_sync_trades(AsyncSessionLocal, _ReplicaSession, lookback_hours=24)
        else:
            logger.info("Daily sync: trades already running, skipped")

    # ant_acc
    if not await _is_running("ant_acc"):
        log_id = await _create_log("ant_acc_full")
        await _run_full_sync_ant_acc(log_id)
    else:
        logger.info("Daily sync: ant_acc already running, skipped")

    # vtiger_trading_accounts
    if not await _is_running("vta"):
        log_id = await _create_log("vta_full")
        await _run_full_sync_vta(log_id)
    else:
        logger.info("Daily sync: vta already running, skipped")

    # vtiger_mttransactions
    if not await _is_running("mtt"):
        log_id = await _create_log("mtt_full")
        await _run_full_sync_mtt(log_id)
    else:
        logger.info("Daily sync: mtt already running, skipped")

    # dealio_users — 24h incremental (safe: no truncate, upserts only)
    if _ReplicaSession is not None:
        if not await _is_running("dealio_users"):
            await incremental_sync_dealio_users(AsyncSessionLocal, _ReplicaSession, lookback_hours=24)
        else:
            logger.info("Daily sync: dealio_users already running, skipped")

    # extensions
    if not await _is_running("extensions"):
        log_id = await _create_log("extensions_full")
        await _run_full_sync_extensions(log_id)
    else:
        logger.info("Daily sync: extensions already running, skipped")

    logger.info("Daily full sync complete")
    await refresh_retention_mv()


# ---------------------------------------------------------------------------
# vtiger_users / vtiger_campaigns — hourly full-refresh jobs
# ---------------------------------------------------------------------------

async def hourly_sync_vtiger_users(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of vtiger_users."""
    if await _is_running("vtiger_users"):
        logger.info("ETL vtiger_users: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="vtiger_users_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_vtiger_users(log_id)
    except Exception as e:
        logger.error("Hourly vtiger_users sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


async def hourly_sync_vtiger_campaigns(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of vtiger_campaigns."""
    if await _is_running("vtiger_campaigns"):
        logger.info("ETL vtiger_campaigns: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="vtiger_campaigns_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_vtiger_campaigns(log_id)
    except Exception as e:
        logger.error("Hourly vtiger_campaigns sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# vtiger_users (report.vtiger_users)
# ---------------------------------------------------------------------------

_VTIGER_USERS_SELECT = (
    "SELECT id, user_name, first_name, last_name, email, phone, department, status, office, position, fax"
    " FROM report.vtiger_users"
)

_VTIGER_USERS_UPSERT = (
    "INSERT INTO vtiger_users"
    " (id, user_name, first_name, last_name, email, phone, department, status, office, position, fax)"
    " VALUES"
    " (:id, :user_name, :first_name, :last_name, :email, :phone, :department, :status, :office, :position, :fax)"
    " ON CONFLICT (id) DO UPDATE SET"
    " user_name = EXCLUDED.user_name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,"
    " email = EXCLUDED.email, phone = EXCLUDED.phone, department = EXCLUDED.department,"
    " status = EXCLUDED.status, office = EXCLUDED.office, position = EXCLUDED.position, fax = EXCLUDED.fax"
)

_vtiger_users_map = lambda r: {  # noqa: E731
    "id": str(r["id"]) if r["id"] is not None else None,
    "user_name": r["user_name"], "first_name": r["first_name"], "last_name": r["last_name"],
    "email": r["email"], "phone": r["phone"], "department": r["department"],
    "status": r["status"], "office": r["office"], "position": r["position"], "fax": r["fax"],
}


async def _run_full_sync_vtiger_users(log_id: int) -> None:
    # Never truncate vtiger_users — upsert only.
    # Truncating causes a ~1s window where the table is empty; if the retention_mv
    # refresh fires during that window every agent_name becomes '' and stays empty
    # until the next MV refresh (up to 1 hour).  The ON CONFLICT upsert handles
    # updates/inserts correctly without any truncation needed.
    await _mssql_full_sync(log_id, "vtiger_users_full", _VTIGER_USERS_SELECT, "vtiger_users", _VTIGER_USERS_UPSERT, _vtiger_users_map, lazy_truncate=False, skip_truncate=True)




# ---------------------------------------------------------------------------
# vtiger_campaigns (report.vtiger_campaigns)
# ---------------------------------------------------------------------------

_VTIGER_CAMPAIGNS_SELECT = (
    "SELECT crmid, campaign_id, campaign_name, campaign_legacy_id, campaign_channel, campaign_sub_channel"
    " FROM report.vtiger_campaigns"
)

_VTIGER_CAMPAIGNS_UPSERT = (
    "INSERT INTO vtiger_campaigns"
    " (crmid, campaign_id, campaign_name, campaign_legacy_id, campaign_channel, campaign_sub_channel)"
    " VALUES"
    " (:crmid, :campaign_id, :campaign_name, :campaign_legacy_id, :campaign_channel, :campaign_sub_channel)"
    " ON CONFLICT (crmid) DO UPDATE SET"
    " campaign_id = EXCLUDED.campaign_id, campaign_name = EXCLUDED.campaign_name,"
    " campaign_legacy_id = EXCLUDED.campaign_legacy_id, campaign_channel = EXCLUDED.campaign_channel,"
    " campaign_sub_channel = EXCLUDED.campaign_sub_channel"
)

_vtiger_campaigns_map = lambda r: {  # noqa: E731
    "crmid": str(r["crmid"]) if r["crmid"] is not None else None,
    "campaign_id": str(r["campaign_id"]) if r["campaign_id"] is not None else None,
    "campaign_name": r["campaign_name"],
    "campaign_legacy_id": str(r["campaign_legacy_id"]) if r["campaign_legacy_id"] is not None else None,
    "campaign_channel": r["campaign_channel"],
    "campaign_sub_channel": r["campaign_sub_channel"],
}


async def _run_full_sync_vtiger_campaigns(log_id: int) -> None:
    await _mssql_full_sync(log_id, "vtiger_campaigns_full", _VTIGER_CAMPAIGNS_SELECT, "vtiger_campaigns", _VTIGER_CAMPAIGNS_UPSERT, _vtiger_campaigns_map)


# ---------------------------------------------------------------------------
# extensions (report.Extension_new) — hourly full-refresh
# ---------------------------------------------------------------------------

_EXTENSIONS_SELECT = (
    "SELECT [Name], [Extension], [User_name], [Agent_name], [Manager], [Position], [Office], [Email], [ManagerEmail]"
    " FROM [report].[Extension_new]"
)

_EXTENSIONS_UPSERT = (
    "INSERT INTO extensions"
    " (name, extension, user_name, agent_name, manager, position, office, email, manager_email, synced_at)"
    " VALUES"
    " (:name, :extension, :user_name, :agent_name, :manager, :position, :office, :email, :manager_email, NOW())"
    " ON CONFLICT (extension) DO UPDATE SET"
    " name = EXCLUDED.name, user_name = EXCLUDED.user_name, agent_name = EXCLUDED.agent_name,"
    " manager = EXCLUDED.manager, position = EXCLUDED.position, office = EXCLUDED.office,"
    " email = EXCLUDED.email, manager_email = EXCLUDED.manager_email, synced_at = NOW()"
)

_extensions_map = lambda r: {  # noqa: E731
    "name": r["Name"],
    "extension": str(r["Extension"]) if r["Extension"] is not None else None,
    "user_name": r["User_name"],
    "agent_name": r["Agent_name"],
    "manager": r["Manager"],
    "position": r["Position"],
    "office": r["Office"],
    "email": r["Email"],
    "manager_email": r["ManagerEmail"],
}


async def _run_full_sync_extensions(log_id: int) -> None:
    await _mssql_full_sync(log_id, "extensions_full", _EXTENSIONS_SELECT, "extensions", _EXTENSIONS_UPSERT, _extensions_map)


async def hourly_sync_extensions(session_factory: async_sessionmaker) -> None:
    """Hourly full truncate+reload of extensions."""
    if await _is_running("extensions"):
        logger.info("ETL extensions: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="extensions_full", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
        await _run_full_sync_extensions(log_id)
    except Exception as e:
        logger.error("Hourly extensions sync failed: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# agent_targets (report.target) — daily sync (CLAUD-56 performance bar)
# ---------------------------------------------------------------------------

_AGENT_TARGETS_SELECT = """
    SELECT t.agent_id, u.email, t.date, t.net
    FROM report.target t
    LEFT JOIN report.vtiger_user u ON u.id = t.agent_id
    WHERE t.date >= DATEADD(month, -1, DATEADD(day, 1-DAY(GETDATE()), CAST(GETDATE() AS DATE)))
      AND t.net IS NOT NULL
      AND t.agent_id IS NOT NULL
"""

_AGENT_TARGETS_UPSERT = """
    INSERT INTO agent_targets (agent_id, agent_email, month_date, net)
    VALUES (:agent_id, :agent_email, :month_date, :net)
    ON CONFLICT (agent_id, month_date) DO UPDATE SET
        net = EXCLUDED.net,
        agent_email = EXCLUDED.agent_email,
        updated_at = NOW()
"""

_agent_targets_map = lambda r: {  # noqa: E731
    "agent_id": int(r["agent_id"]),
    "agent_email": str(r["email"]).strip() if r["email"] is not None else None,
    "month_date": r["date"].date() if hasattr(r["date"], "date") else r["date"],
    "net": float(r["net"]) if r["net"] is not None else 0,
}


async def sync_agent_targets(session_factory: async_sessionmaker) -> None:
    """Daily sync of agent targets from report.target → agent_targets."""
    if await _is_running("agent_targets"):
        logger.info("ETL agent_targets: skipping — sync already in progress")
        return
    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type="agent_targets_daily", status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        rows = await execute_query(_AGENT_TARGETS_SELECT, ())
        if rows:
            mapped = [_agent_targets_map(r) for r in rows]
            async with session_factory() as db:
                await db.execute(text(_AGENT_TARGETS_UPSERT), mapped)
                await db.commit()

        async with session_factory() as db:
            log = await db.get(EtlSyncLog, log_id)
            if log:
                log.status = "completed"
                log.rows_synced = len(rows) if rows else 0
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()

        logger.info("ETL agent_targets: synced %d rows", len(rows) if rows else 0)

    except Exception as e:
        logger.error("ETL agent_targets sync failed: %s", e)
        if log_id:
            try:
                async with session_factory() as db:
                    log = await db.get(EtlSyncLog, log_id)
                    if log:
                        log.status = "error"
                        log.error_message = str(e)
                        log.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Dynamic retention MV builder
# ---------------------------------------------------------------------------

def _build_mv_sql(extra_cols: list) -> str:
    """Build the CREATE MATERIALIZED VIEW retention_mv SQL dynamically."""

    dealio_extras = [c for c in extra_cols if c["source_table"] == "dealio_users"]
    ant_acc_extras = [c for c in extra_cols if c["source_table"] == "ant_acc"]
    trades_extras = [c for c in extra_cols if c["source_table"] == "trades_mt4"]
    mtt_extras = [c for c in extra_cols if c["source_table"] == "vtiger_mttransactions"]
    vta_extras = [c for c in extra_cols if c["source_table"] == "vtiger_trading_accounts"]

    trades_agg_extras = ""
    for c in trades_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        trades_agg_extras += ",\n                    COALESCE(" + agg + "(t." + col + "), 0) AS " + col

    deposits_agg_extras = ""
    for c in mtt_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        deposits_agg_extras += ",\n                    COALESCE(" + agg + "(mtt." + col + "), 0) AS " + col

    balance_agg_extras = ""
    for c in dealio_extras:
        agg = c["agg_fn"]
        col = c["source_column"]
        balance_agg_extras += ",\n                    COALESCE(" + agg + "(du." + col + "), 0) AS " + col

    vta_extras_cte = ""
    vta_extras_join = ""
    if vta_extras:
        vta_select_parts = ["ql.accountid"]
        for c in vta_extras:
            agg = c["agg_fn"]
            col = c["source_column"]
            vta_select_parts.append("COALESCE(" + agg + "(vta." + col + "), 0) AS " + col)
        vta_select_str = ",\n                    ".join(vta_select_parts)
        vta_extras_cte = (
            ",\nvta_extras_agg AS (\n"
            "    SELECT\n"
            "                    " + vta_select_str + "\n"
            "    FROM qualifying_logins ql\n"
            "    LEFT JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = ql.accountid\n"
            "    GROUP BY ql.accountid\n"
            ")"
        )
        vta_extras_join = "\n            INNER JOIN vta_extras_agg vea ON vea.accountid = a.accountid"

    final_select_extras = ""
    for c in trades_extras:
        col = c["source_column"]
        final_select_extras += ",\n                ta." + col
    for c in mtt_extras:
        col = c["source_column"]
        final_select_extras += ",\n                da." + col
    for c in dealio_extras:
        col = c["source_column"]
        final_select_extras += ",\n                ab." + col
    for c in ant_acc_extras:
        col = c["source_column"]
        final_select_extras += ",\n                a." + col + " AS " + col
    for c in vta_extras:
        col = c["source_column"]
        final_select_extras += ",\n                vea." + col

    # Use chr(39) to embed SQL single quotes in the generated SQL
    sq = chr(39)
    sql = (
        "CREATE MATERIALIZED VIEW retention_mv AS\n"
        "            WITH qualifying_logins AS (\n"
        "                SELECT vta.login, a.accountid\n"
        "                FROM ant_acc a\n"
        "                INNER JOIN vtiger_trading_accounts vta ON vta.vtigeraccountid = a.accountid\n"
        "                WHERE a.client_qualification_date IS NOT NULL\n"
        "                  AND a.client_qualification_date >= " + sq + "2024-01-01" + sq + "\n"
        "                  AND (a.is_test_account IS NULL OR a.is_test_account = 0)\n"
        "            ),\n"
        "            trades_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COUNT(t.ticket) AS trade_count,\n"
        "                    COALESCE(SUM(t.computed_profit), 0) AS total_profit,\n"
        "                    MAX(t.open_time) AS last_trade_date,\n"
        "                    MAX(CASE WHEN t.close_time > '1971-01-01' THEN t.close_time END) AS last_close_time,\n"
        "                    ROUND(COUNT(CASE WHEN t.computed_profit > 0 THEN 1 END)::numeric / NULLIF(COUNT(t.ticket), 0) * 100, 1) AS win_rate,\n"
        "                    ROUND(COALESCE(AVG(t.notional_value), 0)::numeric, 2) AS avg_trade_size" + trades_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)\n"
        "                    AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN (" + sq + "inactivity" + sq + ", " + sq + "zeroingusd" + sq + ", " + sq + "spread" + sq + "))\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            deposits_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COUNT(mtt.mttransactionsid) AS deposit_count,\n"
        "                    COALESCE(SUM(mtt.usdamount), 0) AS total_deposit,\n"
        "                    MAX(mtt.confirmation_time) AS last_deposit_time" + deposits_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN vtiger_mttransactions mtt ON mtt.login = ql.login\n"
        "                    AND mtt.transactionapproval = " + sq + "Approved" + sq + "\n"
        "                    AND mtt.transactiontype = " + sq + "Deposit" + sq + "\n"
        "                    AND (mtt.payment_method IS NULL OR mtt.payment_method != " + sq + "BonusProtectedPositionCashback" + sq + ")\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            balance_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COALESCE(SUM(du.compbalance), 0) AS total_balance,\n"
        "                    COALESCE(SUM(du.compcredit), 0) AS total_credit,\n"
        "                    COALESCE(SUM(du.compprevequity), 0) AS total_equity" + balance_agg_extras + "\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN dealio_users du ON du.login = ql.login\n"
        "                GROUP BY ql.accountid\n"
        "            ),\n"
        "            trading_activity_agg AS (\n"
        "                SELECT\n"
        "                    ql.accountid,\n"
        "                    COALESCE(GREATEST(\n"
        "                        COUNT(CASE WHEN t.open_time >= CURRENT_DATE - 30 THEN 1 END)::numeric / 21,\n"
        "                        COUNT(CASE WHEN t.open_time >= CURRENT_DATE - 7 THEN 1 END)::numeric / 5\n"
        "                    ), 0) AS max_open_trade,\n"
        "                    COALESCE(GREATEST(\n"
        "                        SUM(CASE WHEN t.open_time >= CURRENT_DATE - 30 THEN COALESCE(t.notional_value, 0) ELSE 0 END)::numeric / 21,\n"
        "                        SUM(CASE WHEN t.open_time >= CURRENT_DATE - 7 THEN COALESCE(t.notional_value, 0) ELSE 0 END)::numeric / 5\n"
        "                    ), 0) AS max_volume\n"
        "                FROM qualifying_logins ql\n"
        "                LEFT JOIN trades_mt4 t ON t.login = ql.login AND t.cmd IN (0, 1)\n"
        "                    AND t.open_time >= CURRENT_DATE - 30\n"
        "                    AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN (" + sq + "inactivity" + sq + ", " + sq + "zeroingusd" + sq + ", " + sq + "spread" + sq + "))\n"
        "                GROUP BY ql.accountid\n"
        "            )" + vta_extras_cte + "\n"
        "            SELECT\n"
        "                a.accountid,\n"
        "                TRIM(COALESCE(a.full_name, '')) AS full_name,\n"
        "                a.client_qualification_date,\n"
        "                a.sales_client_potential,\n"
        "                a.birth_date,\n"
        "                a.assigned_to,\n"
        "                ta.trade_count,\n"
        "                ta.total_profit,\n"
        "                ta.last_trade_date,\n"
        "                ta.last_close_time,\n"
        "                da.deposit_count,\n"
        "                da.total_deposit,\n"
        "                da.last_deposit_time,\n"
        "                ab.total_balance,\n"
        "                ab.total_credit,\n"
        "                ab.total_equity,\n"
        "                ROUND(taa.max_open_trade::numeric, 1) AS max_open_trade,\n"
        "                ROUND(taa.max_volume::numeric, 1) AS max_volume,\n"
        "                ta.win_rate,\n"
        "                ta.avg_trade_size,\n"
        "                TRIM(COALESCE(vu.first_name, '') || ' ' || COALESCE(vu.last_name, '')) AS agent_name" + final_select_extras + "\n"
        "            FROM ant_acc a\n"
        "            INNER JOIN trades_agg ta ON ta.accountid = a.accountid\n"
        "            INNER JOIN deposits_agg da ON da.accountid = a.accountid\n"
        "            INNER JOIN balance_agg ab ON ab.accountid = a.accountid\n"
        "            INNER JOIN trading_activity_agg taa ON taa.accountid = a.accountid" + vta_extras_join + "\n"
        "            LEFT JOIN vtiger_users vu ON vu.id = a.assigned_to\n"
        "            WHERE a.client_qualification_date IS NOT NULL\n"
        "              AND (a.is_test_account IS NULL OR a.is_test_account = 0)\n"
        "            WITH NO DATA"
    )

    return sql


async def rebuild_retention_mv() -> None:
    """Rebuild retention_mv from scratch using current extra columns config."""
    logger.info("rebuild_retention_mv: starting")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT source_table, source_column, agg_fn, display_name FROM retention_extra_columns ORDER BY id")
        )
        extra_cols = [
            {"source_table": r[0], "source_column": r[1], "agg_fn": r[2], "display_name": r[3]}
            for r in result.fetchall()
        ]

    mv_sql = _build_mv_sql(extra_cols)

    async with AsyncSessionLocal() as db:
        await db.execute(text("DROP MATERIALIZED VIEW IF EXISTS retention_mv CASCADE"))
        await db.commit()

    logger.info("rebuild_retention_mv: dropped existing MV")

    async with AsyncSessionLocal() as db:
        await db.execute(text(mv_sql))
        await db.commit()

    logger.info("rebuild_retention_mv: created new MV definition")

    async with AsyncSessionLocal() as db:
        await db.execute(text("CREATE UNIQUE INDEX retention_mv_accountid ON retention_mv (accountid)"))
        await db.commit()

    logger.info("rebuild_retention_mv: unique index created")

    # Refresh (non-concurrent since freshly created)
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("SET work_mem = '256MB'"))
        await conn.execute(text("REFRESH MATERIALIZED VIEW retention_mv"))

    logger.info("rebuild_retention_mv: MV refreshed with data")

    # Compute scores for ALL clients and store in client_scores
    # Uses the shared recalculate_all_scores() from client_scoring (CLAUD-57)
    try:
        from app.routers.client_scoring import recalculate_all_scores
        await recalculate_all_scores()
    except Exception as score_err:
        logger.warning("rebuild_retention_mv: score computation failed: %s", score_err)

    # Pre-compute task assignments for all clients so per-page lookup is O(1)
    await rebuild_task_assignments()


async def rebuild_task_assignments() -> None:
    """Recompute client_task_assignments for all accounts in retention_mv.

    Called after every MV rebuild and whenever tasks are created/updated/deleted.
    Replaces the previous approach of running N per-page queries (one per task).
    """
    import json as _json
    from sqlalchemy import select as _select
    from app.models.retention_task import RetentionTask
    from app.routers.retention_tasks import _build_task_where

    try:
        async with AsyncSessionLocal() as db:
            tasks_result = await db.execute(_select(RetentionTask).order_by(RetentionTask.id))
            tasks = tasks_result.scalars().all()

            # Always truncate so stale assignments are removed even when no tasks exist
            await db.execute(text("TRUNCATE TABLE client_task_assignments"))

            if tasks:
                assignments: list[dict] = []
                for task in tasks:
                    try:
                        conditions = _json.loads(task.conditions)
                    except Exception:
                        continue
                    t_where, t_params = _build_task_where(conditions)
                    # _MV_ACTIVE uses :activity_days — supply default
                    t_params.setdefault("activity_days", 35)
                    t_where_clause = " AND ".join(t_where)
                    q = text(f"SELECT m.accountid FROM retention_mv m WHERE {t_where_clause}")
                    matched = await db.execute(q, t_params)
                    for row in matched.fetchall():
                        assignments.append({"accountid": str(row[0]), "task_id": task.id})

                if assignments:
                    await db.execute(
                        text(
                            "INSERT INTO client_task_assignments (accountid, task_id) "
                            "VALUES (:accountid, :task_id) ON CONFLICT DO NOTHING"
                        ),
                        assignments,
                    )

            await db.commit()
            logger.info("rebuild_task_assignments: stored %d assignments", len(assignments) if tasks else 0)
    except Exception as ta_err:
        logger.warning("rebuild_task_assignments failed: %s", ta_err)


# ---------------------------------------------------------------------------
# Retention materialized view refresh
# ---------------------------------------------------------------------------

async def refresh_retention_mv() -> None:
    """Refresh retention_mv. Skips if a full ETL sync is running to avoid
    conflicting with TRUNCATE. Uses CONCURRENTLY when populated so reads
    never block; falls back to regular REFRESH on first population."""
    try:
        async with AsyncSessionLocal() as db:
            # Skip if any full sync is running — REFRESH reads conflict with TRUNCATE
            running = (await db.execute(text(
                "SELECT 1 FROM etl_sync_log WHERE status = 'running' AND sync_type LIKE '%_full' LIMIT 1"
            ))).first()
            if running:
                logger.info("retention_mv refresh skipped — full ETL sync in progress")
                return

            result = await db.execute(text("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'retention_mv'"))
            row = result.first()
            ispopulated = bool(row[0]) if row else False

        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(text("SET work_mem = '64MB'"))
            if ispopulated:
                await conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY retention_mv"))
            else:
                logger.info("retention_mv not yet populated — running initial population...")
                await conn.execute(text("REFRESH MATERIALIZED VIEW retention_mv"))
        logger.info("retention_mv refreshed (concurrent=%s)", ispopulated)
    except Exception as e:
        logger.error("retention_mv refresh failed: %s", e)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.post("/etl/sync-trades")
async def sync_trades(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("trades"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="trades_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_trades, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-ant-acc")
async def sync_ant_acc(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("ant_acc"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="ant_acc_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_ant_acc, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-vta")
async def sync_vta(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("vta"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vta_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vta, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-dealio-users")
async def sync_dealio_users(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("dealio_users"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="dealio_users_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_dealio_users, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-mtt")
async def sync_mtt(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("mtt"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="mtt_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_mtt, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-vtiger-users")
async def sync_vtiger_users(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("vtiger_users"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vtiger_users_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vtiger_users, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-vtiger-campaigns")
async def sync_vtiger_campaigns(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("vtiger_campaigns"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="vtiger_campaigns_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_vtiger_campaigns, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-extensions")
async def sync_extensions(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("extensions"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="extensions_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_extensions, log.id)
    return {"status": "started", "log_id": log.id}


async def _run_full_sync_open_pnl(log_id: int) -> None:
    """Sync open PNL (replica, optional) and exposure_usd (local trades_mt4, always) per login.

    Exposure formula (CLAUD-49): net notional per (login, symbol) → ABS → sum.
    Exposure is sourced from the LOCAL trades_mt4 table so it always works even when
    the replica is unreachable. PNL comes from replica dealio.trades_mt4 when available;
    defaults to 0 when the replica cannot be reached.
    """
    from app.replica_database import _ReplicaSession

    # Step 1: Exposure from local trades_mt4 — no replica needed.
    exp_dict: dict = {}
    try:
        async with AsyncSessionLocal() as db:
            exp_result = await db.execute(
                text(
                    "SELECT login::text, SUM(ABS(symbol_net)) AS exposure_usd"
                    " FROM ("
                    "  SELECT login::text, symbol,"
                    "         SUM(CASE WHEN cmd = 0 THEN notional_value ELSE -notional_value END) AS symbol_net"
                    "  FROM trades_mt4"
                    "  WHERE cmd IN (0, 1) AND close_time < '1971-01-01'"
                    "  GROUP BY login, symbol"
                    " ) ps"
                    " GROUP BY login"
                )
            )
            exp_dict = {str(r[0]): float(r[1] or 0) for r in exp_result.fetchall()}
    except Exception as e:
        await _update_log(log_id, "error", error=f"Local exposure query failed: {e}")
        logger.error("sync_open_pnl: local exposure query failed: %s", e)
        return

    # Step 2: Live PNL from replica dealio.trades_mt4 (optional).
    pnl_dict: dict = {}
    if _ReplicaSession is not None:
        try:
            async with _ReplicaSession() as replica:
                pnl_result = await replica.execute(
                    text(
                        "SELECT login::text, SUM(computed_profit) AS pnl"
                        " FROM dealio.trades_mt4"
                        " WHERE close_time < '1971-01-01'"
                        " GROUP BY login"
                    )
                )
                pnl_dict = {str(r[0]): float(r[1] or 0) for r in pnl_result.fetchall()}
        except Exception as e:
            logger.warning("sync_open_pnl: replica PNL unavailable (pnl=0): %s", e)

    # Step 3: Merge and write.
    all_logins = set(exp_dict.keys()) | set(pnl_dict.keys())
    rows = [
        {
            "login": login,
            "pnl": pnl_dict.get(login, 0.0),
            "exposure_usd": exp_dict.get(login, 0.0),
        }
        for login in all_logins
    ]

    async with AsyncSessionLocal() as db:
        await db.execute(text("TRUNCATE TABLE open_pnl_cache"))
        if rows:
            await db.execute(
                text(
                    "INSERT INTO open_pnl_cache (login, pnl, exposure_usd, updated_at) "
                    "VALUES (:login, :pnl, :exposure_usd, NOW())"
                ),
                rows,
            )
        await db.commit()

    await _update_log(log_id, "completed", rows_synced=len(rows))
    logger.info("sync_open_pnl: synced %d logins (%d with live PNL)", len(rows), len(pnl_dict))


async def sync_open_pnl_background() -> None:
    """Scheduler-triggered silent sync of open_pnl_cache — no ETL log entry.

    Exposure uses local trades_mt4 (always works; no replica needed).
    PNL uses replica dealio.trades_mt4 when available; defaults to 0 otherwise.
    """
    from app.replica_database import _ReplicaSession

    # Exposure from local trades_mt4 — always runs regardless of replica status.
    exp_dict: dict = {}
    try:
        async with AsyncSessionLocal() as db:
            exp_result = await db.execute(
                text(
                    "SELECT login::text, SUM(ABS(symbol_net)) AS exposure_usd"
                    " FROM ("
                    "  SELECT login::text, symbol,"
                    "         SUM(CASE WHEN cmd = 0 THEN notional_value ELSE -notional_value END) AS symbol_net"
                    "  FROM trades_mt4"
                    "  WHERE cmd IN (0, 1) AND close_time < '1971-01-01'"
                    "  GROUP BY login, symbol"
                    " ) ps"
                    " GROUP BY login"
                )
            )
            exp_dict = {str(r[0]): float(r[1] or 0) for r in exp_result.fetchall()}
    except Exception as e:
        logger.warning("sync_open_pnl_background: local exposure query failed: %s", e)
        return

    # Live PNL from replica (optional).
    pnl_dict: dict = {}
    if _ReplicaSession is not None:
        try:
            async with _ReplicaSession() as replica:
                pnl_result = await replica.execute(
                    text(
                        "SELECT login::text, SUM(computed_profit) AS pnl"
                        " FROM dealio.trades_mt4"
                        " WHERE close_time < '1971-01-01'"
                        " GROUP BY login"
                    )
                )
                pnl_dict = {str(r[0]): float(r[1] or 0) for r in pnl_result.fetchall()}
        except Exception as e:
            logger.warning("sync_open_pnl_background: replica PNL unavailable (pnl=0): %s", e)

    all_logins = set(exp_dict.keys()) | set(pnl_dict.keys())
    rows = [
        {
            "login": login,
            "pnl": pnl_dict.get(login, 0.0),
            "exposure_usd": exp_dict.get(login, 0.0),
        }
        for login in all_logins
    ]

    async with AsyncSessionLocal() as db:
        await db.execute(text("TRUNCATE TABLE open_pnl_cache"))
        if rows:
            await db.execute(
                text(
                    "INSERT INTO open_pnl_cache (login, pnl, exposure_usd, updated_at) "
                    "VALUES (:login, :pnl, :exposure_usd, NOW())"
                ),
                rows,
            )
        await db.commit()
    logger.info("sync_open_pnl_background: synced %d logins (%d with live PNL)", len(rows), len(pnl_dict))


_EXPOSURE_SELECT = (
    "SELECT "
    "  vta.vtigeraccountid AS accountid, "
    "  COALESCE(exp.exposure_usd, 0) AS exposure_usd, "
    "  CASE WHEN COALESCE(exp.exposure_usd, 0) > 0 "
    "    THEN ROUND((ABS(COALESCE(du.compbalance, 0) + COALESCE(du.compcredit, 0)) "
    "         / NULLIF(exp.exposure_usd, 0) * 100)::numeric, 2) "
    "    ELSE NULL END AS exposure_pct, "
    "  NOW() "
    "FROM ({driver_subquery}) vta "
    "LEFT JOIN ("
    "  SELECT vta2.vtigeraccountid, SUM(ABS(ps.symbol_net)) AS exposure_usd"
    "  FROM ("
    "    SELECT login, symbol,"
    "           SUM(CASE WHEN cmd = 0 THEN notional_value ELSE -notional_value END) AS symbol_net"
    "    FROM trades_mt4"
    "    WHERE cmd IN (0, 1) AND close_time < '1971-01-01'"
    "    GROUP BY login, symbol"
    "  ) ps"
    "  JOIN vtiger_trading_accounts vta2 ON vta2.login::text = ps.login::text"
    "  WHERE vta2.vtigeraccountid IS NOT NULL"
    "  GROUP BY vta2.vtigeraccountid"
    ") exp ON exp.vtigeraccountid = vta.vtigeraccountid "
    "LEFT JOIN ("
    "  SELECT vta3.vtigeraccountid,"
    "         SUM(COALESCE(du2.compbalance, 0)) AS compbalance,"
    "         SUM(COALESCE(du2.compcredit, 0))  AS compcredit"
    "  FROM vtiger_trading_accounts vta3"
    "  JOIN dealio_users du2 ON du2.login = vta3.login"
    "  WHERE vta3.vtigeraccountid IS NOT NULL"
    "  GROUP BY vta3.vtigeraccountid"
    ") du ON du.vtigeraccountid = vta.vtigeraccountid"
)


async def sync_account_exposure_cache() -> None:
    """CLAUD-77 / CLAUD-81 / CLAUD-87: Pre-compute per-accountid exposure_usd and exposure_pct.

    Drives from vtiger_trading_accounts (all known accounts) so that every
    accountid always gets a cache row -- even when the account has no open
    trades (exposure_usd = 0, exposure_pct = NULL).

    CLAUD-87 ETL scoping: active clients are processed first (priority pass),
    then non-active clients in a second pass.  This ensures the retention grid
    for active agents is always fresh, even if the full sync is slow.

    Formula:
      exposure_usd = SUM of ABS net notional per (login, symbol) across all
                     open trades (cmd IN (0,1), close_time < 1971-01-01)
                     sourced directly from trades_mt4 (no replica needed).
      exposure_pct = (live_equity / exposure_usd) * 100  where live_equity
                     = ABS(compbalance + compcredit) from dealio_users.
                     NULL when exposure_usd = 0.

    Runs every 3 min (scheduled in main.py lifespan).
    """
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("TRUNCATE TABLE account_exposure_cache"))

            # CLAUD-87 Priority pass: active clients first
            # Uses ON CONFLICT DO NOTHING because the table was just truncated,
            # but is safe if client_active_status is empty (falls through to pass 2).
            active_driver = (
                "SELECT DISTINCT vta_inner.vtigeraccountid "
                "FROM vtiger_trading_accounts vta_inner "
                "JOIN client_active_status cas ON cas.accountid = vta_inner.vtigeraccountid "
                "WHERE vta_inner.vtigeraccountid IS NOT NULL AND cas.is_active = TRUE"
            )
            active_sql = _EXPOSURE_SELECT.format(driver_subquery=active_driver)
            await db.execute(text(
                "INSERT INTO account_exposure_cache (accountid, exposure_usd, exposure_pct, updated_at) "
                + active_sql
                + " ON CONFLICT (accountid) DO NOTHING"
            ))
            active_result = await db.execute(text("SELECT COUNT(*) FROM account_exposure_cache"))
            active_count = active_result.scalar() or 0
            logger.info("CLAUD-87: exposure priority pass — %d active accounts synced", active_count)

            # Pass 2: remaining accounts (non-active or not yet classified)
            remaining_driver = (
                "SELECT DISTINCT vtigeraccountid FROM vtiger_trading_accounts "
                "WHERE vtigeraccountid IS NOT NULL "
                "AND vtigeraccountid NOT IN (SELECT accountid FROM account_exposure_cache)"
            )
            remaining_sql = _EXPOSURE_SELECT.format(driver_subquery=remaining_driver)
            await db.execute(text(
                "INSERT INTO account_exposure_cache (accountid, exposure_usd, exposure_pct, updated_at) "
                + remaining_sql
                + " ON CONFLICT (accountid) DO NOTHING"
            ))

            result = await db.execute(text("SELECT COUNT(*) FROM account_exposure_cache"))
            count = result.scalar() or 0
            await db.commit()
        logger.info("CLAUD-81: sync_account_exposure_cache: synced %d accounts (%d active priority)", count, active_count)
    except Exception as e:
        logger.error("CLAUD-81: sync_account_exposure_cache failed: %s", e)


# ---------------------------------------------------------------------------
# CLAUD-86: Active Client Auto-Classification
# ---------------------------------------------------------------------------

async def compute_active_status() -> None:
    """CLAUD-86: Compute is_active flag for every client in retention_mv.

    Conditions (any one makes a client "active"):
      cond1 — traded (buy/sell) in the last 35 days  (trades_mt4 via vtiger_trading_accounts)
      cond2 — approved deposit in the last 35 days   (vtiger_mttransactions via vtiger_trading_accounts)
      cond3 — has open positions (exposure_usd > 0)   (account_exposure_cache)

    Manual overrides are preserved if not yet expired.
    Expired overrides are cleared in the same pass.

    Runs every 15 min (scheduled in main.py lifespan).
    """
    try:
        async with AsyncSessionLocal() as db:
            # Step 1: Expire stale overrides
            expired = await db.execute(text(
                "UPDATE client_active_status "
                "SET is_manual_override = FALSE "
                "WHERE is_manual_override = TRUE "
                "  AND override_expires_at IS NOT NULL "
                "  AND override_expires_at <= NOW()"
            ))
            expired_count = expired.rowcount
            if expired_count:
                logger.info("CLAUD-86: expired %d manual overrides", expired_count)

            # Step 2: Bulk upsert computed status for all accounts in retention_mv,
            #         skipping rows that have active manual overrides.
            # CTE-based approach: pre-aggregate qualifying account sets first,
            # then join — avoids per-row LATERAL subqueries which are too slow
            # on large production datasets (tens of thousands of accounts).
            await db.execute(text("""
                WITH active_trades AS (
                    SELECT DISTINCT vta.vtigeraccountid AS accountid
                    FROM vtiger_trading_accounts vta
                    JOIN trades_mt4 t ON t.login::text = vta.login::text
                    WHERE t.cmd IN (0, 1)
                      AND t.open_time >= NOW() - INTERVAL '35 days'
                      AND vta.vtigeraccountid IS NOT NULL
                ),
                active_deposits AS (
                    SELECT DISTINCT vta.vtigeraccountid AS accountid
                    FROM vtiger_trading_accounts vta
                    JOIN vtiger_mttransactions mtt ON mtt.login = vta.login
                    WHERE mtt.transactiontype = 'Deposit'
                      AND mtt.transactionapproval = 'Approved'
                      AND mtt.confirmation_time >= NOW() - INTERVAL '35 days'
                      AND vta.vtigeraccountid IS NOT NULL
                ),
                active_exposure AS (
                    SELECT accountid
                    FROM account_exposure_cache
                    WHERE exposure_usd > 0
                )
                INSERT INTO client_active_status
                    (accountid, is_active, cond_open_positions, cond_recent_trade, cond_recent_deposit, computed_at)
                SELECT
                    r.accountid,
                    (at.accountid IS NOT NULL
                     OR ad.accountid IS NOT NULL
                     OR ae.accountid IS NOT NULL) AS is_active,
                    (ae.accountid IS NOT NULL) AS cond_open_positions,
                    (at.accountid IS NOT NULL) AS cond_recent_trade,
                    (ad.accountid IS NOT NULL) AS cond_recent_deposit,
                    NOW() AS computed_at
                FROM (SELECT DISTINCT accountid FROM retention_mv) r
                LEFT JOIN active_trades at ON at.accountid = r.accountid
                LEFT JOIN active_deposits ad ON ad.accountid = r.accountid
                LEFT JOIN active_exposure ae ON ae.accountid = r.accountid
                ON CONFLICT (accountid) DO UPDATE SET
                    is_active = EXCLUDED.is_active,
                    cond_open_positions = EXCLUDED.cond_open_positions,
                    cond_recent_trade = EXCLUDED.cond_recent_trade,
                    cond_recent_deposit = EXCLUDED.cond_recent_deposit,
                    computed_at = EXCLUDED.computed_at
                WHERE NOT client_active_status.is_manual_override
                   OR client_active_status.override_expires_at IS NULL
                   OR client_active_status.override_expires_at <= NOW()
            """))

            # Step 3: Update system_metrics with the active count
            active_count_row = await db.execute(text(
                "SELECT COUNT(*) FROM client_active_status WHERE is_active = TRUE"
            ))
            active_count = active_count_row.scalar() or 0

            await db.execute(text(
                "INSERT INTO system_metrics (key, value, computed_at) "
                "VALUES ('active_client_count', :val, NOW()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at"
            ), {"val": str(active_count)})

            await db.commit()

        logger.info("CLAUD-86: compute_active_status complete — %d active clients", active_count)
    except Exception as e:
        logger.error("CLAUD-86: compute_active_status failed: %s", e)


async def sync_card_types_background() -> None:
    """Background sync: look up IIN card types via iinlist API and store per-client card type.

    1. Fetches any IINs not yet in iin_cache from vtiger_mttransactions.creditcardlast.
    2. Calls {base_url}{iin} (base_url stored as "https://api.iinlist.com/cards?iin=").
    3. Stores _embedded.cards[0].product.name in iin_cache.
    4. Computes per-client card type (most-recent approved deposit) into client_card_type.
    """
    try:
        # Step 1: Get iinlist integration config
        # base_url is stored as the full URL prefix e.g. "https://api.iinlist.com/cards?iin="
        async with AsyncSessionLocal() as db:
            row = (await db.execute(
                text("SELECT base_url, auth_key FROM integrations WHERE LOWER(name) = 'iinlist' AND is_active = TRUE LIMIT 1")
            )).fetchone()
            if not row:
                logger.info("sync_card_types: iinlist integration not configured, skipping")
                return
            base_url = str(row[0])  # keep as-is: already includes path + query prefix
            api_key = str(row[1])

        # Step 2: Find unique IINs not yet in cache
        # Only extract IINs where the first 6 chars are all digits (skip masked/dirty data)
        async with AsyncSessionLocal() as db:
            # Diagnostic: log sample values to verify data format on live server
            sample_result = await db.execute(text(
                "SELECT creditcardlast FROM vtiger_mttransactions"
                " WHERE creditcardlast IS NOT NULL LIMIT 5"
            ))
            samples = [r[0] for r in sample_result.fetchall()]
            logger.info("sync_card_types: sample creditcardlast values: %s", samples)

            counts = await db.execute(text(
                "SELECT"
                " COUNT(*) FILTER (WHERE creditcardlast IS NOT NULL) AS total,"
                " COUNT(*) FILTER (WHERE creditcardlast IS NOT NULL AND LEFT(creditcardlast,6) ~ '^[0-9]{6}$') AS valid_numeric"
                " FROM vtiger_mttransactions"
            ))
            cnt = counts.fetchone()
            logger.info("sync_card_types: creditcardlast total=%s, valid numeric IINs=%s", cnt[0], cnt[1])

            # Also re-fetch IINs previously stored as 'Unknown' (from earlier broken runs)
            result = await db.execute(text(
                "SELECT DISTINCT LEFT(creditcardlast, 6) AS iin"
                " FROM vtiger_mttransactions"
                " WHERE creditcardlast IS NOT NULL AND LENGTH(creditcardlast) >= 6"
                " AND LEFT(creditcardlast, 6) ~ '^[0-9]{6}$'"
                " AND LEFT(creditcardlast, 6) NOT IN ("
                "   SELECT iin FROM iin_cache WHERE card_type != 'Unknown'"
                " )"
            ))
            new_iins = [r[0] for r in result.fetchall()]

        # Step 3: Look up each new IIN
        logger.info("sync_card_types: %d new IINs to look up", len(new_iins))
        looked_up = 0
        if new_iins:
            async with httpx.AsyncClient(timeout=10.0) as client:
                for iin in new_iins:
                    try:
                        resp = await client.get(
                            f"{base_url}{iin}",
                            headers={"X-API-Key": api_key},
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            # Log raw response for first IIN to verify parsing
                            if looked_up == 0:
                                logger.info("sync_card_types: sample API response for IIN %s: %s", iin, str(data)[:500])
                            card_type: str | None = None
                            if isinstance(data, dict):
                                # Response: {"_embedded": {"cards": [{"product": {"name": "..."}}]}}
                                cards = (data.get("_embedded") or {}).get("cards") or []
                                if cards and isinstance(cards[0], dict):
                                    product = cards[0].get("product") or {}
                                    card_type = product.get("name")
                            if not card_type:
                                card_type = "Unknown"
                            async with AsyncSessionLocal() as db:
                                await db.execute(
                                    text(
                                        "INSERT INTO iin_cache (iin, card_type, updated_at) VALUES (:iin, :ct, NOW())"
                                        " ON CONFLICT (iin) DO UPDATE SET card_type = EXCLUDED.card_type, updated_at = NOW()"
                                    ),
                                    {"iin": iin, "ct": card_type},
                                )
                                await db.commit()
                            looked_up += 1
                        else:
                            logger.warning("sync_card_types: IIN %s returned HTTP %s", iin, resp.status_code)
                        await asyncio.sleep(0.2)  # rate-limit: ~5 req/s
                        if looked_up % 100 == 0 and looked_up > 0:
                            logger.info("sync_card_types: progress %d/%d IINs", looked_up, len(new_iins))
                    except Exception as iin_e:
                        logger.warning("sync_card_types: IIN lookup failed for %s: %s", iin, iin_e)

        # Step 4: Recompute per-client card type (most-recent approved deposit per accountid)
        # Deduplicate by accountid using ROW_NUMBER to avoid CardinalityViolationError
        # (one accountid can have multiple logins each contributing rn=1 rows)
        async with AsyncSessionLocal() as db:
            await db.execute(text("""
                INSERT INTO client_card_type (accountid, card_type, updated_at)
                SELECT accountid, card_type, NOW()
                FROM (
                    SELECT
                        vta.vtigeraccountid AS accountid,
                        ic.card_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY vta.vtigeraccountid
                            ORDER BY mtt.confirmation_time DESC NULLS LAST
                        ) AS rn
                    FROM vtiger_mttransactions mtt
                    JOIN vtiger_trading_accounts vta ON vta.login = mtt.login
                    JOIN iin_cache ic ON ic.iin = LEFT(mtt.creditcardlast, 6)
                    WHERE mtt.creditcardlast IS NOT NULL AND LENGTH(mtt.creditcardlast) >= 6
                      AND LEFT(mtt.creditcardlast, 6) ~ '^[0-9]{6}$'
                      AND mtt.transactionapproval = 'Approved'
                      AND mtt.transactiontype = 'Deposit'
                      AND vta.vtigeraccountid IS NOT NULL
                ) deduped
                WHERE rn = 1
                ON CONFLICT (accountid) DO UPDATE SET card_type = EXCLUDED.card_type, updated_at = NOW()
            """))
            await db.commit()

        logger.info("sync_card_types: completed (new IINs looked up: %d)", len(new_iins))
    except Exception as e:
        logger.error("sync_card_types failed: %s", e)


@router.post("/etl/sync-agent-targets")
async def sync_agent_targets_endpoint(
    background_tasks: BackgroundTasks,
    _=Depends(require_admin),
) -> dict:
    """Manual trigger for agent_targets sync (CLAUD-70)."""
    background_tasks.add_task(sync_agent_targets, AsyncSessionLocal)
    return {"status": "started"}


@router.post("/etl/sync-card-types")
async def sync_card_types_endpoint(
    background_tasks: BackgroundTasks,
    _=Depends(require_admin),
) -> dict:
    background_tasks.add_task(sync_card_types_background)
    return {"status": "started"}


@router.post("/etl/sync-open-pnl")
async def sync_open_pnl(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    if await _is_running("open_pnl"):
        return {"status": "already_running"}
    log = EtlSyncLog(sync_type="open_pnl_full", status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_full_sync_open_pnl, log.id)
    return {"status": "started", "log_id": log.id}


@router.post("/etl/sync-exposure")
async def sync_exposure(
    background_tasks: BackgroundTasks,
    _=Depends(require_admin),
) -> dict:
    """CLAUD-77: Manual trigger to rebuild account_exposure_cache."""
    background_tasks.add_task(sync_account_exposure_cache)
    return {"status": "started"}


@router.post("/etl/compute-active-status")
async def compute_active_status_endpoint(
    background_tasks: BackgroundTasks,
    _=Depends(require_admin),
) -> dict:
    """CLAUD-86: Manual trigger to recompute active client classification."""
    background_tasks.add_task(compute_active_status)
    return {"status": "started"}


async def _safe_count(db: AsyncSession, sql: str) -> int:
    """Run a COUNT query with a short lock_timeout so a locked table never hangs the endpoint."""
    try:
        await db.execute(text("SET LOCAL lock_timeout = '3s'"))
        return (await db.execute(text(sql))).scalar() or 0
    except Exception:
        await db.rollback()
        return 0


async def _safe_last(db: AsyncSession, sql: str):
    """Run a last-row query with a short lock_timeout."""
    try:
        await db.execute(text("SET LOCAL lock_timeout = '3s'"))
        return (await db.execute(text(sql))).first()
    except Exception:
        await db.rollback()
        return None


@router.get("/etl/sync-status")
async def sync_status(
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> dict:
    # CLAUD-36: Return cached response if still fresh (30s TTL).
    # With 70 users polling every 10s, this reduces DB load from ~600 queries/min
    # to ~40 queries/min (one actual query per 30s window).
    global _sync_status_cache, _sync_status_cache_expires
    now = time.time()
    if _sync_status_cache is not None and _sync_status_cache_expires > now:
        return _sync_status_cache

    logs_result = await db.execute(
        text("SELECT * FROM etl_sync_log ORDER BY started_at DESC LIMIT 100")
    )
    rows = logs_result.mappings().all()

    trades_count = await _safe_count(db, "SELECT COUNT(*) FROM trades_mt4")
    ant_acc_count = await _safe_count(db, "SELECT COUNT(*) FROM ant_acc")
    vta_count = await _safe_count(db, "SELECT COUNT(*) FROM vtiger_trading_accounts")
    mtt_count = await _safe_count(db, "SELECT COUNT(*) FROM vtiger_mttransactions")
    dealio_users_count = await _safe_count(db, "SELECT COUNT(*) FROM dealio_users")
    vtiger_users_count = await _safe_count(db, "SELECT COUNT(*) FROM vtiger_users")
    vtiger_campaigns_count = await _safe_count(db, "SELECT COUNT(*) FROM vtiger_campaigns")
    extensions_count = await _safe_count(db, "SELECT COUNT(*) FROM extensions")
    open_pnl_count = await _safe_count(db, "SELECT COUNT(*) FROM open_pnl_cache")
    proline_count = await _safe_count(db, "SELECT COUNT(*) FROM proline_data")
    agent_targets_count = await _safe_count(db, "SELECT COUNT(*) FROM agent_targets")
    exposure_cache_count = await _safe_count(db, "SELECT COUNT(*) FROM account_exposure_cache")
    active_status_count = await _safe_count(db, "SELECT COUNT(*) FROM client_active_status")
    active_clients_count = await _safe_count(db, "SELECT COUNT(*) FROM client_active_status WHERE is_active = TRUE")

    def _last_row(row) -> dict | None:
        if row is None:
            return None
        return {"id": str(row[0]), "modified": row[1].isoformat() if row[1] else None}

    trades_last = _last_row(await _safe_last(db, "SELECT ticket, last_modified FROM trades_mt4 WHERE last_modified <= NOW() ORDER BY last_modified DESC NULLS LAST LIMIT 1"))
    ant_acc_last = _last_row(await _safe_last(db, "SELECT accountid, modifiedtime FROM ant_acc WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))
    vta_last = _last_row(await _safe_last(db, "SELECT login, modifiedtime FROM vtiger_trading_accounts WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))
    mtt_last = _last_row(await _safe_last(db, "SELECT mttransactionsid, modifiedtime FROM vtiger_mttransactions WHERE modifiedtime <= NOW() ORDER BY modifiedtime DESC NULLS LAST LIMIT 1"))
    dealio_users_last = _last_row(await _safe_last(db, "SELECT login, lastupdate FROM dealio_users WHERE lastupdate <= NOW() ORDER BY lastupdate DESC NULLS LAST LIMIT 1"))
    vtiger_users_last = _last_row(await _safe_last(db, "SELECT id, NULL FROM vtiger_users LIMIT 1"))
    vtiger_campaigns_last = _last_row(await _safe_last(db, "SELECT crmid, NULL FROM vtiger_campaigns LIMIT 1"))
    extensions_last = _last_row(await _safe_last(db, "SELECT extension, synced_at FROM extensions ORDER BY synced_at DESC NULLS LAST LIMIT 1"))
    open_pnl_last = _last_row(await _safe_last(db, "SELECT login, updated_at FROM open_pnl_cache ORDER BY updated_at DESC NULLS LAST LIMIT 1"))
    proline_last = _last_row(await _safe_last(db, "SELECT account_id, synced_at FROM proline_data ORDER BY synced_at DESC NULLS LAST LIMIT 1"))
    agent_targets_last = _last_row(await _safe_last(db, "SELECT agent_id, updated_at FROM agent_targets ORDER BY updated_at DESC NULLS LAST LIMIT 1"))
    exposure_cache_last = _last_row(await _safe_last(db, "SELECT accountid, updated_at FROM account_exposure_cache ORDER BY updated_at DESC NULLS LAST LIMIT 1"))
    active_status_last = _last_row(await _safe_last(db, "SELECT accountid, computed_at FROM client_active_status ORDER BY computed_at DESC NULLS LAST LIMIT 1"))

    result = {
        "trades_row_count": trades_count,
        "ant_acc_row_count": ant_acc_count,
        "vta_row_count": vta_count,
        "mtt_row_count": mtt_count,
        "dealio_users_row_count": dealio_users_count,
        "vtiger_users_row_count": vtiger_users_count,
        "vtiger_campaigns_row_count": vtiger_campaigns_count,
        "extensions_row_count": extensions_count,
        "open_pnl_row_count": open_pnl_count,
        "proline_row_count": proline_count,
        "agent_targets_row_count": agent_targets_count,
        "exposure_cache_row_count": exposure_cache_count,
        "active_status_row_count": active_status_count,
        "active_clients_count": active_clients_count,
        "trades_last": trades_last,
        "ant_acc_last": ant_acc_last,
        "vta_last": vta_last,
        "mtt_last": mtt_last,
        "dealio_users_last": dealio_users_last,
        "vtiger_users_last": vtiger_users_last,
        "vtiger_campaigns_last": vtiger_campaigns_last,
        "extensions_last": extensions_last,
        "open_pnl_last": open_pnl_last,
        "proline_last": proline_last,
        "agent_targets_last": agent_targets_last,
        "exposure_cache_last": exposure_cache_last,
        "active_status_last": active_status_last,
        "logs": [
            {
                "id": r["id"],
                "sync_type": r["sync_type"],
                "status": r["status"],
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                "rows_synced": r["rows_synced"],
                "error_message": r["error_message"],
            }
            for r in rows
        ],
    }

    # Cache the result for 30 seconds
    _sync_status_cache = result
    _sync_status_cache_expires = now + _SYNC_STATUS_TTL

    return result


@router.get("/etl/diagnose-account")
async def diagnose_account(
    accountid: str,
    login: str = "",
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Diagnostic: check why a specific account may have 0 trades in the retention MV."""
    result: dict = {"accountid": accountid, "login_checked": login}

    # 1. Check ant_acc
    r = (await db.execute(
        text("SELECT accountid, client_qualification_date, is_test_account, assigned_to FROM ant_acc WHERE accountid = :aid"),
        {"aid": accountid},
    )).fetchone()
    result["ant_acc"] = dict(r._mapping) if r else None

    # 2. Check vtiger_trading_accounts for this accountid
    vta_rows = (await db.execute(
        text("SELECT login, vtigeraccountid, balance, credit FROM vtiger_trading_accounts WHERE vtigeraccountid = :aid"),
        {"aid": accountid},
    )).fetchall()
    result["vtiger_trading_accounts_for_account"] = [dict(r._mapping) for r in vta_rows]

    # 3. If a specific login was supplied, check trades_mt4 for it
    if login:
        trade_rows = (await db.execute(
            text(
                "SELECT COUNT(*) AS total, "
                "COUNT(CASE WHEN cmd IN (0,1) THEN 1 END) AS buy_sell_count, "
                "MIN(open_time) AS first_trade, MAX(open_time) AS last_trade, "
                "ROUND(SUM(computed_profit)::numeric, 2) AS total_profit "
                "FROM trades_mt4 WHERE login = :login"
            ),
            {"login": login},
        )).fetchone()
        result["trades_mt4_for_login"] = dict(trade_rows._mapping) if trade_rows else None

        # Also check if the login exists at all in vtiger_trading_accounts
        vta_login = (await db.execute(
            text("SELECT login, vtigeraccountid FROM vtiger_trading_accounts WHERE login = :login"),
            {"login": login},
        )).fetchone()
        result["vtiger_trading_accounts_for_login"] = dict(vta_login._mapping) if vta_login else None

    # 4. Check open_pnl_cache for all logins linked to this account
    linked_logins = [r[0] for r in vta_rows] if vta_rows else []
    if login and login not in linked_logins:
        linked_logins.append(login)
    if linked_logins:
        pnl_rows = (await db.execute(
            text("SELECT login, pnl FROM open_pnl_cache WHERE login = ANY(:logins)"),
            {"logins": linked_logins},
        )).fetchall()
        result["open_pnl_cache"] = [dict(r._mapping) for r in pnl_rows]

    # 5. Check retention_mv row for this account
    mv_row = (await db.execute(
        text("SELECT accountid, trade_count, total_profit, last_trade_date, deposit_count, total_balance, total_credit FROM retention_mv WHERE accountid = :aid"),
        {"aid": accountid},
    )).fetchone()
    result["retention_mv"] = dict(mv_row._mapping) if mv_row else None

    return result
