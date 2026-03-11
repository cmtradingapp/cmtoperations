import logging
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth_deps import require_admin
from app.models.etl_sync_log import EtlSyncLog
from app.models.integration import Integration
from app.pg_database import AsyncSessionLocal, get_db

logger = logging.getLogger(__name__)
router = APIRouter()

_PROLINE_SYNC_TYPE = "proline_sync"

_PROLINE_UPSERT = (
    "INSERT INTO proline_data"
    " (account_id, affiliate_id, performance_commission, qualified_ftd_date, synced_at)"
    " VALUES"
    " (:account_id, :affiliate_id, :performance_commission, :qualified_ftd_date, NOW())"
    " ON CONFLICT (account_id) DO UPDATE SET"
    " affiliate_id = EXCLUDED.affiliate_id,"
    " performance_commission = EXCLUDED.performance_commission,"
    " qualified_ftd_date = EXCLUDED.qualified_ftd_date,"
    " synced_at = NOW()"
)


async def _is_running() -> bool:
    """Return True if a Proline sync is already running."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text(
                "SELECT 1 FROM etl_sync_log"
                " WHERE sync_type = :sync_type AND status = 'running' LIMIT 1"
            ),
            {"sync_type": _PROLINE_SYNC_TYPE},
        )
        return result.first() is not None


async def _update_log(
    log_id: int,
    status: str,
    rows_synced: int | None = None,
    error: str | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        log = await db.get(EtlSyncLog, log_id)
        if log:
            log.status = status
            log.rows_synced = rows_synced
            log.error_message = error
            log.completed_at = datetime.now(timezone.utc)
            await db.commit()


def _build_request_body() -> dict:
    """Build the Proline API request body with today and 31-days-ago dates."""
    now = datetime.now(timezone.utc)
    from_date = now - timedelta(days=31)
    now_str = now.strftime("%Y/%m/%d")
    from_str = from_date.strftime("%Y/%m/%d")

    return {
        "userId": 110002,
        "reportName": "Customers",
        "selections": {
            "columns": [
                "AccountId", "CampaignId", "Signups", "QualifiedFTDs",
                "AffiliateId", "CurrentDealId", "FTDs", "CrmId",
                "FirstDeposits", "Deposits", "PerformanceCommissionMainCurrency",
                "CPAs", "LandingPageId", "SignupDate", "QualifiedFtdDate",
                "CpaCommissionMainCurrency", "RevShareCommissionMainCurrency",
                "AcquisitionDate", "AcquisitionCost", "AcquisitionDealId",
                "AcquisitionDealType", "Equity", "Balance", "Refferal",
            ],
            "filters": {
                "ActivityRange": {
                    "value": f"2007/01/01,{now_str}",
                    "isNot": False,
                },
                "Affiliate": {
                    "value": "1000",
                    "isNot": True,
                },
                "BusinessUnit": {
                    "value": "3",
                    "isNot": False,
                },
                "CustomerActivityKind": {
                    "value": "QualifiedDate",
                    "isNot": False,
                },
                "CustomerActivityRange": {
                    "value": f"{from_str}, {now_str}",
                    "isNot": False,
                },
            },
            "drillDownBy": [],
        },
    }


def _parse_row(row: dict) -> dict:
    """Map a single API response row to local table columns."""
    raw_date = row.get("QualifiedFtdDate")
    qualified_ftd_date = None
    if raw_date:
        try:
            # API may return ISO-8601 strings like "2024-03-15T00:00:00" or similar
            qualified_ftd_date = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            qualified_ftd_date = None

    raw_commission = row.get("PerformanceCommissionMainCurrency")
    performance_commission = None
    if raw_commission is not None:
        try:
            performance_commission = float(raw_commission)
        except (ValueError, TypeError):
            performance_commission = None

    account_id = str(row.get("AccountId", "")).strip() if row.get("AccountId") is not None else None
    affiliate_id = str(row.get("AffiliateId", "")).strip() if row.get("AffiliateId") is not None else None

    return {
        "account_id": account_id,
        "affiliate_id": affiliate_id,
        "performance_commission": performance_commission,
        "qualified_ftd_date": qualified_ftd_date,
    }


async def sync_proline_data(session_factory: async_sessionmaker) -> None:
    """Main Proline sync function — called by the APScheduler cron job."""
    if await _is_running():
        logger.info("Proline ETL: skipping scheduled run — sync already in progress")
        return

    log_id: int | None = None
    try:
        async with session_factory() as db:
            log = EtlSyncLog(sync_type=_PROLINE_SYNC_TYPE, status="running")
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id

        await _run_proline_sync(log_id)

    except Exception as e:
        logger.error("Proline ETL outer error: %s", e)
        if log_id:
            await _update_log(log_id, "error", error=str(e))


async def _run_proline_sync(log_id: int) -> None:
    """Core sync logic — fetches from Proline API and upserts into proline_data."""
    try:
        # Look up the 'Proline' integration record
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Integration).where(
                    Integration.name == "Proline",
                    Integration.is_active.is_(True),
                )
            )
            integration = result.scalar_one_or_none()

        if integration is None:
            logger.warning(
                "Proline ETL: no active 'Proline' integration found in integrations table — skipping sync"
            )
            await _update_log(
                log_id,
                "error",
                error="No active 'Proline' integration configured",
            )
            return

        base_url = integration.base_url.rstrip("/")
        auth_key = integration.auth_key

        request_body = _build_request_body()

        # Use a short-lived client — scheduler jobs must not rely on app.state
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                base_url,
                json=request_body,
                headers={
                    "Authorization": auth_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

        if response.status_code != 200:
            error_msg = f"Proline API returned HTTP {response.status_code}: {response.text[:500]}"
            logger.error("Proline ETL: %s", error_msg)
            await _update_log(log_id, "error", error=error_msg)
            return

        try:
            payload = response.json()
        except Exception as parse_err:
            error_msg = f"Proline API returned non-JSON response: {response.text[:200]}"
            logger.error("Proline ETL: %s (parse error: %s)", error_msg, parse_err)
            await _update_log(log_id, "error", error=error_msg)
            return

        data_table = payload.get("DataTable")
        if not isinstance(data_table, list):
            error_msg = f"Proline API: 'DataTable' missing or not a list — keys: {list(payload.keys())}"
            logger.error("Proline ETL: %s", error_msg)
            await _update_log(log_id, "error", error=error_msg)
            return

        if not data_table:
            logger.info("Proline ETL: API returned 0 rows — nothing to upsert")
            await _update_log(log_id, "completed", rows_synced=0)
            return

        rows = [_parse_row(r) for r in data_table if r.get("AccountId") is not None]

        if rows:
            async with AsyncSessionLocal() as db:
                await db.execute(text(_PROLINE_UPSERT), rows)
                await db.commit()

        await _update_log(log_id, "completed", rows_synced=len(rows))
        logger.info("Proline ETL: completed — %d rows upserted", len(rows))

    except Exception as e:
        logger.error("Proline ETL sync failed: %s", e)
        await _update_log(log_id, "error", error=str(e))


# ---------------------------------------------------------------------------
# Manual trigger endpoint (admin-only)
# ---------------------------------------------------------------------------

@router.post("/etl/sync-proline")
async def trigger_proline_sync(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Manually trigger a Proline affiliate data sync (admin only)."""
    if await _is_running():
        return {"status": "already_running"}

    log = EtlSyncLog(sync_type=_PROLINE_SYNC_TYPE, status="running")
    db.add(log)
    await db.commit()
    await db.refresh(log)
    background_tasks.add_task(_run_proline_sync, log.id)
    return {"status": "started", "log_id": log.id}
