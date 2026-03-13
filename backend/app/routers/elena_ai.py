"""Elena AI — upload clients to SquareTalk campaign + sync call results to MSSQL."""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user, decode_jwt
from app.pg_database import get_db, AsyncSessionLocal
from app.rbac import make_page_guard

_require_elena = make_page_guard("elena-ai-results")
from app.services.internal_api import get_crm_data
from app.database import execute_query as mssql_query

logger = logging.getLogger(__name__)

router = APIRouter(tags=["elena-ai"])

SQUARETALK_API_TOKEN = "155f6364-3b95-4ac9-b9c6-876dd59aa504"
SQUARETALK_BASE_URL = "https://ai.agent.squaretalk.com/api/campaigns"

_CONCURRENCY = 20


# ── Pydantic models ────────────────────────────────────────────────────

class CampaignUploadRow(BaseModel):
    accountid: str
    campaign_id: str


class CampaignUploadResult(BaseModel):
    accountid: str
    campaign_id: str
    status: str
    error: str | None = None


class CampaignConfigIn(BaseModel):
    campaign_id: str
    label: str | None = None


# ── MSSQL batch helpers ────────────────────────────────────────────────

def _batch_sync_page_sync(rows: list[tuple]) -> tuple[int, int]:
    """
    Single MSSQL connection per page:
    1. Check which CallIDs already exist (one IN query)
    2. Bulk-insert only the new ones (executemany)
    Returns (inserted, skipped).
    """
    import pyodbc
    from app.config import settings

    if not rows:
        return 0, 0

    conn = pyodbc.connect(settings.mssql_connection_string)
    try:
        cursor = conn.cursor()
        call_ids = [r[0] for r in rows]

        # Check existing in one query
        placeholders = ",".join("?" * len(call_ids))
        cursor.execute(
            f"SELECT CallID FROM [cmt_main].[dbo].[Elena_AI_Results] WHERE CallID IN ({placeholders})",
            call_ids,
        )
        existing = {r[0] for r in cursor.fetchall()}

        new_rows = [r for r in rows if r[0] not in existing]
        skipped = len(rows) - len(new_rows)

        if new_rows:
            cursor.executemany(
                "INSERT INTO [cmt_main].[dbo].[Elena_AI_Results] "
                "(CallID, UserID, Campaign, Duration, Call_start, Call_Status, Goal_Reached, Modification_Date) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                new_rows,
            )
            conn.commit()

        return len(new_rows), skipped
    finally:
        conn.close()


async def batch_sync_page(rows: list[tuple]) -> tuple[int, int]:
    """Async wrapper for _batch_sync_page_sync."""
    return await asyncio.to_thread(_batch_sync_page_sync, rows)


# ── Upload clients to campaign ─────────────────────────────────────────

async def _process_row(http_client, row: CampaignUploadRow, sem: asyncio.Semaphore) -> CampaignUploadResult:
    async with sem:
        try:
            crm_data = await get_crm_data(http_client, row.accountid)
            if not crm_data.phone:
                return CampaignUploadResult(
                    accountid=row.accountid, campaign_id=row.campaign_id,
                    status="error", error="No phone number found in CRM",
                )
            payload = {
                "name": crm_data.first_name or "",
                "phoneNumber": crm_data.phone,
                "data": {"additionalProp1": row.accountid, "additionalProp2": "", "additionalProp3": ""},
                "archived": False,
            }
            resp = await http_client.post(
                f"{SQUARETALK_BASE_URL}/{row.campaign_id}/contacts",
                json=payload,
                headers={"Authorization": SQUARETALK_API_TOKEN},
                timeout=15.0,
            )
            resp.raise_for_status()
            return CampaignUploadResult(accountid=row.accountid, campaign_id=row.campaign_id, status="success")
        except Exception as exc:
            return CampaignUploadResult(
                accountid=row.accountid, campaign_id=row.campaign_id,
                status="error", error=str(exc),
            )


@router.post("/elena-ai/campaign-upload", response_model=List[CampaignUploadResult])
async def campaign_upload(
    rows: List[CampaignUploadRow],
    request: Request,
    _user=Depends(get_current_user),
):
    http_client = request.app.state.http_client
    sem = asyncio.Semaphore(_CONCURRENCY)
    tasks = [_process_row(http_client, row, sem) for row in rows]
    return list(await asyncio.gather(*tasks))


# ── Campaign config CRUD ───────────────────────────────────────────────

@router.get("/elena-ai/campaign-configs")
async def list_campaign_configs(
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_elena),
):
    rows = await db.execute(
        text("SELECT id, campaign_id, label, created_at FROM elena_ai_campaign_configs ORDER BY created_at ASC")
    )
    return [
        {"id": r[0], "campaign_id": r[1], "label": r[2],
         "created_at": r[3].isoformat() if r[3] else None}
        for r in rows.fetchall()
    ]


@router.post("/elena-ai/campaign-configs", status_code=201)
async def create_campaign_config(
    body: CampaignConfigIn,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_elena),
):
    result = await db.execute(
        text(
            "INSERT INTO elena_ai_campaign_configs (campaign_id, label) "
            "VALUES (:cid, :label) "
            "ON CONFLICT (campaign_id) DO UPDATE SET label = EXCLUDED.label "
            "RETURNING id"
        ),
        {"cid": body.campaign_id, "label": body.label},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"status": "ok", "id": new_id}


@router.delete("/elena-ai/campaign-configs/{config_id}")
async def delete_campaign_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(_require_elena),
):
    result = await db.execute(
        text("DELETE FROM elena_ai_campaign_configs WHERE id = :id"),
        {"id": config_id},
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, "Config not found")
    return {"status": "ok"}


# ── SSE sync stream ────────────────────────────────────────────────────

def _sse(event_type: str, data: dict) -> str:
    """Format a Server-Sent Event."""
    payload = json.dumps({"type": event_type, **data})
    return f"data: {payload}\n\n"


def _safe(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


async def _stream_sync(http_client, configs: list, per_page: int = 300, max_pages: int = 0) -> AsyncGenerator[str, None]:
    """
    Generator that yields SSE events while syncing each campaign.
    Events: campaign_start | page_progress | campaign_done | complete | error
    """
    grand_total = {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0}

    for campaign_id, label in configs:
        campaign_name = label or campaign_id

        yield _sse("campaign_start", {"campaign_id": campaign_id, "label": label or campaign_id})

        # Fetch campaign name from SquareTalk
        try:
            resp = await http_client.get(
                f"{SQUARETALK_BASE_URL}/{campaign_id}",
                headers={"Authorization": SQUARETALK_API_TOKEN},
                timeout=15.0,
            )
            if resp.status_code == 200:
                campaign_name = resp.json().get("name") or campaign_name
        except Exception as e:
            logger.warning("Elena AI | cannot fetch campaign name %s: %s", campaign_id, e)

        cam_totals = {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0}

        page = 0
        while True:
            page += 1
            try:
                resp = await http_client.get(
                    f"{SQUARETALK_BASE_URL}/{campaign_id}/calls",
                    params={"page": page, "perPage": per_page},
                    headers={"Authorization": SQUARETALK_API_TOKEN},
                    timeout=30.0,
                )
                if resp.status_code != 200:
                    yield _sse("page_error", {
                        "campaign_id": campaign_id, "page": page,
                        "reason": f"HTTP {resp.status_code}",
                    })
                    break

                raw = resp.json()
                if isinstance(raw, list):
                    calls = raw
                elif isinstance(raw, dict):
                    calls = raw.get("data") or raw.get("calls") or raw.get("items") or []
                else:
                    calls = []

                if not calls:
                    break  # No more pages

                # Build rows for batch processing — skip calls with no ID
                page_rows = []
                page_errors = 0
                for call in calls:
                    try:
                        call_id = _safe(call.get("id"))
                        if not call_id:
                            page_errors += 1
                            continue
                        user_id = _safe((call.get("promptVars") or {}).get("additionalProp1"))
                        goal_reached = 1 if (call.get("goal") or {}).get("reached") else 0
                        call_status = _safe(call.get("status"))
                        duration = call.get("duration") or 0
                        updated_at = _safe(call.get("updatedAt"))
                        created_at = _safe(call.get("createdAt"))
                        page_rows.append((call_id, user_id, campaign_name, duration,
                                          created_at, call_status, goal_reached, updated_at))
                    except Exception as e:
                        logger.warning("Elena AI | parse call error: %s", e)
                        page_errors += 1

                # One MSSQL round-trip for the whole page
                try:
                    page_inserted, page_skipped = await batch_sync_page(page_rows)
                except Exception as e:
                    logger.warning("Elena AI | batch_sync_page error page %d: %s", page, e)
                    page_inserted, page_skipped = 0, 0
                    page_errors += len(page_rows)

                cam_totals["fetched"] += len(calls)
                cam_totals["inserted"] += page_inserted
                cam_totals["skipped"] += page_skipped
                cam_totals["errors"] += page_errors

                yield _sse("page_progress", {
                    "campaign_id": campaign_id,
                    "page": page,
                    "page_fetched": len(calls),
                    "page_inserted": page_inserted,
                    "page_skipped": page_skipped,
                    "page_errors": page_errors,
                    "total_fetched": cam_totals["fetched"],
                    "total_inserted": cam_totals["inserted"],
                    "total_skipped": cam_totals["skipped"],
                    "total_errors": cam_totals["errors"],
                })

                # Stop if last page (fewer results than requested)
                if len(calls) < per_page:
                    break
                # Stop if max_pages limit reached (0 = unlimited)
                if max_pages > 0 and page >= max_pages:
                    break

            except Exception as e:
                yield _sse("page_error", {"campaign_id": campaign_id, "page": page, "reason": str(e)})
                cam_totals["errors"] += 1
                break

        for k in grand_total:
            grand_total[k] += cam_totals[k]

        yield _sse("campaign_done", {"campaign_id": campaign_id, "label": label or campaign_id, **cam_totals})

    yield _sse("complete", grand_total)


@router.get("/elena-ai/sync-stream")
async def sync_stream(
    request: Request,
    token: str = Query(...),
    per_page: int = Query(300, ge=1, le=1000),
    max_pages: int = Query(0, ge=0),  # 0 = unlimited
):
    """SSE endpoint — streams real-time sync progress. Auth via ?token= query param."""
    # Validate token (EventSource can't send headers)
    try:
        decode_jwt(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    async with AsyncSessionLocal() as db:
        rows = await db.execute(
            text("SELECT campaign_id, label FROM elena_ai_campaign_configs ORDER BY created_at ASC")
        )
        configs = rows.fetchall()

    if not configs:
        async def _empty():
            yield _sse("complete", {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0,
                                    "message": "No campaigns configured"})
        return StreamingResponse(_empty(), media_type="text/event-stream")

    http_client = request.app.state.http_client
    return StreamingResponse(
        _stream_sync(http_client, configs, per_page=per_page, max_pages=max_pages),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Results viewer (from MSSQL) ────────────────────────────────────────

@router.get("/elena-ai/results")
async def get_results(
    campaign: str | None = Query(None),
    user_id: str | None = Query(None),
    call_status: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _=Depends(_require_elena),
):
    """Paginated results from MSSQL Elena_AI_Results."""
    conditions: list[str] = []
    params: list[Any] = []

    if campaign:
        conditions.append("Campaign = ?")
        params.append(campaign)
    if user_id:
        conditions.append("UserID = ?")
        params.append(user_id)
    if call_status:
        conditions.append("Call_Status = ?")
        params.append(call_status)
    if date_from:
        conditions.append("Call_start >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("Call_start < ?")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    count_rows = await mssql_query(
        f"SELECT COUNT(*) AS cnt FROM [cmt_main].[dbo].[Elena_AI_Results] {where}",
        tuple(params),
    )
    total = count_rows[0]["cnt"] if count_rows else 0

    data_rows = await mssql_query(
        f"SELECT CallID, UserID, Campaign, Duration, Call_start, Call_Status, Goal_Reached, Modification_Date "
        f"FROM [cmt_main].[dbo].[Elena_AI_Results] {where} "
        f"ORDER BY Call_start DESC "
        f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        tuple(params) + (offset, page_size),
    )

    items = [
        {
            "call_id": r.get("CallID"),
            "user_id": r.get("UserID"),
            "campaign": r.get("Campaign"),
            "duration": r.get("Duration"),
            "call_start": r["Call_start"].isoformat() if r.get("Call_start") else None,
            "call_status": r.get("Call_Status"),
            "goal_reached": bool(r.get("Goal_Reached")),
            "modification_date": r["Modification_Date"].isoformat() if r.get("Modification_Date") else None,
        }
        for r in (data_rows or [])
    ]

    return {"total": total, "page": page, "page_size": page_size, "items": items}


@router.get("/elena-ai/results/campaigns")
async def get_result_campaigns(_=Depends(_require_elena)):
    """Distinct campaign names from MSSQL for filter dropdown."""
    rows = await mssql_query(
        "SELECT DISTINCT Campaign FROM [cmt_main].[dbo].[Elena_AI_Results] "
        "WHERE Campaign IS NOT NULL ORDER BY Campaign",
        (),
    )
    return [r["Campaign"] for r in (rows or [])]


@router.get("/elena-ai/results/summary")
async def get_results_summary(_=Depends(_require_elena)):
    """
    Aggregated stats per campaign:
    - total calls, goal reached count, total duration in minutes
    - call count broken down by status
    """
    rows = await mssql_query(
        "SELECT Campaign, Call_Status, "
        "COUNT(*) AS status_count, "
        "SUM(CASE WHEN LOWER(CAST(Goal_Reached AS NVARCHAR(10))) IN ('1', 'true') THEN 1 ELSE 0 END) AS goal_reached_count, "
        "SUM(ISNULL(Duration, 0)) AS total_duration_secs "
        "FROM [cmt_main].[dbo].[Elena_AI_Results] "
        "WHERE Campaign IS NOT NULL "
        "GROUP BY Campaign, Call_Status "
        "ORDER BY Campaign, Call_Status",
        (),
    )

    # Reshape: group by campaign
    campaigns: dict[str, dict] = {}
    for r in (rows or []):
        cam = r["Campaign"]
        if cam not in campaigns:
            campaigns[cam] = {
                "campaign": cam,
                "total_calls": 0,
                "goal_reached": 0,
                "total_duration_minutes": 0.0,
                "statuses": {},
            }
        status = r["Call_Status"] or "unknown"
        count = int(r["status_count"] or 0)
        campaigns[cam]["total_calls"] += count
        campaigns[cam]["goal_reached"] += int(r["goal_reached_count"] or 0)
        campaigns[cam]["total_duration_minutes"] += round(float(r["total_duration_secs"] or 0) / 60, 1)
        campaigns[cam]["statuses"][status] = count

    return list(campaigns.values())
