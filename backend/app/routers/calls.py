import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.config import settings
from app.history_db import insert_call_history
from app.models.call_mapping import CallMapping
from app.pg_database import get_db
from app.schemas.call import CallRequest, CallResponse, CallStatus, ClientCallResult
from app.services.elevenlabs_service import initiate_call
from app.services.internal_api import get_crm_data

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory cache for the calls dashboard (expensive: up to 100 ElevenLabs pages)
_calls_dashboard_cache: dict[str, tuple[float, dict]] = {}
_CALLS_DASHBOARD_TTL = 300  # 5 minutes

# Cost cache: conversation_id → cost (float or None). Persists for process lifetime.
# None means "fetched but no cost found"; missing key means "not yet fetched".
_cost_cache: dict[str, float | None] = {}


async def _enrich_with_costs(http_client: Any, conversations: list[dict]) -> None:
    """Fetch individual conversation details for any IDs not yet in the cost cache."""
    new_ids = [
        c["conversation_id"]
        for c in conversations
        if c.get("conversation_id") and c["conversation_id"] not in _cost_cache
    ]
    if not new_ids:
        return

    sem = asyncio.Semaphore(10)

    async def fetch_one(conv_id: str) -> None:
        async with sem:
            try:
                resp = await http_client.get(
                    f"https://api.elevenlabs.io/v1/convai/conversations/{conv_id}",
                    headers={"xi-api-key": settings.elevenlabs_api_key},
                    timeout=10.0,
                )
                resp.raise_for_status()
                data = resp.json()
                cost = (data.get("metadata") or {}).get("cost")
                _cost_cache[conv_id] = float(cost) if cost is not None else None
            except Exception as e:
                logger.warning("Failed to fetch cost for %s: %s", conv_id, e)
                _cost_cache[conv_id] = None  # cache None so we don't retry endlessly

    await asyncio.gather(*[fetch_one(cid) for cid in new_ids])

    # Inject cost into conversation dicts
    for c in conversations:
        cid = c.get("conversation_id")
        if cid and _cost_cache.get(cid) is not None:
            c.setdefault("metadata", {})["cost"] = _cost_cache[cid]


@router.post("/calls/initiate", response_model=CallResponse)
async def initiate_calls(request: Request, body: CallRequest, db: AsyncSession = Depends(get_db)) -> CallResponse:
    http_client = request.app.state.http_client

    async def call_one(client_id: str) -> ClientCallResult:
        crm = await get_crm_data(http_client, client_id)
        if not crm.phone:
            result = ClientCallResult(
                client_id=client_id,
                status=CallStatus.failed,
                error="Could not retrieve phone number for client",
            )
        else:
            result = await initiate_call(
                http_client,
                client_id,
                crm.phone,
                first_name=crm.first_name,
                email=crm.email,
                agent_id=body.agent_id,
                agent_phone_number_id=body.agent_phone_number_id,
                call_provider=body.call_provider,
            )
        await insert_call_history(
            client_id=client_id,
            client_name=crm.first_name,
            phone_number=crm.phone,
            conversation_id=result.conversation_id,
            status=result.status.value,
            error=result.error,
            agent_id=body.agent_id,
        )
        if result.conversation_id:
            db.add(CallMapping(conversation_id=result.conversation_id, account_id=client_id))
        return result

    results = await asyncio.gather(*[call_one(cid) for cid in body.client_ids])
    await db.commit()
    return CallResponse(results=list(results))


@router.get("/calls/history")
async def get_call_history(
    request: Request,
    agent_id: Optional[str] = Query(None),
    call_successful: Optional[str] = Query(None),
    page_size: int = Query(100, ge=1, le=100),
    cursor: Optional[str] = Query(None),
) -> Any:
    http_client = request.app.state.http_client
    params: dict[str, Any] = {"page_size": page_size}
    if agent_id:
        params["agent_id"] = agent_id
    if call_successful:
        params["call_successful"] = call_successful
    if cursor:
        params["cursor"] = cursor
    try:
        response = await http_client.get(
            "https://api.elevenlabs.io/v1/convai/conversations",
            params=params,
            headers={"xi-api-key": settings.elevenlabs_api_key},
        )
        response.raise_for_status()
        data = response.json()
        conversations = data.get("conversations") or []
        await _enrich_with_costs(http_client, conversations)
        return data
    except Exception as e:
        logger.error(f"Failed to fetch ElevenLabs conversations: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch call history from ElevenLabs")


@router.get("/calls/dashboard")
async def get_calls_dashboard(
    request: Request,
    days: int = Query(30, ge=0),
    _: Any = Depends(get_current_user),
) -> dict:
    cache_key = str(days)
    now = time.time()
    if cache_key in _calls_dashboard_cache:
        cached_at, cached_data = _calls_dashboard_cache[cache_key]
        if now - cached_at < _CALLS_DASHBOARD_TTL:
            logger.info("calls/dashboard served from cache (age=%.0fs)", now - cached_at)
            return cached_data

    http_client = request.app.state.http_client
    cutoff_unix = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp()) if days > 0 else 0

    agent_stats: dict[str, dict] = {}
    cursor = None
    total_calls = 0

    for _ in range(100):  # max 100 pages = 10,000 conversations
        params: dict[str, Any] = {"page_size": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            response = await http_client.get(
                "https://api.elevenlabs.io/v1/convai/conversations",
                params=params,
                headers={"xi-api-key": settings.elevenlabs_api_key},
            )
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            resp_body = getattr(getattr(e, 'response', None), 'text', '')
            detail = f"Failed to fetch from ElevenLabs: {e}"
            if resp_body:
                detail += f" | Response: {resp_body[:500]}"
            logger.error(detail)
            raise HTTPException(status_code=502, detail=detail)

        conversations = data.get("conversations", [])
        stop = False
        for conv in conversations:
            if cutoff_unix and (conv.get("start_time_unix_secs") or 0) < cutoff_unix:
                stop = True
                continue
            agent_id = conv.get("agent_id") or "unknown"
            agent_name = conv.get("agent_name") or agent_id
            status = conv.get("call_successful") or "unknown"
            duration = conv.get("call_duration_secs") or 0

            if agent_id not in agent_stats:
                agent_stats[agent_id] = {"agent_id": agent_id, "agent_name": agent_name, "statuses": {}}
            if conv.get("agent_name"):
                agent_stats[agent_id]["agent_name"] = agent_name
            s = agent_stats[agent_id]["statuses"]
            if status not in s:
                s[status] = {"count": 0, "duration_secs": 0}
            s[status]["count"] += 1
            s[status]["duration_secs"] += duration
            total_calls += 1

        if stop or not data.get("has_more") or not conversations:
            break
        cursor = data.get("next_cursor")

    result = []
    for stats in agent_stats.values():
        statuses = stats["statuses"]
        row: dict[str, Any] = {
            "agent_id": stats["agent_id"],
            "agent_name": stats["agent_name"],
            "total_calls": sum(s["count"] for s in statuses.values()),
            "total_duration_mins": round(sum(s["duration_secs"] for s in statuses.values()) / 60, 1),
        }
        for key in ("success", "failure", "unknown"):
            sd = statuses.get(key, {"count": 0, "duration_secs": 0})
            row[f"{key}_count"] = sd["count"]
            row[f"{key}_duration_mins"] = round(sd["duration_secs"] / 60, 1)
        result.append(row)

    result.sort(key=lambda x: x["total_calls"], reverse=True)
    response_data = {"agents": result, "total_calls": total_calls}
    _calls_dashboard_cache[cache_key] = (time.time(), response_data)
    return response_data


@router.get("/calls/conversation/{conversation_id}")
async def get_conversation_detail(conversation_id: str, request: Request) -> Any:
    """Proxy single ElevenLabs conversation detail (includes cost metadata)."""
    try:
        response = await request.app.state.http_client.get(
            f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}",
            headers={"xi-api-key": settings.elevenlabs_api_key},
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch conversation {conversation_id}: {e}")
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/elevenlabs/agents")
async def list_elevenlabs_agents(request: Request, _user=Depends(get_current_user)):
    """Proxy ElevenLabs agent list so the API key stays server-side."""
    try:
        response = await request.app.state.http_client.get(
            "https://api.elevenlabs.io/v1/convai/agents",
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json()
        agents = [
            {"agent_id": a["agent_id"], "name": a.get("name") or a["agent_id"]}
            for a in data.get("agents", [])
        ]
        return {"agents": agents}
    except Exception as e:
        logger.warning("Failed to fetch ElevenLabs agents: %s", e)
        return {"agents": []}
