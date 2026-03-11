import csv
import io
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.config import settings
from app.models.call_mapping import CallMapping
from app.pg_database import get_db

router = APIRouter()


class LookupRequest(BaseModel):
    conversation_ids: List[str]


@router.post("/call-mappings/lookup")
async def lookup_mappings(
    body: LookupRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> dict:
    if not body.conversation_ids:
        return {"mappings": {}}
    result = await db.execute(
        select(CallMapping).where(CallMapping.conversation_id.in_(body.conversation_ids))
    )
    mappings = {m.conversation_id: m.account_id for m in result.scalars().all()}
    return {"mappings": mappings}


@router.get("/call-mappings/export-unknown")
async def export_unknown_full(
    request: Request,
    agent_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
) -> StreamingResponse:
    """
    Fetches ALL pages from ElevenLabs for the given agent_id,
    collects every unknown conversation, looks up account IDs,
    and returns a complete CSV — regardless of what is loaded on the frontend.
    """
    http_client = request.app.state.http_client
    conversations: List[dict] = []
    cursor: Optional[str] = None

    # Paginate through all ElevenLabs results
    while True:
        params: dict = {"page_size": 100, "call_successful": "unknown"}
        if agent_id:
            params["agent_id"] = agent_id
        if cursor:
            params["cursor"] = cursor

        resp = await http_client.get(
            "https://api.elevenlabs.io/v1/convai/conversations",
            params=params,
            headers={"xi-api-key": settings.elevenlabs_api_key},
        )
        resp.raise_for_status()
        data = resp.json()

        for conv in data.get("conversations", []):
            if conv.get("conversation_id"):
                conversations.append(conv)

        cursor = data.get("next_cursor")
        if not cursor:
            break

    # Look up account IDs from our mapping table
    account_map: dict = {}
    if conversations:
        conv_ids = [c["conversation_id"] for c in conversations]
        result = await db.execute(
            select(CallMapping).where(CallMapping.conversation_id.in_(conv_ids))
        )
        account_map = {m.conversation_id: m.account_id for m in result.scalars().all()}

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["account_id", "conversation_id", "date", "agent_name", "duration_secs"])
    for conv in conversations:
        cid = conv.get("conversation_id", "")
        start = conv.get("start_time_unix_secs")
        date_str = __import__("datetime").datetime.utcfromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S") if start else ""
        writer.writerow([
            account_map.get(cid, ""),
            cid,
            date_str,
            conv.get("agent_name", ""),
            conv.get("call_duration_secs", ""),
        ])

    output.seek(0)
    filename = f"unknown_calls{'_' + agent_id if agent_id else ''}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
