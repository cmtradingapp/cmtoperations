import asyncio
import json
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.auth_deps import get_current_user
from app.history_db import insert_call_history
from app.models.call_mapping import CallMapping
from app.pg_database import AsyncSessionLocal
from app.schemas.call import CallStatus
from app.services.elevenlabs_service import initiate_call

logger = logging.getLogger(__name__)
router = APIRouter()

# job_id -> asyncio.Task
_running_tasks: dict[str, asyncio.Task] = {}
# job_id -> cancel flag
_cancel_flags: dict[str, bool] = {}


class BatchClientItem(BaseModel):
    id: str
    first_name: str = ""
    email: str = ""
    phone: str
    retention_rep: str = ""
    retention_status_display: str = ""


class BatchCallStartRequest(BaseModel):
    clients: list[BatchClientItem]
    agent_id: Optional[str] = None
    agent_phone_number_id: Optional[str] = None
    call_provider: str = "twilio"
    concurrency: int = 1


async def _run_batch_job(
    job_id: str,
    clients: list[BatchClientItem],
    agent_id: Optional[str],
    agent_phone_number_id: Optional[str],
    call_provider: str,
    http_client,
    initial_processed: int = 0,
    concurrency: int = 1,
) -> None:
    """Background asyncio task — runs independently of the HTTP session.

    Uses an asyncio.Semaphore to process up to `concurrency` calls simultaneously.
    """
    processed = initial_processed
    failed = 0
    effective_concurrency = max(1, min(concurrency, 100))
    sem = asyncio.Semaphore(effective_concurrency)

    async with AsyncSessionLocal() as session:
        await session.execute(
            text("UPDATE batch_jobs SET status='running', started_at=COALESCE(started_at, NOW()) WHERE job_id=:jid"),
            {"jid": job_id},
        )
        await session.commit()

    async def process_one(client: BatchClientItem) -> None:
        nonlocal processed, failed
        if _cancel_flags.get(job_id):
            return
        async with sem:
            if _cancel_flags.get(job_id):
                return
            try:
                result = await initiate_call(
                    http_client,
                    client.id,
                    client.phone,
                    first_name=client.first_name,
                    email=client.email,
                    agent_id=agent_id,
                    agent_phone_number_id=agent_phone_number_id,
                    call_provider=call_provider,
                    retention_rep=client.retention_rep,
                    retention_status=client.retention_status_display,
                )
                processed += 1
                if result.status != CallStatus.initiated:
                    failed += 1
                await insert_call_history(
                    client_id=client.id,
                    client_name=client.first_name,
                    phone_number=client.phone,
                    conversation_id=result.conversation_id,
                    status=result.status.value,
                    error=result.error,
                    agent_id=agent_id,
                )
                if result.conversation_id:
                    async with AsyncSessionLocal() as session:
                        session.add(CallMapping(conversation_id=result.conversation_id, account_id=client.id))
                        await session.commit()
            except Exception as e:
                logger.error("batch_job %s: error on client %s: %s", job_id, client.id, e)
                processed += 1
                failed += 1

            try:
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        text("UPDATE batch_jobs SET processed_records=:p, failed_records=:f WHERE job_id=:jid"),
                        {"p": processed, "f": failed, "jid": job_id},
                    )
                    await session.commit()
            except Exception as e:
                logger.warning("batch_job %s: progress update failed: %s", job_id, e)

    await asyncio.gather(*[process_one(c) for c in clients], return_exceptions=True)

    cancelled = _cancel_flags.get(job_id, False)
    final_status = "cancelled" if cancelled else "completed"

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                text(
                    "UPDATE batch_jobs SET status=:s, completed_at=NOW(), "
                    "processed_records=:p, failed_records=:f WHERE job_id=:jid"
                ),
                {"s": final_status, "p": processed, "f": failed, "jid": job_id},
            )
            await session.commit()
    except Exception as e:
        logger.error("batch_job %s: failed to write final status: %s", job_id, e)

    _running_tasks.pop(job_id, None)
    _cancel_flags.pop(job_id, None)
    logger.info(
        "batch_job %s finished: status=%s processed=%d failed=%d concurrency=%d",
        job_id, final_status, processed, failed, effective_concurrency,
    )


def start_batch_task(
    job_id: str,
    clients: list[BatchClientItem],
    agent_id: Optional[str],
    agent_phone_number_id: Optional[str],
    call_provider: str,
    http_client,
    initial_processed: int = 0,
    concurrency: int = 1,
) -> None:
    """Create and register an asyncio background task for a batch job."""
    task = asyncio.create_task(
        _run_batch_job(
            job_id, clients, agent_id, agent_phone_number_id,
            call_provider, http_client, initial_processed, concurrency,
        )
    )
    _running_tasks[job_id] = task


# NOTE: /batch-calls/history must be declared before /batch-calls/{job_id}/status
# so FastAPI doesn't match "history" as a job_id path parameter.
@router.get("/batch-calls/history")
async def get_batch_history(user=Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            text(
                "SELECT job_id, status, total_records, processed_records, failed_records, "
                "error_message, started_at, completed_at, created_at, concurrency "
                "FROM batch_jobs WHERE created_by=:uid "
                "ORDER BY created_at DESC LIMIT 50"
            ),
            {"uid": user.id},
        )).fetchall()

    return [
        {
            "job_id": r.job_id,
            "status": r.status,
            "total_records": r.total_records,
            "processed_records": r.processed_records,
            "failed_records": r.failed_records,
            "error_message": r.error_message,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "concurrency": r.concurrency if r.concurrency else 1,
        }
        for r in rows
    ]


@router.post("/batch-calls/start")
async def start_batch_call(
    request: Request,
    body: BatchCallStartRequest,
    user=Depends(get_current_user),
):
    if not body.clients:
        raise HTTPException(status_code=400, detail="No clients provided")

    concurrency = max(1, min(body.concurrency, 100))
    job_id = uuid.uuid4().hex
    clients_data = [c.model_dump() for c in body.clients]

    async with AsyncSessionLocal() as session:
        await session.execute(
            text(
                "INSERT INTO batch_jobs "
                "(job_id, status, created_by, agent_id, agent_phone_number_id, "
                "call_provider, clients_json, total_records, concurrency) "
                "VALUES (:jid, 'queued', :uid, :aid, :pid, :cp, :cj, :tr, :con)"
            ),
            {
                "jid": job_id,
                "uid": user.id,
                "aid": body.agent_id,
                "pid": body.agent_phone_number_id,
                "cp": body.call_provider,
                "cj": json.dumps(clients_data),
                "tr": len(body.clients),
                "con": concurrency,
            },
        )
        await session.commit()

    start_batch_task(
        job_id,
        body.clients,
        body.agent_id,
        body.agent_phone_number_id,
        body.call_provider,
        request.app.state.http_client,
        concurrency=concurrency,
    )

    logger.info("batch_job %s queued: user=%s clients=%d concurrency=%d", job_id, user.id, len(body.clients), concurrency)
    return {"job_id": job_id, "status": "queued", "total_records": len(body.clients), "concurrency": concurrency}


@router.get("/batch-calls/{job_id}/status")
async def get_batch_status(job_id: str, user=Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            text(
                "SELECT job_id, status, total_records, processed_records, failed_records, "
                "error_message, started_at, completed_at, created_at, concurrency "
                "FROM batch_jobs WHERE job_id=:jid AND created_by=:uid"
            ),
            {"jid": job_id, "uid": user.id},
        )).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": row.job_id,
        "status": row.status,
        "total_records": row.total_records,
        "processed_records": row.processed_records,
        "failed_records": row.failed_records,
        "error_message": row.error_message,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "concurrency": row.concurrency if row.concurrency else 1,
    }


@router.post("/batch-calls/{job_id}/cancel")
async def cancel_batch_job(job_id: str, user=Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            text("SELECT status FROM batch_jobs WHERE job_id=:jid AND created_by=:uid"),
            {"jid": job_id, "uid": user.id},
        )).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    if row.status not in ("queued", "running"):
        raise HTTPException(status_code=400, detail=f"Job is already {row.status}")

    _cancel_flags[job_id] = True
    return {"job_id": job_id, "message": "Cancellation requested"}
