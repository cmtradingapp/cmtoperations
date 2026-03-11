"""Elena AI — upload clients to SquareTalk campaign."""

import asyncio
import logging
from typing import List

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from app.auth_deps import get_current_user
from app.services.internal_api import get_crm_data

logger = logging.getLogger(__name__)

router = APIRouter(tags=["elena-ai"])

SQUARETALK_API_TOKEN = "155f6364-3b95-4ac9-b9c6-876dd59aa504"
SQUARETALK_BASE_URL = "https://ai.agent.squaretalk.com/api/campaigns"

# Max concurrent CRM + SquareTalk calls — prevents rate-limit hammering
_CONCURRENCY = 20


# ── Request / Response schemas ────────────────────────────────────────

class CampaignUploadRow(BaseModel):
    accountid: str
    campaign_id: str


class CampaignUploadResult(BaseModel):
    accountid: str
    campaign_id: str
    status: str  # "success" | "error"
    error: str | None = None


# ── Helpers ───────────────────────────────────────────────────────────

async def _process_row(
    http_client,
    row: CampaignUploadRow,
    sem: asyncio.Semaphore,
) -> CampaignUploadResult:
    """Look up CRM data for one account and push it to SquareTalk."""
    async with sem:
        try:
            crm_data = await get_crm_data(http_client, row.accountid)

            if not crm_data.phone:
                return CampaignUploadResult(
                    accountid=row.accountid,
                    campaign_id=row.campaign_id,
                    status="error",
                    error="No phone number found in CRM",
                )

            payload = {
                "name": crm_data.first_name or "",
                "phoneNumber": crm_data.phone,
                "data": {
                    "additionalProp1": row.accountid,
                    "additionalProp2": "",
                    "additionalProp3": "",
                },
                "archived": False,
            }

            url = f"{SQUARETALK_BASE_URL}/{row.campaign_id}/contacts"
            resp = await http_client.post(
                url,
                json=payload,
                headers={"Authorization": SQUARETALK_API_TOKEN},
                timeout=15.0,
            )
            resp.raise_for_status()

            logger.info(
                "Elena AI | uploaded %s to campaign %s",
                row.accountid,
                row.campaign_id,
            )
            return CampaignUploadResult(
                accountid=row.accountid,
                campaign_id=row.campaign_id,
                status="success",
            )

        except Exception as exc:
            logger.warning(
                "Elena AI | failed %s → campaign %s: %s",
                row.accountid,
                row.campaign_id,
                exc,
            )
            return CampaignUploadResult(
                accountid=row.accountid,
                campaign_id=row.campaign_id,
                status="error",
                error=str(exc),
            )


# ── Endpoint ──────────────────────────────────────────────────────────

@router.post(
    "/elena-ai/campaign-upload",
    response_model=List[CampaignUploadResult],
)
async def campaign_upload(
    rows: List[CampaignUploadRow],
    request: Request,
    _user=Depends(get_current_user),
):
    """Upload a batch of clients to their SquareTalk campaign.

    Processes up to _CONCURRENCY rows in parallel to avoid overwhelming
    the CRM API and SquareTalk rate limits.
    """
    http_client = request.app.state.http_client
    sem = asyncio.Semaphore(_CONCURRENCY)
    tasks = [_process_row(http_client, row, sem) for row in rows]
    results = await asyncio.gather(*tasks)
    return list(results)
