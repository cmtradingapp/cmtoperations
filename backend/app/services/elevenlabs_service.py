import logging
import uuid
from typing import Optional

import httpx

from app.config import settings
from app.schemas.call import CallStatus, ClientCallResult

logger = logging.getLogger(__name__)

_PROVIDER_URLS = {
    "twilio":    "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
    "sip_trunk": "https://api.elevenlabs.io/v1/convai/sip-trunk/outbound-call",
}


async def initiate_call(
    client: httpx.AsyncClient,
    client_id: str,
    phone_number: str,
    first_name: Optional[str] = None,
    email: Optional[str] = None,
    agent_id: Optional[str] = None,
    agent_phone_number_id: Optional[str] = None,
    call_provider: str = "twilio",
    retention_rep: Optional[str] = None,
    retention_status: Optional[str] = None,
) -> ClientCallResult:
    # Use values from request; fall back to config
    effective_agent_id = agent_id or settings.elevenlabs_agent_id
    effective_phone_id = agent_phone_number_id or settings.elevenlabs_agent_phone_number_id

    if settings.mock_mode:
        logger.info(f"[MOCK] Simulating outbound call to {phone_number} for {client_id}")
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.initiated,
            conversation_id=f"mock-conv-{uuid.uuid4().hex[:8]}",
        )

    try:
        numeric_id = int(client_id) if client_id.isdigit() else client_id
        e164_number = phone_number if phone_number.startswith("+") else f"+{phone_number}"
        url = _PROVIDER_URLS.get(call_provider, _PROVIDER_URLS["twilio"])
        logger.info(
            "ElevenLabs call | client=%s phone=%s first_name=%r email=%r agent=%s phone_id=%s provider=%s",
            client_id, e164_number, first_name, email, effective_agent_id, effective_phone_id, call_provider,
        )
        payload = {
            "agent_id": effective_agent_id,
            "agent_phone_number_id": effective_phone_id,
            "to_number": e164_number,
            "conversation_initiation_client_data": {
                "dynamic_variables": {
                    "Client_first_name": first_name or "",
                    "user_email": email or "",
                    "clientid": numeric_id,
                    "retention_rep": retention_rep or "",
                    "retention_status": retention_status or "",
                }
            },
        }
        response = await client.post(
            url,
            json=payload,
            headers={
                "xi-api-key": settings.elevenlabs_api_key,
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        data = response.json()
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.initiated,
            conversation_id=data.get("conversation_id") or data.get("callSid"),
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            f"ElevenLabs call failed for {client_id}: "
            f"{e.response.status_code} {e.response.text}"
        )
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.failed,
            error=f"HTTP {e.response.status_code}: {e.response.text}",
        )
    except Exception as e:
        logger.error(f"ElevenLabs call error for {client_id}: {e}")
        return ClientCallResult(
            client_id=client_id,
            status=CallStatus.failed,
            error=str(e),
        )
