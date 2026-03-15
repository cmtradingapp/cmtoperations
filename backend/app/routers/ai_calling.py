import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.config import settings
from app.models.calling_agent import CallingAgent
from app.pg_database import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------

class GenerateScriptRequest(BaseModel):
    opportunity_type: str
    description: str
    client_context: Optional[str] = ""


class CreateAgentRequest(BaseModel):
    name: str
    opportunity_type: str
    description: Optional[str] = ""
    system_prompt: str
    first_message: Optional[str] = ""
    voice_id: Optional[str] = ""
    voice_name: Optional[str] = ""
    create_on_elevenlabs: bool = True


class UpdateAgentRequest(BaseModel):
    name: Optional[str] = None
    system_prompt: Optional[str] = None
    first_message: Optional[str] = None
    voice_id: Optional[str] = None
    voice_name: Optional[str] = None
    status: Optional[str] = None


def _agent_to_dict(agent: CallingAgent) -> dict:
    return {
        "id": agent.id,
        "name": agent.name,
        "opportunity_type": agent.opportunity_type,
        "description": agent.description,
        "system_prompt": agent.system_prompt,
        "first_message": agent.first_message,
        "voice_id": agent.voice_id,
        "voice_name": agent.voice_name,
        "elevenlabs_agent_id": agent.elevenlabs_agent_id,
        "status": agent.status,
        "created_at": agent.created_at.isoformat() if agent.created_at else None,
        "updated_at": agent.updated_at.isoformat() if agent.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Pydantic: test call
# ---------------------------------------------------------------------------

class TestCallRequest(BaseModel):
    to_number: str
    phone_number_id: str
    call_provider: str = "twilio"


# ---------------------------------------------------------------------------
# GET /calling/voices — proxy ElevenLabs voices
# ---------------------------------------------------------------------------

@router.get("/calling/voices")
async def list_voices(
    request: Request,
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client
    try:
        response = await http_client.get(
            "https://api.elevenlabs.io/v2/voices",
            params={"page_size": 100},
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=15.0,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("voices", [])
    except Exception as e:
        logger.error("Failed to fetch ElevenLabs voices: %s", e)
        raise HTTPException(status_code=502, detail="Failed to fetch voices from ElevenLabs")


# ---------------------------------------------------------------------------
# GET /calling/phone-numbers — proxy ElevenLabs phone numbers
# ---------------------------------------------------------------------------

@router.get("/calling/phone-numbers")
async def list_phone_numbers(
    request: Request,
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client
    try:
        response = await http_client.get(
            "https://api.elevenlabs.io/v1/convai/phone-numbers",
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=15.0,
        )
        response.raise_for_status()
        data = response.json()
        # API returns a list directly or {"phone_numbers": [...]}
        if isinstance(data, list):
            return data
        return data.get("phone_numbers", data)
    except Exception as e:
        logger.error("Failed to fetch ElevenLabs phone numbers: %s", e)
        raise HTTPException(status_code=502, detail="Failed to fetch phone numbers from ElevenLabs")


# ---------------------------------------------------------------------------
# POST /calling/agents/{id}/test-call — initiate a test call
# ---------------------------------------------------------------------------

@router.post("/calling/agents/{agent_id}/test-call")
async def test_call(
    agent_id: int,
    body: TestCallRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    result = await db.execute(select(CallingAgent).where(CallingAgent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    if not agent.elevenlabs_agent_id:
        raise HTTPException(status_code=400, detail="Agent has not been synced to ElevenLabs yet")

    http_client = request.app.state.http_client
    e164 = body.to_number if body.to_number.startswith("+") else f"+{body.to_number}"

    provider_urls = {
        "twilio": "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
        "sip_trunk": "https://api.elevenlabs.io/v1/convai/sip-trunk/outbound-call",
    }
    url = provider_urls.get(body.call_provider, provider_urls["twilio"])

    payload = {
        "agent_id": agent.elevenlabs_agent_id,
        "agent_phone_number_id": body.phone_number_id,
        "to_number": e164,
    }

    try:
        response = await http_client.post(
            url,
            json=payload,
            headers={
                "xi-api-key": settings.elevenlabs_api_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        logger.info("Test call initiated: agent=%s to=%s conv=%s", agent.elevenlabs_agent_id, e164, data)
        return {"success": True, "conversation_id": data.get("conversation_id") or data.get("callSid"), "to_number": e164}
    except Exception as e:
        resp_text = getattr(getattr(e, "response", None), "text", "")
        detail = f"{e}" + (f" | {resp_text}" if resp_text else "")
        logger.error("Test call failed: %s", detail)
        raise HTTPException(status_code=502, detail=detail)


# ---------------------------------------------------------------------------
# GET /calling/conversations/{conversation_id} — proxy single conversation detail
# ---------------------------------------------------------------------------

@router.get("/calling/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    request: Request,
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client
    try:
        response = await http_client.get(
            f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}",
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=15.0,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------------------------------------------------------------------------
# POST /calling/conversations/{conversation_id}/analyze — Claude analyzes
# transcript and suggests script improvements
# ---------------------------------------------------------------------------

_ANALYSIS_SYSTEM_PROMPT = """You are an expert AI call script analyst for CMTrading, a forex and CFD broker.
You review transcripts of AI voice agent calls and provide specific, actionable improvements to the agent's system prompt.

Your analysis must be honest and critical — if the call went poorly, say why clearly.
Focus on:
- Where the conversation broke down or felt unnatural
- Objections the agent handled poorly or missed
- Moments where the client lost interest or got confused
- Specific phrases that worked well vs. poorly
- Missing information or CTAs that should have been included

Output format — return JSON with these fields:
{
  "call_quality": "excellent|good|fair|poor",
  "summary": "2-3 sentence summary of how the call went",
  "strengths": ["list of what worked well"],
  "weaknesses": ["list of specific problems"],
  "suggested_prompt_changes": [
    {
      "section": "name of the section to change (e.g. OBJECTION HANDLING)",
      "issue": "what the problem was",
      "suggestion": "exact text or instruction to add/change in the system prompt"
    }
  ],
  "updated_system_prompt": "The FULL updated system prompt with all suggestions already applied. This should be ready to use."
}

Return ONLY valid JSON, no markdown."""


class AnalyzeCallRequest(BaseModel):
    system_prompt: str
    agent_id: int


@router.post("/calling/conversations/{conversation_id}/analyze")
async def analyze_conversation(
    conversation_id: str,
    body: AnalyzeCallRequest,
    request: Request,
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client

    # 1. Fetch the conversation from ElevenLabs
    try:
        el_resp = await http_client.get(
            f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}",
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=15.0,
        )
        el_resp.raise_for_status()
        conv_data = el_resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch conversation: {e}")

    # 2. Build transcript text
    transcript_items = conv_data.get("transcript", [])
    transcript_text = "\n".join(
        f"{item.get('role', '?').upper()}: {item.get('message', '')}"
        for item in transcript_items
        if item.get("message")
    )
    if not transcript_text:
        raise HTTPException(status_code=400, detail="No transcript available for this conversation yet")

    analysis = conv_data.get("analysis") or {}
    summary = analysis.get("transcript_summary", "")
    call_successful = conv_data.get("call_successful", "unknown")
    duration = conv_data.get("call_duration_secs", 0)

    user_message = f"""Analyze this AI agent call and suggest improvements to the system prompt.

CALL OUTCOME: {call_successful}
DURATION: {duration}s
AI ANALYSIS SUMMARY: {summary or "N/A"}

TRANSCRIPT:
{transcript_text}

CURRENT SYSTEM PROMPT:
{body.system_prompt}

Provide specific improvements based on what actually happened in this call."""

    # 3. Call Claude
    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": 4000,
        "system": _ANALYSIS_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_message}],
    }
    headers = {
        "x-api-key": settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    try:
        resp = await http_client.post(
            "https://api.anthropic.com/v1/messages",
            json=payload,
            headers=headers,
            timeout=90.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {e}")

    try:
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("\n", 1)[-1].rsplit("```", 1)[0]
        result = json.loads(stripped.strip())
    except Exception as e:
        logger.error("Failed to parse analysis JSON: %s | raw: %s", e, data)
        raise HTTPException(status_code=502, detail="Failed to parse analysis from Claude")

    # Also include raw conversation data for the frontend
    result["conversation_id"] = conversation_id
    result["call_successful"] = call_successful
    result["duration_secs"] = duration
    result["transcript"] = transcript_items
    result["audio_url"] = f"/api/calling/conversations/{conversation_id}/audio"
    return result


# ---------------------------------------------------------------------------
# GET /calling/conversations/{conversation_id}/audio — proxy audio download
# ---------------------------------------------------------------------------

@router.get("/calling/conversations/{conversation_id}/audio")
async def get_conversation_audio(
    conversation_id: str,
    request: Request,
    _user: Any = Depends(get_current_user),
):
    from fastapi.responses import StreamingResponse
    http_client = request.app.state.http_client
    try:
        response = await http_client.get(
            f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}/audio",
            headers={"xi-api-key": settings.elevenlabs_api_key},
            timeout=60.0,
        )
        response.raise_for_status()
        return StreamingResponse(
            iter([response.content]),
            media_type="audio/mpeg",
            headers={"Content-Disposition": f"attachment; filename=call_{conversation_id}.mp3"},
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------------------------------------------------------------------------
# POST /calling/agents/generate-script — call Anthropic to generate script
# ---------------------------------------------------------------------------

_SCRIPT_SYSTEM_PROMPT = """You are an expert content writer for CMTrading, a forex and CFD broker at www.cmtrading.com.
You write scripts for AI voice agents that make outbound retention calls to clients.

Key knowledge about margin calls:
- Margin Level = (Equity / Used Margin) × 100%
- A Margin Call is triggered when margin level drops to 100% — the client's equity equals their used margin, meaning all funds are tied up
- A Stop Out is triggered at 50% margin level — the broker automatically closes positions starting with the largest losing position first
- Between margin call and stop out, clients CANNOT open new trades
- If stop out triggers, clients lock in real losses — positions are closed whether in profit or loss
- The ONLY solutions are: deposit more funds OR close some positions manually
- This is TIME SENSITIVE — market moves fast

CMTrading brand guidelines:
- Professional, warm, genuinely helpful tone
- Never mention competitor brands
- Always direct clients to www.cmtrading.com
- Do not guarantee profits
- Focus on protecting the client's existing investment

Output format — return JSON with these fields:
{
  "system_prompt": "Full system prompt for the ElevenLabs voice AI agent (500-800 words). Include: agent persona, situation awareness, how to explain the margin call clearly, objection handling (I'll check later / I don't understand / I'm busy), how to handle silence, clear CTA to deposit at cmtrading.com or close positions. The agent should also ask if the client wants to be transferred to a live agent.",
  "first_message": "The very first spoken sentence (1-2 sentences max, warm greeting with urgency)",
  "evaluation_criteria": ["list of 3-5 criteria to evaluate call success, e.g. 'Client acknowledged the margin call situation'"]
}

Return ONLY valid JSON, no markdown."""


@router.post("/calling/agents/generate-script")
async def generate_script(
    request: Request,
    body: GenerateScriptRequest,
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client
    user_message = (
        f"Write a script for a {body.opportunity_type} call. "
        f"Description: {body.description}. "
        f"{body.client_context or ''}"
    ).strip()

    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": 2000,
        "system": _SCRIPT_SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": user_message},
        ],
    }
    headers = {
        "x-api-key": settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        response = await http_client.post(
            "https://api.anthropic.com/v1/messages",
            json=payload,
            headers=headers,
            timeout=60.0,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error("Anthropic API call failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {e}")

    # Extract text content from the response
    try:
        content_blocks = data.get("content", [])
        text_content = ""
        for block in content_blocks:
            if block.get("type") == "text":
                text_content += block.get("text", "")
        # Strip markdown code fences if present (e.g. ```json ... ```)
        stripped = text_content.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("\n", 1)[-1]
            stripped = stripped.rsplit("```", 1)[0]
        result = json.loads(stripped.strip())
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.error("Failed to parse Anthropic response as JSON: %s | raw: %s", e, data)
        raise HTTPException(status_code=502, detail="Failed to parse script from Anthropic response")

    return result


# ---------------------------------------------------------------------------
# POST /calling/agents — create a CallingAgent (optionally on ElevenLabs too)
# ---------------------------------------------------------------------------

@router.post("/calling/agents")
async def create_agent(
    request: Request,
    body: CreateAgentRequest,
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    http_client = request.app.state.http_client
    elevenlabs_agent_id: Optional[str] = None
    status = "active"

    if body.create_on_elevenlabs and body.voice_id:
        el_payload = {
            "name": body.name,
            "conversation_config": {
                "agent": {
                    "prompt": {
                        "prompt": body.system_prompt,
                        "llm": "claude-haiku-4-5",
                        "temperature": 0.5,
                    },
                    "first_message": body.first_message or "",
                    "language": "en",
                },
                "tts": {
                    "voice_id": body.voice_id,
                    "model_id": "eleven_turbo_v2_5",
                },
            },
        }
        try:
            el_response = await http_client.post(
                "https://api.elevenlabs.io/v1/convai/agents",
                json=el_payload,
                headers={
                    "xi-api-key": settings.elevenlabs_api_key,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
            el_response.raise_for_status()
            el_data = el_response.json()
            elevenlabs_agent_id = el_data.get("agent_id")
            logger.info("Created ElevenLabs agent: %s", elevenlabs_agent_id)
        except Exception as e:
            logger.error("ElevenLabs agent creation failed, saving as draft: %s", e)
            status = "draft"

    agent = CallingAgent(
        name=body.name,
        opportunity_type=body.opportunity_type,
        description=body.description,
        system_prompt=body.system_prompt,
        first_message=body.first_message,
        voice_id=body.voice_id,
        voice_name=body.voice_name,
        elevenlabs_agent_id=elevenlabs_agent_id,
        status=status,
    )
    db.add(agent)
    await db.commit()
    await db.refresh(agent)
    return _agent_to_dict(agent)


# ---------------------------------------------------------------------------
# GET /calling/agents — list all agents
# ---------------------------------------------------------------------------

@router.get("/calling/agents")
async def list_agents(
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    result = await db.execute(
        select(CallingAgent).order_by(CallingAgent.created_at.desc())
    )
    agents = result.scalars().all()
    return [_agent_to_dict(a) for a in agents]


# ---------------------------------------------------------------------------
# GET /calling/agents/{id} — get single agent
# ---------------------------------------------------------------------------

@router.get("/calling/agents/{agent_id}")
async def get_agent(
    agent_id: int,
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    result = await db.execute(select(CallingAgent).where(CallingAgent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return _agent_to_dict(agent)


# ---------------------------------------------------------------------------
# PATCH /calling/agents/{id} — update agent
# ---------------------------------------------------------------------------

@router.patch("/calling/agents/{agent_id}")
async def update_agent(
    agent_id: int,
    body: UpdateAgentRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    result = await db.execute(select(CallingAgent).where(CallingAgent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    # Apply updates to DB model
    if body.name is not None:
        agent.name = body.name
    if body.system_prompt is not None:
        agent.system_prompt = body.system_prompt
    if body.first_message is not None:
        agent.first_message = body.first_message
    if body.voice_id is not None:
        agent.voice_id = body.voice_id
    if body.voice_name is not None:
        agent.voice_name = body.voice_name
    if body.status is not None:
        agent.status = body.status

    agent.updated_at = datetime.now(timezone.utc)

    # Sync to ElevenLabs if agent exists there
    if agent.elevenlabs_agent_id:
        http_client = request.app.state.http_client
        el_patch: dict[str, Any] = {}
        if body.name is not None:
            el_patch["name"] = agent.name
        if body.system_prompt is not None or body.first_message is not None or body.voice_id is not None:
            el_patch["conversation_config"] = {
                "agent": {
                    "prompt": {
                        "prompt": agent.system_prompt,
                        "llm": "claude-haiku-4-5",
                        "temperature": 0.5,
                    },
                    "first_message": agent.first_message or "",
                    "language": "en",
                },
                "tts": {
                    "voice_id": agent.voice_id or "",
                    "model_id": "eleven_turbo_v2_5",
                },
            }

        if el_patch:
            try:
                el_response = await http_client.patch(
                    f"https://api.elevenlabs.io/v1/convai/agents/{agent.elevenlabs_agent_id}",
                    json=el_patch,
                    headers={
                        "xi-api-key": settings.elevenlabs_api_key,
                        "Content-Type": "application/json",
                    },
                    timeout=30.0,
                )
                el_response.raise_for_status()
                logger.info("Updated ElevenLabs agent %s", agent.elevenlabs_agent_id)
            except Exception as e:
                logger.error(
                    "Failed to update ElevenLabs agent %s: %s",
                    agent.elevenlabs_agent_id,
                    e,
                )
                # Continue — DB update succeeds even if ElevenLabs sync fails

    await db.commit()
    await db.refresh(agent)
    return _agent_to_dict(agent)


# ---------------------------------------------------------------------------
# DELETE /calling/agents/{id} — delete agent
# ---------------------------------------------------------------------------

@router.delete("/calling/agents/{agent_id}")
async def delete_agent(
    agent_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _user: Any = Depends(get_current_user),
) -> Any:
    result = await db.execute(select(CallingAgent).where(CallingAgent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.elevenlabs_agent_id:
        http_client = request.app.state.http_client
        try:
            el_response = await http_client.delete(
                f"https://api.elevenlabs.io/v1/convai/agents/{agent.elevenlabs_agent_id}",
                headers={"xi-api-key": settings.elevenlabs_api_key},
                timeout=15.0,
            )
            el_response.raise_for_status()
            logger.info("Deleted ElevenLabs agent %s", agent.elevenlabs_agent_id)
        except Exception as e:
            logger.error(
                "Failed to delete ElevenLabs agent %s: %s",
                agent.elevenlabs_agent_id,
                e,
            )
            # Continue — delete from DB regardless

    await db.delete(agent)
    await db.commit()
    return {"detail": "Agent deleted"}
