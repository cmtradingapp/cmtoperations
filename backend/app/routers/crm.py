import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.config import settings
from app.models.audit_log import AuditLog
from app.models.extension import Extension
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

# CLAUD-65: Approved retention status keys (9 statuses only)
VALID_STATUS_KEYS = {0, 3, 19, 20, 23, 28, 34, 35, 36}

# CLAUD-114: Map status key -> human-readable label (mirrors frontend RETENTION_STATUSES)
_STATUS_KEY_TO_LABEL: dict[int, str] = {
    0:  "New",
    3:  "No Answer",
    19: "Potential",
    20: "Appointment",
    23: "Call Again",
    28: "Reassigned",
    34: "Terminated/Complain/Legal",
    35: "Remove From my Portfolio",
    36: "Daily Trading with me",
}


def _crm_headers() -> dict[str, str]:
    """Build common headers for all CRM API requests."""
    return {
        "x-crm-api-token": settings.crm_api_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _handle_crm_response(response: httpx.Response, context: str) -> dict:
    """Inspect CRM response and raise appropriate HTTPException on error.

    The CRM API returns HTTP 200 for all responses. Errors are indicated
    by the JSON body: {"success": false, "error": {...}, "result": null}.

    For non-200 HTTP codes (rare), we map them to appropriate status codes.
    For 200 responses with success=false, we inspect the error details.
    """
    # Handle non-200 HTTP responses (rare but possible)
    if response.status_code >= 400:
        body_preview = response.text[:500] if response.text else ""
        logger.error("CRM API HTTP error [%s] %d: %s", context, response.status_code, body_preview)

        if response.status_code in (401, 403):
            raise HTTPException(
                status_code=502,
                detail="CRM authentication failed. Please contact an administrator.",
            )
        if response.status_code >= 500:
            raise HTTPException(
                status_code=503,
                detail="CRM service is temporarily unavailable. Please try again later.",
            )
        raise HTTPException(
            status_code=502,
            detail=f"CRM returned an unexpected error ({response.status_code}).",
        )

    # Parse JSON body
    try:
        data = response.json()
    except Exception:
        logger.error("CRM API returned non-JSON response [%s]: %s", context, response.text[:200])
        raise HTTPException(
            status_code=502,
            detail="CRM returned an unexpected response format.",
        )

    # Check CRM-level success flag
    if data.get("success") is False:
        error_info = data.get("error")
        if isinstance(error_info, dict):
            error_details = error_info.get("errorDetails", "")
            error_desc = error_info.get("errorDesc", "")
        elif isinstance(error_info, str):
            error_details = error_info
            error_desc = ""
        else:
            error_details = str(error_info) if error_info else ""
            error_desc = ""

        logger.error("CRM API business error [%s]: %s — %s", context, error_desc, error_details)

        # Detect "not found" pattern
        if "not found" in error_details.lower():
            raise HTTPException(
                status_code=404,
                detail=f"Client not found in CRM: {error_details}",
            )
        # Detect empty/invalid input pattern
        if "not valid" in error_details.lower() or "cannot be empty" in error_details.lower():
            raise HTTPException(
                status_code=400,
                detail=f"CRM validation error: {error_details}",
            )
        # Generic CRM error
        raise HTTPException(
            status_code=502,
            detail=f"CRM error: {error_details or error_desc or 'Unknown error'}",
        )

    return data


async def _log_audit(
    db: AsyncSession,
    agent_id: int,
    agent_username: str,
    client_account_id: str,
    action_type: str,
    action_value: str | None = None,
) -> None:
    """Insert a row into audit_log. Best-effort: logs a warning on failure."""
    try:
        entry = AuditLog(
            agent_id=agent_id,
            agent_username=agent_username,
            client_account_id=client_account_id,
            action_type=action_type,
            action_value=action_value,
        )
        db.add(entry)
        await db.commit()
        logger.info(
            "Audit logged: %s by %s (id=%d) on client %s",
            action_type, agent_username, agent_id, client_account_id,
        )
    except Exception as exc:
        logger.warning("Failed to write audit log: %s", exc)


class RetentionStatusUpdate(BaseModel):
    status_key: int


@router.put("/clients/{account_id}/retention-status")
async def update_retention_status(
    account_id: str,
    body: RetentionStatusUpdate,
    request: Request,
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Proxy endpoint to update a client's retention status via the CRM API."""
    if body.status_key not in VALID_STATUS_KEYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid retention status key: {body.status_key}. Must be one of the valid CRM status codes.",
        )

    crm_url = f"{settings.crm_api_base_url}/crm-api/retention"
    params: dict[str, Any] = {
        "userId": account_id,
        "retentionStatus": body.status_key,
    }

    logger.info(
        "CRM API call: updating retention status for account %s to %d",
        account_id,
        body.status_key,
    )

    try:
        http_client = request.app.state.http_client
        response = await http_client.put(crm_url, params=params, headers=_crm_headers())
        _handle_crm_response(response, f"update_retention_status({account_id})")

        logger.info(
            "CRM API success: account %s retention status updated to %d",
            account_id,
            body.status_key,
        )
        # CLAUD-114: Persist the approved retention status locally so the grid
        # reads from client_retention_status instead of the legacy sales_client_potential.
        _status_label = _STATUS_KEY_TO_LABEL.get(body.status_key, str(body.status_key))
        await db.execute(
            text("""
                INSERT INTO client_retention_status (accountid, status_key, status_label, updated_at)
                VALUES (:accountid, :status_key, :status_label, NOW())
                ON CONFLICT (accountid) DO UPDATE SET
                    status_key   = EXCLUDED.status_key,
                    status_label = EXCLUDED.status_label,
                    updated_at   = NOW()
            """),
            {"accountid": account_id, "status_key": body.status_key, "status_label": _status_label},
        )
        await db.commit()
        await _log_audit(
            db, user.id, user.username, account_id,
            "status_change", str(body.status_key),
        )
        return {
            "success": True,
            "message": f"Retention status updated to {body.status_key} for account {account_id}",
        }
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("CRM API timeout: update_retention_status(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
    except Exception as e:
        logger.error("CRM API request failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")


class AddNoteBody(BaseModel):
    note: str


@router.post("/clients/{account_id}/note")
async def add_client_note(
    account_id: str,
    body: AddNoteBody,
    request: Request,
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Proxy endpoint to add a note for a client via the CRM API."""
    if not body.note or not body.note.strip():
        raise HTTPException(status_code=400, detail="Note text cannot be empty.")

    crm_url = f"{settings.crm_api_base_url}/crm-api/user-note"
    params: dict[str, Any] = {
        "userId": account_id,
        "note": body.note.strip(),
    }

    logger.info("CRM API call: adding note for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.post(crm_url, params=params, headers=_crm_headers())
        _handle_crm_response(response, f"add_client_note({account_id})")

        logger.info("CRM API success: note added for account %s", account_id)
        await _log_audit(
            db, user.id, user.username, account_id,
            "note_added", body.note.strip(),
        )
        return {
            "success": True,
            "message": f"Note added for account {account_id}",
        }
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("CRM API timeout: add_client_note(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
    except Exception as e:
        logger.error("CRM API request failed (add note): %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")


@router.get("/clients/{account_id}/crm-user")
async def get_crm_user(
    account_id: str,
    request: Request,
    log_whatsapp: bool = False,
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Proxy endpoint to fetch CRM user details (including phone number).

    When the frontend opens WhatsApp, it passes ?log_whatsapp=true so the
    backend can record the action in the audit log.
    """
    crm_url = f"{settings.crm_api_base_url}/crm-api/user"
    params: dict[str, Any] = {"id": account_id}

    logger.info("CRM API call: fetching user details for account %s", account_id)

    try:
        http_client = request.app.state.http_client
        response = await http_client.get(crm_url, params=params, headers=_crm_headers())
        data = _handle_crm_response(response, f"get_crm_user({account_id})")

        # CRM wraps the user object in a "result" field
        result = data.get("result")
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Client not found in CRM. Please verify the account ID.",
            )

        logger.info("CRM API success: fetched user details for account %s", account_id)
        if log_whatsapp:
            await _log_audit(
                db, user.id, user.username, account_id,
                "whatsapp_opened", None,
            )
        return result
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("CRM API timeout: get_crm_user(%s)", account_id)
        raise HTTPException(
            status_code=503,
            detail="CRM service timed out. Please try again later.",
        )
    except Exception as e:
        logger.error("CRM API request failed (get user): %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")


# ── SquareTalk call integration ──────────────────────────────────────────

SQUARETALK_BASE_URL = "https://cmtrading.squaretalk.com/Integration"


async def _get_user_extension(account_id: str, db: AsyncSession) -> str:
    """Look up the assigned agent's SquareTalk extension for a given client account.

    Resolution chain:
      1. ant_acc (accountid) -> assigned_to  (the agent ID assigned to this client)
      2. vtiger_users (id = assigned_to) -> email  (the agent's email)
      3. extensions (email) -> extension  (the SquareTalk extension number)
    """
    # Step 1: get assigned_to from ant_acc
    row = await db.execute(
        text("SELECT assigned_to FROM ant_acc WHERE accountid = :account_id"),
        {"account_id": account_id},
    )
    assigned_to = row.scalar_one_or_none()

    if not assigned_to:
        raise HTTPException(
            status_code=404,
            detail="No assigned agent found for this client. "
                   "Please verify the account ID and that an agent is assigned.",
        )

    # Step 2: get email from vtiger_users
    row = await db.execute(
        text("SELECT email FROM vtiger_users WHERE id = :assigned_to"),
        {"assigned_to": assigned_to},
    )
    agent_email = row.scalar_one_or_none()

    if not agent_email:
        raise HTTPException(
            status_code=404,
            detail=f"Assigned agent (ID {assigned_to}) not found in vtiger_users or has no email.",
        )

    # Step 3: get extension from extensions table
    result = await db.execute(
        select(Extension).where(Extension.email == agent_email)
    )
    ext = result.scalar_one_or_none()
    if not ext or not ext.extension:
        raise HTTPException(
            status_code=404,
            detail=f"No phone extension found for agent email {agent_email}. "
                   "Please verify the agent's extension is configured in the system.",
        )
    return ext.extension


async def _get_client_phone(account_id: str, http_client: httpx.AsyncClient) -> str:
    """Fetch client phone number from CRM API (reuses CRM user lookup logic)."""
    crm_url = f"{settings.crm_api_base_url}/crm-api/user"
    params: dict[str, Any] = {"id": account_id}

    try:
        response = await http_client.get(crm_url, params=params, headers=_crm_headers())
        data = _handle_crm_response(response, f"get_client_phone({account_id})")
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=503, detail="CRM service timed out while fetching client phone.")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to reach CRM API: {e}")

    result = data.get("result")
    if not result:
        raise HTTPException(status_code=404, detail="Client not found in CRM. Please verify the account ID.")

    # Try multiple phone field names (CRM schema varies)
    phone = (
        result.get("fullTelephone")
        or result.get("telephone")
        or result.get("phone")
        or result.get("Phone")
        or result.get("phoneNumber")
        or result.get("PhoneNumber")
        or result.get("mobile")
        or result.get("Mobile")
    )
    if not phone:
        raise HTTPException(
            status_code=404,
            detail="No phone number found for this client in the CRM.",
        )
    # Clean phone: digits only (strip +, spaces, dashes)
    return str(phone).replace(" ", "").replace("-", "").replace("+", "")


@router.post("/clients/{account_id}/call")
async def initiate_call(
    account_id: str,
    request: Request,
    user: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Initiate a SquareTalk call from the assigned agent's extension to the client's phone."""
    # 1. Resolve assigned agent's extension (ant_acc -> vtiger_users -> extensions)
    extension = await _get_user_extension(account_id, db)

    # 2. Fetch client phone from CRM
    http_client = request.app.state.http_client
    destination = await _get_client_phone(account_id, http_client)

    # 3. Call SquareTalk API
    squaretalk_url = f"{SQUARETALK_BASE_URL}/api_call.php"
    params = {
        "Extension": extension,
        "Destination": destination,
        "Mode": "Call",
    }

    logger.info(
        "SquareTalk call: extension=%s, destination=%s, account=%s",
        extension, destination, account_id,
    )

    try:
        response = await http_client.post(
            squaretalk_url,
            params=params,
            headers={"accept": "application/json"},
        )

        if response.status_code >= 400:
            body_preview = response.text[:500] if response.text else ""
            logger.error(
                "SquareTalk API error %d: %s", response.status_code, body_preview
            )
            raise HTTPException(
                status_code=502,
                detail=f"SquareTalk API returned error ({response.status_code}). Please try again.",
            )

        # Try to parse JSON response
        try:
            result = response.json()
        except Exception:
            result = {"raw": response.text[:500]}

        logger.info(
            "SquareTalk call initiated: extension=%s -> destination=%s, response=%s",
            extension, destination, result,
        )
        await _log_audit(
            db, user.id, user.username, account_id,
            "call_initiated", extension,
        )
        return {
            "success": True,
            "message": "Call initiated successfully",
        }
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("SquareTalk API timeout: extension=%s, destination=%s", extension, destination)
        raise HTTPException(
            status_code=503,
            detail="SquareTalk service timed out. Please try again later.",
        )
    except Exception as e:
        logger.error("SquareTalk API request failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Failed to reach SquareTalk API: {e}")
