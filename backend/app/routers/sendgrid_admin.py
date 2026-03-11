import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import require_admin
from app.config import settings
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

_KEYS = ("sendgrid_api_key", "sendgrid_from_email")


def _mask(key: str | None) -> str | None:
    if not key:
        return None
    if len(key) <= 8:
        return "*" * len(key)
    return key[:4] + "*" * (len(key) - 8) + key[-4:]


async def _get_setting(db: AsyncSession, key: str) -> str | None:
    row = await db.execute(
        text("SELECT value FROM app_settings WHERE key = :key"), {"key": key}
    )
    result = row.fetchone()
    return result[0] if result else None


async def get_sendgrid_config(db: AsyncSession) -> tuple[str, str]:
    """Return (api_key, from_email), preferring DB values over env defaults."""
    api_key = await _get_setting(db, "sendgrid_api_key") or settings.sendgrid_api_key
    from_email = await _get_setting(db, "sendgrid_from_email") or settings.sendgrid_from_email
    return api_key, from_email


class SendGridConfigRequest(BaseModel):
    api_key: str
    from_email: str


class SendGridTestRequest(BaseModel):
    to_email: str


async def _upsert_setting(db: AsyncSession, key: str, value: str) -> None:
    await db.execute(
        text(
            "INSERT INTO app_settings (key, value) VALUES (:key, :value) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ),
        {"key": key, "value": value},
    )


@router.get("/admin/sendgrid")
async def get_sendgrid(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Return current SendGrid configuration (API key masked)."""
    api_key = await _get_setting(db, "sendgrid_api_key") or settings.sendgrid_api_key
    from_email = await _get_setting(db, "sendgrid_from_email") or settings.sendgrid_from_email
    return {
        "api_key": _mask(api_key),
        "from_email": from_email,
        "configured": bool(api_key),
    }


@router.put("/admin/sendgrid")
async def update_sendgrid(
    body: SendGridConfigRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Save SendGrid API key and from-email to the database."""
    await _upsert_setting(db, "sendgrid_api_key", body.api_key)
    await _upsert_setting(db, "sendgrid_from_email", body.from_email)
    await db.commit()
    logger.info("SendGrid configuration updated")
    return {"message": "SendGrid configuration saved.", "from_email": body.from_email}


@router.post("/admin/sendgrid/test")
async def test_sendgrid(
    body: SendGridTestRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
) -> dict:
    """Send a test email using the current SendGrid configuration."""
    api_key, from_email = await get_sendgrid_config(db)
    if not api_key:
        raise HTTPException(status_code=400, detail="SendGrid API key is not configured.")
    try:
        message = Mail(
            from_email=from_email,
            to_emails=body.to_email,
            subject="CMTrading Back Office — SendGrid Test",
            html_content="<p>This is a test email from the CMTrading Back Office SendGrid integration.</p>",
            plain_text_content="This is a test email from the CMTrading Back Office SendGrid integration.",
        )
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        logger.info("SendGrid test email sent to %s (status %s)", body.to_email, response.status_code)
        return {"message": f"Test email sent to {body.to_email}."}
    except Exception as exc:
        logger.warning("SendGrid test email failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"SendGrid error: {exc}")
