import hashlib
import logging
import re
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.pg_database import get_db
from app.routers.sendgrid_admin import get_sendgrid_config
from app.seed import hash_password

logger = logging.getLogger(__name__)

router = APIRouter()

# Minimum password requirements
_PASSWORD_RE = re.compile(r'^(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$')

_SUCCESS_MSG = "If this email is registered, you'll receive a reset link shortly."

_EMAIL_HTML = """\
<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;padding:24px;border:1px solid #e5e7eb;border-radius:8px">
  <h2 style="color:#1d4ed8;margin-bottom:8px">CMTrading Back Office</h2>
  <p style="color:#374151;font-size:14px">You requested a password reset for your account.</p>
  <p style="color:#374151;font-size:14px">Click the button below to set a new password. This link expires in <strong>1 hour</strong> and can only be used once.</p>
  <div style="text-align:center;margin:24px 0">
    <a href="{reset_url}" style="background:#1d4ed8;color:#fff;padding:12px 28px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:600">Reset My Password</a>
  </div>
  <p style="color:#6b7280;font-size:12px">If you didn't request this, you can safely ignore this email. Your password will not change.</p>
  <p style="color:#6b7280;font-size:12px">— CMTrading Back Office Team</p>
</div>"""

_EMAIL_PLAIN = """\
CMTrading Back Office — Password Reset

You requested a password reset. Click the link below (expires in 1 hour):

{reset_url}

If you didn't request this, ignore this email."""


def _sha256(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _send_reset_email(to_email: str, reset_url: str, api_key: str, from_email: str) -> None:
    """Send password reset email via SendGrid. Raises on failure."""
    html_body = _EMAIL_HTML.format(reset_url=reset_url)
    plain_body = _EMAIL_PLAIN.format(reset_url=reset_url)

    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject="CMTrading Back Office — Password Reset",
        html_content=html_body,
        plain_text_content=plain_body,
    )

    sg = SendGridAPIClient(api_key)
    sg.send(message)


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


@router.post("/auth/forgot-password")
async def forgot_password(
    body: ForgotPasswordRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Initiate password reset. Always returns the same message to prevent user enumeration."""
    try:
        # Look up user by email (case-insensitive)
        row = await db.execute(
            text("SELECT id, email FROM users WHERE LOWER(email) = LOWER(:email) LIMIT 1"),
            {"email": body.email.strip()},
        )
        user_row = row.fetchone()

        if user_row is None:
            # User not found — return success silently (never reveal)
            return {"message": _SUCCESS_MSG}

        user_id: int = user_row[0]
        user_email: str = user_row[1]

        # Rate limit: max 3 requests per user per hour
        rate_row = await db.execute(
            text(
                "SELECT COUNT(*) FROM password_resets "
                "WHERE user_id = :uid AND created_at > (NOW() AT TIME ZONE 'UTC') - INTERVAL '1 hour'"
            ),
            {"uid": user_id},
        )
        count = rate_row.scalar() or 0
        if count >= 3:
            # Silently do nothing — never reveal rate limit
            return {"message": _SUCCESS_MSG}

        # Generate raw token and store its SHA-256 hash
        raw_token = secrets.token_urlsafe(32)
        token_hash = _sha256(raw_token)
        expires_at = datetime.utcnow() + timedelta(hours=1)

        await db.execute(
            text(
                "INSERT INTO password_resets (user_id, token_hash, expires_at) "
                "VALUES (:uid, :hash, :exp)"
            ),
            {"uid": user_id, "hash": token_hash, "exp": expires_at},
        )

        await db.commit()

        # Write audit log — best-effort (separate transaction so failure can't roll back the reset)
        try:
            await db.execute(
                text(
                    "INSERT INTO audit_log (agent_id, agent_username, client_account_id, action_type, timestamp) "
                    "VALUES (:aid, :aname, :cid, :action, NOW())"
                ),
                {
                    "aid": user_id,
                    "aname": user_email,
                    "cid": "",
                    "action": "password_reset_requested",
                },
            )
            await db.commit()
        except Exception as audit_exc:
            await db.rollback()
            logger.warning("Failed to write audit log for password_reset_requested: %s", audit_exc)

        # Send email — best-effort (do not fail the request if email fails)
        reset_url = f"{settings.app_base_url}/reset-password?token={raw_token}"
        try:
            sg_api_key, sg_from_email = await get_sendgrid_config(db)
            _send_reset_email(user_email, reset_url, sg_api_key, sg_from_email)
            logger.info("Password reset email sent to user_id=%d", user_id)
        except Exception as email_exc:
            logger.warning("Failed to send password reset email to user_id=%d: %s", user_id, email_exc)

    except Exception as exc:
        logger.warning("forgot_password endpoint error (suppressed): %s", exc)

    return {"message": _SUCCESS_MSG}


@router.post("/auth/reset-password")
async def reset_password(
    body: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Complete password reset using a valid, unexpired, unused token."""
    token_hash = _sha256(body.token)

    # Look up token row
    row = await db.execute(
        text(
            "SELECT id, user_id FROM password_resets "
            "WHERE token_hash = :hash "
            "AND used_at IS NULL "
            "AND expires_at > (NOW() AT TIME ZONE 'UTC') "
            "LIMIT 1"
        ),
        {"hash": token_hash},
    )
    reset_row = row.fetchone()

    if reset_row is None:
        raise HTTPException(status_code=400, detail="Reset link is invalid or has expired.")

    reset_id: int = reset_row[0]
    user_id: int = reset_row[1]

    # Validate password strength
    if not _PASSWORD_RE.match(body.new_password):
        raise HTTPException(
            status_code=422,
            detail=(
                "Password must be at least 8 characters and contain at least "
                "one uppercase letter, one digit, and one special character."
            ),
        )

    # Hash the new password with bcrypt
    new_hashed = hash_password(body.new_password)

    # Update user's password
    await db.execute(
        text("UPDATE users SET hashed_password = :pw WHERE id = :uid"),
        {"pw": new_hashed, "uid": user_id},
    )

    # Mark token as used
    await db.execute(
        text("UPDATE password_resets SET used_at = NOW() WHERE id = :rid"),
        {"rid": reset_id},
    )

    await db.commit()

    # Write audit log — best-effort (separate transaction so failure can't roll back the reset)
    try:
        u_row = await db.execute(
            text("SELECT email FROM users WHERE id = :uid LIMIT 1"),
            {"uid": user_id},
        )
        u_data = u_row.fetchone()
        agent_username = u_data[0] if u_data else str(user_id)

        await db.execute(
            text(
                "INSERT INTO audit_log (agent_id, agent_username, client_account_id, action_type, timestamp) "
                "VALUES (:aid, :aname, :cid, :action, NOW())"
            ),
            {
                "aid": user_id,
                "aname": agent_username,
                "cid": "",
                "action": "password_reset_completed",
            },
        )
        await db.commit()
    except Exception as audit_exc:
        await db.rollback()
        logger.warning("Failed to write audit log for password_reset_completed: %s", audit_exc)

    logger.info("Password reset completed for user_id=%d", user_id)
    return {"message": "Password updated successfully."}
