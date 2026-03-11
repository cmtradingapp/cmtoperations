from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.pg_database import Base

ALL_PAGES = [
    # Admin
    "users",
    "roles",
    "permissions",
    "integrations",
    "audit-log",
    # Marketing
    "challenges",
    "action-bonuses",
    # AI Calls
    "call-manager",
    "call-history",
    "call-dashboard",
    "batch-call",
    "elena-ai-upload",
]


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    permissions: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
