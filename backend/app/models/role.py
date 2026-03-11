from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.pg_database import Base

ALL_PAGES = [
    "performance-dashboard",
    "call-manager",
    "call-history",
    "call-dashboard",
    "batch-call",
    "retention",
    "retention-tasks",
    "client-scoring",
    "retention-dial",
    "elena-ai-upload",
    # CLAUD-154: Additional pages that exist in the system
    "retention_grid_export",
    "users",
    "roles",
    "data-sync",
    "challenges",
    "action-bonuses",
    # CLAUD-156: Agent Activity page
    "agent_activity",
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
