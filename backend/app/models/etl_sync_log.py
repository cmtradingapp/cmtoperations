from sqlalchemy import Column, DateTime, Integer, String, Text, func

from app.pg_database import Base


class EtlSyncLog(Base):
    __tablename__ = "etl_sync_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_type = Column(String(50), nullable=False)   # "full" | "incremental"
    status = Column(String(20), nullable=False, default="running")  # "running" | "completed" | "error"
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    rows_synced = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
