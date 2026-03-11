from sqlalchemy import Column, DateTime, Integer, JSON, String
from sqlalchemy.sql import func

from app.pg_database import Base


class UserPreferences(Base):
    __tablename__ = "user_preferences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, nullable=False, index=True)
    retention_column_order = Column(JSON, nullable=True)  # list of column keys
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
