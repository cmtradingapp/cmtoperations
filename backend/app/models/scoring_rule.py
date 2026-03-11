from sqlalchemy import Column, DateTime, Integer, Numeric, String
from sqlalchemy.sql import func
from app.pg_database import Base


class ScoringRule(Base):
    __tablename__ = "scoring_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    field = Column(String(64), nullable=False)
    operator = Column(String(8), nullable=False)
    value = Column(String(64), nullable=False)
    score = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    value_min = Column(Numeric(15, 4), nullable=True)
    value_max = Column(Numeric(15, 4), nullable=True)
