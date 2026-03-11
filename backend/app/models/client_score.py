from sqlalchemy import Column, Integer, String, DateTime, func
from app.pg_database import Base


class ClientScore(Base):
    __tablename__ = "client_scores"

    accountid = Column(String, primary_key=True)
    score = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
