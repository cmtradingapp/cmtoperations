from sqlalchemy import Column, DateTime, Integer, String, Text, func

from app.pg_database import Base


class Extension(Base):
    __tablename__ = "extensions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=True)
    extension = Column(String(50), nullable=True, unique=True)
    user_name = Column(Text, nullable=True)
    agent_name = Column(Text, nullable=True)
    manager = Column(Text, nullable=True)
    position = Column(Text, nullable=True)
    office = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    manager_email = Column(Text, nullable=True)
    synced_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
