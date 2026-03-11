from sqlalchemy import Column, DateTime, Index, SmallInteger, String, Text

from app.pg_database import Base


class VtigerUser(Base):
    __tablename__ = "vtiger_users"

    userid = Column(String(50), primary_key=True)
    user_name = Column(Text, nullable=True)
    first_name = Column(Text, nullable=True)
    last_name = Column(Text, nullable=True)
    email1 = Column(Text, nullable=True)
    title = Column(Text, nullable=True)
    department = Column(Text, nullable=True)
    phone_work = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    is_admin = Column(Text, nullable=True)
    roleid = Column(Text, nullable=True)
    user_type = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    reports_to_id = Column(Text, nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)
    date_entered = Column(DateTime(timezone=False), nullable=True)
    deleted = Column(SmallInteger, nullable=True)

    __table_args__ = (
        Index("ix_vtiger_users_modifiedtime", "modifiedtime"),
    )
