from sqlalchemy import Column, Date, DateTime, Float, Index, Integer, SmallInteger, String, Text

from app.pg_database import Base


class VtigerCampaign(Base):
    __tablename__ = "vtiger_campaigns"

    campaignid = Column(String(50), primary_key=True)
    campaignname = Column(Text, nullable=True)
    campaigntype = Column(Text, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    closingdate = Column(Date, nullable=True)
    campaignstatus = Column(Text, nullable=True)
    budget = Column(Float, nullable=True)
    actual_cost = Column(Float, nullable=True)
    expected_revenue = Column(Float, nullable=True)
    targetsize = Column(Integer, nullable=True)
    currency_id = Column(String(50), nullable=True)
    assigned_user_id = Column(String(50), nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)
    date_entered = Column(DateTime(timezone=False), nullable=True)
    deleted = Column(SmallInteger, nullable=True)

    __table_args__ = (
        Index("ix_vtiger_campaigns_modifiedtime", "modifiedtime"),
    )
