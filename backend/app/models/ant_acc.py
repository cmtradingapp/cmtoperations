from sqlalchemy import Column, Date, DateTime, Index, Integer, SmallInteger, String

from app.pg_database import Base


class AntAcc(Base):
    __tablename__ = "ant_acc"

    accountid = Column(String(50), primary_key=True)
    client_qualification_date = Column(Date, nullable=True)
    modifiedtime = Column(DateTime(timezone=False), nullable=True)
    is_test_account = Column(SmallInteger, nullable=True)
    sales_client_potential = Column(String(100), nullable=True)
    retention_status = Column(Integer, nullable=True)
    birth_date = Column(Date, nullable=True)
    assigned_to = Column(String(50), nullable=True)
    firstname = Column(String(200), nullable=True)
    lastname = Column(String(200), nullable=True)
    full_name = Column(String(400), nullable=True)
    # Extended columns synced from MSSQL report.ant_acc
    email = Column(String(255), nullable=True)
    country_iso = Column(String(10), nullable=True)
    customer_language = Column(String(50), nullable=True)
    accountstatus = Column(String(255), nullable=True)
    regulation = Column(String(255), nullable=True)
    createdtime = Column(DateTime(timezone=False), nullable=True)
    first_deposit_date = Column(DateTime(timezone=False), nullable=True)
    ftd_amount = Column(Integer, nullable=True)
    total_deposit = Column(Integer, nullable=True)
    total_withdrawal = Column(Integer, nullable=True)
    net_deposit = Column(Integer, nullable=True)
    funded = Column(SmallInteger, nullable=True)
    original_affiliate = Column(String(255), nullable=True)
    customer_id = Column(String(100), nullable=True)  # CLAUD-167: legacy client ID from old system

    __table_args__ = (
        Index("ix_ant_acc_modifiedtime", "modifiedtime"),
        Index("ix_ant_acc_qual_date", "client_qualification_date"),
    )
