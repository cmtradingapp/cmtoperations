from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, Index, Integer, String, Text

from app.pg_database import Base


class DealioUser(Base):
    __tablename__ = "dealio_users"

    login = Column(BigInteger, primary_key=True)
    lastupdate = Column(DateTime(timezone=True), nullable=True)
    sourceid = Column(Text, nullable=True)
    sourcename = Column(Text, nullable=True)
    sourcetype = Column(Text, nullable=True)
    groupname = Column(Text, nullable=True)
    groupcurrency = Column(Text, nullable=True)
    userid = Column(Text, nullable=True)
    actualuserid = Column(Text, nullable=True)
    regdate = Column(DateTime(timezone=False), nullable=True)
    lastdate = Column(DateTime(timezone=False), nullable=True)
    agentaccount = Column(BigInteger, nullable=True)
    lastip = Column(String(50), nullable=True)
    balance = Column(Float, nullable=True)
    prevmonthbalance = Column(Float, nullable=True)
    prevbalance = Column(Float, nullable=True)
    prevequity = Column(Float, nullable=True)
    credit = Column(Float, nullable=True)
    name = Column(Text, nullable=True)
    country = Column(Text, nullable=True)
    city = Column(Text, nullable=True)
    state = Column(Text, nullable=True)
    zipcode = Column(Text, nullable=True)
    address = Column(Text, nullable=True)
    phone = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    compbalance = Column(Float, nullable=True)
    compprevbalance = Column(Float, nullable=True)
    compprevmonthbalance = Column(Float, nullable=True)
    compprevequity = Column(Float, nullable=True)
    compcredit = Column(Float, nullable=True)
    conversionratio = Column(Float, nullable=True)
    book = Column(Text, nullable=True)
    isenabled = Column(Boolean, nullable=True)
    status = Column(Text, nullable=True)
    prevmonthequity = Column(Float, nullable=True)
    compprevmonthequity = Column(Float, nullable=True)
    comment = Column(Text, nullable=True)
    color = Column(BigInteger, nullable=True)
    leverage = Column(Integer, nullable=True)
    condition = Column(Integer, nullable=True)
    calculationcurrency = Column(Text, nullable=True)
    calculationcurrencydigits = Column(Integer, nullable=True)
    equity = Column(Float, nullable=True)

    __table_args__ = (
        Index("ix_dealio_users_lastupdate", "lastupdate"),
    )
