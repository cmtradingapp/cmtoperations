from sqlalchemy import BigInteger, Column, Float, Index

from app.pg_database import Base


class DealioPosition(Base):
    __tablename__ = "dealio_positions"

    positionid = Column(BigInteger, primary_key=True)
    login = Column(BigInteger, nullable=True)
    computedprofit = Column(Float, nullable=True)

    __table_args__ = (
        Index("ix_dealio_positions_login", "login"),
    )
