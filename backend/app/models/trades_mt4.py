from sqlalchemy import BigInteger, Column, DateTime, Index, Numeric, SmallInteger, String

from app.pg_database import Base


class TradesMt4(Base):
    __tablename__ = "trades_mt4"

    ticket = Column(BigInteger, primary_key=True)
    login = Column(BigInteger, nullable=False)
    cmd = Column(SmallInteger, nullable=False)
    profit = Column(Numeric(18, 2), nullable=True)
    computed_profit = Column(Numeric(18, 2), nullable=True)
    notional_value = Column(Numeric(18, 2), nullable=True)
    close_time = Column(DateTime(timezone=False), nullable=True)
    open_time = Column(DateTime(timezone=False), nullable=True)
    symbol = Column(String(50), nullable=True)
    last_modified = Column(DateTime(timezone=False), nullable=True)
    open_price = Column(Numeric(18, 5), nullable=True)
    volume = Column(Numeric(18, 4), nullable=True)  # CLAUD-170: lot volume from MT

    __table_args__ = (
        Index("ix_trades_mt4_login", "login"),
        Index("ix_trades_mt4_login_cmd", "login", "cmd"),
        Index("ix_trades_mt4_close_time", "close_time"),
        Index("ix_trades_mt4_open_time", "open_time"),
    )
