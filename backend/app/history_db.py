import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path("/app/data/call_history.db")


def _init_db_sync() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS call_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                client_name TEXT,
                phone_number TEXT,
                conversation_id TEXT,
                status TEXT NOT NULL,
                called_at TEXT NOT NULL,
                error TEXT,
                agent_id TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


async def init_history_db() -> None:
    await asyncio.to_thread(_init_db_sync)


def _insert_sync(
    client_id: str,
    client_name: Optional[str],
    phone_number: Optional[str],
    conversation_id: Optional[str],
    status: str,
    error: Optional[str],
    agent_id: Optional[str],
) -> None:
    from datetime import datetime, timezone
    called_at = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO call_history
                (client_id, client_name, phone_number, conversation_id, status, called_at, error, agent_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (client_id, client_name, phone_number, conversation_id, status, called_at, error, agent_id),
        )
        conn.commit()
    finally:
        conn.close()


async def insert_call_history(
    client_id: str,
    client_name: Optional[str],
    phone_number: Optional[str],
    conversation_id: Optional[str],
    status: str,
    error: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> None:
    await asyncio.to_thread(
        _insert_sync, client_id, client_name, phone_number, conversation_id, status, error, agent_id
    )


def _query_sync(
    date_from: Optional[str],
    date_to: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conditions = []
        params: list[Any] = []
        if date_from:
            conditions.append("called_at >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("called_at <= ?")
            params.append(date_to + "T23:59:59")
        if status:
            conditions.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params += [limit, offset]
        cursor = conn.execute(
            f"SELECT * FROM call_history {where} ORDER BY called_at DESC LIMIT ? OFFSET ?",
            params,
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


async def query_call_history(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_query_sync, date_from, date_to, status, limit, offset)
