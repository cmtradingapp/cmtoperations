import asyncio
from typing import Any

import pyodbc

from app.config import settings


def _execute_query_sync(query: str, params: tuple) -> list[dict[str, Any]]:
    conn = pyodbc.connect(settings.mssql_connection_string)
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


async def execute_query(query: str, params: tuple = ()) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_execute_query_sync, query, params)
