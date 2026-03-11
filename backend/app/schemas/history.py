from typing import Optional

from pydantic import BaseModel


class CallHistoryRecord(BaseModel):
    id: int
    client_id: str
    client_name: Optional[str] = None
    phone_number: Optional[str] = None
    conversation_id: Optional[str] = None
    status: str
    called_at: str
    error: Optional[str] = None
    agent_id: Optional[str] = None
