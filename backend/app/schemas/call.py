from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class CallStatus(str, Enum):
    initiated = "initiated"
    failed = "failed"


class CallRequest(BaseModel):
    client_ids: List[str]
    agent_id: Optional[str] = None
    agent_phone_number_id: Optional[str] = None
    call_provider: str = "twilio"


class ClientCallResult(BaseModel):
    client_id: str
    status: CallStatus
    conversation_id: Optional[str] = None
    error: Optional[str] = None


class CallResponse(BaseModel):
    results: List[ClientCallResult]
