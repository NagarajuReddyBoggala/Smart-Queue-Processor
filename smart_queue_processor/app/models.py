from pydantic import BaseModel
from typing import Any, Dict

class QueueMessage(BaseModel):
    id: str | None = None
    payload: Dict[str, Any]
    topic: str
