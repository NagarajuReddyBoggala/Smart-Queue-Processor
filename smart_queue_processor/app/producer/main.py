from fastapi import FastAPI, Depends, HTTPException
from ..config import get_settings, Settings
from ..models import QueueMessage
from ..adapters.redis_adapter import RedisQueueAdapter

app = FastAPI()

# Global adapter instance (lazy initialized)
_adapter: RedisQueueAdapter | None = None

async def get_adapter(settings: Settings = Depends(get_settings)):
    global _adapter
    if not _adapter:
        _adapter = RedisQueueAdapter(
            redis_url=settings.redis_url,
            group_name=settings.redis_stream_group,
            consumer_name="producer_default"
        )
        await _adapter.connect()
    return _adapter

@app.get("/")
def read_root(settings: Settings = Depends(get_settings)):
    return {
        "message": f"{settings.app_name} producer is ready.",
        "status": "online"
    }

@app.post("/publish/{topic}")
async def publish_message(
    topic: str, 
    payload: dict, 
    adapter: RedisQueueAdapter = Depends(get_adapter)
):
    try:
        msg = QueueMessage(topic=topic, payload=payload)
        msg_id = await adapter.publish(topic, msg)
        return {"status": "success", "message_id": msg_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


