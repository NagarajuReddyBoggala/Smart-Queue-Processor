import redis.asyncio as redis
from typing import Any, Callable
from .base import BaseQueueAdapter
from ..models import QueueMessage
import json

class RedisQueueAdapter(BaseQueueAdapter):
    def __init__(self, redis_url: str, group_name: str, consumer_name: str):
        self.redis_url = redis_url
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        if not self._client:
            self._client = redis.from_url(self.redis_url, decode_responses=True)
            # Verify connection
            await self._client.ping()

    async def publish(self, topic: str, message: QueueMessage) -> str:
        if not self._client:
            await self.connect()
        
        # Redis XADD: topic = stream name, * = auto ID, payload as fields
        # We store the payload as a JSON string in a field called 'data'
        data = {"data": json.dumps(message.payload)}
        message_id = await self._client.xadd(topic, data)
        return message_id

    async def subscribe(self, topic: str, callback: Callable[[QueueMessage], Any]) -> None:
        if not self._client:
            await self.connect()

        # Auto-setup: Create group if it doesn't exist
        try:
            await self._client.xgroup_create(topic, self.group_name, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

        # Listening loop
        while True:
            # XREADGROUP: block=0 means wait indefinitely
            # count=1 means one message at a time
            # '>' means new messages never delivered to others
            streams = await self._client.xreadgroup(
                self.group_name, 
                self.consumer_name, 
                {topic: ">"}, 
                count=1, 
                block=5000 # Wait up to 5s if empty
            )

            if not streams:
                continue

            for stream_name, messages in streams:
                for message_id, fields in messages:
                    # Convert Redis fields back to QueueMessage
                    payload = json.loads(fields.get("data", "{}"))
                    msg = QueueMessage(id=message_id, topic=stream_name, payload=payload)
                    
                    # Execute callback
                    await callback(msg)

    async def acknowledge(self, topic: str, message_id: str) -> None:
        if not self._client:
            await self.connect()
        await self._client.xack(topic, self.group_name, message_id)
