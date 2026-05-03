import json
import uuid
import aio_pika
from typing import Any, Callable
from .base import BaseQueueAdapter
from ..models import QueueMessage

class RabbitMQAdapter(BaseQueueAdapter):
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.RobustChannel | None = None
        self._unacked_messages = {} # message_id -> aio_pika.IncomingMessage

    async def connect(self) -> None:
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()

    async def publish(self, topic: str, message: QueueMessage) -> str:
        if not self.channel:
            await self.connect()
            
        # Ensure message has an ID before sending
        msg_id = message.id or str(uuid.uuid4())
        message.id = msg_id
        
        # We use the default exchange where routing_key == queue_name
        exchange = self.channel.default_exchange
        
        body = json.dumps(message.payload).encode()
        amqp_msg = aio_pika.Message(
            body=body,
            message_id=msg_id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await exchange.publish(amqp_msg, routing_key=topic)
        return msg_id

    async def subscribe(self, topic: str, callback: Callable[[QueueMessage], Any]) -> None:
        if not self.channel:
            await self.connect()

        # Declare queue (auto-setup)
        queue = await self.channel.declare_queue(topic, durable=True)
        
        # Set prefetch count to 1 to ensure fair dispatching
        await self.channel.set_qos(prefetch_count=1)

        async with queue.iterator() as queue_iter:
            async for amqp_message in queue_iter:
                # We do NOT use the process() context manager here because we want 
                # manual control over when the message is acked via the generic interface.
                payload = json.loads(amqp_message.body.decode())
                msg_id = amqp_message.message_id or str(uuid.uuid4())
                msg = QueueMessage(
                    id=msg_id, 
                    topic=topic, 
                    payload=payload
                )
                
                # Store the amqp_message so we can ack it later
                self._unacked_messages[msg_id] = amqp_message
                
                # Execute callback
                await callback(msg)

    async def acknowledge(self, topic: str, message_id: str) -> None:
        """Acknowledge successful processing of a message."""
        amqp_msg = self._unacked_messages.pop(message_id, None)
        if amqp_msg:
            await amqp_msg.ack()
