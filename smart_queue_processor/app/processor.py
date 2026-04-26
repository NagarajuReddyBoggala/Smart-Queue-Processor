from typing import Any, Callable
from .adapters.base import BaseQueueAdapter
from .models import QueueMessage
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, adapter: BaseQueueAdapter):
        self.adapter = adapter
        self._running = False

    async def start(self, topic: str, handler: Callable[[QueueMessage], Any]):
        """
        Starts the processor loop. 
        'handler' should be an async function that processes the QueueMessage.
        """
        logger.info(f"Starting processor for topic: {topic}")
        self._running = True
        
        async def wrapped_callback(msg: QueueMessage):
            try:
                logger.info(f"Processing message: {msg.id}")
                await handler(msg)
                # Acknowledge after successful handling
                await self.adapter.acknowledge(topic, msg.id)
                logger.info(f"Acknowledged message: {msg.id}")
            except Exception as e:
                logger.error(f"Error processing message {msg.id}: {e}")
                # For now, we don't acknowledge, which means it will stay in PEL (Pending Entries List)
                # Redis will redeliver it depending on implementation details
        
        await self.adapter.subscribe(topic, wrapped_callback)

    def stop(self):
        self._running = False
        logger.info("Processor stopped.")
