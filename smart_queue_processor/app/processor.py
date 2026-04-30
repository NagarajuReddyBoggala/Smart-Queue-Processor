from typing import Any, Callable
import asyncio
import logging
from .adapters.base import BaseQueueAdapter
from .models import QueueMessage
from .config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, adapter: BaseQueueAdapter):
        self.adapter = adapter
        self._running = False
        self.settings = get_settings()

    async def start(self, topic: str, handler: Callable[[QueueMessage], Any]):
        """
        Starts the processor loop. 
        'handler' should be an async function that processes the QueueMessage.
        """
        logger.info(f"Starting processor for topic: {topic}")
        self._running = True
        
        async def process_with_retry(msg: QueueMessage):
            max_retries = self.settings.max_retries
            base_backoff = self.settings.base_backoff_seconds
            
            for attempt in range(max_retries + 1):
                try:
                    logger.info(f"Processing message: {msg.id} (Attempt {attempt + 1})")
                    await handler(msg)
                    # Acknowledge after successful handling
                    await self.adapter.acknowledge(topic, msg.id)
                    logger.info(f"Acknowledged message: {msg.id}")
                    return # Success, exit retry loop
                except Exception as e:
                    if attempt < max_retries:
                        delay = base_backoff * (2 ** attempt)
                        logger.warning(f"Error processing message {msg.id}: {e}. Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Message {msg.id} failed after {max_retries} retries. Sending to DLQ.")
                        # Publish to DLQ
                        dlq_topic = f"{topic}.dlq"
                        # Add error detail to payload
                        msg.payload["error_detail"] = str(e)
                        await self.adapter.publish(dlq_topic, msg)
                        # Acknowledge original message so it's not redelivered
                        await self.adapter.acknowledge(topic, msg.id)
                        logger.info(f"Message {msg.id} moved to DLQ and acknowledged.")

        async def wrapped_callback(msg: QueueMessage):
            # Spawn a background task so we don't block the main consumer loop
            asyncio.create_task(process_with_retry(msg))
        
        await self.adapter.subscribe(topic, wrapped_callback)

    def stop(self):
        self._running = False
        logger.info("Processor stopped.")
