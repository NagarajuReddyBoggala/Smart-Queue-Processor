import asyncio
import logging
import signal
import sys
import os

# Ensure the app module can be imported when running this script directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from smart_queue_processor.app.config import get_settings
from smart_queue_processor.app.adapters.redis_adapter import RedisQueueAdapter
from smart_queue_processor.app.processor import StreamProcessor
from smart_queue_processor.app.models import QueueMessage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_order(msg: QueueMessage):
    """
    Dummy business logic handler.
    In a real app, this would process an order, send an email, etc.
    """
    logger.info(f"==> Business Logic Executing for message {msg.id}")
    logger.info(f"==> Payload Data: {msg.payload}")
    
    # Simulate a crash if the payload specifically asks for it (for testing DLQ)
    if msg.payload.get("simulate_crash") is True:
        raise ValueError("Simulated crash requested by message payload.")
        
    # Simulate some work
    await asyncio.sleep(0.5)
    logger.info(f"==> Business Logic Complete for message {msg.id}")

async def main():
    settings = get_settings()
    topic = "orders" # Default topic to listen to
    
    logger.info(f"Starting Consumer for {settings.app_name}...")
    
    adapter = RedisQueueAdapter(
        redis_url=settings.redis_url,
        group_name=settings.redis_stream_group,
        consumer_name=settings.redis_consumer_name
    )
    
    try:
        await adapter.connect()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    processor = StreamProcessor(adapter)
    
    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    
    def shutdown_handler():
        logger.info("Received shutdown signal. Stopping gracefully...")
        processor.stop()
        stop_event.set()
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler)

    # Start the processor in a background task
    processor_task = asyncio.create_task(processor.start(topic, process_order))
    
    logger.info(f"Consumer is now listening to topic: '{topic}'. Press Ctrl+C to stop.")
    
    # Wait until a shutdown signal is received
    await stop_event.wait()
    
    # Cleanup
    processor_task.cancel()
    if adapter._client:
        await adapter._client.close()
    logger.info("Consumer shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handled by signal handler, but just in case
        pass
