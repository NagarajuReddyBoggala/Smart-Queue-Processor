import asyncio
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from smart_queue_processor.app.config import get_settings
from smart_queue_processor.app.adapters.redis_adapter import RedisQueueAdapter
from smart_queue_processor.app.processor import StreamProcessor
from smart_queue_processor.app.models import QueueMessage

async def main():
    settings = get_settings()
    topic = "test_retry_stream"
    
    # 1. Initialize Adapter
    adapter = RedisQueueAdapter(
        redis_url=settings.redis_url,
        group_name="test_retry_group",
        consumer_name="test_consumer"
    )
    await adapter.connect()
    
    # Cleanup old data
    try:
        await adapter._client.delete(topic)
        await adapter._client.delete(f"{topic}.dlq")
    except:
        pass

    processor = StreamProcessor(adapter)
    
    attempts = 0
    
    async def failing_handler(msg: QueueMessage):
        nonlocal attempts
        attempts += 1
        print(f"Handler called for attempt {attempts}")
        raise ValueError("Intentional failure for testing retry/DLQ")

    # Start processor
    processor_task = asyncio.create_task(processor.start(topic, failing_handler))
    
    await asyncio.sleep(1)
    
    # Publish message
    print("Publishing message that will fail...")
    await adapter.publish(topic, QueueMessage(topic=topic, payload={"data": "fail_me"}))
    
    # Wait long enough for 3 retries (base 2s -> 2s, 4s, 8s = 14s total wait)
    print("Waiting for retries to complete (approx 15 seconds)...")
    await asyncio.sleep(16)
    
    # Check DLQ
    dlq_messages = await adapter._client.xread({f"{topic}.dlq": "0-0"})
    
    if dlq_messages:
        print("✅ SUCCESS: Message was found in DLQ!")
        for stream, msgs in dlq_messages:
            for msg_id, fields in msgs:
                print(f"DLQ Message data: {fields.get('data')}")
    else:
        print("❌ FAILURE: Message was NOT found in DLQ.")

    if attempts == 4: # 1 initial + 3 retries
        print("✅ SUCCESS: Correct number of handler attempts executed.")
    else:
        print(f"❌ FAILURE: Handler was called {attempts} times instead of 4.")

    processor_task.cancel()
    await adapter._client.close()

if __name__ == "__main__":
    asyncio.run(main())
