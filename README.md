# Smart Queue Processor

Minimal project scaffold for a queue processing system.

## What to do next

1. Define the queue adapter interface.
2. Implement Redis Streams support.
3. Add RabbitMQ streaming support.
4. Build producer, processor, and consumer modules.
5. Add retry logic with exponential backoff.
6. Add dead-letter queue handling.
7. Add metrics and observability.

## Supported backends

- Redis Streams
- RabbitMQ Streaming (planned)

## Notes

- Keep business logic separate from queue backend details.
- Use an adapter layer so Redis and RabbitMQ can share the same processing flow.
- Route permanently failed messages to a DLQ for later inspection.

## Repository layout

```text
smart_queue_processor/
  app/
    producer/
      main.py
    processor.py
    consumer/
      main.py
    config.py
    models.py
    metrics.py
```

## Push checklist

- Verify project files reflect the intended architecture.
- Keep README short and task-focused.
- Add implementation and tests after the initial push.
