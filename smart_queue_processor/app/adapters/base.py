from abc import ABC, abstractmethod
from typing import Any, Callable
from ..models import QueueMessage

class BaseQueueAdapter(ABC):
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the queue backend."""
        pass

    @abstractmethod
    async def publish(self, topic: str, message: QueueMessage) -> str:
        """
        Publish a message to a specific topic/stream.
        Returns the message ID assigned by the backend.
        """
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[QueueMessage], Any]) -> None:
        """
        Subscribe to a topic/stream and process messages using the callback.
        """
        pass

    @abstractmethod
    async def acknowledge(self, topic: str, message_id: str) -> None:
        """Acknowledge successful processing of a message."""
        pass
