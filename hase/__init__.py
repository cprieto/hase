import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, Awaitable, Optional, NamedTuple
import orjson
from aio_pika import IncomingMessage, connect_robust, ExchangeType


class ValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class QueueProcessor(NamedTuple):
    fn: Callable[[Dict[str, Any]], Awaitable]
    options: Dict


class SerDe(ABC):
    @abstractmethod
    def serialize(self, data: Dict[str, Any]) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        pass


class JsonSerDe(SerDe):
    def serialize(self, data: Dict[str, Any]) -> bytes:
        return orjson.dumps(data)

    def deserialize(self, data: bytes) -> Dict[str, Any]:
        return orjson.loads(data)


@dataclass
class MessageProcessor:
    fn: Callable[[Dict[str, Any]], Awaitable]
    serde: SerDe

    async def process(self, message: IncomingMessage):
        async with message.process():
            data = self.serde.deserialize(message.body)
            await self.fn(data)

    async def __call__(self, message: IncomingMessage):
        await self.process(message)


@dataclass
class Hase:
    host: str
    exchange: str
    serde: SerDe = JsonSerDe()

    def __post_init__(self):
        url = urlparse(self.host)
        if url.scheme != 'amqp':
            raise ValidationError('Hase only supports amqp protocol')

        self._routes = {}
        self._exchange = None
        self._queues = []

    def register(self, topic: str, fn: Callable[[Dict[str, Any]], Awaitable]):
        self._routes[topic] = MessageProcessor(fn, self.serde)

    async def process_queue(self, queue):
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                topic = message.routing_key
                await self._routes[topic](message)

    async def arun(self):
        connection = await connect_robust(self.host)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(self.exchange, ExchangeType.TOPIC)

        queues = []
        for topic in self._routes.keys():
            queue = await channel.declare_queue()
            await queue.bind(exchange, topic)
            queues.append(queue)

        await asyncio.wait([
            asyncio.create_task(self.process_queue(queue)) for queue in queues
        ])

    def run(self):
        asyncio.run(self.arun())


__all__ = ['Hase']
