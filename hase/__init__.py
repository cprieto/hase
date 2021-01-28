import asyncio
import logging
import inspect
from dataclasses import dataclass
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, Awaitable, Optional, NamedTuple, Union
import orjson
from aio_pika import IncomingMessage, connect_robust, ExchangeType


class ValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class ArgumentError(Exception):
    pass


class SerDe(ABC):
    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        pass


class JsonSerDe(SerDe):
    def serialize(self, data: Any) -> bytes:
        return orjson.dumps(data)

    def deserialize(self, data: bytes) -> Any:
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


class TopicQueueOptions(NamedTuple):
    fn: MessageProcessor
    options: Dict[str, Union[str, bool]]


@dataclass
class Hase:
    host: str
    exchange: str
    serde: SerDe = JsonSerDe()

    def __post_init__(self):
        url = urlparse(self.host)
        if url.scheme != 'amqp':
            raise ValidationError('Hase only supports amqp protocol')

        self._topics: Dict[str, TopicQueueOptions] = {}
        self._exchange = None
        self._queues = []

    def register(self, topic: str, fn: Callable[[Dict[str, Any]], Awaitable], *, name: Optional[str] = None,
                 exclusive: Optional[bool] = None, durable: Optional[bool] = None, auto_delete: Optional[bool] = None):
        if not inspect.iscoroutinefunction(fn):
            raise ArgumentError('Currently we only support handling coroutines (async functions)')

        options = {k: v for k, v in
                   {'name': name, 'exclusive': exclusive, 'durable': durable, 'auto_delete': auto_delete}.items()
                   if v is not None}

        logging.debug(f'registered handler for topic {topic}')

        self._topics[topic] = TopicQueueOptions(MessageProcessor(fn, self.serde), options)

    async def process_queue(self, queue):
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                topic = message.routing_key
                logging.debug(f"got message for topic {topic}")
                await self._topics[topic].fn(message)

    async def arun(self):
        connection = await connect_robust(self.host)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(self.exchange, ExchangeType.TOPIC)

        topic_queues = []
        for topic in self._topics.keys():
            options = self._topics[topic].options
            queue = await (channel.declare_queue(**options) if options else channel.declare_queue())
            logging.debug(f'created queue {queue.name}')

            await queue.bind(exchange, topic)
            logging.debug(f"bound queue '{queue.name}' to topic '{topic}'")

            topic_queues.append(queue)

        await asyncio.wait([
            asyncio.create_task(self.process_queue(queue)) for queue in topic_queues
        ])

    def run(self):
        logging.debug(f'running consumers')
        asyncio.run(self.arun())


__all__ = ['Hase']
