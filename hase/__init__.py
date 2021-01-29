import asyncio
import logging
import inspect
from urllib.parse import urlparse
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, Awaitable, Optional, NamedTuple, Union, Type

import orjson
from aio_pika import IncomingMessage, connect_robust, ExchangeType, Message


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
    exception_handlers: Dict[Type[Exception], Callable[[IncomingMessage, Exception], Awaitable]] = field(
        default_factory=dict)
    serde: SerDe = JsonSerDe()

    def __post_init__(self):
        url = urlparse(self.host)
        if url.scheme != 'amqp':
            raise ValidationError('Hase only supports amqp protocol')

        self._topics: Dict[str, TopicQueueOptions] = {}
        self._exchange = None

    async def _handle_error(self, message: IncomingMessage, exception: Exception):
        if (key := type(exception)) in self.exception_handlers.keys():
            self.exception_handlers[key](message, exception)
        else:
            raise exception

    async def _process_queue(self, queue):
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                topic = message.routing_key
                logging.debug(f"got message for topic {topic}")
                try:
                    await self._topics[topic].fn(message)
                except Exception as ex:
                    await self._handle_error(message, ex)

    def handle_exception(self, exception: Type[Exception]) -> Callable[..., None]:
        """
        Decorator to identify a handler to handle a given exception when processing a message
        :param exception: Exception to handle
        """

        def _(fn):
            self.exception_handlers[exception] = fn

        return _

    def topic(self, topic: str, *, name: Optional[str] = None,
              exclusive: Optional[bool] = None, durable: Optional[bool] = None, auto_delete: Optional[bool] = None) -> \
            Callable[..., None]:
        """
        Decorator to identify a given handler for a given topic

        :param topic: what topic to consume
        :param name: optional, name for the queue when consuming
        :param exclusive: optional, is the queue exclusive?
        :param durable: optional, is the queue durable?
        :param auto_delete: optional, should the queue be deleted when the consumer disconnects?
        """

        def _(fn):
            self.register(topic, fn, name=name, exclusive=exclusive, durable=durable, auto_delete=auto_delete)

        return _

    def register(self, topic: str, fn: Callable[[Any], Awaitable], *, name: Optional[str] = None,
                 exclusive: Optional[bool] = None, durable: Optional[bool] = None, auto_delete: Optional[bool] = None):
        """
        Registers a handler with a given topic

        :param topic: topic to consume
        :param fn: async handler to consume the topic
        :param name: optional, name for the given queue for the topic
        :param exclusive: optional, should be the queue exclusive for this handler?
        :param durable: optional, should be the queue durable?
        :param auto_delete: optional, should be the queue deleted when the client disconects?
        :return: None
        """
        valid = inspect.iscoroutinefunction(fn) or (hasattr(fn, '__call__') and inspect.iscoroutinefunction(fn.__call__))
        if not valid:
            raise ArgumentError('currently we only support handling coroutines (async functions)')

        options = {k: v for k, v in
                   {'name': name, 'exclusive': exclusive, 'durable': durable, 'auto_delete': auto_delete}.items()
                   if v is not None}

        logging.debug(f'registered handler for topic {topic}')

        self._topics[topic] = TopicQueueOptions(MessageProcessor(fn, self.serde), options)

    async def run(self) -> None:
        """
        Asynchronous run, starts all the consumers
        """

        # TODO: This whole initialization could be moved to the __post_init__ method
        connection = await connect_robust(self.host)
        channel = await connection.channel()
        self._exchange = await channel.declare_exchange(self.exchange, ExchangeType.TOPIC)

        topic_queues = []
        for topic in self._topics.keys():
            options = self._topics[topic].options
            queue = await (channel.declare_queue(**options) if options else channel.declare_queue())
            logging.debug(f'created queue {queue.name}')

            await queue.bind(self._exchange, topic)
            logging.debug(f"bound queue '{queue.name}' to topic '{topic}'")

            topic_queues.append(queue)

        await asyncio.wait([
            asyncio.create_task(self._process_queue(queue)) for queue in topic_queues
        ])

    async def publish(self, what: Any, route: str, **kwargs):
        """
        Publishes a message to the exchange, this method uses the defined SerDe in the Hase constructor

        :param what: message body to be published
        :param route: route to publish the message (ie. topic)
        :param kwargs: optional, parameters to pass to the exchange publishing
        """
        if not self._exchange:
            raise RuntimeError('you can only publish when the application is running')
        message = Message(body=self.serde.serialize(what))
        await self._exchange.publish(message, routing_key=route, **kwargs)
        logging.debug(f'published message to route {route} in exchange {self._exchange}')


__all__ = ['Hase']
