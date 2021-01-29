# Hase, a nice RabbitMQ consumer

![](hare.png)

Hase (German for hare, bunny) is a nicer way to interact and consume [RabbitMQ](https://www.rabbitmq.com/) topics and queues. When dealing with topic consumers we end up repeating 80% of the code, so why not making something easy to reuse?

Hase has been designed to look a lot like [FastAPI](https://fastapi.tiangolo.com/) and [Flask](https://flask.palletsprojects.com/en/1.1.x/) so it will be easy to move from one paradigm to another.

## Ping pong

A simple consumer for the message `sample.ping`, emitting the message `sample.pong` and consuming it is very simple, we just define two handlers for it and that is all!

```python
import asyncio
from typing import Any
from hase import Hase

app = Hase(host='amqp://localhost/', exchange='sample')

@app.topic('ping')
async def handle_ping(_: Any):
    print('ping!')
    app.publish({'message': 'pong'}, 'pong')

@app.topic('pong')
async def handle_pong(data: Any):
    print('pong', data['message'])

# This will handle any ValueError thrown by a handler
@app.handle_exception(ValueError)
async def handler_error(message, exception):
    print('I got some awful error!')
    
asyncio.run(app.run())
```

If you create a message with the data `{"ping": True}` it will work as expected!

## Limitations

 - By default, Hase expects your message to be a valid JSON message, it uses `orjson` for serialization/deserialization, but you can change this simply creating your own `hase.SerDe` and passing it in the `Hase` constructor.
 - We only support topics, maybe I will add more functionality if I needed
 - Async methods are the only supported methods for handlers
 - We work so far with a single event loop

## Installation

There is no official package for Hase yet, but you can use the master branch in this repository to play and use it:

```bash
pip install https+git://github.com/cprieto/hase.git#egg=hase
```

or using the fantastic Poetry:

```bash
poetry add https+git://github.com/cprieto/hase.git
```
