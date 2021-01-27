# Hase, a nice RabbitMQ consumer

Hase (German for hare, bunny) is a nicer way to interact and consume [RabbitMQ](https://www.rabbitmq.com/) topics and queues. When dealing with topic consumers we end up repeating 80% of the code, so why not making something easy to reuse?

Hase has been designed to look a lot like [FastAPI](https://fastapi.tiangolo.com/) and [Flask](https://flask.palletsprojects.com/en/1.1.x/) so it will be easy to move from one paradigm to another.

## Ping pong

A simple consumer for the message `sample.ping`, emitting the message `sample.pong` and consuming it is very simple, we just define two handlers for it and that is all!

```python
from typing import Any, Dict
from hase import Hase

app = Hase(host='amqp://localhost/', exchange='sample')

@app.topic('ping')
def handle_ping(_: Dict[str, Any]):
    print('ping!')
    app.publish({'pong': True}, 'pong')

@app.topic('pong')
def handle_pong(_: Dict[str, Any]):
    print('pong')

app.run()
```

If you create a message with the data `{"ping": True}` it will work as expected!

## Limitations

 - By default Hase expects your message to be a valid JSON message, it uses `rapidjson` for serialization/deserialization but you can change this simply creating your own `hase.SerDe` and passing it in the `Hase` constructor.
 - We only support topics, maybe I will add more functionality if I needed
 - Async methods are the only supported methods right now, I think adding support for non-async methods won't be complex
 - We work so far with a single event loop, I am exploring cases to improve the performance and this is an important thing I am working on at this moment

## Installation

There is no official package for Hase yet, but you can use the master branch in this repository to play and use it:

```bash
pip install https+git://github.com/cprieto/hase.git#egg=hase
```
