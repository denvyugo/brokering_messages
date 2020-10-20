import asyncio
import os

import aio_pika
from aiohttp import web
from gino.ext.aiohttp import Gino


class RabbitConnectError(Exception):
    def __init__(self, description):
        self.message = f'Connect Error with {description}'
        super().__init__(self.message)


class RabbitWorkingError(Exception):
    def __init__(self, description):
        self.message = f'Working Error with {description}'
        super().__init__(self.message)


# working with rabbitmq
async def send_message(text, source):
    routing = source
    try:
        connection = await(aio_pika.connect(
            "amqp://{user}:{user}@{host}/".format(
                user=os.getenv('MQ_USER', 'guest'),
                host=os.getenv('MQ_HOST', 'mq'))))
    except Exception as exp:
        raise RabbitConnectError(str(exp))
    
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange('messages',
                                                  aio_pika.ExchangeType.DIRECT)
        await exchange.publish(
            aio_pika.Message(
                body=text.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing
            )
        await connection.close()
    except Exception as exp:
        raise RabbitWorkingError(f'{str(exp)}')


async def produce_message(message):
   await send_message(str(message.id), str(message.source))
    


# Database Configuration
PG_URL = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
    host=os.getenv("DB_HOST", "db"),
    port=os.getenv("DB_PORT", 5432),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_DATABASE", "postgres"),
)

# Initialize Gino instance
db = Gino()

# Initialize aiohttp app
app = web.Application(middlewares=[db])
db.init_app(app, dict(dsn=PG_URL))


# Definition of table
class Message(db.Model):
    __tablename__ = "messages"

    id = db.Column(db.BigInteger(), primary_key=True)
    recipient = db.Column(db.Unicode())
    source = db.Column(db.Integer())
    status = db.Column(db.Unicode(), default="new")
    body = db.Column(db.Unicode())


# Definition of routes
routes = web.RouteTableDef()


@routes.get("/")
async def index(request):
    return web.Response(text="receiver")


@routes.get("/message/{uid}")
async def get_message(request):
    uid = int(request.match_info["uid"])
    message = Message.query.where(Message.id == uid)
    return web.json_response((await message.gino.first_or_404()).to_dict())


@routes.get(r"/messages/{number:\d*}")
async def get_messages(request):
    number = request.match_info["number"]
    qty = int(number) if number else 0
    qwery = messages = await Message.query.gino.all()
    if qty > 0:
        messages = qwery[:qty]
    else:
        messages = qwery
    response = []
    for message in messages:
        response.append(message.to_dict())
    return web.json_response(response)


@routes.post("/messages")
async def add_message(request):
    form = await request.json()
    try:
        message = await Message.create(recipient=form.get("recipient"),
                                   source=form.get("source"),
                                   body=form.get("body"))
        await produce_message(message)
        return web.json_response({'id': message.id})
    except Exception as exp:
        return web.json_response({'error': str(exp)})


app.router.add_routes(routes)


async def create(app_):
    await db.gino.create_all()

 
app.on_startup.append(create)

if __name__ == "__main__":
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    web.run_app(
        app,
        host=host,
        port=port,
    )
