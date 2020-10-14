import asyncio
import os
import sys

import aio_pika
from gino import Gino

loop = asyncio.get_event_loop()

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

# Definition of table
class Message(db.Model):
    __tablename__ = "messages"

    id = db.Column(db.BigInteger(), primary_key=True)
    recipient = db.Column(db.Unicode())
    source = db.Column(db.Integer())
    status = db.Column(db.Unicode(), default="new")
    body = db.Column(db.Unicode())


async def work_message(message_id):
    message = await Message.query.where(
        Message.id == int(message_id)).gino.first()
    if message:
        print(message.body)
        if '9999' in message.body:
            await message.update(status = 'err').apply()
        else:
            await message.update(status = 'wrk').apply()


async def on_message(message):
    await work_message(message.body)


async def main(source_id):
    # bind to Database
    await db.set_bind(PG_URL)
    # Perform connection
    host = os.getenv('MQ_HOST', 'mq')
    connection = await aio_pika.connect(
            "amqp://{user}:{user}@{host}/".format(
                user=os.getenv('MQ_USER', 'guest'),
                host=host),
            loop=loop)
    # Creating a channel
    channel = await connection.channel()
    exchange = await channel.declare_exchange('messages',
                                                  aio_pika.ExchangeType.DIRECT)

    # Declaring queue
    queue = await channel.declare_queue(
        f"{source_id}-queue",
        durable=True
    )
    await queue.bind(exchange, source_id)

    # Start listening the queue with name 'task_queue'
    await queue.consume(on_message)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        source_id = sys.argv[1]
        loop = asyncio.get_event_loop()
        loop.create_task(main(source_id))
        loop.run_forever()
    else:
        print("Parameters Error: no source_id was given!")
