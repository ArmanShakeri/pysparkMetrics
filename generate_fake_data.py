import asyncio
import json
import os

from aiokafka import AIOKafkaProducer
from time import time
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
faker = Faker()
kafka_bootstrap_server=os.environ["Kafka_bootstrap_server"]
kafka_topic=os.environ["Kafka_topic"]
num_threads=int(os.environ["Faker_num_threads"])
faker_sleep_s=int(os.environ["Faker_sleep_s"])

async def get_producer():
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_server,value_serializer=str.encode)
    await producer.start()
    return producer

async def send_one(event,producer,topic):
    await producer.send(topic , event)

async def store_data_point(device_id: str):
    producer = await get_producer()
    while True:
        data = dict(
            device_id=device_id,
            temperature=faker.random_int(10, 50),
            location=json.dumps(dict(latitude=str(faker.latitude()), longitude=str(faker.longitude()))),
            time=str(int(time()))
        )
        data=str(data)

        await send_one(data,producer,kafka_topic)
        await asyncio.sleep(faker_sleep_s)

def main():
    for _ in range(num_threads):
        asyncio.ensure_future(
            store_data_point(
                device_id=str(faker.uuid4())
            )
        )

    loop = asyncio.get_event_loop()
    loop.run_forever()


