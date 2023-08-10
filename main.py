import time
import asyncio
import os
import threading
from stats_listener import StatsListener
from dotenv import load_dotenv
import generate_fake_data as faker
from spark_app import create_session,kafka_consumer,sink,process

load_dotenv()

if bool(os.environ["Faker_enable"]):
    def run_faker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(faker.main())
    faker_thread = threading.Thread(target=run_faker)
    faker_thread.daemon = True
    faker_thread.start()

session = create_session()
df = kafka_consumer(session)
df = process(df)
query = sink(df)
session.streams.addListener(StatsListener())
query.awaitTermination()



