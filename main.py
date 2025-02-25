import asyncio
import logging
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from consume.kafka import AsyncConsumer
from config.utils import get_env_value
from datastore.sqlite_store import AsyncSessionLocal, save_weather_data, bulk_write_to_clickhouse

kafka_broker = get_env_value("KAFKA_BROKER")
kafka_consume_topic = get_env_value("KAFKA_CONSUME_TOPIC")
kafka_consumer_group = get_env_value("KAFKA_CONSUMER_GROUP")

app = FastAPI()

consumer = AsyncConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

scheduler = BackgroundScheduler()
scheduler.start()

async def consume_and_store():
    async for message in consumer.get_messages():
        data = message
        async with AsyncSessionLocal() as session:
            await save_weather_data(session, data)
        logging.info("Uploaded data to SQLite")
        yield f"data: {message}\n\n"

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer and schedule ClickHouse uploads on FastAPI startup."""
    await consumer.start()
    asyncio.create_task(consumer.consume())

    scheduler.add_job(
        bulk_write_to_clickhouse,
        trigger=IntervalTrigger(minutes=1),
        id="clickhouse_upload",
        replace_existing=True
    )
    logging.info("Scheduled ClickHouse upload job every 1 minute.")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop Kafka consumer and scheduler on FastAPI shutdown."""
    await consumer.stop()
    scheduler.shutdown()

@app.get("/ping")
async def healthcheck():
    """Basic health check."""
    return {"status": "healthy"}

@app.get("/stream")
async def stream():
    """SSE endpoint to stream Kafka messages."""
    return StreamingResponse(consume_and_store(), media_type="text/event-stream")

import asyncio
import json
from aiokafka import AIOKafkaConsumer
from config.logging import Logger

class AsyncConsumer:
    """
    Kafka Consumer that listens to a topic and streams data via SSE.
    """
    def __init__(self, kafka_broker, topic, group_id):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.logger = Logger().setup_logger(service_name="consumer")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.queue = asyncio.Queue()

    async def start(self):
        """Start the Kafka consumer."""
        await self.consumer.start()
        self.logger.info(f" [*] Kafka consumer started on topic: {self.topic}")

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()
        self.logger.info(" [*] Kafka consumer stopped.")

    async def consume(self):
        """Continuously consume messages and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                self.logger.info("Received data from Kafka")
                await self.queue.put(message.value)
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")

    async def get_messages(self):
        """Async generator to retrieve messages from the queue."""
        while True:
            msg = await self.queue.get()
            yield f"data: {json.dumps(msg)}\n\n"
