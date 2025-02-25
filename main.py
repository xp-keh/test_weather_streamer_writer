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
        yield f"data: {message}\n\n"

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer and schedule ClickHouse uploads on FastAPI startup."""
    await consumer.start()
    asyncio.create_task(consumer.consume())

    scheduler.add_job(
        bulk_write_to_clickhouse,
        trigger=IntervalTrigger(hours=1),
        id="clickhouse_upload",
        replace_existing=True
    )
    logging.info("Scheduled hourly ClickHouse upload job.")

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
