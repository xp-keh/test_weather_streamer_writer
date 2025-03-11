import asyncio
import logging
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
# from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from consume.kafka import AsyncConsumer
from config.utils import get_env_value
from datastore.redis_store import init_redis
from writer.clickhouse_writer import bulk_write_to_clickhouse

kafka_broker = get_env_value("KAFKA_BROKER")
kafka_consume_topic = get_env_value("KAFKA_CONSUME_TOPIC")
kafka_consumer_group = get_env_value("KAFKA_CONSUMER_GROUP")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
consumer = AsyncConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group)

scheduler = AsyncIOScheduler()
scheduler.start()

async def stream_data():
    """SSE streaming from Kafka (data already saved in Redis)."""
    async for message in consumer.get_messages():
        yield message

async def async_bulk_write_to_clickhouse():
    """Async function to bulk write to ClickHouse."""
    await bulk_write_to_clickhouse()

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer and schedule ClickHouse uploads on FastAPI startup."""
    await init_redis()
    await consumer.start()
    asyncio.create_task(consumer.consume()) 

    scheduler.add_job(
        async_bulk_write_to_clickhouse,
        trigger=IntervalTrigger(minutes=1),
        id="clickhouse_upload",
        replace_existing=True
    )

    logging.info("Scheduled ClickHouse upload job every 1 minute.")

@app.get("/stream")
async def stream():
    """SSE endpoint to stream Kafka messages"""
    return StreamingResponse(stream_data(), media_type="text/event-stream")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop Kafka consumer and scheduler on FastAPI shutdown"""
    await consumer.stop()
    scheduler.shutdown()

@app.get("/ping")
async def healthcheck():
    """Basic health check"""
    return {"status": "healthy"}
