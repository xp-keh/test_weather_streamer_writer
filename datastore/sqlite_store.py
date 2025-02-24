import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from consume.kafka import AsyncConsumer
from config.utils import get_env_value

load_dotenv()

kafka_broker = get_env_value("KAFKA_BROKER")
kafka_consume_topic = "weather_raw" 
kafka_consumer_group = get_env_value("KAFKA_CONSUMER_GROUP")

app = FastAPI()

consumer = AsyncConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group)

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on FastAPI startup."""
    await consumer.start()
    asyncio.create_task(consumer.consume())

@app.on_event("shutdown")
async def shutdown_event():
    """Stop Kafka consumer on FastAPI shutdown."""
    await consumer.stop()

@app.get("/ping")
async def healthcheck():
    """Basic health check."""
    return {"status": "healthy"}

@app.get("/stream")
async def stream():
    """SSE endpoint to stream Kafka messages."""
    return StreamingResponse(consumer.get_messages(), media_type="text/event-stream")
