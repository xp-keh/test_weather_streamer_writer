import asyncio
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from aiokafka import AIOKafkaConsumer

app = FastAPI()

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather_data"

async def kafka_consumer():
    """Async generator that consumes messages from Kafka."""
    consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER, group_id="sse-group")
    await consumer.start()
    
    try:
        async for message in consumer:
            yield f"data: {message.value.decode('utf-8')}\n\n"  # SSE format
    finally:
        await consumer.stop()

@app.get("/stream")
async def stream():
    """SSE endpoint to send Kafka messages to the frontend."""
    return StreamingResponse(kafka_consumer(), media_type="text/event-stream")
