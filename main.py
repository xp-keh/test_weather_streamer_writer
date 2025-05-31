import asyncio
import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, WebSocket, Query
from consume.websocket_manager import WebSocketManager
from consume.kafka import AsyncConsumer
from config.utils import get_env_value
from datastore.redis_store import init_redis

kafka_broker = get_env_value("KAFKA_BROKER")
kafka_consume_topic = get_env_value("KAFKA_CONSUME_TOPIC")
kafka_consumer_group = get_env_value("KAFKA_CONSUMER_GROUP")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
websocket_manager = WebSocketManager()
consumer = AsyncConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group, websocket_manager)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming."""
    await websocket_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        logging.info(f"WebSocket disconnected: {e}")
    finally:
        await websocket_manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    await init_redis()
    await consumer.start()
    asyncio.create_task(consumer.consume())


@app.on_event("shutdown")
async def shutdown_event():
    await consumer.stop()

@app.get("/ping")
async def healthcheck():
    return {"status": "healthy"}

@app.get("/consume-message")
async def consume_n_messages(count: int = Query(..., ge=1, le=10000)):
    await consumer.consume_n_messages(count)
    return {"status": f"{count} messages consumed and logged"}

@app.get("/produce-websocket-start")
async def produce_to_websocket(count: int = Query(..., ge=1, le=10000)):
    await consumer.produce_to_websocket(count)
    return {"status": f"Produced {count} dummy messages to websocket"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
