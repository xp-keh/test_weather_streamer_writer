import asyncio
import logging
import time
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, WebSocket, Query
from consume.websocket_manager import WebSocketManager
from consume.kafka import AsyncConsumer
from config.utils import get_env_value
from datastore.redis_store import init_redis, save_weather_data
from writer.clickhouse_writer import bulk_write_to_clickhouse

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

@app.post("/test-batch-upload")
async def test_batch_upload(n: int = Query(..., ge=1, le=10000)):
    """
    Melakukan batch test:
    1. Memasukkan n dummy data ke Redis
    2. Memulai timer
    3. Mengambil semua data dari Redis
    4. Upload ke ClickHouse
    5. Return jumlah row, waktu, kecepatan upload
    """

    dummy_data = {
        'dt': 1748705146,
        'lat': -7.9923,
        'lon': 110.2973,
        'location': 'Bambanglipuro',
        'temp': 299.04,
        'feels_like': 299.72,
        'pressure': 1013,
        'humidity': 78,
        'wind_speed': 6.13,
        'wind_deg': 129,
        'wind_gust': 9.34,
        'clouds': 100,
        'description': 'light rain'
    }
    json_data = json.dumps(dummy_data)
    total_bytes = len(json_data.encode("utf-8")) * n
    byte_size_per_data = len(json_data.encode("utf-8"))

    tasks = []
    for i in range(n):
        key = f"weather:{dummy_data['dt']}_{i}"
        tasks.append(save_weather_data(key, dummy_data))

    await asyncio.gather(*tasks)

    logging.info(f"[Batch Upload] Completed writing {n} entries to Redis")

    start_time = time.time()

    await bulk_write_to_clickhouse()

    end_time = time.time()
    total_time = end_time - start_time
    rows_uploaded = n
    upload_speed =  n / total_time if total_time > 0 else 0

    logging.info(f"[Batch Upload] Byte size per message   : {byte_size_per_data} bytes")
    logging.info(f"[Batch Upload] Rows Uploaded           : {n}")
    logging.info(f"[Batch Upload] Total Time to Upload    : {total_time:.4f} seconds")
    logging.info(f"[Batch Upload] Upload Speed            : {upload_speed:.2f} rows/second")


    return {
        "rows_uploaded": rows_uploaded,
        "total_time_seconds": round(total_time, 4),
        "upload_speed_rows_per_second": round(upload_speed, 2)
    }


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
