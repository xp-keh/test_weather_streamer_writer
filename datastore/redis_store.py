import aioredis
import logging
import json
from config.utils import get_env_value
from collections import defaultdict

logger = logging.getLogger("redis_store")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

REDIS_HOST = get_env_value("REDIS_HOST")
REDIS_PORT = get_env_value("REDIS_PORT")
REDIS_DB = get_env_value("REDIS_DB")
REDIS_TTL = 7200

redis_client = None

async def init_redis():
    global redis_client
    redis_client = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)

async def save_weather_data(key: str, data: dict):
    if redis_client is None:
        await init_redis()
    try:
        await redis_client.setex(key, REDIS_TTL, json.dumps(data)) # type: ignore
    except Exception as e:
        logger.error(f"Failed to save data to Redis: {e}")

async def get_all_weather_data():
    if redis_client is None:
        await init_redis()
    else: 
        keys = await redis_client.keys("weather:*")
        data = [json.loads(await redis_client.get(key)) for key in keys]
        logging.info(f"Fetched {len(data)} records from Redis.")
        return data

async def clear_redis():
    if redis_client is None: 
        await init_redis()
    
    try:
        await redis_client.flushdb() # type: ignore
        logging.info("✅ Redis flush successful.")
    except Exception as e:
        logging.error(f"❌ Redis flush failed: {e}")


async def get_weather_data_by_keys(keys: list[str]):
    global redis_client
    if redis_client is None:
        await init_redis()

    if not keys:
        return []

    try:
        raw_values = await redis_client.mget(*keys)
        return [json.loads(value) for value in raw_values if value]
    except Exception as e:
        logger.error(f"Failed to fetch weather data by keys: {e}")
        return []

async def group_keys_by_location() -> dict[str, list[str]]:
    global redis_client
    if redis_client is None:
        await init_redis()

    try:
        keys = await redis_client.keys("weather:*")
        grouped = defaultdict(list)

        for key in keys:
            if "_" in key:
                _, location = key.rsplit("_", 1)
                grouped[location].append(key)

        return dict(grouped)
    except Exception as e:
        logger.error(f"Failed to group Redis keys by location: {e}")
        return {}