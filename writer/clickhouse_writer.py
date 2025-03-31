import logging
import clickhouse_connect
from datetime import datetime, timezone
from config.utils import get_env_value
from datastore.redis_store import get_all_weather_data, clear_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLICKHOUSE_HOST = get_env_value("CLICKHOUSE_HOST")
CLICKHOUSE_DATABASE = get_env_value("CLICKHOUSE_DATABASE")
CLICKHOUSE_USER = get_env_value("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = get_env_value("CLICKHOUSE_PASSWORD")

async def bulk_write_to_clickhouse():
    """Fetch all weather data from Redis and bulk write it to ClickHouse."""
    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=8123,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
    except Exception as e:
        logging.error(f"Failed to connect to ClickHouse: {e}")
        return

    data = await get_all_weather_data()
    
    if not data:
        logging.info("No new data to write to ClickHouse.")
        return

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
    table_name = f"weather_{timestamp_str}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        location String,
        temp Float32,
        feels_like Float32,
        temp_min Float32,
        temp_max Float32,
        pressure Int32,
        humidity Int32,
        wind_speed Float32,
        wind_deg Int32,
        wind_gust Int32,
        clouds Int32,
        timestamp Int32,
        dt Int32
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    clickhouse_client.command(create_table_query)

    data_to_insert = [
        (
            row["location"],
            row.get("temp", 0.0), 
            row.get("feels_like", 0.0),
            row.get("temp_min", 0.0),
            row.get("temp_max", 0.0),
            row.get("pressure", 0), 
            row.get("humidity", 0),
            row.get("wind_speed", 0.0),
            row.get("wind_deg", 0),
            row.get("wind_gust", 0),
            row.get("clouds", 0),
            row.get("timestamp", 0),
            row.get("dt", 0),
        )
        for row in data
    ]

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S GMT+0")
    logging.info(f"Uploading {len(data)} records to ClickHouse at {utc_now}")

    clickhouse_client.insert(table_name, data_to_insert)

    await clear_redis()  
    logging.info(f"Uploaded {len(data)} records to ClickHouse and cleared Redis.")

    clickhouse_client.close()
