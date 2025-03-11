import logging
import clickhouse_connect
from datetime import datetime
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
        clouds Int32,
        timestamp Int32
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    clickhouse_client.command(create_table_query)

    data_to_insert = [
        (
            row["location"], row["temp"], row["feels_like"], row["temp_min"],
            row["temp_max"], row["pressure"], row["humidity"], row["wind_speed"],
            row["wind_deg"], row["clouds"], row["timestamp"]
        )
        for row in data
    ]

    logging.info(f"Uploading {len(data)} records to ClickHouse...")

    insert_query = f"""
    INSERT INTO {table_name} (location, temp, feels_like, temp_min, temp_max, pressure, humidity, 
                              wind_speed, wind_deg, clouds, timestamp)
    VALUES
    """
    clickhouse_client.insert(insert_query, data_to_insert)

    await clear_redis()  
    logging.info(f"Uploaded {len(data)} records to ClickHouse and cleared Redis.")

    # Close connection explicitly
    clickhouse_client.close()
