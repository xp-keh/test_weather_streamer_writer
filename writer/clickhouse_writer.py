import logging
import clickhouse_connect
import httpx
from datetime import datetime, timezone, timedelta
from config.utils import get_env_value
from datastore.redis_store import get_all_weather_data, group_keys_by_location, get_weather_data_by_keys, clear_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLICKHOUSE_HOST = get_env_value("CLICKHOUSE_HOST")
CLICKHOUSE_DATABASE = get_env_value("CLICKHOUSE_DATABASE")
CLICKHOUSE_USER = get_env_value("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = get_env_value("CLICKHOUSE_PASSWORD")


locations = {
    "TNTI": ("0.7718", "127.3667"),
    "PMBI": ("-2.9024", "104.6993"),
    "BKB": ("-1.1073", "116.9048"),
    "SOEI": ("-9.7553", "124.2672"),
    "MMRI": ("-8.6357", "122.2376"),
}

async def register_table_api(table_name, location, timestamp_str):
    url = "http://xp-keh:4000/catalog/register"
    data = {
        "table_name": table_name,
        "data_type": "weather",
        "station": location,
        "date": timestamp_str,
        "latitude": locations.get(location, ("unknown", "unknown"))[0],
        "longitude": locations.get(location, ("unknown", "unknown"))[1],
    }
    # logging.info(f"Sending registration request to {url} with JSON payload: {data}")

    
    async with httpx.AsyncClient() as client:
        # try:
        response = await client.post(url, json=data)
            # if response.status_code in (200, 201):
            #     logging.info(f"Registered table for {table_name} successfully")
            # else:
            #     logging.error(f"Failed to register table for location {location}. Status: {response.status_code}")
        # except Exception as e:
            # logging.error(f"Exception registering table for location {location}: {e}")
    
def format_unix_to_utc_string(unix_ts):
    try:
        dt_utc = datetime.utcfromtimestamp(unix_ts)
        return dt_utc.strftime("%d-%m-%YT%H:%M:%S")
    except Exception:
        return ""
    
def safe_float(val: str) -> float:
    try:
        return float(val.replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0 
    
async def bulk_write_to_clickhouse():
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

    # grouped_keys = await group_keys_by_location()
    # if not grouped_keys:
    #     logging.info("No new data to write to ClickHouse.")
    #     return

    previous_hour = datetime.now(timezone.utc) - timedelta(hours=1)
    timestamp_str = previous_hour.strftime("%Y%m%d_%H%M%S")

    total_inserted = 0

    # for location, keys in grouped_keys.items():
    weather_data = await get_all_weather_data()
    # if not weather_data:
    #     continue

    table_name = f"weather_{timestamp_str}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        dt Int32,
        timestamp String,
        lat Float64,
        lon Float64,
        location String,
        description String,
        temp Float32,
        feels_like Float32,
        pressure Int32,
        humidity Int32,
        wind_speed Float32,
        wind_deg Int32,
        wind_gust Float32,
        clouds Int32,
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    clickhouse_client.command(create_table_query)

    # await register_table_api(table_name, location, timestamp_str)

    data_to_insert = [
        (
            row.get("dt", 0),
            format_unix_to_utc_string(row.get("dt", 0)),
            safe_float(row.get("lat", 0.0)),
            safe_float(row.get("lon", 0.0)),
            row["location"],
            row["description"],
            row.get("temp", 0.0), 
            row.get("feels_like", 0.0),
            row.get("pressure", 0), 
            row.get("humidity", 0),
            row.get("wind_speed", 0.0),
            row.get("wind_deg", 0),
            row.get("wind_gust", 0.0),
            row.get("clouds", 0),
        )
        for row in weather_data # type: ignore
    ]

    clickhouse_client.insert(table_name, data_to_insert)
    logging.info(f"[Batch Upload] Uploaded {len(data_to_insert)} records to table {table_name}.")

    total_inserted += len(data_to_insert)

    if total_inserted > 0:
        await clear_redis()
        # logging.info(f"Uploaded a total of {total_inserted} records to ClickHouse and cleared Redis.")
    else:
        logging.info("No data was uploaded to ClickHouse.")

    clickhouse_client.close()
