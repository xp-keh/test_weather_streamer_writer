import logging
import clickhouse_connect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, Float, String, JSON
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DATABASE_URL = "sqlite+aiosqlite:///./weather_temp.db"
engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

CLICKHOUSE_HOST = "data-station-clickhouse"
CLICKHOUSE_DATABASE = "weather_dev_1"
CLICKHOUSE_USER = "abby"
CLICKHOUSE_PASSWORD = "SpeakLouder"

try:
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=8123,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    logging.info("Connected to ClickHouse successfully!")
except Exception as e:
    logging.error(f"Failed to connect to ClickHouse: {e}")

class WeatherData(Base):
    """SQLite Table for temporary weather data storage."""
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(String, index=True)
    temp = Column(Float)
    feels_like = Column(Float)
    temp_min = Column(Float)
    temp_max = Column(Float)
    pressure = Column(Integer)
    humidity = Column(Integer)
    wind_speed = Column(Float)
    wind_deg = Column(Integer)
    clouds = Column(Integer)
    timestamp = Column(Integer)
    raw_data = Column(JSON)

async def init_db():
    """Initialize SQLite database."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def save_weather_data(session: AsyncSession, data: dict):
    """Save a single Kafka message into SQLite."""
    weather_entry = WeatherData(
        location=data.get("location", "Unknown"),
        temp=data["main"]["temp"],
        feels_like=data["main"].get("feels_like"),
        temp_min=data["main"].get("temp_min"),
        temp_max=data["main"].get("temp_max"),
        pressure=data["main"].get("pressure"),
        humidity=data["main"].get("humidity"),
        wind_speed=data["wind"].get("speed"),
        wind_deg=data["wind"].get("deg"),
        clouds=data["clouds"].get("all"),
        timestamp=data.get("dt"),
        raw_data=data
    )
    session.add(weather_entry)

async def bulk_write_to_clickhouse():
    """Periodically write SQLite data to ClickHouse and clear SQLite."""
    now = datetime.now()
    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Fetching data from SQLite at {timestamp_str}")

    async with AsyncSessionLocal() as session:
        result = await session.execute("SELECT * FROM weather_data")
        rows = result.fetchall()

        if not rows:
            logging.info("No new data to write to ClickHouse.")
            return

        table_name = f"weather_{now.strftime('%Y%m%d_%H%M')}"

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

        data_to_insert = [(row.location, row.temp, row.feels_like, row.temp_min, row.temp_max,
                           row.pressure, row.humidity, row.wind_speed, row.wind_deg, row.clouds, row.timestamp)
                          for row in rows]

        logging.info(f"Uploading data to ClickHouse at {timestamp_str}")

        clickhouse_client.insert(table_name, data_to_insert)

        await session.execute("DELETE FROM weather_data")
        await session.commit()
        logging.info(f"Data upload complete, SQLite cleared at {timestamp_str}")
