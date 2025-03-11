import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from datastore.redis_store import save_weather_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AsyncConsumer:
    """Kafka Consumer that listens to a topic, saves data to Redis, and streams via SSE."""
    
    def __init__(self, kafka_broker, topic, group_id):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.logger = Logger().setup_logger(service_name="consumer")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.queue = asyncio.Queue()

    async def start(self):
        """Start the Kafka consumer."""
        await self.consumer.start()

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()

    async def consume(self):
        """Continuously consume messages, save them to Redis, and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                raw_data = message.value  # Original Kafka message

                logging.info(f"Received Kafka message: {raw_data}")  # Debugging

                # Extracting and flattening necessary fields
                weather_data = {
                    "location": raw_data.get("location", "unknown"),
                    "temp": raw_data.get("main", {}).get("temp", 0.0),
                    "feels_like": raw_data.get("main", {}).get("feels_like", 0.0),
                    "temp_min": raw_data.get("main", {}).get("temp_min", 0.0),
                    "temp_max": raw_data.get("main", {}).get("temp_max", 0.0),
                    "pressure": raw_data.get("main", {}).get("pressure", 0),
                    "humidity": raw_data.get("main", {}).get("humidity", 0),
                    "wind_speed": raw_data.get("wind", {}).get("speed", 0.0),
                    "wind_deg": raw_data.get("wind", {}).get("deg", 0),
                    "clouds": raw_data.get("clouds", {}).get("all", 0),
                    "timestamp": raw_data.get("dt", 0)
                }

                key = f"weather:{weather_data['timestamp']}"
                await save_weather_data(key, weather_data)

                logging.info(f"Saved data to Redis: {key} -> {weather_data}")

                await self.queue.put(f"data: {json.dumps(weather_data)}\n\n")

        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")

    async def get_messages(self):
        """Async generator to retrieve messages from the queue."""
        while True:
            msg = await self.queue.get()
            yield msg
