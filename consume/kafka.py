import asyncio
import json
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from datastore.sqlite_store import AsyncSessionLocal, save_weather_data

class AsyncConsumer:
    """Kafka Consumer that listens to a topic, saves data, and streams via SSE."""
    
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
        # self.logger.info(f" [*] Kafka consumer started on topic: {self.topic}")

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()
        # self.logger.info(" [*] Kafka consumer stopped.")

    async def consume(self):
        """Continuously consume messages, save them to SQLite, and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                # self.logger.info("Received data from Kafka")

                async with AsyncSessionLocal() as session:
                    await save_weather_data(session, message.value)
                    await session.commit()
                # self.logger.info("Uploaded data to SQLite")

                await self.queue.put(f"data: {json.dumps(message.value)}\n\n")
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")

    async def get_messages(self):
        """Async generator to retrieve messages from the queue."""
        while True:
            msg = await self.queue.get()
            yield msg
