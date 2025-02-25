import asyncio
import json
from aiokafka import AIOKafkaConsumer
from config.logging import Logger

class AsyncConsumer:
    """
    Kafka Consumer that listens to a topic and streams data via SSE.
    """
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
        self.logger.info(f" [*] Kafka consumer started on topic: {self.topic}")

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()
        self.logger.info(" [*] Kafka consumer stopped.")

    async def consume(self):
        """Continuously consume messages and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                self.logger.info(f" [*] Received message: {message.value}")
                await self.queue.put(message.value)
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")

    async def get_messages(self):
        """Async generator to retrieve messages from the queue."""
        while True:
            msg = await self.queue.get()
            yield f"data: {json.dumps(msg)}\n\n"
