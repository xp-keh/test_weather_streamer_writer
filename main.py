import threading
import asyncio
from dotenv import load_dotenv 
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from config.utils import get_env_value

# Load environment variables
load_dotenv()

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_consume_topic = get_env_value('KAFKA_CONSUME_TOPIC')
kafka_consumer_group = get_env_value('KAFKA_CONSUMER_GROUP')

app = FastAPI()

class KafkaConsumerService:
    """
    Kafka Consumer that continuously listens to a topic and processes messages.
    """
    def __init__(self, kafka_broker, topic, group_id):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            self.topic, 
            bootstrap_servers=self.kafka_broker, 
            group_id=self.group_id,
            auto_offset_reset="earliest"
        )
        self.loop = asyncio.new_event_loop()

    async def start(self):
        """Start the Kafka consumer."""
        await self.consumer.start()
        print("Kafka consumer started...")

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()
        print("Kafka consumer stopped.")

    async def consume(self):
        """Continuously consume messages from Kafka."""
        try:
            async for message in self.consumer:
                print(f"Received: {message.value.decode('utf-8')}")
        except Exception as e:
            print(f"Error in consumer: {e}")

    def run_consumer(self):
        """Run the Kafka consumer in a background thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.start())
        self.loop.run_until_complete(self.consume())

consumer_service = KafkaConsumerService(kafka_broker, kafka_consume_topic, kafka_consumer_group)

consumer_thread = threading.Thread(target=consumer_service.run_consumer, daemon=True)
consumer_thread.start()

@app.get("/ping")
async def healthcheck():
    return {"status": "healthy"}
