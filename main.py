import threading
import asyncio
from dotenv import load_dotenv 
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from aiokafka import AIOKafkaConsumer
from config.utils import get_env_value

load_dotenv()

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_consume_topic = get_env_value('KAFKA_CONSUME_TOPIC')
kafka_consumer_group = get_env_value('KAFKA_CONSUMER_GROUP')

app = FastAPI()

class KafkaSSEConsumer:
    """
    Kafka Consumer that continuously listens to a topic and streams data via SSE.
    """
    def __init__(self, kafka_broker, topic, group_id):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.loop = asyncio.new_event_loop()
        self.consumer = AIOKafkaConsumer(
            self.topic, 
            bootstrap_servers=self.kafka_broker, 
            group_id=self.group_id,
            auto_offset_reset="earliest"
        )
    
    async def start(self):
        """Start the Kafka consumer."""
        await self.consumer.start()

    async def stop(self):
        """Stop the Kafka consumer."""
        await self.consumer.stop()
    
    async def consume(self):
        """Async generator to consume messages."""
        try:
            async for message in self.consumer:
                yield f"data: {message.value.decode('utf-8')}\n\n"
        except Exception as e:
            print(f"Error in consumer: {e}")

    def run_consumer(self):
        """Run the consumer in an event loop."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.start())
        self.loop.run_forever()

sse_consumer = KafkaSSEConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group)

t_consumer = threading.Thread(target=sse_consumer.run_consumer, daemon=True)
t_consumer.start()

@app.get("/ping")
async def healthcheck():
    return {"status": "healthy"}

@app.get("/stream")
async def stream():
    """SSE endpoint to send Kafka messages to the frontend."""
    return StreamingResponse(sse_consumer.consume(), media_type="text/event-stream")
