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

    async def run_consumer(self):
        """Run the Kafka consumer in FastAPI's event loop."""
        await self.start()
        while True:
            await asyncio.sleep(1) 

sse_consumer = KafkaSSEConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group)

@app.on_event("startup")
async def startup_event():
    """Start the Kafka consumer on FastAPI startup."""
    asyncio.create_task(sse_consumer.run_consumer())

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the Kafka consumer on FastAPI shutdown."""
    await sse_consumer.stop()

@app.get("/ping")
async def healthcheck():
    return {"status": "healthy"}

@app.get("/stream")
async def stream():
    """SSE endpoint to send Kafka messages to the frontend."""
    return StreamingResponse(sse_consumer.consume(), media_type="text/event-stream")
