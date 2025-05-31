import json
import logging
import traceback
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from consume.websocket_manager import WebSocketManager
from starlette.websockets import WebSocketDisconnect
from time import perf_counter
import time


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AsyncConsumer:
    def __init__(self, kafka_broker, topic, group_id, websocket_manager: WebSocketManager):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.logger = Logger().setup_logger(service_name="consumer")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: m,
        )
        self.websocket_manager = websocket_manager

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def consume(self):
        try:
            async for message in self.consumer:
                try:
                    raw_data = json.loads(message.value)
                    # self.logger.info("received")
                    
                    raw_data["streamer_consume_dt"] = int(time.time() * 1000)

                    weather_data = {
                        "dt": int(str(raw_data.get("dt", 0))[:10]),
                        "lat": raw_data.get("lat", "unknown"),
                        "lon": raw_data.get("lon", "unknown"),
                        "location": raw_data.get("location", "unknown"),
                        "temp": raw_data.get("main", {}).get("temp", 0.0),
                        "feels_like": raw_data.get("main", {}).get("feels_like", 0.0),
                        "pressure": raw_data.get("main", {}).get("pressure", 0),
                        "humidity": raw_data.get("main", {}).get("humidity", 0),
                        "wind_speed": raw_data.get("wind", {}).get("speed", 0.0),
                        "wind_deg": raw_data.get("wind", {}).get("deg", 0),
                        "wind_gust": raw_data.get("wind", {}).get("gust", 0.0),  
                        "clouds": raw_data.get("clouds", {}).get("all", 0),
                        "description": raw_data.get("weather", [{}])[0].get("description", "unknown"),
                        "source_dt": raw_data.get("source_dt"), 
                        "streamer_consume_dt": raw_data["streamer_consume_dt"],
                        "streamer_produce_dt": int(time.time() * 1000),
                    }

                    await self.websocket_manager.broadcast(json.dumps(weather_data))
                    # self.logger.info(f"Data sent to Websocket: {weather_data}")

                except WebSocketDisconnect:
                    self.logger.warning("WebSocket disconnected. Skipping message broadcast.")
                except Exception as e:
                    self.logger.error(f"Error transforming or sending data: {e}")

                try:
                    await self.consumer.commit()
                except Exception as e:
                    self.logger.error(f"Error committing Kafka offset: {e}")
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")
            self.logger.error(traceback.format_exc())

    async def consume_n_messages(self, count: int):
        consumed = 0
        total_bytes = 0
        start_time = perf_counter()

        try:
            async for message in self.consumer:
                msg_size = len(message.value)
                total_bytes += msg_size
                consumed += 1
                if consumed >= count:
                    break
        except Exception as e:
            self.logger.error(f"Error during batch consume: {e}")
            return

        total_time = perf_counter() - start_time
        throughput = total_bytes / total_time if total_time > 0 else 0
        byte_size_per_data = total_bytes // count if count > 0 else 0

        self.logger.info(f"[Metric] Byte size per message                 : {byte_size_per_data} bytes")
        self.logger.info(f"[Metric] Total messages consumed from Kafka    : {count}")
        self.logger.info(f"[Metric] Total data size consumed from Kafka   : {total_bytes} bytes")
        self.logger.info(f"[Metric] Time taken to consume from Kafka      : {total_time:.4f} seconds")
        self.logger.info(f"[Metric] Kafka Consumer Throughput             : {throughput:.2f} bytes/sec")

    async def produce_to_websocket(self, count: int):
        import json
        from time import perf_counter
        from starlette.websockets import WebSocketDisconnect

        dummy_data = {
            'dt': 1748705146,
            'lat': -7.9923,
            'lon': 110.2973,
            'location': 'Bambanglipuro',
            'temp': 299.04,
            'feels_like': 299.72,
            'pressure': 1013,
            'humidity': 78,
            'wind_speed': 6.13,
            'wind_deg': 129,
            'wind_gust': 9.34,
            'clouds': 100,
            'description': 'light rain'
        }

        json_data = json.dumps(dummy_data)
        total_bytes = len(json_data.encode("utf-8")) * count
        byte_size_per_data = len(json_data.encode("utf-8"))

        sent = 0
        start_time = perf_counter()

        for _ in range(count):
            try:
                await self.websocket_manager.broadcast(json_data)
                sent += 1
            except WebSocketDisconnect:
                self.logger.warning("WebSocket disconnected during dummy broadcast.")

        total_time = perf_counter() - start_time
        throughput = total_bytes / total_time if total_time > 0 else 0.0

        self.logger.info(f"[WebSocket Producer] Byte size per message                  : {byte_size_per_data} bytes")
        self.logger.info(f"[WebSocket Producer] Total messages produced to Websocket   : {sent}")
        self.logger.info(f"[WebSocket Producer] Total data size produced to Websocket  : {total_bytes} bytes")
        self.logger.info(f"[WebSocket Producer] Time taken to produce to Websocket     : {total_time:.4f} seconds")
        self.logger.info(f"[WebSocket Producer] WebSocket Producer Throughput          : {throughput:.2f} bytes/sec")

