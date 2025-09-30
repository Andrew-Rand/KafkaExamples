import asyncio
from aiokafka import AIOKafkaConsumer
import json

async def consume():
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers='localhost:9092',
        group_id="async-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    print('Consumer started.')
    try:
        async for msg in consumer:
            print(f"Received: {msg.value}")
    finally:
        print('Stopping consumer...')
        await consumer.stop()
        print('Consumer stopped.')


if __name__ == "__main__":
    asyncio.run(consume())