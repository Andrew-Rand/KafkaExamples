import asyncio
from aiokafka import AIOKafkaProducer
import json

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    print('Producer started.')
    try:
        for i in range(5):
            message = {'id': i, 'text': f'Async Kafka {i}'}
            await producer.send_and_wait("test-topic", message)
            print(f"Sent: {message}")
            await asyncio.sleep(1)
    finally:
        print('Stopping producer...')
        await producer.stop()
        print('Producer stopped.')

if __name__ == "__main__":
    asyncio.run(produce())