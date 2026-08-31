"""
Assignment 2 - Food Delivery Kafka Producer
Streams sample data from Assignment 1 sources to two Kafka topics.

Topics:
    order-events-topic
    delivery-tracking-topic

Run:
    python producer.py

Requirement:
    pip install kafka-python
"""

import csv
import json
import time
from pathlib import Path
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
ORDER_TOPIC = "order-events-topic"
TRACKING_TOPIC = "delivery-tracking-topic"

BASE_DIR = Path(__file__).resolve().parent
ORDER_FILE = BASE_DIR / "order_events_stream.jsonl"
TRACKING_FILE = BASE_DIR / "delivery_tracking_stream.jsonl"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

def stream_file(file_path, topic, delay=1.0, limit=100):
    """Read JSONL records and publish them one by one to Kafka."""
    sent = 0

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if sent >= limit:
                break

            record = json.loads(line)
            future = producer.send(topic, value=record)
            metadata = future.get(timeout=10)

            print(
                f"[SENT] topic={metadata.topic} "
                f"partition={metadata.partition} offset={metadata.offset} "
                f"order_id={record.get('order_id')} "
                f"event={record.get('order_status', record.get('delivery_status'))}"
            )

            sent += 1
            time.sleep(delay)

    producer.flush()
    return sent

def main():
    print("=" * 70)
    print("FOOD DELIVERY - KAFKA PRODUCER")
    print("=" * 70)
    print(f"Kafka broker : {BOOTSTRAP_SERVERS}")
    print(f"Order topic  : {ORDER_TOPIC}")
    print(f"Tracking topic: {TRACKING_TOPIC}")
    print()

    print("Streaming order events...")
    order_count = stream_file(
        ORDER_FILE,
        ORDER_TOPIC,
        delay=1.0,
        limit=50
    )

    print()
    print("Streaming delivery-tracking events...")
    tracking_count = stream_file(
        TRACKING_FILE,
        TRACKING_TOPIC,
        delay=1.0,
        limit=50
    )

    print()
    print("=" * 70)
    print(f"Completed: {order_count} order messages + {tracking_count} tracking messages")
    print("=" * 70)

    producer.close()

if __name__ == "__main__":
    main()
