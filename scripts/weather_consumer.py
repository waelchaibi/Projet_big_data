import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"


def wait_for_kafka(max_retries=30, retry_delay=2):
    """Attendre que Kafka soit disponible"""
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                consumer_timeout_ms=1000
            )
            consumer.close()
            print("Kafka is ready!", flush=True)
            return True
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries})", flush=True)
                time.sleep(retry_delay)
            else:
                print("Kafka not available after max retries", flush=True)
                return False
    return False


def main():
    if not wait_for_kafka():
        print("Failed to connect to Kafka. Exiting.", flush=True)
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000
    )

    print(f"Listening to topic: {KAFKA_TOPIC}", flush=True)
    print("Waiting for messages...\n", flush=True)

    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            print("=" * 60, flush=True)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Message #{message_count} received:", flush=True)
            print(json.dumps(message.value, indent=2), flush=True)
            print("=" * 60, flush=True)
            print("", flush=True)
    except KeyboardInterrupt:
        print("\nStopping consumer...", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
