import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )

    print(f"Listening to topic: {KAFKA_TOPIC}")
    print("Waiting for messages...\n")

    try:
        for message in consumer:
            print("=" * 50)
            print(f"Received message:")
            print(json.dumps(message.value, indent=2))
            print("=" * 50)
            print()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
