import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 52.52, 13.41  

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"


def wait_for_kafka(max_retries=30, retry_delay=2):
    """Attendre que Kafka soit disponible"""
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(0, 10, 1)
            )
            producer.close()
            print("Kafka is ready!")
            return True
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"Waiting for Kafka... ({i+1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print("Kafka not available after max retries")
                return False
    return False


def fetch_weather():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current_weather": "true"
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})


def transform_weather(record: dict) -> dict:
    if "temperature" in record:
        record["temp_f"] = record["temperature"] * 9/5 + 32
    record["high_wind_alert"] = record.get("windspeed", 0) > 10
    return record


def main():
    # Attendre que Kafka soit prêt
    if not wait_for_kafka():
        print("Failed to connect to Kafka. Exiting.")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(0, 10, 1)
    )

    print("Weather streaming producer started...", flush=True)
    message_count = 0
    while True:
        try:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Fetching weather data...", flush=True)
            weather = fetch_weather()
            if weather:
                transformed = transform_weather(weather)
                future = producer.send(KAFKA_TOPIC, transformed)
                producer.flush()
                message_count += 1
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Message #{message_count} sent to {KAFKA_TOPIC}:", flush=True)
                print(json.dumps(transformed, indent=2), flush=True)
                print("-" * 50, flush=True)
            else:
                print("Warning: No weather data received", flush=True)
        except Exception as e:
            print(f"Error fetching or sending weather: {e}", flush=True)
            import traceback
            traceback.print_exc()
        time.sleep(30)  


if __name__ == "__main__":
    main()
