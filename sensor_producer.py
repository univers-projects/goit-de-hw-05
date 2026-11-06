"""Simulate exactly one building sensor per program run."""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVERS, SENSOR_TOPIC, kafka_options


def serialize(data: dict) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def main() -> None:
    sensor_id = f"sensor-{random.randint(1000, 9999)}"
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
        **kafka_options(),
    )
    print(f"Started {sensor_id}; sending readings to {SENSOR_TOPIC}. Press Ctrl+C to stop.")
    try:
        while True:
            reading = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": round(random.uniform(25, 45), 2),
                "humidity": round(random.uniform(15, 85), 2),
            }
            metadata = producer.send(SENSOR_TOPIC, value=reading).get(timeout=10)
            print(f"Sent {reading} (partition={metadata.partition}, offset={metadata.offset})")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nSensor simulation stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
