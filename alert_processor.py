"""Filter sensor readings and publish threshold alerts to Kafka."""

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from config import (
    BOOTSTRAP_SERVERS,
    HUMIDITY_ALERTS_TOPIC,
    SENSOR_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    kafka_options,
)


def serialize(data: dict) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def alert(readout: dict, message: str) -> dict:
    return {
        "sensor_id": readout["sensor_id"],
        "timestamp": readout["timestamp"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "temperature": readout["temperature"],
        "humidity": readout["humidity"],
        "message": message,
    }


def main() -> None:
    consumer = KafkaConsumer(
        SENSOR_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="iot-alert-processor-v1",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=deserialize,
        **kafka_options(),
    )
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=serialize, **kafka_options())
    print(f"Processing readings from {SENSOR_TOPIC}. Press Ctrl+C to stop.")
    try:
        for record in consumer:
            data = record.value
            print(f"Received: {data}")
            if data["temperature"] > 40:
                payload = alert(data, "Temperature exceeds 40°C")
                producer.send(TEMPERATURE_ALERTS_TOPIC, value=payload).get(timeout=10)
                print(f"  -> temperature alert sent: {payload}")
            if data["humidity"] > 80 or data["humidity"] < 20:
                boundary = "above 80%" if data["humidity"] > 80 else "below 20%"
                payload = alert(data, f"Humidity is {boundary}")
                producer.send(HUMIDITY_ALERTS_TOPIC, value=payload).get(timeout=10)
                print(f"  -> humidity alert sent: {payload}")
            producer.flush()
    except KeyboardInterrupt:
        print("\nAlert processor stopped.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
