"""Display temperature and humidity alerts from both alert topics."""

import json

from kafka import KafkaConsumer

from config import BOOTSTRAP_SERVERS, HUMIDITY_ALERTS_TOPIC, TEMPERATURE_ALERTS_TOPIC, kafka_options


def deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def main() -> None:
    consumer = KafkaConsumer(
        TEMPERATURE_ALERTS_TOPIC,
        HUMIDITY_ALERTS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="iot-alert-screen-v1",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
        **kafka_options(),
    )
    print("Waiting for alerts. Press Ctrl+C to stop.")
    try:
        for record in consumer:
            print(f"[{record.topic}] {record.value}")
    except KeyboardInterrupt:
        print("\nAlert consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
