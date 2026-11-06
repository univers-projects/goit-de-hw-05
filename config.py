"""Shared Kafka configuration and topic names."""

import os
import re
from typing import Any

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = [
    server.strip()
    for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    if server.strip()
]

TOPIC_SUFFIX = re.sub(r"[^a-zA-Z0-9._-]", "_", os.getenv("TOPIC_SUFFIX", "univers"))
SENSOR_TOPIC = f"building_sensors_{TOPIC_SUFFIX}"
TEMPERATURE_ALERTS_TOPIC = f"temperature_alerts_{TOPIC_SUFFIX}"
HUMIDITY_ALERTS_TOPIC = f"humidity_alerts_{TOPIC_SUFFIX}"
ALL_TOPICS = (SENSOR_TOPIC, TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC)


def kafka_options() -> dict[str, Any]:
    """Return optional security parameters when they are present in .env."""
    mapping = {
        "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
        "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME"),
        "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD"),
    }
    return {key: value for key, value in mapping.items() if value}
