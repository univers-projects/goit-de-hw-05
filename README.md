# IoT Kafka monitoring

Pipeline for readings from simulated temperature/humidity sensors and threshold alerts.

## Topics

The suffix from `TOPIC_SUFFIX` prevents collisions with other students' topics:

- `building_sensors_univers`
- `temperature_alerts_univers`
- `humidity_alerts_univers`

## Setup

```bash
cd goit-de-hw-05
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `KAFKA_BOOTSTRAP_SERVERS` (and, if required, security credentials) in `.env`.

## Start Kafka and Kafka UI

Docker Desktop must be running. Start the local single-node Kafka broker and its web UI:

```bash
docker compose up -d
docker compose ps
```

Open [Kafka UI](http://localhost:8080) in a browser. No login is required for this local setup.
Choose **local-kafka** → **Topics** to view the topics and their messages. The broker is available to the Python scripts at `localhost:9092`.

## Run

Use four terminal windows. The producer can be started more than once; every process receives a new sensor ID.

```bash
# terminal 1: create topics once
python create_topics.py

# terminal 2: process readings and create alerts
python alert_processor.py

# terminal 3: show alerts from both alert topics
python alert_consumer.py

# terminals 4 and 5: two independent sensors
python sensor_producer.py
python sensor_producer.py
```

Sensor messages include `sensor_id`, UTC timestamp, temperature (25–45 °C), and humidity (15–85%).
The processor sends a temperature alert when temperature is over 40 °C and a humidity alert when humidity is over 80% or below 20%.

## Evidence for submission

Before making screenshots, open Kafka UI at `http://localhost:8080` and leave it open in a browser tab. In the **Topics** screen, refresh the page after the scripts send messages.

Take and add the required screenshots after connecting to Kafka:

1. **Topics created.** Run `python create_topics.py`, then in Kafka UI open **Topics**. Screenshot the three topics: `building_sensors_univers`, `temperature_alerts_univers`, and `humidity_alerts_univers`.
2. **Two sensors.** Screenshot two terminal windows running `python sensor_producer.py`; both must show `Sent` records and different `sensor_id` values.
3. **Filtering.** Screenshot `alert_processor.py` after it displays a `Received: ...` reading together with an alert condition (temperature over 40 °C, humidity over 80%, or humidity below 20%).
4. **Alerts sent.** In the same processor terminal, screenshot the corresponding `-> temperature alert sent` and/or `-> humidity alert sent` line. Optionally show Kafka UI's **Messages** tab for that alert topic beside the terminal.
5. **Final result.** Screenshot `alert_consumer.py` showing messages from `[temperature_alerts_univers]` and/or `[humidity_alerts_univers]`. Kafka UI can additionally show the same records: **Topics** → select topic → **Messages** → search.

For a fresh demonstration, stop containers and erase prior topic data before restarting:

```bash
docker compose down -v
docker compose up -d
```
