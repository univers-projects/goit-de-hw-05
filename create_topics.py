"""Create all Kafka topics required for the IoT monitoring pipeline."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import ALL_TOPICS, BOOTSTRAP_SERVERS, kafka_options


def main() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, **kafka_options())
    try:
        new_topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in ALL_TOPICS]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print("Topics created:")
        except TopicAlreadyExistsError:
            print("Topics already exist:")

        available = set(admin.list_topics())
        for topic in ALL_TOPICS:
            print(f"  {'OK' if topic in available else 'MISSING'}  {topic}")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
