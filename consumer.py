from confluent_kafka import Consumer, KafkaError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Kafka consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",  # Kafka broker addresses
        "group.id": "at-least-once-group",  # Consumer group ID
        "auto.offset.reset": "earliest",  # Start reading from the earliest offset
        "enable.auto.commit": False,  # Disable automatic offset committing
        "auto.commit.interval.ms": 5000,  # Commit offset every 5 seconds (default)
    }

    # Create a Kafka consumer instance
    consumer = Consumer(conf)

    # Subscribe to the topic
    topic = "atleast-once-semantic"
    consumer.subscribe([topic])

    logger.info("Consumer started, waiting for messages...")

    try:
        while True:
            msg = consumer.poll(
                timeout=1.0
            )  # Poll for new messages with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info(
                        f"Reached end of partition {msg.topic()} [{msg.partition()}]"
                    )
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                logger.info(
                    f"Consumed message: {msg.value().decode('utf-8')}, Partition: {msg.partition()}, Offset: {msg.offset()}"
                )
                # Manually commit the offset after processing the message
                consumer.commit(message=msg)

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer to release resources
        consumer.close()


if __name__ == "__main__":
    main()
