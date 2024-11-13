import json
import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionalMessage:
    def __init__(self, id, content):
        self.id = id
        self.content = content


def process_message(message):
    """Simulate processing of the message"""
    logger.info(f"Processing message: ID={message.id}, Content={message.content}")


def main():
    # Kafka consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "group.id": "exactly-once-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Disable auto-commit of offsets
        "isolation.level": "read_committed",  # Only read messages from committed transactions
    }

    # Initialize Kafka consumer
    consumer = Consumer(conf)

    # Subscribe to the topic
    topic = "exactly-once-topic"
    consumer.subscribe([topic])

    logger.info("Consumer started, waiting for messages...")

    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0)  # Poll with a timeout of 1 second
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
                # Deserialize message
                try:
                    transaction_message = TransactionalMessage(
                        **json.loads(msg.value().decode("utf-8"))
                    )
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to deserialize message: {e}")
                    continue

                # Simulate processing the message
                process_message(transaction_message)

                # Commit the message's offset after successful processing
                try:
                    consumer.commit(message=msg)
                    logger.info(
                        f"Commit success: Offset={msg.offset()}, Partition={msg.partition()}"
                    )
                except KafkaError as e:
                    logger.error(f"Failed to commit message: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer to release resources
        consumer.close()


if __name__ == "__main__":
    main()
