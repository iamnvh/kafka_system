import json
import logging
import socket
import time
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionalMessage:
    def __init__(self, id, content):
        self.id = id
        self.content = content


def generate_transactional_id():
    hostname = socket.gethostname()
    timestamp = int(time.time() * 1000)  # Convert current time to milliseconds
    return f"{hostname}-{timestamp}"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def main():
    # Generate a unique transactional ID
    transactional_id = generate_transactional_id()

    # Kafka producer configuration with Exactly Once Semantics enabled
    conf = {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "acks": "all",
        "enable.idempotence": True,  # Enable idempotence for Exactly Once Semantics
        "transactional.id": transactional_id,  # Unique transactional ID for this producer
        "max.in.flight.requests.per.connection": 1,  # Ensure message order
    }

    # Initialize Kafka producer
    producer = Producer(**conf)

    try:
        # Initialize transactions
        producer.init_transactions()

        # Begin a new transaction
        producer.begin_transaction()

        # Create a sample message
        message = TransactionalMessage(
            id=transactional_id, content="Xin chào Kafka với ngữ nghĩa Exactly Once"
        )

        # Serialize the message to JSON
        message_bytes = json.dumps(message.__dict__).encode("utf-8")

        # Define the Kafka topic
        topic = "exactly-once-topic"

        # Produce the message
        producer.produce(topic, value=message_bytes, callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush()

        # Commit the transaction to ensure the message is sent exactly once
        producer.commit_transaction()

        logger.info("Message sent successfully with Exactly Once Semantics")

    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        # Abort the transaction in case of an error
        producer.abort_transaction()

    finally:
        # Close the producer to release resources
        producer.flush()


if __name__ == "__main__":
    for i in range(0, 1000):
        main()
