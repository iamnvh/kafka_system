from confluent_kafka import Producer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def main(number):
    # Kafka producer configuration
    conf = {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",  # Kafka broker addresses
        "acks": "1",  # Wait for leader acknowledgment (default behavior)
        "retries": 3,  # Retry in case of error (can lead to duplicate messages)
    }

    # Create a Kafka producer instance
    producer = Producer(**conf)

    # Define the topic
    topic = "atleast-once-semantic"

    # Message you want to send
    message = f"hello-world-{number}"

    try:
        # Produce the message
        producer.produce(topic, value=message.encode("utf-8"), callback=delivery_report)

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        logger.info(f"Message produced: {message}")

    except Exception as e:
        logger.error(f"Failed to produce message: {e}")

    finally:
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush(15 * 1000)  # 15 seconds timeout


if __name__ == "__main__":
    for i in range(1000, 10000):
        main(i)
