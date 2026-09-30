from confluent_kafka import Consumer
import json
import time

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "screening-completed"
CONSUMER_GROUP = "slow-notification-group"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([INPUT_TOPIC])

print("Slow Notification Consumer started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print("\nReceived message")
        print(f"Partition: {msg.partition()}")
        print(f"Offset: {msg.offset()}")
        print(f"Candidate: {event['candidate_id']}")

        print("Processing... sleeping for 10 seconds")
        time.sleep(10)

        print("Processing completed")

        consumer.commit(msg)

        print(
            f"Committed offset {msg.offset()} "
            f"from partition {msg.partition()}"
        )

except KeyboardInterrupt:
    print("\nStopping slow consumer...")

finally:
    consumer.close()