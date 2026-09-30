from confluent_kafka import Consumer
import json
import os

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "screening-completed"
CONSUMER_GROUP = "crash-test-group"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([INPUT_TOPIC])

print("Crash Test Consumer started...")

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

        if event["candidate_id"] != "crash-test-001":
            consumer.commit(msg)
            print("Not our test message. Committed and moving on...")
            continue

        print("Processing message...")

        print("💥 Simulating consumer crash BEFORE commit!")

        os._exit(1)

except KeyboardInterrupt:
    print("\nStopping crash test consumer...")

finally:
    consumer.close()
    