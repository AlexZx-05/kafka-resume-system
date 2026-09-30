from confluent_kafka import Consumer, Producer
import json
import time

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "screening-completed"
DLT_TOPIC = "resume-processing-dlt"

CONSUMER_GROUP = "retry-test-group"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})

consumer.subscribe([INPUT_TOPIC])

print("Retry Consumer started...")

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print("\nReceived message:")
        print(json.dumps(event, indent=2))

        # Simulate processing failure
        success = False

        for attempt in range(1, 4):

            print(f"Attempt {attempt}/3")

            if success:
                break

            print("Processing failed!")
            time.sleep(2)

        # After 3 failed attempts
        if not success:

            print("All 3 attempts failed.")
            print("Sending message to DLT...")

            dlt_event = {
                "original_topic": msg.topic(),
                "original_partition": msg.partition(),
                "original_offset": msg.offset(),
                "event_id": event["event_id"],
                "error": "Processing failed after 3 attempts",
                "event": event,
            }

            producer.produce(
                DLT_TOPIC,
                key=event["candidate_id"],
                value=json.dumps(dlt_event),
            )

            producer.flush()

            print("Message published to DLT!")

            consumer.commit(msg)

            print(
                f"Committed original offset {msg.offset()}"
            )

except KeyboardInterrupt:
    print("\nStopping retry consumer...")

finally:
    consumer.close()
    producer.flush()