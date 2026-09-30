
from confluent_kafka import Consumer, Producer
import json

from consumers.resume_parser.database import get_connection


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "resume-uploaded-api"
OUTPUT_TOPIC = "resume-parsed"
CONSUMER_GROUP = "resume-parser-api-group"


# Kafka consumer
consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    }
)


# Kafka producer
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
)


# Subscribe to the input topic
consumer.subscribe([INPUT_TOPIC])

print("Resume Parser Consumer started...")


try:
    while True:
        # Poll Kafka for a new message
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize Kafka message
        event = json.loads(msg.value().decode("utf-8"))

        print("\nReceived resume event:")
        print(json.dumps(event, indent=2))

        print(f"Processing resume: {event['resume_id']}")
        print(f"Candidate: {event['candidate_id']}")
        print(f"Filename: {event['data']['filename']}")

        # --------------------------------------------------
        # Step 1: Save resume information to PostgreSQL
        # --------------------------------------------------

        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO resumes (
                    resume_id,
                    candidate_id,
                    filename
                )
                VALUES (%s, %s, %s)
                """,
                (
                    event["resume_id"],
                    event["candidate_id"],
                    event["data"]["filename"],
                ),
            )

        conn.commit()
        conn.close()

        print("Resume saved to PostgreSQL!")

        # --------------------------------------------------
        # Step 2: Create resume-parsed event
        # --------------------------------------------------

        parsed_event = {
            "event_id": event["event_id"],
            "event_type": "resume-parsed",
            "timestamp": event["timestamp"],
            "candidate_id": event["candidate_id"],
            "resume_id": event["resume_id"],
            "data": {
                "filename": event["data"]["filename"],
                "status": "parsed",
            },
        }

        # --------------------------------------------------
        # Step 3: Publish resume-parsed event to Kafka
        # --------------------------------------------------

        producer.produce(
            OUTPUT_TOPIC,
            key=event["candidate_id"],
            value=json.dumps(parsed_event),
        )

        # Wait until the message is delivered
        producer.flush()

        print("Published resume-parsed event!")

        # --------------------------------------------------
        # Step 4: Commit Kafka offset
        # --------------------------------------------------

        # Commit only after database save and event
        # publishing have completed successfully.
        consumer.commit(msg)

        print(
            f"Committed offset {msg.offset()} "
            f"from partition {msg.partition()}"
        )


except KeyboardInterrupt:
    print("\nStopping consumer...")


finally:
    consumer.close()
    producer.flush()

