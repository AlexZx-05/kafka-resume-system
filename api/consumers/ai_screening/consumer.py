from confluent_kafka import Consumer, Producer
import json

from consumers.resume_parser.database import get_connection


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "resume-parsed"
CONSUMER_GROUP = "ai-screening-group"
CONSUMER_NAME = "ai-screening"
OUTPUT_TOPIC = "screening-completed"


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


# Subscribe to resume-parsed topic
consumer.subscribe([INPUT_TOPIC])

print("AI Screening Consumer started...")


try:
    while True:

        # --------------------------------------------------
        # Poll Kafka for a message
        # --------------------------------------------------

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # --------------------------------------------------
        # Convert Kafka message into Python dictionary
        # --------------------------------------------------

        event = json.loads(msg.value().decode("utf-8"))

        # --------------------------------------------------
        # Check whether event was already processed
        # --------------------------------------------------

        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM processed_events
                WHERE event_id = %s
                  AND consumer_name = %s
                """,
                (
                    event["event_id"],
                    CONSUMER_NAME,
                ),
            )

            already_processed = cursor.fetchone() is not None

        conn.close()

        if already_processed:
            print("Event already processed. Skipping...")
            consumer.commit(msg)
            continue

        print("\nReceived resume-parsed event:")
        print(json.dumps(event, indent=2))

        print(f"Screening resume: {event['resume_id']}")
        print(f"Candidate: {event['candidate_id']}")

        # --------------------------------------------------
        # Step 1: Simulate AI screening
        # --------------------------------------------------

        score = 82
        recommendation = "interview"

        print(f"Screening score: {score}")
        print(f"Recommendation: {recommendation}")

        # --------------------------------------------------
        # Step 2: Save screening result to PostgreSQL
        # --------------------------------------------------

        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO screening_results (
                    resume_id,
                    candidate_id,
                    score,
                    recommendation
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    event["resume_id"],
                    event["candidate_id"],
                    score,
                    recommendation,
                ),
            )

        conn.commit()
        conn.close()

        print("Screening result saved to PostgreSQL!")

        # --------------------------------------------------
        # Step 3: Mark event as processed
        # --------------------------------------------------

        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processed_events (
                    event_id,
                    consumer_name
                )
                VALUES (%s, %s)
                """,
                (
                    event["event_id"],
                    CONSUMER_NAME,
                ),
            )

        conn.commit()
        conn.close()

        print("Event marked as processed!")

        # --------------------------------------------------
        # Step 4: Create screening-completed event
        # --------------------------------------------------

        screening_event = {
            "event_id": event["event_id"],
            "event_type": "screening-completed",
            "timestamp": event["timestamp"],
            "candidate_id": event["candidate_id"],
            "resume_id": event["resume_id"],
            "data": {
                "score": score,
                "recommendation": recommendation,
            },
        }

        # --------------------------------------------------
        # Step 5: Publish screening result to Kafka
        # --------------------------------------------------

        producer.produce(
            OUTPUT_TOPIC,
            key=event["candidate_id"],
            value=json.dumps(screening_event),
        )

        producer.flush()

        print("Published screening-completed event!")

        # --------------------------------------------------
        # Step 6: Commit Kafka offset
        # --------------------------------------------------

        consumer.commit(msg)

        print(
            f"Committed offset {msg.offset()} "
            f"from partition {msg.partition()}"
        )


except KeyboardInterrupt:
    print("\nStopping AI Screening Consumer...")


finally:
    consumer.close()
    producer.flush()