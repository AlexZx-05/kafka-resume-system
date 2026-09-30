from confluent_kafka import Consumer
import json
import time

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "screening-completed"
CONSUMER_GROUP = "notification-group"


consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    }
)


consumer.subscribe([INPUT_TOPIC])

print("Notification Consumer started...")


try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Convert Kafka message to Python dictionary
        event = json.loads(msg.value().decode("utf-8"))
        
        time.sleep(10)
        
        print("\nReceived screening-completed event:")
        print(json.dumps(event, indent=2))

        # Extract screening information
        candidate_id = event["candidate_id"]
        resume_id = event["resume_id"]
        score = event["data"]["score"]
        recommendation = event["data"]["recommendation"]

        # Simulate sending a notification
        print("\n===================================")
        print("        CANDIDATE NOTIFICATION")
        print("===================================")
        print(f"Candidate: {candidate_id}")
        print(f"Resume ID: {resume_id}")
        print(f"Score: {score}")
        print(f"Recommendation: {recommendation}")
        print("===================================")

        # Commit offset after successful processing
        consumer.commit(msg)

        print(
            f"Committed offset {msg.offset()} "
            f"from partition {msg.partition()}"
        )


except KeyboardInterrupt:
    print("\nStopping Notification Consumer...")


finally:
    consumer.close()