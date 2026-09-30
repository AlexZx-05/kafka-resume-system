from confluent_kafka import Producer
import json


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "resume-uploaded-api"


producer = Producer({ #this is the producer connect with kafka broker
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"at offset {msg.offset()}"
        )


def publish_resume_event(event: dict):
    producer.produce(
        TOPIC_NAME,
        key=event["candidate_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )

    producer.flush()