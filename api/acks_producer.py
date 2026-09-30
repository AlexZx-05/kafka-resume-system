from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "resume-uploaded-api"

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
     "retries": 3,
})


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered successfully | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()}"
        )


for i in range(1, 4):
    message = f"acks-test-message-{i}"

    producer.produce(
        TOPIC,
        value=message,
        callback=delivery_report,
    )

    print(f"Sent: {message}")


producer.flush()

print("Producer finished.")