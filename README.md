
# Kafka Resume Processing System

An event-driven resume processing system built with **Apache Kafka, Python, and FastAPI**.

The project demonstrates how Kafka can be used to build a scalable asynchronous pipeline for processing resumes through multiple independent consumers such as resume parsing, AI screening, and notifications.

## 🚀 Project Overview

The system accepts resume-processing requests through a FastAPI service and publishes events to Kafka.

Different Kafka consumers independently process these events:

```text
                    ┌──────────────────┐
                    │   FastAPI API    │
                    │  Resume Upload   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    │     Topics       │
                    └────────┬─────────┘
                             │
             ┌───────────────┼────────────────┐
             │               │                │
             ▼               ▼                ▼
      ┌─────────────┐ ┌─────────────┐ ┌──────────────┐
      │   Resume    │ │ AI Screening│ │ Notification │
      │   Parser    │ │   Consumer  │ │   Consumer   │
      └─────────────┘ └─────────────┘ └──────────────┘
             │               │                │
             ▼               ▼                ▼
          Parsed          Candidate        Notification
          Resume          Evaluation          Event
```

## ✨ Features

* FastAPI-based REST API
* Apache Kafka event-driven architecture
* Kafka producers and consumers
* Multiple independent consumer services
* Resume processing pipeline
* Resume parser consumer
* AI screening consumer
* Notification consumer
* Retry consumer
* Slow consumer simulation
* Crash/failure consumer simulation
* Kafka acknowledgements (`acks`)
* Kafka topic and partition management
* Consumer groups
* Message retry and failure handling
* Docker-based Kafka environment

## 🛠️ Tech Stack

| Technology   | Purpose                         |
| ------------ | ------------------------------- |
| Python       | Application and Kafka consumers |
| FastAPI      | REST API                        |
| Apache Kafka | Event streaming and messaging   |
| Docker       | Containerized infrastructure    |
| Uvicorn      | FastAPI application server      |

## 📁 Project Structure

```text
kafka-resume-system/
│
├── api/
│   │
│   ├── app/
│   │   ├── main.py
│   │   ├── producer.py
│   │   └── schemas.py
│   │
│   ├── consumers/
│   │   │
│   │   ├── ai_screening/
│   │   │   └── consumer.py
│   │   │
│   │   ├── notification/
│   │   │   ├── consumer.py
│   │   │   ├── crash_consumer.py
│   │   │   ├── retry_consumer.py
│   │   │   └── slow_consumer.py
│   │   │
│   │   └── resume_parser/
│   │       ├── consumer.py
│   │       └── database.py
│   │
│   ├── acks_producer.py
│   └── requirements.txt
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

## ⚙️ Architecture

The project follows an asynchronous event-driven architecture.

### 1. Producer

The FastAPI application acts as the entry point.

When a resume-processing request is received, the producer publishes an event to Kafka instead of directly calling every processing service.

```text
Client
   │
   ▼
FastAPI
   │
   ▼
Kafka Producer
   │
   ▼
Kafka Topic
```

This allows downstream services to process the event independently.

### 2. Kafka

Kafka acts as the central event streaming platform.

It provides:

* Topics
* Partitions
* Consumer groups
* Message offsets
* Replication
* Acknowledgements
* Fault tolerance

### 3. Resume Parser

The resume parser consumer receives resume events and performs resume-processing logic.

```text
Kafka
  │
  ▼
Resume Parser Consumer
  │
  ▼
Extract Resume Data
```

### 4. AI Screening

The AI screening consumer represents the next stage of the pipeline.

It can be extended to perform:

* Resume analysis
* Candidate scoring
* Skill extraction
* Job-description matching
* Candidate ranking

```text
Kafka
  │
  ▼
AI Screening Consumer
  │
  ▼
Candidate Evaluation
```

### 5. Notification Service

The notification consumer handles notification events.

It can be extended to send:

* Candidate notifications
* Recruiter notifications
* Interview notifications
* Processing-status updates

## 🔄 Message Flow

A typical event flow looks like:

```text
Resume Upload
     │
     ▼
FastAPI
     │
     ▼
Kafka Producer
     │
     ▼
resume-uploaded topic
     │
     ├──────────────► Resume Parser
     │
     ├──────────────► AI Screening
     │
     └──────────────► Notification
```

Because Kafka decouples producers from consumers, individual services can process messages independently.

## 🐳 Running Kafka with Docker

Start the Kafka environment:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

View Kafka logs:

```bash
docker compose logs -f kafka
```

Stop the environment:

```bash
docker compose down
```

## 📌 Kafka Topics

Example topic used in the project:

```text
resume-uploaded
```

Create a topic:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic resume-uploaded \
  --partitions 3 \
  --replication-factor 1
```

List topics:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe a topic:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic resume-uploaded
```

## 📤 Testing with Kafka Producer

You can test Kafka directly using the console producer:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic resume-uploaded
```

Then enter messages:

```text
hello kafka
resume uploaded
candidate processing started
```

## 📥 Testing with Kafka Consumer

Read messages from the beginning:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic resume-uploaded \
  --from-beginning
```

This allows you to observe messages produced to the topic.

## 🔁 Consumer Groups

Kafka consumer groups allow multiple consumers to share the processing of partitions.

For example:

```text
Topic: resume-uploaded

Partition 0 ──► Consumer 1
Partition 1 ──► Consumer 2
Partition 2 ──► Consumer 3
```

If one consumer fails, Kafka can rebalance partitions among the remaining consumers.

## 🧩 Kafka Concepts Demonstrated

This project is also a practical implementation of important Kafka concepts:

### Topics

Logical streams of events.

### Partitions

Topics are divided into partitions to provide scalability and parallel processing.

### Offsets

Each message has an offset within its partition.

### Consumer Groups

Multiple consumers can work together to process messages.

### Acknowledgements

The producer configuration demonstrates Kafka acknowledgement behavior using `acks`.

### Replication

The project also explores Kafka replication, replicas, leaders, and ISR (In-Sync Replicas).

### Retry Handling

The retry consumer demonstrates how failed processing can be handled without immediately losing the event.

### Failure Handling

The crash consumer is used to understand what happens when a consumer fails during message processing.

### Slow Consumers

The slow consumer demonstrates how processing delays can affect consumer behavior and Kafka consumer groups.

## 🔐 Producer Acknowledgements

Kafka producers can use different acknowledgement levels.

For example:

```text
acks=0
acks=1
acks=all
```

`acks=all` provides the strongest acknowledgement guarantee because the producer waits for the required replicas to acknowledge the record.

The project includes `acks_producer.py` for experimenting with producer acknowledgement behavior.

## 🔄 Retry and Failure Flow

A simplified failure flow:

```text
             Kafka
               │
               ▼
        Consumer Processing
               │
          ┌────┴────┐
          │         │
       Success    Failure
          │         │
          ▼         ▼
       Commit     Retry
                    │
                    ▼
             Re-process Event
```

This helps demonstrate real-world event-processing reliability patterns.

## 🎯 Learning Objectives

The main goal of this project is to gain practical experience with Apache Kafka and event-driven systems.

Through this project, the following concepts are explored:

* Kafka architecture
* Producers
* Consumers
* Topics
* Partitions
* Consumer groups
* Offsets
* Message acknowledgements
* Replication
* ISR
* Consumer rebalancing
* Retry handling
* Consumer failures
* Slow consumers
* Docker-based Kafka setup
* Event-driven architecture
* Asynchronous processing

## 🚧 Future Improvements

Possible extensions include:

* Add PostgreSQL for persistent resume data
* Add Redis for caching
* Integrate an actual resume parser
* Add an LLM-based resume screening service
* Add Dead Letter Topics
* Add Kafka Schema Registry
* Add authentication and authorization
* Add monitoring with Prometheus and Grafana
* Add Kubernetes deployment
* Add GitHub Actions CI/CD
* Add Kafka Connect
* Add multiple Kafka brokers for a production-like cluster
* Add automated integration tests

## 📚 What This Project Demonstrates

This project demonstrates how a traditional synchronous application can be converted into an **event-driven architecture** using Kafka.

Instead of:

```text
API
 │
 ├── Parser
 ├── AI Screening
 └── Notification
```

the system uses:

```text
API
 │
 ▼
Kafka
 │
 ├── Parser
 ├── AI Screening
 └── Notification
```

This provides better service decoupling and allows different consumers to process events independently.

## 👨‍💻 Author

**AlexZx-05**

GitHub:
https://github.com/AlexZx-05

---

⭐ If you find this project useful for learning Kafka and event-driven architecture, feel free to explore the repository.
