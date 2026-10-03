# Kafka Learning Project

## Overview
Hands-on learning project for a **beginner** preparing for a "DevOps, Kafka & Kubernetes Developer" role
(Data Platform Engineering, Deloitte client via Quess, Hyderabad; 5+ yrs expected; 2 interview rounds, one with the client).

Goal: learn by building, with **deep conceptual understanding** that holds up in interviews, not just commands that run.

### Job requirements to cover
- Kafka engineering + architecture knowledge
- Configuring Kafka components (brokers, topics, producers/consumers, Schema Registry, Connect)
- Confluent Control Center
- Terraform (hands-on), plus shell scripts that wrap Terraform
- Azure and GCP cloud
- Kubernetes; deploying Kafka infrastructure
- Client-facing communication (explaining designs clearly)

## Project theme
An **e-commerce order pipeline** (orders → payments / inventory / analytics) that grows in each module.

## Roadmap (one folder per module)
| # | Folder | Topic | Status |
|---|---|---|---|
| 1 | `01-kafka-basics` | Single-node KRaft broker, CLI tools, topics/partitions/offsets/consumer groups | In progress |
| 2 | `02-producers-consumers` | Python clients: keys, acks, idempotence, batching, commits, rebalancing, delivery semantics | Todo |
| 3 | `03-multi-broker` | 3-broker cluster: replication, ISR, min.insync.replicas, leader failover, failure drills | Todo |
| 4 | `04-confluent-platform` | Schema Registry (Avro), Kafka Connect, **Control Center** monitoring & lag | Todo |
| 5 | `05-config-security` | Broker/topic tuning, retention/compaction, SASL/SSL, ACLs | Todo |
| 6 | `06-kubernetes` | kind cluster, Helm, Confluent for Kubernetes (CFK) operator, Kafka on K8s | Todo |
| 7 | `07-terraform` | Terraform basics → Kafka topics as code → AKS (Azure) / GKE (GCP) provisioning | Todo |
| 8 | `08-automation` | Shell scripts wrapping Terraform, health checks, CI/CD-style deploys | Todo |
| 9 | `09-interview-prep` | Architecture design questions, troubleshooting scenarios, resume talking points | Todo |

Each module has `CONCEPTS.md` (theory and interview questions), `EXERCISES.md` (hands-on steps), and code/config.

## Environment (WSL2 Linux, 24 cores, 15 GB RAM)
- Installed: Docker 29.8 + Compose v5.5, Python 3.14 (venv ok, no global pip), Terraform 1.16, Azure CLI 2.90
- Not installed yet: Java, kubectl, kind, helm, gcloud (install when the module needs them)
- Kafka image: `confluentinc/cp-kafka:8.0.0` (Confluent Platform 8, KRaft only, no ZooKeeper)

## Conventions
- Explain the "why" behind every config. User is a beginner; teach concepts before commands.
- Run Kafka CLI via `docker exec kafka <tool>` (Confluent images use names without `.sh`, e.g. `kafka-topics`).
- Python deps go in a per-module venv (`python3 -m venv .venv`).
- Keep this file updated as progress is made (status column + Decisions & Notes).

## Decisions & Notes
- 2026-10-01: Project created. Chose Confluent images (JD mentions Control Center) and KRaft mode.
- 2026-10-01: Module 1 created (docker-compose, CONCEPTS.md, EXERCISES.md).
- 2026-10-01: EXERCISES.md expanded with a detailed explanation for every step (flags, internals, outputs, interview angles). Outputs checked against the running cp-kafka 8.0.0.

## Open Tasks
- User: work through `01-kafka-basics/EXERCISES.md` and answer the self-check questions.
- Resume tailoring for the JD: pending; the user needs to share their resume.
