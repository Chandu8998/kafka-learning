# Module 1 — Kafka Core Concepts

## 1. What problem does Kafka solve?
Without Kafka, every system talks directly to every other system (orders → payments, orders → inventory,
orders → analytics...). N systems = up to N×N fragile point-to-point integrations.

Kafka is a **distributed, durable, append-only commit log**. Producers write events once; any number of
consumers read them independently, at their own pace, and can even re-read the past.

> Interview line: "Kafka decouples producers from consumers in **time** (consumer can be offline),
> **space** (they don't know each other), and **rate** (each consumes at its own speed)."

## 2. Core vocabulary

| Term | Meaning |
|---|---|
| **Event / Record / Message** | A key, value, timestamp, and optional headers. Bytes to Kafka. |
| **Topic** | Named stream of events, e.g. `orders`. Like a table name, but append-only. |
| **Partition** | A topic is split into partitions. Each partition is an ordered, immutable log file on disk. **Unit of parallelism and ordering.** |
| **Offset** | Position of a record within a partition (0, 1, 2…). Never reused. |
| **Broker** | A Kafka server that stores partitions and serves clients. |
| **Cluster** | A group of brokers. |
| **Controller** | Manages cluster metadata: which broker leads which partition, topic configs, broker liveness. |
| **Producer** | Client that writes records. |
| **Consumer** | Client that reads records. |
| **Consumer group** | Consumers sharing a `group.id` split partitions among themselves. |

## 3. Partitions — the most important concept
```
Topic "orders" (3 partitions)
 P0: [0][1][2][3][4]      → new writes appended at the end
 P1: [0][1][2]
 P2: [0][1][2][3]
```
- **Ordering is guaranteed only within a partition**, never across the whole topic.
- **Which partition?** If the record has a key → `hash(key) % numPartitions`. Same key → same partition →
  ordered. No key → producer spreads records (sticky partitioner: fills a batch per partition, then switches).
- **Why partition?** Scale. Partitions spread across brokers (storage + throughput), and a consumer group
  can have at most one active consumer per partition.
- **Gotcha:** Adding partitions later changes `hash % N` → keys move to different partitions → ordering
  per key breaks for a while. Pick partition count carefully up front.

## 4. Storage internals (architect-level)
- Each partition = a directory: `orders-0/`. Inside are **segment** files:
  `00000000000000000000.log` (data), `.index` (offset → byte position), `.timeindex` (time → offset).
- Only the newest segment (the *active segment*) is written to. Old segments are closed.
- Kafka is fast because: sequential disk writes, the OS page cache, **zero-copy** (`sendfile`) from disk
  to network, batching, and compression.
- **Retention** deletes whole closed segments:
  - `retention.ms` (default 7 days) / `retention.bytes`
  - `cleanup.policy=delete` (by age/size) or `compact` (keep latest value per key — used for "current state" topics).
- Consuming does **not** delete data. That's a big difference from traditional queues (RabbitMQ, MQ).

## 5. Replication & durability (preview — deep dive in Module 3)
- `replication.factor=3` → each partition has 1 **leader** + 2 **followers** on different brokers.
- Clients read from and write to the **leader**. Followers copy it.
- **ISR (In-Sync Replicas)**: replicas that are caught up with the leader.
- Producer `acks`:
  - `acks=0` fire-and-forget, `acks=1` leader wrote it, `acks=all` all ISR have it (safest; the default since Kafka 3.0).
- `min.insync.replicas=2` + `acks=all` + RF=3 = the standard production durability setup:
  you can lose 1 broker with no data loss and keep writing.

## 6. KRaft vs ZooKeeper
- Old Kafka stored metadata in a separate **ZooKeeper** cluster.
- **KRaft** (Kafka Raft) moves metadata into Kafka itself: a quorum of *controller* nodes uses the Raft
  consensus protocol and stores metadata in an internal topic `__cluster_metadata`.
- Kafka 4.0+ / Confluent Platform 8.0+ **removed ZooKeeper entirely**. You *will* be asked about this.
- Benefits: one system to run, faster controller failover, supports millions of partitions.
- Production: 3 (or 5) dedicated controller nodes + N broker nodes. Our lab: 1 node doing both roles.

## 7. Listeners (the #1 cause of "can't connect" problems)
- `listeners` = the interfaces/ports the broker **binds** to.
- `advertised.listeners` = the address the broker **tells clients** to use after the first connection.
- How a client connects: it calls a *bootstrap server* → gets metadata (the list of brokers + their
  advertised addresses) → connects **directly to the leader broker** of each partition.
  If the advertised address isn't reachable from the client, the connection fails even though bootstrap worked.
- That's why our compose file has two: `kafka:29092` for containers on the Docker network,
  `localhost:9092` for your laptop. Same idea applies in Kubernetes (internal vs. external listeners).

## 8. Consumer groups & offsets (preview — deep dive in Module 2)
```
Topic with 3 partitions, group "billing" with 2 consumers:
  C1 ← P0, P1
  C2 ← P2
Add a 3rd consumer → rebalance → one partition each.
Add a 4th consumer → it sits idle (more consumers than partitions).
```
- Each group tracks its own progress by committing offsets to the internal topic `__consumer_offsets`.
- Two different groups each get **all** messages (pub/sub). Consumers in the same group **share** them (queue).
- **Consumer lag** = latest offset − committed offset. This is the #1 metric you'll watch in Control Center.

## Interview questions to be able to answer after this module
1. How does Kafka guarantee ordering? (Per partition only. Use a key for per-entity ordering.)
2. What happens if you have more consumers than partitions? (The extra ones are idle.)
3. Difference between Kafka and a traditional message queue? (Log retention, replay, many independent groups, pull-based.)
4. What is KRaft and why was ZooKeeper removed?
5. `listeners` vs `advertised.listeners`?
6. How does Kafka get high throughput? (Sequential I/O, page cache, zero-copy, batching, compression, partitioning.)
7. What is log compaction and when would you use it?
