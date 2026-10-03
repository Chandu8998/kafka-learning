# Module 1 — Hands-on Exercises (with explanations)

Project theme for the whole course: an **e-commerce order pipeline** (orders → payments, inventory, analytics).

Run everything from `~/kafka/01-kafka-basics`.

### Read this first: how every command here works
```
docker exec  [-it]  kafka   kafka-topics  --bootstrap-server localhost:9092  ...
└── run a command inside   └── container  └── Kafka CLI tool └── "which Kafka do I talk to?"
    a running container        name
```
- `docker exec` runs a program *inside* the `kafka` container. The Kafka CLI tools are already installed there, so you don't need Java on your laptop.
- `-it` = interactive + terminal. Use it when **you** will type input or press Ctrl+C (producer/consumer). Leave it out for one-shot commands.
- `--bootstrap-server localhost:9092` = the first broker the tool contacts. From that broker it learns about the whole cluster (see `CONCEPTS.md` §7). Inside the container, `localhost:9092` is the broker itself.
- The Kafka CLI tools are just regular Kafka clients (producers, consumers, admin clients). Your Python/Java apps later will do exactly the same things through code.

---

## Ex 1 — Start the cluster
```bash
docker compose up -d
docker compose logs -f kafka     # wait for "Kafka Server started", then Ctrl+C
docker compose ps
```
**What each command does**
- `docker compose up -d`: reads `docker-compose.yml` and starts the `kafka` service. `-d` (detached) runs it in the background so your terminal stays free.
- `docker compose logs -f kafka`: shows the broker's log; `-f` keeps following new lines. Ctrl+C stops *watching* the logs, not Kafka.
- `docker compose ps`: lists the containers and whether they're running.

**What happens inside on startup**
1. The node reads `KAFKA_PROCESS_ROLES=broker,controller`, so it acts as both.
2. The **controller** part starts a Raft quorum with one voter (`1@kafka:29093`) and elects itself leader. Metadata goes into the internal `__cluster_metadata` log.
3. The **broker** part registers with the controller, opens its listeners (9092 for you, 29092 for other containers), and is ready.

**Why it matters:** in production you'd read these same logs to debug a broker that won't start. Usual causes: wrong listener config, a mismatched `CLUSTER_ID`, or a full disk.

---

## Ex 2 — Create a topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```
**What each part means**
- `--create --topic orders`: makes a topic called `orders`. (Auto-creation is turned off in our compose file, so a typo in a topic name gives an error instead of silently creating a new topic. Production clusters usually do the same.)
- `--partitions 3`: split the topic into 3 logs (P0, P1, P2). This sets the **maximum parallelism**: at most 3 consumers in one group can read at the same time.
- `--replication-factor 1`: keep 1 copy of each partition. We only have 1 broker, so we can't have more. (Try `--replication-factor 3` on another topic: it fails with an error because there aren't enough brokers.)
- `--list`: lists the topic names.
- `--describe`: shows the details of each partition.

**Reading the `--describe` output**
```
Topic: orders  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```
| Field | Meaning |
|---|---|
| `Leader: 1` | Broker with node ID 1 handles all reads/writes for this partition |
| `Replicas: 1` | List of brokers that hold a copy (only broker 1) |
| `Isr: 1` | In-sync replicas: copies that are fully caught up |
| `Elr` / `LastKnownElr` | Newer KRaft feature (Eligible Leader Replicas), used for safe leader election. Ignore for now. |

**Answer to "why only one replica?"** RF=1 and there's 1 broker. If this broker dies, the data is unavailable (and lost if the disk is lost). In Module 3, with 3 brokers, you'll see `Replicas: 1,2,3`.

**Interview angle:** "How do you choose a partition count?" Base it on the throughput you need ÷ throughput per partition, and on how many consumers you want running in parallel. Leave headroom, because adding partitions later re-maps keys to different partitions.

---

## Ex 3 — Produce without keys
```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic orders
> order-1 created
> order-2 created
> order-3 created
(Ctrl+C)
```
**What's happening**
- `kafka-console-producer` turns each line you type into one **record**. The line becomes the record's *value*, and the *key* is `null`.
- The `>` prompt means it's waiting for input. Ctrl+C sends anything still buffered and exits.
- **No key → the producer picks the partition.** Modern Kafka uses the *sticky partitioner*: it fills a batch for one partition, then moves on to another. Typing slowly by hand, your messages may all end up in one partition, or be spread out. Both are normal.
- Behind the scenes the producer: asks the broker for metadata → finds the leader of the chosen partition → sends a batch → waits for an acknowledgement (`acks=all` by default) → the broker appends the record and assigns it the next **offset**.

---

## Ex 4 — Consume
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning \
  --property print.partition=true --property print.offset=true
```
**What each flag does**
- `--from-beginning`: start at offset 0 of every partition. Without it, a new consumer starts at the **end** and only sees *new* messages (`auto.offset.reset=latest`).
- `--property print.partition=true` / `print.offset=true`: show where each record lives. By default only the value is printed.
- There's no `--group` here, so the tool makes up a random throwaway group (`console-consumer-12345`).
- It keeps running and waiting for new messages until you press Ctrl+C. Consumers **pull**: they keep asking the broker "anything new after offset X?"

**Example output**
```
Partition:1   Offset:0   order-1 created
Partition:1   Offset:1   order-2 created
Partition:0   Offset:0   order-3 created
```
**Answers to the questions**
- Each partition has its own offsets starting from 0. An offset only means something together with its partition (`P1/offset 1`).
- The output order may **differ** from the order you typed them in. The consumer reads several partitions and interleaves them. Kafka only guarantees order *within one partition*, not across the whole topic. This is the most important lesson of the module.

---

## Ex 5 — Produce with keys (per-customer ordering)
```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic orders \
  --property parse.key=true --property key.separator=:
> cust-42:order-10 created
> cust-7:order-11 created
> cust-42:order-10 paid
> cust-42:order-10 shipped
```
Then consume:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true
```
**What's happening**
- `parse.key=true` + `key.separator=:` tells the producer to split each line at `:`, so `cust-42` becomes the key and `order-10 created` the value.
- The partition is chosen by `murmur2(key) % 3`. The same key always gives the same hash, so it always goes to the same partition.
- So `created → paid → shipped` for `cust-42` are stored in order in **one** partition, and any consumer reads them in that order.

**Why it matters (real-world):** if "shipped" could be processed before "paid", downstream systems would break. **Choosing the key = choosing what needs to stay in order.** Typical keys: customer ID, order ID, account ID.

**Gotcha (interview favourite):** a "hot key" (one customer sending 90% of the traffic) overloads one partition while the others sit idle. That's called **skew**.

---

## Ex 6 — Consumer groups (open 3 terminals)
Terminals 1 and 2 (same command in both):
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --group billing --property print.partition=true
```
Terminal 3: produce ~10 keyed messages with different keys (the producer command from Ex 5).

**What's happening**
- `--group billing` puts both consumers in the same **consumer group**. Kafka gives each partition to **exactly one** member of the group.
- With 3 partitions and 2 consumers you'll get something like C1 → P0, P1 and C2 → P2. Each message is processed **once by the group**, which is how you scale out processing.
- A broker called the **group coordinator** manages the group. Consumers send it heartbeats. When a member joins or leaves, the coordinator triggers a **rebalance** and reassigns partitions.

**Stop one consumer (Ctrl+C) and produce more:** after a short pause the remaining consumer takes over all 3 partitions. That pause is the rebalance. If a consumer *crashes* instead of exiting cleanly, the group waits for `session.timeout.ms` (45 s by default) before reacting.

**Try these too**
- Start a **3rd** consumer: each gets one partition.
- Start a **4th**: it gets nothing and sits idle. More consumers than partitions doesn't help.
- Start a consumer with `--group analytics`: it receives **every** message as well. Different groups are completely independent (pub/sub).

> Mental model: inside a group, messages are **shared** (like a queue). Across groups, every group gets **all** messages (like pub/sub). Kafka does both with one design.

---

## Ex 7 — Inspect offsets & lag
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group billing
```
**Reading the output**
```
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
billing  orders  1          2               2               0    -            -     -
```
| Column | Meaning |
|---|---|
| `CURRENT-OFFSET` | The **committed** offset: the *next* message the group will read |
| `LOG-END-OFFSET` | The offset the *next new* message will get (= number of messages written) |
| `LAG` | `LOG-END-OFFSET − CURRENT-OFFSET` = messages written but not yet processed |
| `CONSUMER-ID` | Which group member owns the partition (`-` = nobody is connected right now) |

**Where are offsets stored?** In the internal topic `__consumer_offsets` (50 partitions by default; you'll see them on disk in Ex 9). The console consumer **auto-commits** every 5 seconds.

**Do the experiment:** stop all consumers → produce 5 messages → describe again → `LAG` goes up. Start a consumer → lag drops back to 0.

**Why it matters:** **consumer lag is the #1 Kafka health metric.** Growing lag means consumers can't keep up (too slow, crashed, or too few of them). In Module 4 you'll watch exactly this in Confluent Control Center, and alerting on lag is a standard part of the job.

---

## Ex 8 — Replay (something a normal queue can't do)
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group billing \
  --topic orders --reset-offsets --to-earliest --execute
```
Then start a `billing` consumer again: it re-reads everything.

**What's happening**
- `--reset-offsets --to-earliest` rewrites the group's committed offsets to the start of each partition.
- `--execute` actually applies the change. **Without it, the command only shows what *would* change (a dry run).** Always do the dry run first in production.
- ⚠️ This only works when the group has **no active members**. Stop all `billing` consumers first, or you'll get an error.
- Other options: `--to-latest` (skip everything), `--to-offset 5`, `--shift-by -10`, `--to-datetime 2026-10-01T00:00:00.000`.

**Why it matters:** in RabbitMQ and similar queues, a message is gone once it's consumed. In Kafka, consuming just moves a pointer; the data stays until retention deletes it. Real uses: re-processing after a bug fix, rebuilding a database from events, or backfilling a new analytics system.

---

## Ex 9 — Look at the log files on disk
```bash
docker exec kafka bash -c 'ls /var/lib/kafka/data/'
docker exec kafka bash -c 'ls -l /var/lib/kafka/data/orders-0/ /var/lib/kafka/data/orders-1/ /var/lib/kafka/data/orders-2/'
docker exec kafka kafka-dump-log --print-data-log \
  --files /var/lib/kafka/data/orders-1/00000000000000000000.log
```
**What you'll see**
- The data directory has one folder per partition: `orders-0`, `orders-1`, `orders-2`, plus internal ones: `__consumer_offsets-0…49` (committed offsets) and `__cluster_metadata-0` (KRaft metadata).
- Inside a partition folder:

| File | Purpose |
|---|---|
| `00000000000000000000.log` | The actual records. The file name = the first offset in this **segment**. |
| `.index` | Sparse map: offset → byte position in `.log` (fast seek to an offset) |
| `.timeindex` | Map: timestamp → offset (used by `--to-datetime` resets and time-based retention) |
| `leader-epoch-checkpoint` | Leadership history, used to safely truncate logs after a failover |
| `partition.metadata` | The topic ID |

- `.index` and `.timeindex` show as ~10 MB even when empty: Kafka **pre-allocates** them. The `.log` file grows as data arrives.
- A partition folder can have a **0-byte `.log`** if no key hashed to it. That's why the command lists all three; dump whichever one has data.
- `kafka-dump-log` decodes the binary file. You'll see **record batches** (`baseOffset`, `lastOffset`, `count`, `CreateTime`, `compresscodec`), and with `--print-data-log`, each record's `key` and `payload`.

**Why it matters:** it shows that a topic is really just append-only files on disk. When a segment reaches `segment.bytes` (1 GB by default) or `segment.ms`, Kafka closes it and starts a new file named after the next offset. Retention then deletes whole old segments, never single messages.

---

## Ex 10 — Topic configs
```bash
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders --add-config retention.ms=3600000
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name orders
```
**What's happening**
- `kafka-configs` changes settings **while Kafka is running, without a restart** (dynamic config).
- `--entity-type topics --entity-name orders`: what we're configuring. Other entity types: `brokers`, `users`, `clients`.
- `retention.ms=3600000` = 1 hour (3,600,000 ms). Closed segments whose newest record is older than this get deleted.
- `--describe` only lists **overrides**, not every default. To see everything, add `--all`.
- To undo: `--alter ... --delete-config retention.ms`.

**How config precedence works:** topic-level override → broker default (`log.retention.hours=168`, i.e. 7 days) → built-in default. Interviewers often ask: "the topic has retention.ms set but data isn't being deleted. Why?" Answer: retention only deletes **closed** segments. The active segment is never deleted, so with low traffic a segment can stay open long past retention until `segment.ms` rolls it.

**Other common topic configs to try**
| Config | What it does |
|---|---|
| `cleanup.policy=compact` | Keep only the latest record per key (a "current state" topic) |
| `min.insync.replicas` | Minimum copies that must have a write for `acks=all` to succeed (Module 3) |
| `max.message.bytes` | Largest record batch the topic accepts (~1 MB by default) |
| `segment.ms` / `segment.bytes` | When to roll to a new segment file |

---

## Clean up
```bash
docker compose down        # stops and removes the container
```
We haven't mounted a Docker volume, so **all topics and data are gone** after `down`. (`docker compose stop` would keep them.) In Module 3 we'll add volumes so data survives restarts.

## Self-check
Write your answers to the 7 interview questions at the bottom of `CONCEPTS.md` in your own words.
Explaining it out loud is the real test.
