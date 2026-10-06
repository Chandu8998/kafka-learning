# Interview Crash Prep: Kubernetes, Networking, Docker (scenario-based)

> **You have about 90 minutes. Read in this order:**
> 1. Part 1, your project story (15 min). Learn the 60-second pitch so you can say it without notes.
> 2. Part 5, the scenarios (35 min). This is what the interview will be.
> 3. Parts 2–4, concept refreshers (25 min). Skim, and stop on anything you can't explain.
> 4. Part 6, the commands cheat sheet, plus Part 7, the end-to-end walkthrough (15 min).
>
> **How to answer every scenario question (use this every time):**
> **Symptom → What I check first (and why) → Exact commands → Root cause → Fix → How I prevent it next time.**
> Interviewers score the *method* more than the final answer. Think out loud.

> **A note on honesty.** Present this as *your project*: a Kafka platform you built hands-on, modeled on production
> patterns (multi-zone, operator-managed, Terraform-provisioned). If they ask "was this in production?", say:
> "I built and ran it end-to-end in my own environment with production-grade patterns. Here's what I'd watch for at real scale..."
> A confident, accurate answer beats an inflated one that falls apart on the second follow-up question.

---

## Part 1: Your project, structured like a real platform

### 1.1 The project name and what it does

**"ShopStream": an event-driven order processing platform on Kafka, running on Kubernetes (AKS, with GCP/GKE as the second cloud).**

When a customer places an order, the order service doesn't call payment, inventory, and analytics directly.
It publishes **one event** to Kafka. Each downstream service consumes it independently.
**Why:** services are decoupled. If payments is down for 10 minutes, orders still get accepted, and payments
catches up from its committed offset when it comes back. No lost orders, no cascading failure.

### 1.2 Architecture (draw this on the whiteboard/screen if asked)

```
                         Internet
                            │
                   ┌────────▼────────┐
                   │  Azure LB / App  │  (L7 Ingress for HTTP APIs only)
                   │  Gateway Ingress │
                   └────────┬────────┘
                            │ HTTPS
 ┌──────────────────────────┼───────────────────────── AKS cluster (3 availability zones) ─┐
 │  namespace: shop-apps    │                                                              │
 │   ┌──────────────┐  ┌────▼─────────┐  ┌───────────────┐  ┌──────────────────┐         │
 │   │ payment-svc  │  │ order-svc    │  │ inventory-svc │  │ analytics (Kafka │         │
 │   │ (consumer)   │  │ (producer)   │  │ (consumer)    │  │ Connect sink)    │         │
 │   └──────▲───────┘  └────┬─────────┘  └──────▲────────┘  └────────▲─────────┘         │
 │          │               │ TCP 9092 (SASL_SSL)│                    │                   │
 │  ────────┼───────────────┼───────────────────┼────────────────────┼────── NetworkPolicy │
 │  namespace: kafka        ▼                                                             │
 │   ┌──────────── Confluent for Kubernetes (CFK) operator ────────────┐                  │
 │   │ KRaft controllers: kraft-0, kraft-1, kraft-2   (StatefulSet)     │                  │
 │   │ Brokers:           kafka-0,  kafka-1,  kafka-2  (StatefulSet)    │                  │
 │   │   each broker = 1 pod + its own PVC (Azure Premium SSD)          │                  │
 │   │   spread across zones 1/2/3, broker.rack = zone                  │                  │
 │   │ Schema Registry (2 pods)   Kafka Connect (2 pods)                │                  │
 │   │ Control Center (monitoring, consumer lag, topic health)          │                  │
 │   └──────────────────────────────────────────────────────────────────┘                  │
 └─────────────────────────────────────────────────────────────────────────────────────────┘
        │ Private endpoint                         │ Prometheus metrics → Grafana / alerts
        ▼                                          ▼
   Azure Data Lake / DB (analytics sink)     Alerts: under-replicated partitions, consumer lag, disk %
```

### 1.3 Topics (know these numbers; they are defensible and you can explain each one)

| Topic | Partitions | RF | min.insync.replicas | Key | Why |
|---|---|---|---|---|---|
| `orders.created.v1` | 12 | 3 | 2 | `order_id` | Same key → same partition → events for one order stay **in order** |
| `payments.completed.v1` | 12 | 3 | 2 | `order_id` | Joins with orders on the same key |
| `inventory.reserved.v1` | 6 | 3 | 2 | `sku` | Stock updates per product must be ordered |
| `orders.dlq.v1` | 3 | 3 | 2 | `order_id` | Dead-letter queue: poison messages go here instead of blocking the consumer |

- **Producers:** `acks=all`, `enable.idempotence=true` → no data loss, no duplicates from retries.
- **RF=3, min.insync.replicas=2:** we can lose **one** broker (or one zone) and still accept writes.
- **Why 12 partitions:** partitions = max consumer parallelism. Peak (sale days) needed ~8 consumer pods; 12 leaves headroom. You can increase partitions later but not decrease, and increasing changes key→partition mapping.
- **Scale to quote:** "a few thousand events/sec at peak, with sale-day spikes". Keep it modest and believable.

### 1.4 Repository layout (how a real team organizes this)

```
shopstream-platform/
├── services/                     # application code (Python)
│   ├── order-service/            #   Dockerfile, app code, unit tests
│   ├── payment-service/
│   └── inventory-service/
├── docker/
│   └── docker-compose.yml        # local dev: full stack on a laptop (Kafka KRaft + services)
├── k8s/
│   ├── cfk/                      # Confluent operator custom resources:
│   │   ├── kraftcontroller.yaml  #   KRaftController, Kafka, SchemaRegistry, Connect, ControlCenter
│   │   ├── kafka.yaml
│   │   └── topics.yaml           #   KafkaTopic CRs (topics as code)
│   ├── apps/                     # Helm chart for the microservices
│   │   └── values-{dev,stage,prod}.yaml
│   └── policies/                 # NetworkPolicies, PodDisruptionBudgets, ResourceQuotas
├── terraform/
│   ├── modules/                  # network (VNet, subnets, NSG), aks, gke, acr, keyvault
│   └── envs/{dev,stage,prod}/    # each env = same modules, different variables + own remote state
├── scripts/
│   ├── tf-apply.sh               # wraps terraform init/plan/apply with env checks
│   ├── deploy.sh                 # helm upgrade --install + rollout status
│   └── kafka-healthcheck.sh      # under-replicated partitions, consumer lag, broker count
└── .github/workflows/ (or Azure DevOps pipelines)
    ├── ci.yml                    # build → test → docker build → scan → push to ACR
    └── cd.yml                    # terraform plan (PR) → apply (approval) → helm deploy
```

**Delivery flow:** dev pushes code → CI builds the image, tags it with the **git SHA** (never `latest` in prod),
scans it, pushes to ACR → CD runs `helm upgrade` → `kubectl rollout status` gates the pipeline → health-check script
verifies Kafka is healthy → done. Infra changes go through `terraform plan` on the PR and `apply` after approval.

### 1.5 Your 60-second pitch (memorize this)

> "My project is ShopStream, an event-driven order pipeline. The order service publishes an `orders.created` event to Kafka,
> and payment, inventory, and analytics consume it independently, so a failure in one service doesn't stop orders.
> Kafka runs on Kubernetes in KRaft mode (no ZooKeeper), managed by the Confluent for Kubernetes operator:
> 3 controllers, 3 brokers as StatefulSets, each broker with its own persistent volume, spread across 3 availability zones.
> Topics use replication factor 3 with min in-sync replicas 2, and producers use acks=all with idempotence, so we tolerate
> losing a whole zone without losing data. Schema Registry enforces Avro contracts, and Control Center plus Prometheus watch
> consumer lag and under-replicated partitions. Infrastructure (VNet, AKS, container registry) is Terraform, wrapped in shell scripts
> for CI/CD. Locally the whole stack runs in Docker Compose. The hardest parts were networking: getting advertised listeners
> right so clients inside and outside the cluster can reach each broker, and locking traffic down with NetworkPolicies."

That last sentence is a hook: it steers them toward questions you've prepared for (Scenarios 1 and 6).

---

## Part 2: Docker (concepts you must be able to explain)

| Concept | One-line answer they want |
|---|---|
| Image vs container | Image = read-only layered template. Container = a running instance with a thin writable layer on top. |
| Layers & cache | Each Dockerfile instruction = a layer. Put rarely-changing steps (install deps) **before** frequently-changing ones (copy code) so the cache is reused. |
| Multi-stage build | Build in a fat image (compilers), copy only the output into a slim runtime image → smaller, fewer CVEs. |
| CMD vs ENTRYPOINT | ENTRYPOINT = the executable; CMD = default args (overridable). Use **exec form** `["python","app.py"]`. |
| Why exec form matters | Shell form runs under `/bin/sh -c`, so **sh is PID 1** and doesn't forward SIGTERM → app never shuts down gracefully → killed after the grace period (exit 137). |
| Volume vs bind mount | Volume = Docker-managed, portable, for data (Kafka logs). Bind mount = host path, for dev code. Container filesystem is ephemeral. |
| Networks | Default `bridge`: no DNS by name. **User-defined bridge** (Compose creates one): embedded DNS at `127.0.0.11`, containers reach each other by service name. `host`: shares host network stack. |
| `-p 9092:9092` | Publishes host port → container port via NAT (iptables). Without it, only containers on the same network can connect. |
| `depends_on` | Only waits for **start**, not **ready**. Use `condition: service_healthy` + a healthcheck. |
| Exit codes | `0` ok, `1` app error, `126` not executable, `127` command not found, `137` = SIGKILL (often **OOM**), `143` = SIGTERM (graceful stop). |
| Image security | Non-root `USER`, slim/distroless base, pin versions, scan (Trivy), no secrets in layers (they stay in history even if deleted later). |

**A good Dockerfile for your Python service (be ready to write this):**
```dockerfile
FROM python:3.12-slim AS build
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt   # cached unless requirements change

FROM python:3.12-slim
WORKDIR /app
COPY --from=build /install /usr/local
COPY src/ ./src/
RUN useradd -r -u 10001 appuser
USER 10001                                   # never run as root
ENTRYPOINT ["python", "-m", "src.main"]      # exec form → app is PID 1, receives SIGTERM
```

---

## Part 3: Networking (the area most candidates are weak in)

### 3.1 Fundamentals
- **TCP vs UDP:** Kafka uses **TCP** (reliable, ordered) with its own binary protocol. That makes it **L4**: HTTP L7 ingress/load balancers can't route it by path.
- **DNS:** name → IP. In K8s, CoreDNS resolves `service.namespace.svc.cluster.local`.
- **CIDR:** `10.0.0.0/16` = 65,536 addresses. Subnets carve this up. **Overlapping CIDRs break VNet peering/VPN**, so plan them up front.
- **NAT:** private IP ↔ public IP translation (Docker `-p`, cloud NAT gateways for egress).
- **Firewalls:** Azure **NSG** / GCP **firewall rules** (VPC level) vs **K8s NetworkPolicy** (pod level). Both must allow the traffic.
- **L4 vs L7 load balancer:** L4 = forwards TCP by IP:port (Azure Load Balancer, GCP Network LB). L7 = understands HTTP, routes by host/path, terminates TLS (App Gateway, nginx ingress).
- **Private endpoints:** reach PaaS (storage, DB) over a private IP inside your VNet, needs a **private DNS zone** or the name still resolves to the public IP.

### 3.2 THE Kafka networking concept: listeners vs advertised.listeners
This is the #1 Kafka-on-K8s/Docker interview topic. Your `docker-compose.yml` already has it.

1. Client connects to the **bootstrap** address (any broker).
2. Broker returns **metadata**: "partition 3's leader is broker 2, reachable at `<advertised address>`".
3. Client then connects **directly to that advertised address**.

So bootstrap can succeed and **everything after still fails** if the advertised address isn't reachable from the client.
- `listeners` = where the broker **binds** (interface:port it listens on).
- `advertised.listeners` = the address it **tells clients** to use.
- You need **one listener per network vantage point**: e.g. `INTERNAL://kafka-0.kafka-headless.kafka.svc.cluster.local:9071` for in-cluster pods,
  `EXTERNAL://b0.kafka.shopstream.com:9092` for outside clients.
- **Why a normal single load balancer in front of all brokers doesn't work:** the client must reach a *specific* broker (the partition leader).
  A round-robin LB sends it to a random broker. Fix: **one LB/NodePort per broker**, or a bootstrap LB **plus** per-broker addresses,
  or an ingress with **TLS passthrough + SNI** routing (hostname per broker). CFK supports these as "external access" options.

### 3.3 Kubernetes networking model
- Every pod gets its **own IP**; any pod can reach any pod (flat network) **unless NetworkPolicy restricts it**.
- **CNI** plugin implements this. AKS: Azure CNI (pods get VNet IPs; plan subnet size!) or kubenet/overlay. GKE: VPC-native (alias IPs).
- **kube-proxy** programs iptables/IPVS rules so a **Service virtual IP** load-balances to pod IPs (the "Endpoints").
- **Service types:**
  - `ClusterIP`: internal virtual IP (default).
  - `NodePort`: opens a port (30000–32767) on every node.
  - `LoadBalancer`: cloud provisions an L4 LB → NodePort → pods.
  - **Headless (`clusterIP: None`)**: no virtual IP; DNS returns **pod IPs**, and with a StatefulSet each pod gets a stable name:
    `kafka-0.kafka-headless.kafka.svc.cluster.local`. **This is what Kafka brokers use internally.**
- **Ingress:** L7 HTTP routing. Fine for Schema Registry / Control Center UIs. Not for Kafka's protocol unless TLS passthrough + SNI.
- **NetworkPolicy:** once a pod is selected by any policy, **everything not explicitly allowed is denied** (for that direction).
  Classic trap: you add a default-deny **egress** policy and forget **DNS (UDP/TCP 53 to kube-dns)** → *everything* breaks with "unknown host".
- **ndots:5:** pod resolv.conf tries search domains first for names with fewer than 5 dots → extra DNS lookups. Use FQDNs with a trailing dot for hot paths if DNS latency shows up.

**Example NetworkPolicy (only app pods may reach brokers):**
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: allow-apps-to-kafka, namespace: kafka }
spec:
  podSelector: { matchLabels: { app: kafka } }
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: shop-apps } }
        - podSelector: {}                 # brokers/controllers talk to each other in-namespace
      ports: [ { port: 9071 }, { port: 9092 } ]
```

---

## Part 4: Kubernetes (with the Kafka-specific "why")

| Concept | Why it matters for Kafka |
|---|---|
| **Deployment vs StatefulSet** | Stateless apps (order-svc) → Deployment. Brokers need **stable identity** (`kafka-0` is always broker 0), **stable DNS**, **ordered** rollout, and **their own disk** → StatefulSet. |
| **volumeClaimTemplates / PVC** | Each broker pod gets its own PVC that **follows it** when rescheduled. Deleting the pod does NOT delete the data. |
| **StorageClass** | Premium SSD, `allowVolumeExpansion: true` (to grow disks online), `volumeBindingMode: WaitForFirstConsumer` (disk is created in the zone where the pod is scheduled). `reclaimPolicy: Retain` for prod data. |
| **Zonal disks** | Azure/GCP disks are **zonal**. A broker pod whose disk is in zone 1 can **only** run in zone 1. |
| **Pod anti-affinity / topologySpreadConstraints** | Never put two brokers on the same node/zone. Otherwise one node failure = two replicas gone = writes fail (min ISR 2). |
| **broker.rack** | Set to the zone, so Kafka places the 3 replicas of each partition in **3 different zones**. |
| **PodDisruptionBudget** | `maxUnavailable: 1` → node drains/upgrades can only take down **one broker at a time**. |
| **Probes** | **readiness**: remove from Service endpoints until ready. **liveness**: restart if hung. **startup**: give slow starts time. A broker recovering logs after a crash can take minutes; an aggressive liveness probe will kill it → **CrashLoopBackOff**. Use a startupProbe. |
| **terminationGracePeriodSeconds** | Brokers do **controlled shutdown** (move partition leadership away) on SIGTERM. Give it time (e.g. 180s+), or you get unclean shutdowns and slow recovery. |
| **requests/limits** | Requests = scheduling guarantee; limits = hard cap. Memory over limit → **OOMKilled (137)**. For Kafka: JVM heap ~4–6 GB, **leave the rest of the memory for the OS page cache**, which is what makes Kafka fast. Don't set heap = limit. |
| **QoS classes** | requests == limits → `Guaranteed` (last to be evicted). Brokers should be Guaranteed. |
| **ConfigMap / Secret** | Config vs credentials. Secrets are only base64, not encrypted. Use Azure Key Vault / GCP Secret Manager with the CSI driver, plus RBAC. |
| **Operator (CFK / Strimzi)** | A controller that watches custom resources (`kind: Kafka`) and does the Day-2 work: ordered rolling restarts, waits for ISR to recover before restarting the next broker, cert rotation, scaling. **Why not plain Helm?** Helm installs; an operator *operates*. |
| **HPA / KEDA** | HPA scales on CPU/memory. For consumers, **KEDA scales on consumer lag**, which is the real signal. Max useful replicas = partition count. |
| **RBAC / ServiceAccounts** | Least privilege. Pipelines get a namespace-scoped role, not cluster-admin. Workload Identity for cloud access (no keys in pods). |

---

## Part 5: Realtime scenarios (practice saying these out loud)

### Scenario 1: "Producers inside the cluster work, but an external client connects and then times out."
- **Think:** bootstrap worked (TCP reached *something*), but metadata pointed the client to an address it can't reach.
- **Check:** client logs show it connecting to `kafka-0.kafka-headless.kafka.svc.cluster.local`. That's a cluster-internal name, unresolvable outside.
  `kafka-broker-api-versions --bootstrap-server <external>:9092` and look at the hostnames returned. `nslookup` that name from the client machine.
- **Root cause:** only an internal listener was advertised.
- **Fix:** add an EXTERNAL listener whose `advertised.listeners` is a per-broker externally resolvable name/IP (per-broker LoadBalancer, or SNI ingress with TLS passthrough). Open the port in the NSG/firewall.
- **Docker version of the same bug:** an app *container* using `localhost:9092`. Inside a container, `localhost` is the container itself. Use `kafka:29092` (internal listener). Your compose file has exactly this split.

### Scenario 2: "After a node failure, kafka-1 is stuck in Pending."
- **Check:** `kubectl describe pod kafka-1 -n kafka` → Events: `volume node affinity conflict` / `didn't match PersistentVolume's node affinity`.
- **Root cause:** its PVC is an Azure disk in **zone 2**; no schedulable node in zone 2 has capacity (or the zone's node pool was scaled down).
- **Fix:** scale up / add a node in zone 2 (node pool per zone, cluster autoscaler). Don't delete the PVC to "fix" it; that's data loss (RF=3 would re-replicate, but it costs time and network).
- **Meanwhile:** cluster still serves traffic: RF=3, min ISR 2, the other 2 brokers have the data. Check `kafka-topics --describe --under-replicated-partitions`.
- **Prevention:** a node pool per zone with autoscaling, a PDB, anti-affinity, and an alert on under-replicated partitions.

### Scenario 3: "Broker pod is in CrashLoopBackOff after a config change / upgrade."
- **Check:** `kubectl logs kafka-0 -n kafka --previous` (logs from the crashed container, not the new one). `kubectl describe pod` → last state, exit code, probe failures.
- **Common causes:**
  - Bad config (typo in listener name, listener security protocol map missing an entry) → error in logs, exit code 1. Fix the config, roll back via Helm/GitOps.
  - **Liveness probe killing it during log recovery** after an unclean shutdown → events show `Liveness probe failed`, exit 137/143. Add a `startupProbe` / raise thresholds.
  - Exit code **137 + `OOMKilled`** → see Scenario 4.
  - Permission on the volume (fsGroup/runAsUser mismatch) → `AccessDeniedException` on the log dir. Set `securityContext.fsGroup`.
- **Prevention:** roll out config changes one broker at a time (the operator does this), staging first.

### Scenario 4: "Broker keeps getting OOMKilled."
- **Check:** `kubectl describe pod` → `Last State: Terminated, Reason: OOMKilled, Exit Code: 137`. `kubectl top pod`.
- **Root cause:** container memory limit too close to JVM heap. The JVM also uses off-heap memory (direct buffers, metaspace, threads).
- **Fix:** e.g. limit 12Gi with heap `-Xms6g -Xmx6g`. Page cache is reclaimable and doesn't OOM the pod, but heap + off-heap must stay under the limit.
- **Interview line:** "Kafka performance comes from the OS page cache, not the heap. A bigger heap is usually the wrong fix."

### Scenario 5: "Black Friday: consumer lag on `orders.created` keeps growing."
- **Check:** Control Center / `kafka-consumer-groups --describe --group payment-svc` → LAG per partition. Is lag on **all** partitions or **one**?
  - **All partitions:** consumers too slow or too few. Scale the consumer Deployment (KEDA on lag), up to **12 pods = partition count** (extra pods sit idle).
    Check the downstream dependency (payment gateway latency). Batch processing per poll.
  - **One partition (hot key):** a skewed key (one huge merchant). Scaling won't help. Rethink the key, or isolate that traffic.
  - **Constant rebalancing:** processing exceeds `max.poll.interval.ms` → the consumer gets kicked out of the group → rebalance storm. Lower `max.poll.records` or raise the interval. Use cooperative-sticky assignment and static membership (`group.instance.id`) so pod restarts don't trigger full rebalances.
- **Prevention:** lag alerts, load test before sale events, pre-scale.

### Scenario 6: "We applied NetworkPolicies and now apps can't reach Kafka (or anything)."
- **Check:** `kubectl get networkpolicy -A`. From an app pod: `kubectl exec -it <pod> -- nc -vz kafka-0.kafka-headless.kafka.svc.cluster.local 9071`.
  If it fails with **"unknown host"**, it's DNS. If it **times out**, the port is blocked.
- **Root causes:** (a) default-deny **egress** in `shop-apps` without allowing **UDP/TCP 53 to kube-dns**; (b) ingress policy in `kafka` doesn't select the app namespace (label mismatch); (c) blocked broker↔broker / broker↔controller traffic → under-replicated partitions.
- **Fix:** allow DNS egress, allow egress to `kafka` namespace on the listener ports, allow intra-namespace traffic in `kafka`.
- **Prevention:** test policies in staging with a connectivity check job, roll out incrementally.

### Scenario 7: "Service exists but clients get connection refused / Service has no endpoints."
- **Check:** `kubectl get endpoints <svc>` (or `endpointslices`). Empty → the selector doesn't match pod labels, **or** the pods aren't Ready (failing readiness probe).
  `kubectl get pods --show-labels`, compare with `kubectl get svc -o yaml` selector. Also check `targetPort` matches the containerPort.
- **Fix:** correct labels/selector/targetPort, or fix why readiness fails.

### Scenario 8: "AKS node-pool upgrade took down two brokers at once and producers failed."
- **Why producers failed:** RF=3, min ISR=2. With 2 of 3 brokers down, ISR=1 < 2 → `NotEnoughReplicas` for `acks=all` producers. That's **correct behavior** (safety over availability).
- **Root cause:** no PodDisruptionBudget, or brokers sharing a node.
- **Fix:** PDB `maxUnavailable: 1`, anti-affinity, AKS surge upgrades. Before each drain: `kafka-topics --describe --under-replicated-partitions` must be **empty**. Drain one node at a time.

### Scenario 9: "Broker disk is 95% full."
- **Immediate:** find the biggest topics (`kafka-log-dirs --describe`). Lower `retention.ms`/`retention.bytes` on the offending topic (dynamic, no restart). **Never delete segment files by hand.**
- **Capacity:** expand the PVC (edit `spec.resources.requests.storage`, works if the StorageClass has `allowVolumeExpansion: true`).
- **Root cause:** often a consumer was down, so nothing was wrong in Kafka itself, but someone raised retention, or traffic grew. Kafka retention is time/size based, not "deleted when consumed".
- **Prevention:** alert at 70/80%, capacity planning = throughput × retention × RF.

### Scenario 10: "New image works locally but in AKS the pod shows ImagePullBackOff / exec format error."
- **ImagePullBackOff:** `kubectl describe pod` → `unauthorized` (AKS not attached to ACR: `az aks update --attach-acr`, or missing `imagePullSecret`), `not found` (wrong tag), or a network block to the registry.
- **`exec format error`:** image built on an ARM Mac (arm64), nodes are amd64. Build with `docker buildx build --platform linux/amd64` (or multi-arch).
- **Prevention:** build in CI, not on laptops. Immutable tags (git SHA).

### Scenario 11: "Deploys cause duplicate order processing."
- **Check:** pods get SIGKILLed (exit 137) during rollouts instead of exiting cleanly (143/0).
- **Root cause:** shell-form `CMD`, so the app isn't PID 1 and never sees SIGTERM. Or the grace period is shorter than shutdown time. The consumer processed messages but **never committed offsets** → the next consumer reprocesses them.
- **Fix:** exec-form ENTRYPOINT, handle SIGTERM (finish the current batch, commit, close the consumer), set an adequate `terminationGracePeriodSeconds`, and a `preStop` hook if needed.
- **Design answer:** Kafka is at-least-once by default, so make consumers **idempotent** (dedupe on `order_id` in the DB). For Kafka→Kafka flows, use transactions/EOS.

### Scenario 12: "TLS handshake fails after we enabled SSL on the external listener."
- **Check:** `openssl s_client -connect b0.kafka.shopstream.com:9092 -servername b0.kafka.shopstream.com`. Look at the cert chain and SANs.
- **Common causes:** the cert's **SAN doesn't include the advertised hostname** of each broker (hostname verification fails); the client truststore lacks the CA; the cert expired.
- **Fix:** issue certs with all broker hostnames (or a wildcard) in SANs; cert-manager for auto-rotation; distribute the CA to clients.

### Scenario 13: "Pods in AKS need to reach an on-prem database / another VNet, and it times out."
- **Layers to check, in order:** DNS resolves? (`nslookup`) → route exists? (VNet peering / VPN / ExpressRoute; **overlapping CIDRs** prevent peering) → NSG on the subnet allows it? → on-prem firewall allows the AKS **egress IP** (NAT gateway / LB outbound IP, not the pod IP) → NetworkPolicy egress allows it?
- **Tool:** `kubectl run tmp --rm -it --image=nicolaka/netshoot -- bash`, then `nc -vz`, `dig`, `traceroute`, `curl`.

### Scenario 14: "High cross-zone network cost / latency."
- Consumers read from leaders, which may be in another zone.
- **Fix:** `broker.rack` = zone plus `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector` on brokers, `client.rack` = zone on consumers → **follower fetching** from the same-zone replica.

### Scenario 15: "Docker Compose: the app container starts before Kafka is ready and crashes."
- `depends_on` only waits for the container to start. Add a `healthcheck` to Kafka (e.g. `kafka-broker-api-versions --bootstrap-server kafka:29092`) and `depends_on: { kafka: { condition: service_healthy } }`. Also make the app **retry** on connection failure; in K8s there is no ordering, so retries are mandatory anyway.

### Scenario 16 (design question): "How would you deploy Kafka on Kubernetes for production? Walk me through it."
Answer in layers, in this order:
1. **Infra (Terraform):** VNet with sized subnets, AKS across 3 zones, dedicated node pool for Kafka (taint `dedicated=kafka:NoSchedule` + toleration) with memory-optimized VMs, ACR, Key Vault.
2. **Operator:** CFK (or Strimzi), so rolling restarts are ISR-aware.
3. **Brokers:** StatefulSet ×3 (+3 KRaft controllers), Premium SSD PVCs, anti-affinity + zone spread, `broker.rack`, PDB, Guaranteed QoS, tuned probes, long grace period.
4. **Networking:** internal listener via headless Service; external per-broker LB or SNI; TLS everywhere; SASL/mTLS auth + ACLs; NetworkPolicies.
5. **Data safety:** RF=3, min ISR=2, `acks=all`, idempotence, `unclean.leader.election.enable=false`.
6. **Observability:** JMX → Prometheus/Grafana + Control Center; alerts on under-replicated partitions, offline partitions, consumer lag, disk, request latency.
7. **Operations:** topics as code, GitOps/CI-CD, upgrade runbook, backup/DR (MirrorMaker 2 / Cluster Linking to a second region).

### Scenario 17: "A broker pod was deleted. What happens, step by step?"
1. StatefulSet recreates `kafka-1` with the **same name and same PVC** (its data is still there).
2. While it's gone, partitions it led get **new leaders** elected from in-sync followers (controller does this in seconds). Producers/consumers refresh metadata and continue.
3. `kafka-1` returns, reads its log, **catches up** from leaders, rejoins the ISR.
4. Preferred leader election (`auto.leader.rebalance.enable`) moves leadership back over time.
5. Zero data loss because `acks=all` + min ISR 2 means every acknowledged write was already on ≥2 brokers.

---

## Part 6: Commands cheat sheet (say these naturally in answers)

**Kubernetes triage order:** `get` → `describe` (Events!) → `logs` → `exec` → check the network.
```bash
kubectl get pods -n kafka -o wide                 # status, restarts, node, pod IP
kubectl describe pod kafka-0 -n kafka             # Events: scheduling, probes, OOMKilled, image pulls
kubectl logs kafka-0 -n kafka --previous          # logs from the crashed container
kubectl get events -n kafka --sort-by=.lastTimestamp
kubectl top pod -n kafka                          # live CPU/memory (needs metrics-server)
kubectl get pvc,pv -n kafka                       # Bound? which zone?
kubectl get svc,endpoints -n kafka                # does the Service have endpoints?
kubectl exec -it kafka-0 -n kafka -- bash
kubectl rollout status statefulset/kafka -n kafka
kubectl rollout undo deployment/order-svc -n shop-apps
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data   # respects PDBs
kubectl auth can-i create pods -n kafka --as=system:serviceaccount:ci:deployer
```

**Network debugging:**
```bash
nslookup kafka-0.kafka-headless.kafka.svc.cluster.local   # DNS works?
nc -vz kafka-0.kafka-headless.kafka.svc.cluster.local 9071 # port reachable?
ss -tlnp                                                   # what's listening on this host
curl -v http://schema-registry:8081/subjects               # HTTP services
openssl s_client -connect host:9093 -servername host       # TLS / certs
traceroute / tcpdump -i any port 9092                      # path / packets
kubectl run netshoot --rm -it --image=nicolaka/netshoot -- bash   # debug pod with all tools
```

**Docker:**
```bash
docker ps -a ; docker logs -f kafka ; docker exec -it kafka bash
docker inspect kafka            # IPs, mounts, env, restart count, OOMKilled flag
docker stats                    # live resource usage
docker network ls ; docker network inspect <net>
docker compose up -d ; docker compose logs -f ; docker compose down -v
docker image history <img>      # layers (and leaked secrets)
```

**Kafka health:**
```bash
kafka-topics --bootstrap-server kafka:9092 --describe --under-replicated-partitions   # must be empty
kafka-topics --bootstrap-server kafka:9092 --describe --unavailable-partitions
kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group payment-svc   # LAG
kafka-broker-api-versions --bootstrap-server kafka:9092    # which brokers/addresses clients see
kafka-metadata-quorum --bootstrap-server kafka:9092 describe --status   # KRaft controller health
```

---

## Part 7: Your project explained end to end (read this last, then say it back in your own words)

**Follow one order, "order #1001", through every layer:**

1. **Customer clicks "Buy".** An HTTPS request hits the Azure Application Gateway (an **L7** load balancer). It terminates TLS and routes `/orders` through the Ingress to the `order-svc` **Service** (ClusterIP). kube-proxy's iptables rules pick one healthy `order-svc` pod. "Healthy" means its **readiness probe** passes; unready pods are not in the Endpoints list.

2. **order-svc (a Deployment, stateless, Docker image from ACR tagged with the git SHA)** validates the order and serializes it as Avro. The schema is checked against **Schema Registry**, so a producer can't publish a breaking format.

3. **It produces to `orders.created.v1` with key `order_id=1001`.** The client hashes the key → partition 7. From cached metadata it knows partition 7's leader is `kafka-2`, and it opens a TCP connection to `kafka-2`'s **advertised internal listener** `kafka-2.kafka-headless.kafka.svc.cluster.local:9071`. CoreDNS resolves that **headless Service** name straight to the pod IP. The **NetworkPolicy** allows it because the source is in `shop-apps` and the port is a listener port.

4. **kafka-2 writes the record to its log on its PVC** (an Azure Premium SSD in zone 3). Its followers on `kafka-0` (zone 1) and `kafka-1` (zone 2) fetch it. With `acks=all`, the leader acknowledges only after the in-sync replicas have it, and at least **min.insync.replicas=2** must be in sync. Idempotence means a retried send can't create a duplicate.

5. **Consumers read independently**, each in its own consumer group: `payment-svc`, `inventory-svc`, and the **Kafka Connect** sink that streams to the data lake for analytics. Each group tracks its own offsets in `__consumer_offsets`. Payment charges the card, produces `payments.completed.v1`, and only then **commits its offset** (at-least-once; the DB write is idempotent on `order_id`). A message that keeps failing goes to `orders.dlq.v1` instead of blocking the partition.

6. **Failure: zone 3 goes down.** `kafka-2` is gone. The **KRaft controller quorum** (2 of 3 controllers still alive = majority) elects new leaders for kafka-2's partitions from in-sync followers in zones 1 and 2. Producers get a "not leader" error, refresh metadata, and retry against the new leader. ISR is now 2 = min ISR, so writes **continue**. The **PDB** and anti-affinity guaranteed no two brokers were in that zone. When the zone returns, the StatefulSet restarts `kafka-2` with **the same PVC**, it catches up, rejoins the ISR, and alerts on under-replicated partitions clear.

7. **Deploying a change:** a PR runs CI: tests, `docker build` (multi-stage, non-root, exec-form entrypoint), Trivy scan, push to ACR. CD runs `helm upgrade` → rolling update: new pods must pass readiness before old ones get **SIGTERM**. The consumer handles SIGTERM by finishing its batch, committing offsets, and leaving the group cleanly, so there's no duplicate processing. `kubectl rollout status` gates the pipeline; `kafka-healthcheck.sh` verifies no under-replicated partitions and normal lag.

8. **Infrastructure** (VNet/subnets sized for Azure CNI, NSGs, AKS with a per-zone node pool, a tainted Kafka node pool, ACR, Key Vault) is **Terraform** with remote state per environment. `tf-apply.sh` wraps `init → validate → plan → apply`, refuses to apply to prod without an approved plan file, and is called by the pipeline. The same modules target **GKE** for the GCP environment (different provider, same structure).

**In one sentence:** "Docker packages each service, Kubernetes runs and heals them with stable identities and storage for Kafka,
networking (headless Services, advertised listeners, NetworkPolicies, L4/L7 load balancers) connects them safely, and Kafka's
replication across zones means any single failure (pod, node, or whole zone) causes no data loss and no downtime."

---

### Last-minute tips
- When you don't know: "I haven't hit that exact case, but here's how I'd debug it..." then use the triage order. That's a strong answer.
- Always mention **why** (trade-offs): "RF=3 costs 3× storage, but we survive a zone loss."
- Ask them a question at the end: "How do you run Kafka today: CFK, Strimzi, or Confluent Cloud? And what's the biggest operational pain point?"
- Good luck. You've got this.
