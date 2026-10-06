# Your Project: SAP Event Integration Platform (Java microservices + Kafka on AKS, built with Terraform)

> Built from your resume: **Azure DevOps Engineer, Wipro (Unizen), Project: SAP, Jun 2020 – Present.**
> Everything below extends the bullets already on your resume (Java microservices pipelines, Terraform for AKS, AKS upgrade
> 1.24→1.30, SonarQube, Key Vault, Dev/Test/UAT/Pre-Prod/Prod, Docker security, Azure Monitor).
>
> **Your role in this story:** you are the **DevOps / platform engineer**. You did NOT write the Java business code.
> You **built and ran the platform**: Terraform for Azure + AKS, Kafka on AKS, Docker images, Helm, pipelines, networking,
> monitoring, upgrades, and production troubleshooting. This is the strongest and most defensible position. If asked about Java
> code, say: "Dev team owned the business logic; I owned how it was built, shipped, configured, scaled and kept running."
>
> **If you're short on time, read:** Part 1 → Part 2 (diagram) → Part 7 (scenarios) → Part 9 (explanation).

---

## Part 1: The 60-second introduction (memorize this)

> "I have 6+ years in DevOps and cloud, currently at Wipro on an SAP integration program. The client runs **SAP S/4HANA on-premises**,
> and business events from SAP (sales orders, material/stock changes, deliveries, invoices) need to reach downstream systems in real time.
> We built an **event-driven platform on Azure**: SAP pushes events through SAP Integration Suite to a **Java Spring Boot ingestion service**,
> which publishes them to **Kafka**. About eight Java microservices consume those topics for orders, inventory, billing, notifications and
> write-back to SAP.
>
> Everything runs on **AKS**, provisioned with **Terraform**: hub-spoke VNets, VPN to the SAP data center, private AKS, Azure Firewall
> for egress, App Gateway for ingress, private endpoints for Azure SQL, Key Vault and ACR. Kafka runs on AKS with the **Confluent operator**:
> 3 brokers and 3 KRaft controllers on a dedicated node pool across 3 zones.
>
> I own the **Azure DevOps pipelines** (Maven build, SonarQube gate, Docker build and Trivy scan, push to ACR, Helm deploy across Dev, Test,
> UAT, Pre-Prod and Prod with approvals), the Terraform pipelines with plan approval, and day-2 operations. For example, I upgraded the AKS
> clusters from **1.24 to 1.30** with zero downtime."

Then stop. Let them pick a thread. Most likely: AKS upgrade, networking to SAP, Kafka on K8s, or pipelines. All are in Part 7.

---

## Part 2: Architecture (be able to draw this)

```
 ON-PREM DATA CENTER                          AZURE (Central India region, 3 availability zones)
 ┌──────────────────────┐        ┌───────────────────────── HUB VNet 10.0.0.0/16 ─────────────────────────┐
 │ SAP S/4HANA (ERP)    │  IPsec │  VPN Gateway (S2S)   Azure Firewall (egress + inspection)   Bastion    │
 │ SAP Integration Suite├────────┤  Private DNS zones (privatelink.*) linked to hub + spoke                │
 │ (CPI) / IDocs/OData  │  VPN   └───────────────┬────────────────────────────────────────────────────────┘
 └──────────────────────┘                        │ VNet peering
                                                 ▼
 ┌──────────────────────────────────── SPOKE VNet 10.1.0.0/16 (per environment) ─────────────────────────────┐
 │ snet-appgw 10.1.0.0/24  : Application Gateway v2 + WAF (L7, HTTPS)  ◄── internal/partner HTTPS traffic       │
 │ snet-aks   10.1.16.0/20 : PRIVATE AKS cluster (Azure CNI), UDR → Azure Firewall for all egress              │
 │   node pools:  system (3×D4s_v5)  │  apps (3–10×D8s_v5, autoscale)  │  kafka (3×E8s_v5, tainted, 1 per zone)  │
 │                                                                                                            │
 │   ns: sap-apps   (Java 17 / Spring Boot / Spring Kafka, Deployments + HPA/KEDA)                            │
 │     sap-ingestion-svc ─produce─►  ┌──────────── ns: kafka (Confluent for Kubernetes) ───────────┐           │
 │     order-svc        ◄─consume──  │ KRaft controllers ×3, Brokers ×3 (StatefulSets, PVC each)    │           │
 │     inventory-svc    ◄─consume──  │ Schema Registry ×2, Kafka Connect ×2, Control Center         │           │
 │     billing-svc      ◄─consume──  └──────────────────────────────────────────────────────────────┘           │
 │     notification-svc ◄─consume──                                                                           │
 │     sap-writeback-svc ◄─consume── ──► calls SAP OData APIs back over the VPN (status updates)               │
 │                                                                                                            │
 │ snet-pe  10.1.2.0/24    : Private Endpoints → Azure SQL, Key Vault, ACR, Storage (ADLS)                    │
 │ snet-agents 10.1.3.0/24 : Azure DevOps self-hosted agents (VMSS) → can reach the private AKS API server      │
 └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 Observability: Container Insights + Log Analytics, App Insights Java agent, Managed Prometheus/Grafana, Confluent Control Center
```

### Components and why each exists

| Component | What it does | Why (interview line) |
|---|---|---|
| **SAP S/4HANA + Integration Suite (CPI)** | Source of business events (sales order created, goods issued, invoice posted) | SAP doesn't talk Kafka natively. CPI sends events as HTTPS/JSON (from IDocs/OData) to our ingestion API. |
| **sap-ingestion-svc** (Java producer) | Validates, converts to Avro, publishes to Kafka, returns 202 to SAP | Decouples SAP from consumers. SAP gets a fast ack even if billing is down. |
| **Kafka (CFK on AKS)** | Durable, replicated event log | Many consumers, replay, ordering per key, absorbs month-end spikes. |
| **Schema Registry** | Avro schema contracts, BACKWARD compatibility | SAP adds a field → consumers don't break. |
| **Consumer microservices** | Business processing; write to Azure SQL / PostgreSQL | Each scales and fails independently. |
| **sap-writeback-svc** | Posts status back to SAP via OData | Closes the loop (e.g. "delivery confirmed"). |
| **Kafka Connect (ADLS sink)** | Streams topics to the data lake for reporting | No custom code for analytics. |
| **VPN Gateway** | Private link to the on-prem SAP DC | SAP traffic never goes over the public internet. |
| **Azure Firewall + UDR** | All AKS egress goes through the firewall | Control and audit outbound traffic; a single egress IP for SAP firewall allow-lists. |
| **App Gateway v2 + WAF (AGIC)** | L7 ingress for HTTPS APIs | TLS termination, WAF, path routing. |
| **Private AKS + private endpoints** | No public API server or public PaaS endpoints | Security/compliance requirement for SAP data. |
| **Key Vault + CSI driver + Workload Identity** | Secrets (Kafka SASL creds, DB passwords, certs) mounted into pods | No secrets in Git, images, or pipeline variables. |

### Topics

| Topic | Partitions | RF | min ISR | Key | Consumers |
|---|---|---|---|---|---|
| `sap.salesorder.created.v1` | 12 | 3 | 2 | sales order number | order-svc, billing-svc, notification-svc |
| `sap.material.stock.v1` | 12 | 3 | 2 | material number (SKU) + plant | inventory-svc |
| `sap.delivery.status.v1` | 6 | 3 | 2 | delivery number | order-svc, sap-writeback-svc |
| `sap.invoice.posted.v1` | 6 | 3 | 2 | invoice number | billing-svc, Connect → ADLS |
| `*.dlq` | 3 | 3 | 2 | same | ops team / replay job |

- **Key = SAP document number:** all events for one sales order land on one partition, so they're processed **in order** (create → change → delivery).
- **RF 3 / min ISR 2 / producer `acks=all` + idempotence:** a whole zone can fail with no data loss and no write outage.
- **Volume to quote:** "steady few hundred events/sec, **month-end close spikes 10–20×**". Month-end in SAP (billing runs, invoice posting) is a very real pattern and a great hook for scaling questions.

---

## Part 3: Repositories and Terraform structure

```
sap-platform-infra/                       # Terraform (Azure Repos)
├── modules/
│   ├── network/        # hub & spoke VNets, subnets, NSGs, UDRs, peering, VPN gateway, private DNS zones
│   ├── firewall/       # Azure Firewall + policy (app rules: FQDNs; network rules: SAP IPs/ports)
│   ├── aks/            # private AKS, node pools (system/apps/kafka), Azure CNI, workload identity, AGIC, KEDA add-on
│   ├── acr/            # ACR Premium + private endpoint; AcrPull role to AKS kubelet identity
│   ├── keyvault/       # KV + private endpoint + access policies/RBAC
│   ├── sql/            # Azure SQL + private endpoint
│   ├── appgw/          # App Gateway v2 + WAF policy
│   └── monitoring/     # Log Analytics, App Insights, alerts, action groups
├── envs/
│   ├── dev/  test/  uat/  preprod/  prod/   # main.tf calls modules; terraform.tfvars differ (sizes, counts, CIDRs)
│   │   └── backend.tf   # azurerm backend: storage account, container "tfstate", key "<env>.tfstate"
└── pipelines/terraform.yml

sap-platform-k8s/                         # Kubernetes / Helm (GitOps-style, Azure Repos)
├── kafka/  cfk-operator (helm values), kraftcontroller.yaml, kafka.yaml, schemaregistry.yaml, connect.yaml,
│           controlcenter.yaml, topics/*.yaml (KafkaTopic CRs = topics as code)
├── charts/java-service/   # ONE shared Helm chart for all Java microservices
│   └── templates/ deployment, service, hpa/scaledobject, pdb, networkpolicy, secretproviderclass, ingress
├── values/<service>/values-<env>.yaml
└── policies/  default-deny NetworkPolicies, ResourceQuotas, LimitRanges

sap-<service>/   (one repo per microservice, owned by developers)
├── src/ pom.xml Dockerfile azure-pipelines.yml   (pipeline template from a shared templates repo)
```

### Terraform pieces you should be able to talk through

```hcl
# envs/prod/backend.tf: remote state with locking (Azure blob lease = the lock)
terraform {
  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = "sttfstatesapprod"
    container_name       = "tfstate"
    key                  = "prod.tfstate"
  }
}

# modules/aks/main.tf (trimmed)
resource "azurerm_kubernetes_cluster" "this" {
  name                    = "aks-sap-${var.env}"
  kubernetes_version      = var.k8s_version          # e.g. "1.30"
  private_cluster_enabled = true
  oidc_issuer_enabled       = true                   # for Workload Identity
  workload_identity_enabled = true

  default_node_pool {                                # system pool
    name           = "system"
    vm_size        = "Standard_D4s_v5"
    node_count     = 3
    zones          = ["1", "2", "3"]
    vnet_subnet_id = var.aks_subnet_id
    only_critical_addons_enabled = true
    upgrade_settings { max_surge = "33%" }
  }
  network_profile {
    network_plugin = "azure"                         # Azure CNI
    outbound_type  = "userDefinedRouting"            # egress via Azure Firewall
  }
  key_vault_secrets_provider { secret_rotation_enabled = true }
  oms_agent { log_analytics_workspace_id = var.law_id }
}

resource "azurerm_kubernetes_cluster_node_pool" "kafka" {
  name                  = "kafka"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  vm_size               = "Standard_E8s_v5"          # memory-optimized: Kafka loves page cache
  node_count            = 3
  zones                 = ["1", "2", "3"]
  node_taints           = ["dedicated=kafka:NoSchedule"]   # only Kafka pods (with toleration) land here
  node_labels           = { workload = "kafka" }
  vnet_subnet_id        = var.aks_subnet_id
}

resource "azurerm_role_assignment" "acr_pull" {      # lets nodes pull from ACR (no imagePullSecrets)
  scope                = var.acr_id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
}
```

**Terraform interview points (all on your resume):**
- **Modules + variables + locals + outputs:** one module, five environments, only the tfvars differ.
- **Remote state + locking:** azurerm backend; the blob lease prevents two pipelines applying at once. A stuck lock after a cancelled run → confirm no run is active → `terraform force-unlock <id>`.
- **State recovery:** blob **versioning + soft delete** on the state container, so you can restore a previous state version. `terraform import` for resources created in the portal. `terraform state rm` / `moved {}` blocks for refactors.
- **Drift:** someone changed an NSG rule in the portal → `terraform plan` shows the drift → either codify it or let apply revert it. Azure Policy plus RBAC (no write access in prod portal) prevents it.
- **`lifecycle { prevent_destroy = true }`** on AKS, Key Vault, SQL, and the Kafka disks' storage. `ignore_changes = [default_node_pool[0].node_count]` because the autoscaler changes it.

### Azure DevOps pipelines

**Terraform pipeline:** PR → `fmt -check`, `validate`, `tflint`/`checkov` → `plan -out=tfplan` → plan published as an artifact and shown on the PR → **Environment approval** (pre-deployment condition) → `apply tfplan` (the *exact* plan that was approved). Runs on **self-hosted agents inside the VNet**, using a **service connection with workload identity federation** (no client secrets).

**Microservice pipeline (shared YAML template):**
```yaml
stages:
- stage: CI
  jobs:
  - job: build
    steps:
    - task: Maven@4                     # mvn verify → unit tests + JaCoCo coverage
    - task: SonarQubePrepare@6 / SonarQubeAnalyze@6 / SonarQubePublish@6   # quality gate must pass
    - script: docker build -t $(ACR)/order-svc:$(Build.SourceVersion) .
    - script: trivy image --exit-code 1 --severity CRITICAL,HIGH $(ACR)/order-svc:$(Build.SourceVersion)
    - script: docker push $(ACR)/order-svc:$(Build.SourceVersion)          # immutable tag = git SHA
- stage: Dev     # then Test → UAT → PreProd → Prod, each an ADO Environment with approvals/checks
  jobs:
  - deployment: deploy
    environment: sap-dev
    strategy: { runOnce: { deploy: { steps:
      - script: helm upgrade --install order-svc charts/java-service -f values/order-svc/values-dev.yaml
                --set image.tag=$(Build.SourceVersion) --atomic --timeout 10m -n sap-apps
      - script: kubectl rollout status deploy/order-svc -n sap-apps
    } } }
```
- `--atomic`: if the rollout fails, Helm **automatically rolls back**.
- **Same image** promoted through all environments; only values change. "Build once, deploy many."
- Branching: `main` protected (PR + 2 reviewers + build validation), `feature/*`, `release/*`, `hotfix/*`.

---

## Part 4: Docker for the Java services

```dockerfile
# ---- build stage: has Maven + JDK (big) ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline          # cached layer: re-runs only when pom.xml changes
COPY src ./src
RUN mvn -q package -DskipTests            # tests already ran in the CI stage

# ---- runtime stage: JRE only (small, fewer CVEs) ----
FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd -r -u 10001 app
COPY --from=build /src/target/*.jar app.jar
USER 10001                                 # non-root
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]   # exec form → java is PID 1 → gets SIGTERM → graceful shutdown
```
**Talking points:**
- Multi-stage: the build image is ~800 MB, the runtime ~250 MB. Smaller image means faster pulls during scale-out and fewer vulnerabilities.
- **`MaxRAMPercentage=75`:** the JVM sizes its heap from the **container memory limit** (needs a container-aware JDK). The other 25% is for metaspace, threads, and direct buffers. Never hard-code `-Xmx` equal to the limit.
- `ExitOnOutOfMemoryError`: a JVM that's out of heap is half-dead. Exit so Kubernetes restarts it cleanly.
- Non-root plus `readOnlyRootFilesystem` in the pod securityContext. Trivy scan fails the build on CRITICAL/HIGH.
- Exec-form ENTRYPOINT + Spring Boot `server.shutdown=graceful` → in-flight requests and Kafka batches finish on SIGTERM.

## Part 5: Kubernetes for the Java services (the shared Helm chart)

```yaml
spec:
  replicas: 3
  strategy: { rollingUpdate: { maxUnavailable: 0, maxSurge: 1 } }   # zero-downtime deploys
  template:
    spec:
      serviceAccountName: order-svc            # Workload Identity → Key Vault without secrets
      terminationGracePeriodSeconds: 60
      topologySpreadConstraints: [{ maxSkew: 1, topologyKey: topology.kubernetes.io/zone, whenUnsatisfiable: ScheduleAnyway }]
      containers:
      - name: app
        image: acrsapprod.azurecr.io/order-svc:<git-sha>
        resources:
          requests: { cpu: "500m", memory: "1Gi" }
          limits:   { memory: "1Gi" }          # memory limit; CPU limit often omitted to avoid throttling
        startupProbe:   { httpGet: { path: /actuator/health/liveness,  port: 8080 }, failureThreshold: 30, periodSeconds: 5 }
        livenessProbe:  { httpGet: { path: /actuator/health/liveness,  port: 8080 }, periodSeconds: 10 }
        readinessProbe: { httpGet: { path: /actuator/health/readiness, port: 8080 }, periodSeconds: 5 }
        lifecycle: { preStop: { exec: { command: ["sh", "-c", "sleep 10"] } } }   # let LB/endpoints drain first
        volumeMounts: [{ name: secrets, mountPath: /mnt/secrets, readOnly: true }]   # Key Vault CSI
```
Plus: a **PDB** (`minAvailable: 2`), an **HPA** on CPU for API services, **KEDA ScaledObject on Kafka consumer lag** for consumers
(maxReplicas ≤ partition count), and a **NetworkPolicy** per service.

**Kafka on AKS (CFK):** StatefulSets with a PVC per broker (`managed-csi-premium`, `allowVolumeExpansion`, `WaitForFirstConsumer`),
tolerations + nodeSelector for the `kafka` pool, one broker per zone, `broker.rack` = zone, PDB maxUnavailable 1, JVM heap 6g of 32 GB
node memory (the rest is page cache), listeners: internal (headless Service, SASL_SSL) for in-cluster apps; no public listener at all.
**Why an operator and not a Helm chart:** the operator does **ISR-aware rolling restarts**. It waits for under-replicated partitions to reach 0 before
restarting the next broker. A plain StatefulSet rollout doesn't know Kafka exists.

---

## Part 6: Networking deep-dive (they said networking is a focus)

**Traffic flows to explain:**

1. **SAP → AKS (inbound):** SAP CPI (on-prem) → IPsec **VPN** → hub VNet → peering → **App Gateway private frontend IP** (WAF, TLS termination with a cert from Key Vault) → AGIC routes `/sap/events` → `sap-ingestion-svc` pod IPs (Azure CNI: pods have real VNet IPs, so App Gateway sends to pods directly).
2. **Pod → Kafka (east-west):** `sap-ingestion-svc` → `kafka.kafka.svc.cluster.local:9071` (bootstrap) → metadata returns **advertised listeners** `kafka-N.kafka.kafka.svc.cluster.local` → direct TCP to the partition leader. Resolved by CoreDNS (headless Service → pod IP). Allowed by NetworkPolicy (`sap-apps` → `kafka` on 9071).
3. **Pod → Azure SQL / Key Vault / ACR:** names like `sqlsap.database.windows.net` → CNAME to `privatelink.database.windows.net` → **private DNS zone** linked to the VNet → **private endpoint IP** 10.1.2.x. If the zone isn't linked, it resolves to the **public IP** and is blocked (classic bug).
4. **Pod → SAP OData (writeback) / internet (outbound):** route table (UDR) on the AKS subnet: `0.0.0.0/0 → Azure Firewall`. SAP IPs go via the firewall → VPN. The SAP team allow-lists **one IP: the firewall's private IP** (the SNAT source), not pod IPs.
5. **AKS required egress:** the firewall must allow the `AzureKubernetesService` FQDN tag (MCR, management.azure.com, login.microsoftonline.com, packages, etc.). Missing these → nodes not ready / failed upgrades.
6. **Pipeline → AKS API:** the API server is private (private endpoint + private DNS zone `privatelink.centralindia.azmk8s.io`). Microsoft-hosted ADO agents **can't** reach it, so we use **self-hosted VMSS agents in `snet-agents`**.

**Key numbers/facts:**
- **Azure CNI IP planning:** every pod gets a VNet IP, and IPs are pre-allocated per node (`maxPods`, 30 by default). Subnet needs ≈ (max nodes + surge nodes) × (maxPods + 1). The /20 = 4,096 IPs. Too small → pods stuck with "failed to allocate address", and **upgrades fail because surge nodes can't get IPs**. Alternative: **Azure CNI Overlay** (pods get IPs from a separate private CIDR).
- **App Gateway v2 subnet NSG** must allow inbound **65200–65535 from `GatewayManager`**, or the gateway breaks.
- **NSG vs NetworkPolicy:** NSG = subnet/NIC level (Azure). NetworkPolicy = pod level (Azure NPM / Calico / Cilium). Traffic must pass **both**.
- **L4 vs L7:** Kafka = TCP binary protocol → L4 only (Azure Load Balancer), never App Gateway. HTTP APIs → App Gateway (L7).
- **OSI troubleshooting order:** DNS (`nslookup`) → route (`traceroute`, effective routes on the NIC) → port (`nc -vz`) → TLS (`openssl s_client`) → app (`curl -v`, logs).

---

## Part 7: Real-time scenarios in your SAP project (answer: symptom → check → command → root cause → fix → prevention)

### S1. "Tell me about the AKS upgrade from 1.24 to 1.30." (It's on your resume, so expect it!)
- **Plan:** AKS can't skip minor versions for the control plane → **1.24 → 1.25 → 1.26 → 1.27 → 1.28 → 1.29 → 1.30**, one at a time, Dev → Test → UAT → Pre-Prod → Prod, with validation (smoke tests, Kafka health, consumer lag, SAP end-to-end test order) at each step.
- **Pre-checks:** `az aks get-upgrades`, read the AKS release notes, scan manifests/Helm charts for removed APIs with **kubent / pluto**, check PDBs, confirm subnet IPs for surge nodes, back up manifests, confirm `terraform` `kubernetes_version` matches (or `ignore_changes`, to avoid drift fights).
- **Issues we hit (good stories):**
  1. **Removed APIs:** `policy/v1beta1` PodDisruptionBudget and `batch/v1beta1` CronJob were **removed in 1.25**, `autoscaling/v2beta2` HPA **in 1.26**. Helm releases holding the old API versions fail to upgrade → updated the chart templates to `policy/v1`, `batch/v1`, `autoscaling/v2` **before** the control-plane upgrade. (PodSecurityPolicy also removed in 1.25 → replaced with Pod Security Admission labels / Azure Policy.)
  2. **Java pods OOMKilled after 1.25:** AKS 1.25 moved nodes to **Ubuntu 22.04 with cgroup v2**. Old JDKs (before 8u372 / 11.0.16) can't read cgroup v2 limits, so the JVM sized its heap from **node** memory → exceeded the container limit → OOMKilled (exit 137). Fix: upgraded base images to JDK 17 and used `MaxRAMPercentage`.
  3. **Node drain stuck:** a PDB with `minAvailable` = replica count (e.g. 2 of 2) allows **zero** disruptions → drain blocks forever. Fix: replicas 3 / minAvailable 2 (or `maxUnavailable: 1`).
  4. **Kafka during node upgrades:** the kafka node pool was upgraded separately, `max-surge 1`, one node at a time. Before each step, under-replicated partitions had to be 0. The broker PDB allows only one broker down.
  5. **Image registry change:** `k8s.gcr.io` was frozen → moved add-on images to `registry.k8s.io` / mirrored them into ACR.
- **Result:** zero downtime. Control plane upgrade first, then node pools with surge, validated after each version.

### S2. "SAP says events are failing: CPI shows timeouts posting to your endpoint."
- **Path:** SAP → VPN → hub → peering → App Gateway → pod. Check from the outside in.
- **Check:** VPN connection status (`az network vpn-connection show`, connection "Connected"? BGP/tunnel metrics). App Gateway **backend health** (if unhealthy → 502). Pods ready? `kubectl get pods -n sap-apps`, `kubectl get endpoints sap-ingestion-svc`.
- **Common causes:** tunnel down after the on-prem firewall changed its IPsec/IKE settings (mismatch), App Gateway health probe hitting a path that returns 401/404 → marks the backend unhealthy → 502, an expired TLS cert on the listener, or ingestion pods not ready because **Kafka is unreachable** (readiness includes the Kafka check).
- **Prevention:** alerts on VPN tunnel status, App Gateway unhealthy host count, and cert expiry (Key Vault auto-rotation). The SAP side retries thanks to CPI retry/JMS queue.

### S3. "Month-end close: billing-svc lag is at 2 million messages and invoices are delayed."
- **Check:** Control Center / `kafka-consumer-groups --describe --group billing-svc`. Lag on **all** partitions or **one**?
- **All:** scale out. **KEDA** on lag adds pods up to **12 (= partitions)**. Check the real bottleneck: Azure SQL DTU/vCore at 100%? (Scaling consumers then makes it worse.) Check the App Insights dependency calls.
- **One partition:** a hot key (one huge customer/company code). Scaling doesn't help, so re-key or split.
- **Rebalance storm:** processing time > `max.poll.interval.ms` → consumers get kicked out → constant rebalancing → zero progress. Lower `max.poll.records`, use cooperative-sticky assignment and static membership.
- **Prevention:** pre-scale before month-end (KEDA cron trigger), load test in Pre-Prod, lag alerts.

### S4. "Duplicate invoices were posted back to SAP after a deployment."
- **Root cause:** pods were SIGKILLed (exit 137) during the rollout. They had processed messages but **not committed offsets**, so new pods reprocessed them. Causes: shell-form ENTRYPOINT (java not PID 1), grace period too short, no graceful shutdown in Spring.
- **Fix:** exec-form ENTRYPOINT, `server.shutdown=graceful`, Spring Kafka container stops cleanly and commits, `terminationGracePeriodSeconds` ≥ shutdown time.
- **Design fix (key point):** Kafka is **at-least-once**, so writeback must be **idempotent**: use the SAP document number as an idempotency key (check before posting).

### S5. "SAP added a new field and consumers started failing."
- Schema Registry with **BACKWARD** compatibility: adding an optional field (with a default) is allowed, and old consumers ignore it. A breaking change (removing a required field) is **rejected at registration** in Dev, not discovered in Prod.
- Non-deserializable messages → **DLQ** (Spring Kafka `DeadLetterPublishingRecoverer`) so one poison message doesn't block the partition. Fix, then replay from the DLQ.

### S6. "New release: pods stuck in ImagePullBackOff in Prod but fine in UAT."
- `kubectl describe pod` → event message:
  - `401 unauthorized` → the Prod AKS kubelet identity lacks **AcrPull** on the Prod ACR (Terraform role assignment missing) → `az aks check-acr`.
  - `i/o timeout` / `dial tcp <public IP>` → ACR is private-endpoint only, and `privatelink.azurecr.io` isn't linked to the Prod VNet, so DNS returned the public IP → blocked. Or the firewall blocks it.
  - `manifest unknown` → tag wasn't pushed / wrong tag in values file.
- **Prevention:** same Terraform module for every env, `az aks check-acr` in the pipeline, image promotion between registries.

### S7. "order-svc can't connect to Azure SQL. Works from your laptop on VPN."
- `kubectl exec` → `nslookup sqlsap.database.windows.net`. A public IP means **the private DNS zone isn't linked to the spoke VNet** (or custom DNS servers don't forward to Azure DNS 168.63.129.16).
- Then `nc -vz <private IP> 1433` → timeout = NSG / NetworkPolicy egress / UDR sending it to the firewall without a rule.
- **Fix:** link `privatelink.database.windows.net` to the VNet (in Terraform), NetworkPolicy egress to `snet-pe` on 1433.

### S8. "After we enabled default-deny NetworkPolicies, everything in sap-apps broke."
- Errors were `UnknownHostException`, so it's **DNS**. Default-deny **egress** also blocked UDP/TCP 53 to CoreDNS.
- **Fix:** allow egress to `kube-system` kube-dns on port 53, then allow egress to `kafka` on 9071, to private endpoints, and to the SAP IPs.
- **Process:** apply in Dev with a connectivity test job first, then promote.

### S9. "Pods are Pending with 'failed to allocate address' / an upgrade failed midway."
- **Azure CNI subnet exhausted.** Each node reserves `maxPods + 1` IPs, and surge nodes during an upgrade need IPs too.
- **Fix (short-term):** reduce surge, add a new node pool on a new larger subnet, move workloads. **Long-term:** bigger subnet or Azure CNI Overlay. It's a Terraform change, planned with the network team because CIDRs must not overlap with on-prem SAP ranges (VPN routing).

### S10. "kafka-1 broker is Pending after the kafka node pool was scaled/recreated."
- `kubectl describe pod kafka-1 -n kafka` → `volume node affinity conflict`. Its Azure disk is **zonal (zone 2)**, and there's no kafka node in zone 2.
- **Fix:** make sure the kafka pool has a node in each zone. Never delete the PVC to "fix" it.
- **Meanwhile:** RF 3 / min ISR 2 means writes still work. Watch `--under-replicated-partitions`.

### S11. "Java service in CrashLoopBackOff after a release."
- `kubectl logs <pod> --previous`. `describe` → last state/exit code/probe events.
  - Exit 1 + Spring stack trace → bad config (wrong Key Vault secret name, missing env var, DB unreachable at startup).
  - `Liveness probe failed` during startup → Spring Boot takes 60–90s to start, and the liveness probe kills it at 30s → add a **startupProbe**.
  - Exit 137 + OOMKilled → heap vs limit (S1 point 2).
- Helm `--atomic` already rolled back Prod automatically. You debug in Pre-Prod.

### S12. "External client / Connect worker connects to Kafka bootstrap and then times out."
- Bootstrap worked, but metadata returned **advertised listener** hostnames the client can't resolve or reach (e.g. internal `*.svc.cluster.local` names from outside the cluster).
- Check `kafka-broker-api-versions --bootstrap-server ...` → see which hostnames come back.
- **Fix:** a separate listener per network (internal headless names for in-cluster; a per-broker internal LoadBalancer + DNS for VNet clients). Certificate SANs must include those hostnames.

### S13. "Kafka broker disk at 90%."
- Find the largest partitions (`kafka-log-dirs --describe`). Lower `retention.ms` on the offending topic (dynamic, no restart). Usually a DLQ or a topic with retention set too long.
- **Grow:** edit the PVC size (StorageClass `allowVolumeExpansion: true`; through the CFK `dataVolumeCapacity`). **Never delete log segments by hand.**
- Alert at 70/80%. Capacity = throughput × retention × RF.

### S14. "Terraform pipeline fails: 'state blob is already locked'."
- A previous run was cancelled mid-apply. Confirm no other pipeline is running → `terraform force-unlock <lock-id>` → re-run plan (check for partial changes).
- **Worse case: state corrupted/lost** → restore the previous blob version (versioning enabled) → `plan` to compare → `terraform import` anything created after.

### S15. "Someone needs to rotate the Kafka SASL password / DB password without downtime."
- The secret lives in **Key Vault** → mounted via the **CSI driver** with rotation enabled → but the Java app reads it at startup → so: add the new credential alongside the old (dual-valid), update Key Vault, rolling-restart deployments (`kubectl rollout restart`), then remove the old credential.

### S16. "How do you deploy with zero downtime?"
- Rolling update `maxUnavailable: 0, maxSurge: 1`. Readiness gates traffic. `preStop sleep` lets App Gateway/endpoints drain. Graceful shutdown commits Kafka offsets. PDB protects during node maintenance. `helm --atomic` auto-rollback. Same image promoted from UAT → Prod. Approvals as pre-deployment conditions in ADO Environments.

### S17. "Design question: why Kafka? Why not Azure Service Bus?"
- Service Bus = queue/pub-sub, message deleted once consumed, great for commands/workflows. Kafka = **retained log**: many independent consumers, **replay** (reprocess after a bug fix), per-key ordering at high volume, Kafka Connect ecosystem, Schema Registry. SAP events are consumed by 4–5 systems plus analytics, and need replay → Kafka.
- **Why self-managed on AKS vs Confluent Cloud / Event Hubs Kafka endpoint?** Data residency/private network requirement for SAP data, consistent with the rest of the AKS platform, cost at our volume. The trade-off is that we own operations (upgrades, disks), which is exactly what we automated.

---

## Part 8: Commands to say naturally

```bash
# AKS / Azure
az aks get-upgrades -g rg-sap-prod -n aks-sap-prod -o table
az aks upgrade -g rg-sap-prod -n aks-sap-prod --kubernetes-version 1.30.x --control-plane-only
az aks nodepool upgrade -g rg-sap-prod --cluster-name aks-sap-prod -n kafka --kubernetes-version 1.30.x --max-surge 1
az aks check-acr -g rg-sap-prod -n aks-sap-prod --acr acrsapprod.azurecr.io
az aks command invoke ... --command "kubectl get pods -A"     # run kubectl against a PRIVATE cluster without VNet access
az network vpn-connection show -g rg-hub -n cn-sap-dc --query connectionStatus
az network nic show-effective-route-table ...                 # where is this traffic actually going?

# Kubernetes triage: get → describe (Events) → logs --previous → exec → network
kubectl get pods -n sap-apps -o wide ; kubectl describe pod <p> ; kubectl logs <p> --previous
kubectl get events -A --sort-by=.lastTimestamp | tail -30
kubectl top pods -n sap-apps ; kubectl get endpoints <svc> ; kubectl get pdb -A
kubectl rollout status|undo|restart deploy/order-svc -n sap-apps
kubectl run netshoot --rm -it --image=nicolaka/netshoot -- bash   # nslookup, nc -vz, curl, traceroute, tcpdump

# Kafka
kafka-topics --bootstrap-server kafka:9071 --describe --under-replicated-partitions   # must be empty
kafka-consumer-groups --bootstrap-server kafka:9071 --describe --group billing-svc   # LAG
kafka-metadata-quorum --bootstrap-server kafka:9071 describe --status                # KRaft health

# Docker
docker build / docker image history / docker inspect / docker stats / trivy image <img>
```

---

## Part 9: The whole project explained simply (read this last, then say it in your own words)

**The business problem.** The client's core business runs in **SAP** (orders, stock, deliveries, invoices). Many other systems need to know
the moment something happens in SAP: the order portal, inventory, billing, customer notifications, and reporting. Before, each system
called SAP directly or ran batch jobs. That was slow, overloaded SAP, and was fragile: if one system was down, data was missed.

**The solution.** SAP publishes each business event **once**. Our ingestion service puts it into **Kafka**. Every system that cares reads
it from Kafka at its own pace. Kafka keeps the events (retention), so a system that was down simply catches up, and we can **replay**
events after a bug fix. The SAP document number is the message key, so events for one order are always processed in order.

**Where it runs.** Everything runs on **AKS** in Azure:
- **Java microservices** are Docker containers: small, non-root images, scanned for vulnerabilities, stored in ACR.
- **Kafka** runs on its own dedicated node pool. The Confluent operator manages 3 brokers across 3 zones, each with its own disk. Every message is copied to 3 brokers, so a server or a whole data-center zone can fail without losing data.
- **Kubernetes** keeps everything healthy: probes restart broken pods, rolling updates give zero-downtime releases, autoscaling (KEDA on Kafka lag) adds consumers during the month-end spike, and PDBs protect us during upgrades.

**How it's connected (networking).**
- SAP is on-premises, so it reaches Azure through a **site-to-site VPN** into a **hub VNet**.
- Traffic enters through **App Gateway** (WAF, TLS) to the ingestion pods.
- Inside the cluster, services find Kafka through **Kubernetes DNS**. **NetworkPolicies** allow only the needed flows.
- Databases, Key Vault and the registry are reached over **private endpoints** (no public internet), resolved by **private DNS zones**.
- All outbound traffic goes through **Azure Firewall**, so SAP allow-lists a single IP and we can audit egress.
- The AKS API itself is **private**, so pipelines run on self-hosted agents inside the VNet.

**How it's built and shipped (your core work).**
- **Terraform** modules create the whole thing (network, firewall, AKS and its node pools, ACR, Key Vault, SQL, monitoring) identically for Dev, Test, UAT, Pre-Prod and Prod.
  - State is stored remotely with locking and versioning.
  - Changes go through **plan → approval → apply** in Azure DevOps.
- For every code change, the **Azure DevOps** pipeline does the following:
  1. Builds with Maven and runs the tests.
  2. Enforces the **SonarQube** quality gate.
  3. Builds the Docker image and scans it with **Trivy**.
  4. Pushes it to ACR, tagged with the git SHA.
  5. Deploys the **same image** environment by environment with Helm (`--atomic` auto-rollback), with approvals before Pre-Prod and Prod.
- Secrets come from **Key Vault** through Workload Identity. None are stored in code or pipelines.

**How it's operated.**
- Container Insights, App Insights, Prometheus/Grafana and Confluent Control Center watch pod health, JVM memory, consumer lag, under-replicated partitions and disk usage.
- Alerts go to the on-call team.
- I ran the **AKS upgrades from 1.24 to 1.30** one version at a time across all five environments. That meant fixing removed APIs in our Helm charts beforehand, solving the Java cgroup-v2 OOM issue, fixing PDBs that blocked drains, and upgrading the Kafka nodes one at a time with ISR checks, all with zero downtime.

**One-line summary:** "SAP events flow into Kafka once and fan out to Java microservices on AKS. Terraform builds it, Azure DevOps ships it, Kubernetes
keeps it running, private networking secures it, and Kafka replication across zones means no single failure loses data or stops the business."

---

### Final tips
- You're a **DevOps engineer with 6 years**: answer like an operator. Lead with *how you'd check*, then the fix, then the prevention.
- If you don't know something: "I haven't hit that exact case. Here's how I'd narrow it down..." and use the triage order (DNS → route → port → TLS → app).
- Kafka isn't on your resume yet. If you'll talk about it as part of the SAP project, add a resume line so it isn't a surprise, e.g.
  *"Deployed and operated Confluent Kafka (KRaft) on AKS for SAP event streaming: Terraform-provisioned node pools, Helm/CFK, monitoring of consumer lag and replication."*
  Only claim depth you can defend in follow-ups; the Kafka modules in this repo are where you build that depth.
- A question to ask them at the end: "How is Kafka run for this client today: Confluent Platform on Kubernetes, Confluent Cloud, or VMs? What's the biggest operational pain point?"
