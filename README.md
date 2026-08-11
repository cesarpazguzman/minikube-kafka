# Kubernetes Lab for Data Platforms

A hands-on lab that goes from zero to a working understanding of Kubernetes, Helm,
Operators/CRDs (Strimzi Kafka), Spark on Kubernetes and GitOps (ArgoCD) — all on a
local cluster, no cloud account required.

The goal is **intuition, not a production-ready cluster**. Every step below records
*what* was run and, more importantly, *why that choice was made*.

---

## Table of contents

- [Prerequisites](#prerequisites)
- [Block 0 — Docker vs Kubernetes](#block-0--docker-vs-kubernetes)
- [Block 1 — The cluster and the core objects](#block-1--the-cluster-and-the-core-objects)
- [Block 2 — Helm](#block-2--helm)
- [Interlude — StatefulSets](#interlude--statefulsets)
- [Block 3 — Operators and CRDs: Kafka with Strimzi](#block-3--operators-and-crds-kafka-with-strimzi)
- [Block 4 — Spark on Kubernetes](#block-4--spark-on-kubernetes)
- [Block 5 — GitOps with ArgoCD](#block-5--gitops-with-argocd)
- [Block 6 — Observability](#block-6--observability)

---

## Prerequisites

### Tooling

```bash
brew install kind helm k9s
# kubectl is assumed to be present already
```

| Tool | What it is | Why it was chosen |
|---|---|---|
| **kind** | *Kubernetes IN Docker* — a real Kubernetes cluster where every "node" is a Docker container. | We need a cluster. EKS costs money and takes ~15 min to provision; minikube runs a VM and is heavier. kind boots in ~30s and gives **multi-node clusters for free**, which matters here: seeing the scheduler place pods across different nodes is a large part of the intuition. |
| **helm** | Kubernetes package manager: YAML templating + versioned releases. | Every non-trivial component in this lab (PostgreSQL, Strimzi, Spark Operator, Prometheus, Airflow) ships as a Helm chart. It is the de-facto standard. |
| **kubectl** | HTTP client for the cluster API. | The only way to talk to the cluster. Note it is *stateless* — all state lives in the cluster's etcd, not on your laptop. |
| **k9s** | Terminal UI for browsing the cluster. | Optional, but watching pods appear and die in real time makes reconciliation click much faster than repeated `kubectl get`. |

### Versions used

| Component | Version |
|---|---|
| Docker Engine | 28.5.2 |
| kind | v0.32.0 |
| kubectl | v1.32.7 |
| helm | v4.2.3 |
| k9s | v0.51.0 |

### Docker resources

Kafka (3 brokers) and Spark (driver + 2 executors) running at the same time need
headroom. Recommended: **6 CPU / 10 GB RAM** allocated to Docker.

```bash
docker info --format 'CPUs: {{.NCPU}}  Memory: {{.MemTotal}}'
```

---

## Block 0 — Docker vs Kubernetes

Before touching Kubernetes, pin down the distinction with a minimal experiment.

```bash
docker run -d --name nginx-solo -p 8080:80 nginx:alpine
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080   # 200

docker rm -f nginx-solo
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080   # fails — nothing brings it back
```

**Conclusion.** Docker runs a container on *one* machine and has no opinion about what
happens when it dies. Kubernetes is the orchestrator that maintains a **desired state**
across a **set** of machines. In Block 1 we repeat this exact experiment inside
Kubernetes and get the opposite result — that contrast is the whole point.

**Vocabulary that matters:** an *image* is an immutable artifact (like a `.jar`), a
*container* is one running instance of that image, and an *orchestrator* decides how
many containers run, where they run, and what happens when they fail.

---

## Block 1 — The cluster and the core objects

### 1.1 Create the cluster

```bash
kind create cluster --name data-lab --config kind/kind-config.yaml
kubectl get nodes -o wide
docker ps            # ...the "nodes" are three Docker containers
kubectl get pods -A  # the control plane runs as pods inside the cluster itself
```

See [`kind/kind-config.yaml`](kind/kind-config.yaml) for why the cluster is shaped
this way (3 nodes, host port mappings).

**Why the node image is `kindest/node` and not a normal image.** It is a *machine*
image: it ships `systemd`, `containerd`, `kubelet` and `kubeadm` inside, plus
pre-pulled core Kubernetes images. A normal image runs one process and dies with it;
this one boots an init system and keeps a tree of services alive. It is tagged by
Kubernetes version, which is how you pin the cluster version:
`kind create cluster --image kindest/node:v1.32.0`.

**Version skew gotcha.** `kubectl` is only supported within ±1 minor of the API
server. Docker Desktop / OrbStack ship their own `kubectl` in `/usr/local/bin`, which
can shadow the Homebrew one and silently leave you several minors behind — which
breaks in confusing ways once CRDs are involved. Fix by putting Homebrew first on
`PATH`:

```bash
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc && hash -r
kubectl version   # client and server should match
```

**What `kubectl get pods -A` reveals:**

| Observation | What it teaches |
|---|---|
| `kindnet` and `kube-proxy` appear once per node | That is a **DaemonSet** — the "one pod on every node" pattern used by log collectors, CNI plugins and node exporters. |
| `etcd`, `kube-apiserver`, `kube-scheduler`, `kube-controller-manager` exist only on the control plane | They are **static pods**, started by the kubelet from files on disk rather than through the API. They have to be: the API server cannot start itself through the API server. |
| `local-path-provisioner` | kind's default StorageClass. It is what will back the Kafka brokers' volumes in Block 3. In EKS that role is played by the EBS CSI driver. |

### 1.2 First Deployment and Service

```bash
kubectl apply -f manifests/01-app.yaml
kubectl get pods -n lab -o wide     # note the NODE column: that is the scheduler
curl -s -o /dev/null -w "%{http_code}\n" localhost:30080
kubectl get endpointslice -n lab
```

- **Namespace** — a logical partition, used for RBAC, quotas and name collisions.
- **Deployment** — does not manage pods directly. It creates a **ReplicaSet**, and
  *that* creates the pods. This extra layer is what makes rolling updates possible.
- **Service** — a stable IP and DNS name in front of a changing set of pods. The
  `selector` is the key idea: a Service does not know pods by name, it **finds them by
  label**. That label-based coupling recurs everywhere in Kubernetes.

**No pods landed on the control plane.** That node carries a *taint*
(`node-role.kubernetes.io/control-plane:NoSchedule`) — a repellent that a pod must
explicitly *tolerate* to be scheduled there. Taints/tolerations are how you reserve
GPU nodes, spot nodes, or Kafka-only nodes in a real cluster.

```bash
kubectl describe node data-lab-control-plane | grep -A2 Taints
```

**`Endpoints` vs `EndpointSlice`.** `kubectl get endpoints` still works but warns:
since v1.33 the API is `EndpointSlice`. The old `Endpoints` object was a single list
that did not scale — a Service with 5,000 pods meant one huge object rewritten on
every change. EndpointSlice shards it.

### 1.3 The experiment that explains Kubernetes

```bash
kubectl get pods -n lab -w                  # terminal A
kubectl delete pod -n lab <a-pod-name>      # terminal B
```

A replacement appears within seconds. **Nobody asked for it.** The ReplicaSet
controller observed that actual state (2 replicas) did not match declared state (3)
and acted. That **observe → compare → act** loop is the whole idea of Kubernetes.

> This same loop shows up in three places in this lab: the ReplicaSet controller
> (here), the Strimzi operator rebuilding a Kafka broker (Block 3), and ArgoCD
> reverting a manual change against Git (Block 5). Same pattern, three scales.

```bash
kubectl scale deployment web -n lab --replicas=5
kubectl set image deployment/web nginx=nginx:1.27-alpine -n lab
kubectl rollout status deployment/web -n lab
kubectl get replicaset -n lab      # two ReplicaSets: old at 0, new at 5
kubectl rollout undo deployment/web -n lab
kubectl get replicaset -n lab      # they swap back
```

Three things this shows:

1. **A rollback is a swap, not a rebuild.** Kubernetes keeps old ReplicaSets around
   precisely for this (last 10 by default, see `revisionHistoryLimit`).
2. **`rollout undo` reverts the pod template only, not the replica count.** After the
   undo there are still 5 replicas. Scaling and deploying are independent axes.
3. **`kubectl` warns that the resource no longer matches what was applied.** The
   cluster has *drifted* from the file, and nothing detects it. This is the entire
   motivation for GitOps in Block 5.

### 1.4 Job and CronJob

```bash
kubectl apply -f manifests/02-jobs.yaml
kubectl get cronjob,job -n lab
kubectl logs -n lab job/etl-once
```

**Controllers are siblings, not a hierarchy.** Pods do not live "inside" a Deployment.
Deployment, StatefulSet, DaemonSet, Job and CronJob are all controllers that *produce*
pods, each with a different lifecycle policy. The link between them is an
`ownerReferences` field — ownership, which is also what makes deletion cascade:

```bash
kubectl get job <cronjob-run> -n lab \
  -o jsonpath='{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].name}{"\n"}'
```

| Lifecycle | Controller | Example |
|---|---|---|
| Runs forever, replicas interchangeable | Deployment | an API, the nginx above |
| Runs forever, needs stable identity + disk | **StatefulSet** | **Kafka brokers** (Block 3) |
| One per node | DaemonSet | log agent, CNI |
| Runs to completion | Job | backfill, schema migration |
| Runs to completion, on a schedule | CronJob | Iceberg compaction |

That StatefulSet row is why Kafka cannot be a Deployment: broker 1 is not
interchangeable — it owns *its* partitions on *its* disk, and if it dies it must come
back as broker 1, with the same DNS name and the same volume.

**ECS equivalences** (a Task does not have to live inside a Service — `RunTask` and
scheduled tasks produce standalone Tasks, exactly like Jobs produce standalone pods):

| Kubernetes | ECS |
|---|---|
| Pod | Task |
| pod template (inside the controller) | Task Definition |
| Deployment | Service |
| Job | RunTask (one-off) |
| CronJob | Scheduled Task (EventBridge) |
| ServiceAccount | Task Role |

**Retry and timeout knobs that matter for data workloads:**

| Field | What it caps | Why you need it |
|---|---|---|
| `backoffLimit` | Number of pod failures before the Job is marked Failed (default 6) | Stops a broken task retrying forever. Retries use exponential backoff: 10s, 20s, 40s… capped at 6 min. |
| `activeDeadlineSeconds` | Total wall-clock time | Protects against a **hung** job, which `backoffLimit` does not — a job waiting forever on a connection never "fails". |
| `ttlSecondsAfterFinished` | How long a finished Job is kept | Without it, completed Jobs accumulate in the cluster indefinitely. |
| `completions` + `parallelism` | Fan-out | Native parallel processing: 10 partitions, 3 concurrent workers, no external orchestrator. With `completionMode: Indexed` each pod gets its index as an env var. |

**Gotcha: a Job spec is immutable.** Re-running `kubectl apply` on an existing
completed Job reports `unchanged` and does *not* re-run it. This bites with ArgoCD,
which is why Jobs are declared as Argo *hooks* rather than plain resources.

**Where logs actually live.** Logs only exist on Pods — the only object that runs
anything. `kubectl logs` accepts `deployment/x` and `job/x` as sugar (it resolves a
label selector), but `cronjob/x` fails outright, because a CronJob has no pod selector:
it manages Jobs, not pods.

```
CronJob ──creates──> Job ──creates──> Pod ──has──> logs
```

```bash
kubectl get jobs -n lab --sort-by=.metadata.creationTimestamp
kubectl logs -n lab job/compaction-29774066
```

The Job name suffix (`29774066`) is the scheduled time in minutes since epoch —
deterministic on purpose, so a controller restart cannot double-schedule a run.

**Operational note:** a CronJob keeps only the last 3 successful Jobs
(`successfulJobsHistoryLimit`) and 1 failed one. When the Job is garbage-collected its
pods go too, **and the logs disappear with them**. This is why production logs are not
read with `kubectl` — they are shipped to CloudWatch/Loki by a DaemonSet collector.
Expect the interview question "how do you debug a job that failed last night?" — the
answer is *not* `kubectl logs`.

### 1.5 ConfigMap and Secret

```bash
kubectl create configmap app-config -n lab --from-literal=ENV=dev --from-literal=REGION=eu-west-1
kubectl create secret generic db-creds -n lab --from-literal=password=supersecreto
kubectl get secret db-creds -n lab -o jsonpath='{.data.password}' | base64 -d
```

**Kubernetes Secrets are base64-encoded, not encrypted.** Base64 is an encoding, not
cryptography — one 20-character command reveals the value. Anyone with read access to
Secrets in that namespace has the plaintext.

How secrets are actually handled on EKS:

1. **Have no static secrets at all** — **IRSA** or **EKS Pod Identity**: the pod's
   ServiceAccount is mapped to an IAM role over OIDC and the pod receives *temporary*
   STS credentials. This is how the Spark job reaches S3 in Block 4. Nothing to rotate.
2. **External Secrets Operator** — syncs from AWS Secrets Manager / Parameter Store
   into Kubernetes Secrets. The secret still lands in the cluster, but the source of
   truth and rotation live in AWS.
3. **Encryption at rest with KMS** — encrypts etcd. Protects the disk, not against
   someone holding RBAC read permission.

The practical difference between ConfigMap and Secret is small: base64 storage, hidden
from `describe`, and it can be mounted on `tmpfs`. **The real security boundary is
RBAC, not the object type.**

**Imperative vs declarative.** `kubectl create` fails if the object already exists;
`kubectl apply` creates or updates. The idempotent pattern — render the YAML client
side, then apply it — is what you actually want:

```bash
kubectl create secret generic db-creds -n lab \
  --from-literal=password=supersecreto \
  --dry-run=client -o yaml | kubectl apply -f -
```

Anything done with `create` / `scale` / `set image` is invisible to your repository.
That is the same drift problem that motivates Block 5.

---

## Block 2 — Helm

### 2.1 Install something

> **The lab source is out of date here.** In August 2025 Bitnami restructured its
> catalog: the HTTP repo `charts.bitnami.com/bitnami` was deprecated and versioned
> images moved out of free public access, so the original command tends to end in
> `ImagePullBackOff`. The current path is the OCI registry. This is itself an
> interview point: depending on a third party's public chart repo is a supply-chain
> dependency — the professional answer is to mirror charts and images into your own
> ECR and pin exact versions.

```bash
helm install pg oci://registry-1.docker.io/bitnamicharts/postgresql \
  --namespace lab \
  --set auth.postgresPassword=lab123 \
  --set primary.persistence.size=1Gi

helm list -n lab
kubectl get all,pvc,secret -n lab
```

`oci://` matters: since Helm 3.8 a chart can be stored as just another artifact in a
container registry. On AWS that means charts live in ECR next to your images, under
the same IAM and the same scanning.

One command created a StatefulSet, two Services, a Secret, a ConfigMap and a PVC.
Three things in that output are worth reading carefully:

- **`secret/sh.helm.release.v1.pg.v1`** — this is where **Helm stores its state**: a
  gzipped, base64-encoded Secret *inside the cluster*. Not on your laptop, not in a
  server (Helm 2's Tiller is gone). This is why `helm list` works from any machine
  with cluster access, and why there is no state file to lose. Contrast with
  Terraform, which needs a remote backend (S3 + DynamoDB lock).
- **`pg-postgresql-0`** — an ordinal name, not a random hash. That is StatefulSet
  identity.
- **`data-pg-postgresql-0`** — `<volumeClaimTemplate>-<statefulset>-<ordinal>`. It is
  **not** deleted when the StatefulSet is deleted; losing data must never be a side
  effect.

### 2.2 What Helm actually does

```bash
# THE command to internalise: renders the YAML without touching the cluster
helm template pg oci://registry-1.docker.io/bitnamicharts/postgresql \
  --set auth.postgresPassword=lab123 | head -80

# The chart's contract: everything you are allowed to override
helm show values oci://registry-1.docker.io/bitnamicharts/postgresql | head -60
```

Helm does not talk to the cluster in `helm template`. It takes Go templates, injects
your values, and emits **plain YAML**. That is all Helm is: a templating engine plus a
release manager. The cluster never sees a chart — it sees the rendered result.

**Rule of thumb: never install a chart blind.** `helm show values` to see what you can
configure, `helm template` to see what it will create.

```bash
helm upgrade pg oci://registry-1.docker.io/bitnamicharts/postgresql -n lab \
  --set auth.postgresPassword=lab123 \
  --set primary.resources.requests.memory=256Mi
helm history pg -n lab
helm rollback pg 1 -n lab
helm history pg -n lab
```

A rollback is a **new revision**, not a deletion of the previous one — like
`git revert`, not `git reset`.

**Helm vs Terraform — they do not compete:**

| | Terraform | Helm |
|---|---|---|
| Scope | AWS infrastructure: VPC, subnets, **the EKS cluster itself**, IAM, S3, RDS | Things **inside** the cluster |
| State | Remote `tfstate` (S3 + DynamoDB lock) | A Secret inside the cluster |
| Model | Plan/apply against cloud APIs | Render YAML → versioned `kubectl apply` |

The usual boundary: Terraform provisions EKS and the IRSA IAM roles; from there Helm
(or ArgoCD) owns everything inside.

**Flag worth knowing:** `helm upgrade --install --atomic --wait --timeout 5m`.
`--atomic` rolls back automatically if the release fails instead of leaving it
half-applied. That is the CI-grade invocation.

---

## Interlude — StatefulSets

A **Deployment** treats its pods as cattle: interchangeable, anonymous, owning nothing.
A **StatefulSet** assumes the opposite — each pod is an individual — and provides three
guarantees a Deployment does not:

| Guarantee | Deployment | StatefulSet |
|---|---|---|
| **Name** | Random, changes on every recreation | **Ordinal and stable**: `-0`, `-1`, `-2`. If `-1` dies it comes back as `-1` |
| **DNS** | Only the Service name (load balanced) | **Every pod gets its own DNS name** through the headless Service |
| **Storage** | Shared or none | **One PVC per pod**, reattached to the *same* volume on restart |

Plus **ordered** creation and deletion (`-0`, then `-1`, then `-2`; reverse on delete).

### Why Kafka requires it

1. **Disk.** Broker 1 holds *its* partitions on *its* disk. Coming back with an empty
   volume means re-replicating everything — hours of network traffic on a degraded
   cluster.
2. **Identity.** Kafka cluster metadata records which broker id owns what. A broker
   returning with a different id is a *new* broker, not a recovered one.
3. **Direct addressing.** A Kafka producer **cannot go through a load balancer** — it
   must talk to the **leader of that specific partition**. It needs to resolve
   `broker-1` by name and connect to *that pod*. Only a headless Service
   (`clusterIP: None`) makes that possible: it returns the individual pod IP instead of
   load balancing.

### Seeing it (repeat the Block 1.3 experiment against a StatefulSet)

```bash
kubectl delete pod pg-postgresql-0 -n lab
kubectl get pods,pvc -n lab -w
```

The `web` pod came back with a **new name** and no disk. `pg-postgresql-0` comes back
with the **exact same name**, reattached to the **same PVC** (note the PVC's AGE does
not reset). The data survives.

```bash
kubectl run dns-test -n lab --rm -ti --image=busybox:1.36 --restart=Never -- \
  nslookup pg-postgresql-0.pg-postgresql-hl.lab.svc.cluster.local
```

The name structure is `<pod>.<headless-service>.<namespace>.svc.cluster.local`.

### Growing a broker's disk

```bash
kubectl get statefulset pg-postgresql -n lab -o yaml | grep -A18 volumeClaimTemplates
```

`volumeClaimTemplates` is **immutable** — Kubernetes rejects any change to it on an
existing StatefulSet, and Helm cannot work around that (it only renders YAML; the API
server does the rejecting). The real procedure:

```
1. kubectl delete statefulset <sts> --cascade=orphan   # delete the parent, keep the pods
2. kubectl edit pvc <each-pvc>                          # raise spec.resources.requests.storage
   (needs allowVolumeExpansion: true on the StorageClass — EBS gp3 yes; kind's local-path no)
3. Recreate the StatefulSet with the new size          # it re-adopts the existing pods and PVCs
```

`--cascade=orphan` is the key: delete the parent *without* deleting the children. It is
the one situation where you deliberately break the `ownerReferences` cascade.

---

## Block 3 — Operators and CRDs: Kafka with Strimzi

Two definitions to hold onto:

- A **CRD** extends the Kubernetes API with new types. Right now the cluster knows what
  a `Deployment` is. It has no idea what a `Kafka` is. Installing the operator changes
  that.
- An **Operator** is a controller that reconciles those new types. It is the same
  observe → compare → act loop as the ReplicaSet controller, but written by people who
  know how to operate Kafka: how to roll brokers without losing quorum, how to
  resynchronise a broker that comes back, how to run a staged version upgrade.

### 3.1 Install the operator

```bash
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi strimzi/strimzi-kafka-operator -n kafka

kubectl get crd | grep strimzi
```

New types appear: `Kafka`, `KafkaTopic`, `KafkaUser`, `KafkaConnect`, `KafkaConnector`,
`KafkaNodePool`, `KafkaRebalance`, `StrimziPodSet`. **The cluster did not know what
Kafka was 30 seconds ago.** The operator itself is just an ordinary Deployment — a pod
watching those types.

**`helm repo add` vs installing straight from a URL.** A classic HTTP chart repo serves
an `index.yaml` catalogue plus tarballs, so Helm must download and cache that index
before it can resolve `strimzi/strimzi-kafka-operator`. An OCI registry
(`oci://…`, as used for PostgreSQL in Block 2) needs no index — the URL is already the
full address. The practical consequence: `helm repo add` is **local stateful config**;
a clean CI runner does not have it, which is a classic works-on-my-laptop failure. It
is also why the ecosystem is moving to OCI, where charts and images share one registry,
one IAM policy and one vulnerability scan.

**Always check which Kafka versions your operator supports before writing manifests:**

```bash
kubectl get deployment strimzi-cluster-operator -n kafka \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_KAFKA_IMAGES")].value}{"\n"}'
```

Strimzi 1.1.0 supports only Kafka 4.2.0 / 4.2.1 / 4.3.0 — the 3.8.0 in older tutorials
fails. This command is also the first thing you look at when planning an upgrade.

**`StrimziPodSet` is worth noticing.** Strimzi does **not** use StatefulSets for
brokers; it implements its own controller. A StatefulSet only knows how to restart in
ordinal order, which is dangerous for Kafka: you want to roll the brokers that lead no
partitions first, and the active KRaft controller last. A StatefulSet knows nothing
about partition leadership; the operator does. The guarantees are the same as a
StatefulSet (stable ordinal name, own PVC, per-pod DNS) — only the implementation
differs. **This is what "an operator encodes operational knowledge" means concretely:
they hit the limit of the generic primitive and built their own.**

### 3.2 Declare the cluster

See [`kafka/03-kafka.yaml`](kafka/03-kafka.yaml).

```bash
kubectl apply -f kafka/03-kafka.yaml
kubectl get pods -n kafka -w              # -w accepts only one resource type
kubectl get kafka,kafkanodepool -n kafka
```

**Read that file for what is *not* in it.** No StatefulSet, no Services, no ConfigMaps,
no PVCs, no init containers, no `advertised.listeners` wiring. You declare *what you
want* — 3 nodes, 6 partitions, 3 replicas, 7-day retention — and the operator derives
roughly 15 objects from it. Compare with Block 1, where you wrote the Deployment *and*
the Service *and* the nodePort by hand.

**Where broker count lives.** In the `KafkaNodePool` (`spec.replicas`), not in the
`Kafka` CR. That is the point of node pools: heterogeneous groups within one cluster —
3 small controllers on one pool, 6 large brokers with fast disks on another; migrating
gp2 → gp3 by draining one pool into a new one; spot and on-demand pools side by side.
Total cluster size is the sum of `replicas` across all pools labelled
`strimzi.io/cluster: <name>`.

Scaling works like any other controller, but with a caveat worth knowing:

```bash
kubectl scale kafkanodepool dual-role -n kafka --replicas=4
```

**Adding a broker does not move data to it.** Existing partitions stay where they are;
only new ones use the new node. Rebalancing is a separate step, done through the
`KafkaRebalance` CRD (Cruise Control underneath). Expect the interview question "I
scaled Kafka and throughput did not improve — why?".

**Where Kafka configuration lives.** Kafka has three configuration levels, and Strimzi
maps them onto three places:

| Level | Where in Strimzi | Scope |
|---|---|---|
| **Broker** (`server.properties`) | `spec.kafka.config` in the `Kafka` CR | All brokers. These are the cluster **defaults** |
| **Topic** | `spec.config` in the `KafkaTopic` CR | That topic only; **overrides** the broker default |
| **Client** | Your producer/consumer (`acks`, `enable.idempotence`, …) | That connection |

So `min.insync.replicas` *is* a broker setting — a default that a critical topic can
tighten to 3 while the cluster default stays 2.

Two subtleties:

- `default.replication.factor` applies **only to auto-created topics**. An explicit
  `replicas: 3` in a `KafkaTopic` is what actually decides. But
  `offsets.topic.replication.factor` and `transaction.state.log.*` matter a great deal:
  they configure the **internal** topics (`__consumer_offsets`, `__transaction_state`)
  created the first time the cluster starts. Start with a factor of 1 and those topics
  stay unreplicated forever, short of a manual partition reassignment.
- Strimzi **refuses** `broker.id`, `listeners`, `advertised.listeners`, `log.dirs` and
  friends in `spec.kafka.config`. Those belong to the operator; letting you set them
  would break reconciliation. That is the discipline of the declarative model — the
  operator owns part of the configuration space and you own the rest.

Anything that is *not* Kafka semantics — replicas, storage, resources, `nodeSelector`,
tolerations, affinity — belongs to the **node pool**. The `v1` API enforces this split:
`spec.kafka.resources` does not exist and is rejected.

```bash
# The rendered server.properties the operator hands each broker:
kubectl get configmap lab-cluster-dual-role-0 -n kafka -o yaml | head -60
```

**The durability contract.** `default.replication.factor: 3` + `min.insync.replicas: 2`
combined with `acks=all` on the producer is what gives **RPO ≈ 0**: a write is only
acknowledged once two replicas have it in their log, so losing one broker loses no
acknowledged data, and the cluster stays writable. A second simultaneous failure makes
it **read-only** rather than silently losing data. "Refuse writes rather than lose
them" is the design decision.

### 3.3 Produce and consume

Two terminals. Note the image tag comes from `STRIMZI_KAFKA_IMAGES` above, not from the
tutorial:

```bash
# consumer
kubectl -n kafka run consumer -ti --rm=true --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server lab-cluster-kafka-bootstrap:9092 --topic transactions --from-beginning

# producer
kubectl -n kafka run producer -ti --rm=true --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 -- \
  bin/kafka-console-producer.sh \
  --bootstrap-server lab-cluster-kafka-bootstrap:9092 --topic transactions
```

`--rm=true --restart=Never` creates a **bare Pod** with no controller behind it — the
"you → Pod" row of the controller table. When you exit, nothing brings it back.

Two Services are created, and the difference now has a real reason behind it:

- **`lab-cluster-kafka-bootstrap`** — a normal ClusterIP. The **first contact** point:
  the client asks "who owns what?" and receives the partition map.
- **`lab-cluster-kafka-brokers`** — headless. From then on the client talks **directly**
  to the leader of each partition, by its individual DNS name.

That is the Kafka protocol in two sentences, and it is why Kafka needs stable per-pod
identity rather than a load balancer.

### Troubleshooting: three real failures from this run

These were more instructive than the happy path.

**1. `no matches for kind "KafkaNodePool" in version "kafka.strimzi.io/v1beta2"`**

Strimzi 1.x serves only `v1`; `v1beta2` was removed at 1.0. Diagnose with:

```bash
kubectl get crd kafkas.kafka.strimzi.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
kubectl api-resources --api-group=kafka.strimzi.io
```

A CRD can serve several API versions simultaneously, with exactly one marked
`storage=true` (the one persisted in etcd); Kubernetes converts between them on the
fly. That is how an operator migrates users without downtime — and the lesson is that
**your manifests are tied to the operator version**, so pin chart versions and read the
upgrade guide before a minor bump. (A second, more mundane cause of the same error is
`kubectl`'s 10-minute discovery cache: `rm -rf ~/.kube/cache/discovery`.)

**2. `strict decoding error: unknown field "spec.kafka.resources"`**

Since Kubernetes 1.25 the API server validates fields strictly — an unknown field is an
error, not something silently ignored (a typo like `replicase: 3` used to be accepted).
The schema comes from the **OpenAPI definition the CRD publishes**, so validation
happens before the operator sees anything. Read that schema for *your installed
version* with:

```bash
kubectl explain kafkanodepool.spec
kubectl explain kafka.spec.kafka.config
```

**3. Brokers crash-looping with `HTTP connect timed out` fetching a Secret**

```
Retrieving configuration from Secret lab-cluster-trustbundle in namespace kafka
Caused by: java.net.ConnectException: HTTP connect timed out
```

Not a Kafka problem at all: the broker could not reach the Kubernetes API server.
Strimzi 1.x boots brokers with a config provider that reads the TLS trust bundle from a
Secret **at startup**, so the process dies before Kafka starts — it never even reached
`StorageTool` to format the disk.

The diagnosis that isolated it — same image, same namespace, same command, **only the
labels differ**:

```bash
# no labels -> not selected by the NetworkPolicy -> works
kubectl run nettest -n kafka --rm -ti --image=curlimages/curl:8.11.1 --restart=Never -- \
  curl -sk -m 8 -o /dev/null -w "http=%{http_code}\n" https://kubernetes.default.svc/healthz

# with the Kafka labels -> selected -> times out
kubectl run nettest2 -n kafka --rm -ti --restart=Never --image=curlimages/curl:8.11.1 \
  --labels='strimzi.io/cluster=lab-cluster,strimzi.io/kind=Kafka,strimzi.io/name=lab-cluster-kafka' \
  -- curl -sk -m 8 -o /dev/null -w "http=%{http_code}\n" https://kubernetes.default.svc/healthz
```

Strimzi generates a NetworkPolicy selecting the broker pods. Its `policyTypes` is
`[Ingress]` only, so it does not restrict egress — the outbound SYN leaves fine. What
gets dropped is the **reply**: the API server's SYN-ACK arrives as *inbound* traffic,
and a correct NetworkPolicy implementation allows it via conntrack as established
traffic. **kindnet does not**, so the packet is matched against the ingress rules,
matches no `podSelector` (the control plane is not a pod) and is dropped. Hence a
connect timeout.

This is a **CNI limitation, not a Strimzi bug** — the same policy works on EKS with VPC
CNI or Calico. NetworkPolicy is a standard API whose enforcement depends entirely on
the CNI; kind's default CNI ignored these policies outright until recently, which is why
older tutorials never hit this.

Fix for the lab:

```bash
helm upgrade strimzi strimzi/strimzi-kafka-operator -n kafka --set generateNetworkPolicy=false
kubectl rollout status deploy/strimzi-cluster-operator -n kafka
kubectl get deploy strimzi-cluster-operator -n kafka \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_NETWORK_POLICY_GENERATION")].value}{"\n"}'
kubectl delete networkpolicy lab-cluster-network-policy-kafka -n kafka
kubectl delete pod -n kafka -l strimzi.io/cluster=lab-cluster
```

**The general lesson: in Kubernetes, labels are not decorative metadata — they are the
attachment mechanism for almost everything.** Services, ReplicaSets, NetworkPolicies
and scheduling all bind by label. A pod with the wrong labels silently receives
policies that should not apply to it, or stops receiving traffic, with no error
anywhere.

**And on debugging generally:** `--previous` is what gave the answer here.

```bash
kubectl logs <pod> -n <ns> --previous --tail=60          # logs of the crashed incarnation
kubectl describe pod <pod> -n <ns> | grep -A12 "Last State"   # Reason + Exit Code
```

`Exit Code: 137` / `OOMKilled` means it exceeded `limits.memory`. `Exit Code: 1` means
the process failed on its own — read the log.

---

## Block 4 — Spark on Kubernetes

Same pattern as Block 3 (CRD + controller), opposite semantics: Strimzi manages
something **permanent**, the Spark operator manages something **ephemeral** — a job that
is born, runs and dies.

**Spark does not need this operator.** Since Spark 2.3, `spark-submit --master k8s://…`
creates the pods on its own. What the operator adds is the ability to *declare* a job as
YAML: retries, dependencies, scheduling, and — most importantly — the fact that ArgoCD
can then manage a Spark job like any other resource.

```bash
kubectl create namespace spark          # must exist BEFORE the chart is installed

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={spark}"

kubectl get crd | grep spark
kubectl get pods -n spark-operator      # two deployments: controller + webhook
kubectl get sa -n spark                 # spark-operator-spark
```

**`spark.jobNamespaces` is the number one mistake with this chart.** By default the
operator only watches `default`. Create a `SparkApplication` in another namespace
without telling it, and **nothing happens at all** — no error, no event, the object just
sits there orphaned. A good example of the characteristic operator failure mode: nobody
is listening.

**`--create-namespace` only creates the *release* namespace**, not the job namespaces.
The chart puts a ServiceAccount and RBAC into every job namespace, so those must exist
first — the first install attempt here failed for exactly that reason.

**A failed install still records a release.** `helm list` hides failed releases by
default (`helm list --all` shows them), so a retry fails with the confusing
`cannot reuse a name that is still in use`. Clean up with `helm uninstall`, or better:

```bash
helm upgrade --install --atomic ...
```

`upgrade --install` is idempotent, and `--atomic` cleans up on failure instead of
leaving a half-applied release. Plain `helm install` is fine for exploring by hand and
a liability in a pipeline.

### Running a job

See [`spark/04-spark.yaml`](spark/04-spark.yaml).

```bash
kubectl apply -f spark/04-spark.yaml
kubectl get pods -n spark -w
kubectl get sparkapplication -n spark
kubectl logs -n spark spark-pi-driver | grep "Pi is roughly"
```

**Watch the full lifecycle** — this is the thing to take away:

1. `spark-pi-driver` appears.
2. **The driver** creates the executor pods — not the operator. That is why it needed a
   ServiceAccount with API permissions.
3. The executors finish and disappear.
4. The driver ends in `Completed`, and its pod **stays** — that is where the logs and
   final state live. Clean it up with `spec.timeToLiveSeconds`, the equivalent of a
   Job's `ttlSecondsAfterFinished`.

Under YARN the equivalent would be an ApplicationMaster requesting containers from the
ResourceManager. Here the resource manager is Kubernetes, and the containers are
ordinary pods subject to the same rules as everything else: requests/limits, taints,
nodeSelectors.

### One YAML per job, not per run

The manifest describes *what code to run and with what resources*. What varies per
execution (a date, a partition, an input path) goes in `arguments` or environment
variables — not into a new file:

```yaml
spec:
  mainApplicationFile: "s3a://my-bucket/jars/etl-transactions-1.4.2.jar"
  arguments: ["--date", "2026-08-11", "--input", "s3a://raw/transactions/"]
  sparkConf:
    spark.sql.shuffle.partitions: "200"
```

In a real setup `mainApplicationFile` points at a **versioned artifact in S3**, not at
something baked into the image: the Spark image stays generic and stable, the jar is
what changes.

| Situation | Approach |
|---|---|
| Same job, different parameters per run | `arguments` / `env`. One YAML |
| Recurring on a schedule | **`ScheduledSparkApplication`** — what CronJob is to Job: it *manufactures* SparkApplications |
| 20 similar jobs | A Helm chart with one template and one values file per job, or Kustomize base + overlays |
| Triggered from Airflow | `SparkKubernetesOperator` renders the SparkApplication from a Jinja template on each DAG run |

**A `SparkApplication` is single-use, like a Job.** Once `COMPLETED`, re-applying the
same file does *not* re-run it. Re-running means deleting and recreating, or using
`ScheduledSparkApplication`. Same trap as Block 1.4, and it bites the same way with
ArgoCD.

### Batch vs streaming — the differences that matter

- **`restartPolicy: Always` for streaming.** A streaming job never "completes"; if it
  stops, that is a failure and it must come back. For batch, finishing *is* success.
- **The `serviceAccount` is the IRSA anchor.** Annotate it with
  `eks.amazonaws.com/role-arn` and the pod receives temporary STS credentials over
  OIDC. No static keys, nothing to rotate. This is the answer to "how does your Spark
  job reach S3 without static credentials?"
- **A streaming job is not orchestrated by Airflow.** It lives permanently in the
  cluster, like a Deployment. Airflow orchestrates things with a beginning and an end —
  it is a DAG scheduler, not a process supervisor. Putting a streaming job in a DAG is a
  classic design error: what would "the task failed" even mean for something that was
  never supposed to finish?

---

## Block 5 — GitOps with ArgoCD

### Install

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
kubectl port-forward svc/argocd-server -n argocd 8080:443    # https://localhost:8080, user admin
```

**Note the irony, because it is a good interview question:** we install the GitOps tool
with an imperative `kubectl apply` from a URL. That is the **bootstrap problem** —
something has to create the creator. The production answer is that **Terraform installs
ArgoCD** (via the `helm` provider) as part of cluster creation, and from there ArgoCD
manages everything else — **including itself**, using the *app-of-apps* pattern: an
ArgoCD Application pointing at a repo that contains ArgoCD's own definition.

The pods that matter:

- **`argocd-repo-server`** — clones the repo and renders manifests (running
  `helm template` or `kustomize build` where needed).
- **`argocd-application-controller`** — the reconciliation loop: compares rendered
  output against the cluster.
- **`argocd-server`** — API and UI.

`port-forward` is worth understanding: it tunnels from your machine to a pod **through
the API server**. No LoadBalancer, no Ingress, nothing exposed. It is how you reach
internal UIs (Spark UI, Airflow, Grafana) on EKS too, with the bonus that access control
is your Kubernetes RBAC rather than a separate login.

### The Application

See [`argocd/05-argo-app.yaml`](argocd/05-argo-app.yaml).

```bash
kubectl apply -f argocd/05-argo-app.yaml
kubectl get deployment web -n lab -w
```

By this point in the lab the cluster had drifted: `kubectl scale` in Block 1.3 left the
Deployment at 5 replicas while Git said 3, **and nothing had reported it for an hour**.
Applying the Application snapped it back to 3 within seconds. Nothing was "deployed" —
we only declared *this namespace should look like that folder in that repo*.

**The Application is itself a CRD instance.** ArgoCD is an operator like Strimzi and the
Spark operator: the same reconciliation loop over a different domain — instead of
reconciling a Kafka cluster against a spec, it reconciles an entire cluster against a Git
repository. That is the **third** appearance of the same pattern in this lab.

- **`prune: true`** deletes resources that disappear from Git; without it, removing a
  file leaves an orphan in the cluster forever. It only prunes what ArgoCD created
  (tracked by annotations), so the Helm-installed PostgreSQL in the same namespace is
  untouched.
- **`selfHeal: true`** reverts manual changes made against the cluster.
- **`destination.server: https://kubernetes.default.svc`** means "the cluster ArgoCD
  runs in", but ArgoCD can drive many remote clusters from one control cluster — the
  usual production topology.

**The resource graph in the UI follows `ownerReferences`**, the same field inspected by
hand in Block 1. That is why `CronJob → Job → Pod` appears even though only the CronJob
is declared in Git, and why Job cards appear and vanish as
`successfulJobsHistoryLimit: 3` garbage-collects them. Those Jobs are *not* pruned by
ArgoCD — the CronJob controller deletes them. ArgoCD distinguishes "extraneous relative
to Git" from "a child generated by something that is in Git".

### The two experiments

**Auto-heal** — no Git involved:

```bash
kubectl get deployment web -n lab -w        # terminal A, first
kubectl scale deployment web -n lab --replicas=1   # terminal B
```

The manual change lasts about a second. The controller **watches** the resources it
manages, so it reacts to the event, not to polling — the 3-minute poll is only for
detecting changes *in Git*.

If one replica were genuinely needed, it has to change **in Git**. That is the jump from
"Git is how we deploy" to "Git is the source of truth": the cluster now actively rejects
anything that did not come from the repo.

*(The honest counterpoint, worth being able to discuss: during a serious incident,
`selfHeal` fights you. Hence the `argocd.argoproj.io/compare-options: IgnoreExtraneous`
annotation, disabling auto-sync from the UI, and Sync Windows. Naming the drawback is
what makes you sound like someone who has used it.)*

**Deploy through Git:**

```bash
# change replicas in manifests/01-app.yaml
git commit -am "Scale web" && git push
kubectl get deployment web -n lab -w        # do not touch the cluster
```

It arrives within ~3 minutes. **Your `git push` carried no cluster credentials.** GitHub
never talked to Kubernetes. The cluster pulls from Git, not the other way round.

**Push vs pull — three concrete advantages:**

1. **Attack surface.** With push, CI needs an admin kubeconfig stored as a secret in
   GitHub; whoever compromises your CI compromises production. With pull, that
   credential does not exist — the cluster reaches out to Git, read-only.
2. **Drift detection.** Push deploys and forgets; nobody knows if someone changed
   something afterwards. Pull compares continuously and either reports or corrects.
3. **Audit and recovery.** The cluster's desired state *is* the Git history: who changed
   what, when, under which PR. Recovering a lost cluster means pointing a new ArgoCD at
   the same repo.

**Latency in production is not 3 minutes.** Polling exists because ArgoCD must
`git ls-remote` every repo periodically — expensive at 200 applications. Real setups
configure a **GitHub webhook** to `argocd-server/api/webhook` so deployment starts on
push, leaving polling as the safety net.

### Troubleshooting

**`SYNC STATUS: Unknown` is not `OutOfSync`.** `Unknown` means *"I could not even
compare"* — connectivity or permissions — while `OutOfSync` means *"there are
differences"*. Distinguishing the two points straight at the cause. The message lives in
`status.conditions`, the same place we looked for the Kafka CR: **in any CRD, `status` is
where the controller answers you.**

Here it was the **same NetworkPolicy/kindnet problem as Block 3**:

```
Failed to load live state: ... dial tcp 10.96.0.1:443: i/o timeout
```

ArgoCD's `install.yaml` ships NetworkPolicies for every component. That it appeared twice
in two unrelated products is the useful lesson: **any serious operator ships
NetworkPolicies by default**, and a CNI that implements them badly breaks everything that
talks to the API.

```bash
kubectl delete networkpolicy --all -n argocd
kubectl rollout restart statefulset argocd-application-controller -n argocd
```

That fixes two paths at once: the controller reaching the API server, and the
`repo-server` reaching github.com to clone.

**Forcing a refresh** without the `argocd` CLI (equivalent to Hard Refresh in the UI —
the annotation is consumed and removed automatically):

```bash
kubectl get application lab-app -n argocd \
  -o jsonpath='sync={.status.sync.status}  revision={.status.sync.revision}{"\n"}'

kubectl patch application lab-app -n argocd --type merge \
  -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'
```

**Jobs and GitOps do not mix by default.** When the Job controller creates a Job it adds
generated labels to `spec.template`, which your Git manifest does not have. Since
`spec.template` is immutable, a re-apply fails with `field is immutable` — and because
ArgoCD applies the set as a whole, one failing resource can leave the rest unapplied.
Three fixes, in increasing order of correctness:

```bash
kubectl delete job etl-once -n lab        # A) quick: let ArgoCD create it fresh
```

```yaml
# B) production: declare it as an ArgoCD hook, not a plain resource
metadata:
  annotations:
    argocd.argoproj.io/hook: PostSync
    argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
---
# C) brute force: make ArgoCD delete+create instead of apply
metadata:
  annotations:
    argocd.argoproj.io/sync-options: Replace=true
```

---

## Block 6 — Observability

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install kube-prom prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --set alertmanager.enabled=false \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false

kubectl port-forward -n monitoring svc/kube-prom-kube-prometheus-prometheus 9090:9090
```

**The operator pattern for the fourth time.** With prometheus-operator you never edit a
`prometheus.yml`. You declare `ServiceMonitor` and `PodMonitor` objects saying "scrape
these pods, on this port, every 30s", and the operator *generates* the Prometheus
configuration from them. Also note `node-exporter` in the pod list: a DaemonSet, the
one-per-node pattern from Block 1.

**The two `…SelectorNilUsesHelmValues=false` flags are this chart's number one gotcha.**
By default the chart configures Prometheus to only look at ServiceMonitors carrying its
own release label. Create a PodMonitor for Kafka without them and nothing fails, nothing
errors — it simply is not scraped. **The third silent label-matching failure in this
lab.**

**`kube-controller-manager` shows DOWN on kind, and that is expected.** The error is
`connection refused`, not a timeout: the process is alive but listening on
`127.0.0.1:10257` rather than the node IP — kubeadm's secure default. It is fixed with
`--bind-address=0.0.0.0` in the static pod manifest, and the same applies to
`kube-scheduler` and `etcd`. **On EKS you cannot scrape any of the three**: AWS manages
the control plane and does not expose them, leaving only what AWS publishes to
CloudWatch. That is a real difference between managed and self-run Kubernetes.

### Wiring Kafka metrics

Strimzi does not expose metrics by default. Check which mechanism your version supports
before writing anything — the same habit that saved us twice already:

```bash
kubectl explain kafka.spec.kafka.metricsConfig.type
```

- **`jmxPrometheusExporter`** — the classic: a Java agent translating JMX MBeans into
  Prometheus format, configured with a ConfigMap of ~100 lines of relabelling regexes.
- **`strimziMetricsReporter`** — newer: a native Kafka reporter, no JMX agent, six lines
  of configuration.

The `metricsConfig` block added to [`kafka/03-kafka.yaml`](kafka/03-kafka.yaml) plus the
PodMonitor in [`kafka/06-kafka-metrics.yaml`](kafka/06-kafka-metrics.yaml):

```bash
kubectl apply -f kafka/03-kafka.yaml
kubectl apply -f kafka/06-kafka-metrics.yaml
kubectl get pods -n kafka -w
```

**The `allowList` is not housekeeping.** Kafka exposes tens of thousands of JMX metrics —
one set per topic *per partition*. Scraping all of them hurts Prometheus long before it
hurts Kafka. Cardinality is the thing to control.

**Watch the rolling restart — it is the payoff of the whole lab.** Enabling metrics
changes broker configuration, so the brokers must restart, and the operator rolls them
one at a time: `-0`, wait until healthy and partitions are back in sync, then `-1`, then
`-2`. **Never two at once**, because that would lose quorum. Meanwhile the cluster keeps
accepting writes — produce messages during the restart and nothing is lost. That is
`min.insync.replicas: 2` with 3 replicas doing its job, and it is precisely what a
generic StatefulSet cannot orchestrate, which is why `StrimziPodSet` exists.

### The two queries worth memorising

```promql
kafka_server_replicamanager_underreplicatedpartitions
```

**Kafka's alarm metric.** Should be 0. Sustained above 0 means replicas are lagging; if
the ISR drops below `min.insync.replicas`, writes start failing. This ties directly back
to the RPO discussion.

```promql
kafka_controller_kafkacontroller_activecontrollercount
```

Must sum to **exactly 1** across the cluster. 0 means no controller and no metadata
changes are possible; 2 means split brain.

```bash
kubectl port-forward -n monitoring svc/kube-prom-grafana 3000:80
kubectl get secret kube-prom-grafana -n monitoring -o jsonpath='{.data.admin-password}' | base64 -d
```

Kubernetes dashboards ship with the chart. Strimzi publishes Kafka dashboards in its
repo under `examples/metrics/grafana-dashboards/`, imported through Dashboards → Import.
