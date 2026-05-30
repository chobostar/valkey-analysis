# Valkey Kubernetes Operators — Production Readiness Report

> Evaluated: 2026-05-30
> Analyst role: Senior Principal Platform Engineer
> Target: Managed Valkey service, 1000 developers, Kubernetes
> Reference: Valkey internals documents (`valkey-internals/01–05`)

---

## 1. Executive Summary

| Operator | Verdict | Production-ready? |
|---|---|---|
| **valkey-io/valkey-operator** | **Best candidate, but not yet production-ready** | No — v1alpha1, missing backup/restore, no cert-manager, no external access |
| **SAP/valkey-operator** | Production-ready for standalone/sentinel, but **no cluster mode** | Partial — works well for non-sharded workloads |
| **Hyperspike/valkey-operator** | **Not production-ready** — known replica bug, no failover orchestration, no backup | No |

**Recommendation**: For a managed service requiring **Valkey Cluster** (sharding across 16384 hash slots), **none of the three operators is fully production-ready today**. The **valkey-io operator** has the most correct understanding of Valkey cluster internals (proper slot migration, proactive failover, native ASK/MOVED handling) but is still early-stage (v1alpha1). The **SAP operator** is the most mature and stable but explicitly does not support cluster mode.

If you must ship today:
- Use **SAP operator** for standalone/sentinel workloads (non-sharded)
- Use **valkey-io operator** for cluster workloads, but build the surrounding tooling (backup, cert-manager, external access) yourself
- Do **not** use Hyperspike for production — its replica initialization is broken (documented as issue #186)

---

## 2. Comparison Matrix

| Feature | Hyperspike | SAP | valkey-io |
|---|---|---|---|
| **Deployment modes** | Cluster only (always) | Standalone + Sentinel | Cluster only |
| **Failover mechanism** | Valkey-native only (no operator orchestration) | Sentinel-driven (graceful pre-stop failover) | Valkey-native + **proactive operator failover** (`CLUSTER FAILOVER` before rolling updates) |
| **Upgrade strategy** | Rolling update via StatefulSet image change (downtime risk on primaries) | Rolling update (Helm-driven, chart checksum restart) | **Serial, one-node-at-a-time, replica-first** — proactive failover before primary roll |
| **Backup/restore** | ❌ None | ❌ None | ❌ None |
| **TLS** | ✅ cert-manager integration | ✅ cert-manager + self-signed + existing issuer | ⚠️ Manual TLS Secret only (no cert-manager) |
| **Prometheus metrics** | ✅ Built-in exporter sidecar + ServiceMonitor | ✅ Exporter sidecar + ServiceMonitor + PrometheusRule | ✅ Exporter sidecar + ACL-isolated `_exporter` user |
| **CRD stability** | `hyperspike.io/v1` — mature but replicas field broken | `cache.cs.sap.com/v1alpha1` — stable webhook, immutable fields | `valkey.io/v1alpha1` — early, rich CEL validation |
| **Last release** | Valkey 8.1.4 default | Valkey 8.0.2 (Bitnami chart) | Valkey 9.0.0 default |
| **Crossplane-friendly** | Moderate — single CRD, but broken replicas | Good — single CRD, clean binding secret, but no cluster | Good — clean CRD hierarchy, but ValkeyNode is internal-only |
| **External access** | ✅ LoadBalancer + Envoy proxy | ❌ None built-in | ❌ None |
| **PDB support** | ✅ MaxUnavailable=1 | ✅ minAvailable=50% (sentinel) | ✅ maxUnavailable=1 (managed policy) |
| **ACL support** | ❌ Single password only | ❌ Single password via binding secret | ✅ Full ACL via `spec.users` with Secret refs |
| **Valkey version** | 8.1.4 | 8.0.2 | **9.0.0** |
| **Atomic slot migration** | ❌ Not implemented | N/A (no cluster) | ✅ Valkey 9.0+ `CLUSTER MIGRATESLOTS` |
| **Code quality** | 2809-line monolithic controller | Component-operator-runtime abstraction, ~clean separation | Two-controller split (Cluster + Node), clean pipeline |

---

## 3. Per-Operator Deep Dive

### 3.1 valkey-io/valkey-operator — Deep Dive

**API**: `valkey.io/v1alpha1` — `ValkeyCluster` (user-facing) + `ValkeyNode` (internal)

#### Architecture

Two-controller design:
1. **ValkeyClusterReconciler** — owns the cluster lifecycle: Service, PDB, ACL, ConfigMap, ValkeyNodes, cluster topology (MEET → slots → replicate → rebalance)
2. **ValkeyNodeReconciler** — manages individual pods: PVC, StatefulSet/Deployment (Replicas=1 each), status

This is the most architecturally sound design: each node is a first-class CR, enabling per-node lifecycle management and serial rolling updates.

#### Correctness Against Valkey Internals

| Valkey Requirement | Operator Implementation | Verdict |
|---|---|---|
| 16384 hash slots must be fully covered | Phase 2 assigns slots via `CLUSTER ADDSLOTSRANGE`; verifies all 16384 assigned before Ready | ✅ Correct |
| Cluster MEET before slot assignment | Phase 1 batches `CLUSTER MEET` for all isolated nodes | ✅ Correct |
| Replica attachment via `CLUSTER REPLICATE` | Phase 3 batch-attaches replicas | ✅ Correct |
| Failover election needs majority quorum (05-self-healing §2.1) | Operator does **not** interfere with native election; skips `CLUSTER FORGET` if live replica still references failing node | ✅ Correct |
| Proactive failover before rolling primary | `findFailoverShard()` → `CLUSTER FAILOVER` on synced replica → polls `INFO replication` for `role:master` | ✅ Correct — aligns with coordinated failover flow (03-clustering §8) |
| ASK redirect during migration | Not handled at the operator level (client responsibility) | ⚠️ Not operator concern |
| Config epoch collision | Not handled by operator — Valkey native | ✅ Correct (Valkey handles this) |
| Slot rebalancing via `CLUSTER MIGRATESLOTS` | `PlanRebalanceMove` migrates up to 400 slots at a time atomically | ✅ Correct (Valkey 9.0+ ASM) |
| Serial rolling update, replica-first | Iterates nodes in reverse order (replicas before primary), one spec update per reconcile | ✅ Correct — prevents data loss |

#### Critical Observations

- **No cert-manager integration**: TLS requires manually creating a Kubernetes Secret. This is a significant gap for a managed service at 1000-developer scale.
- **No external access**: No LoadBalancer, Ingress, or proxy support. Users can only access from within the cluster.
- **No backup/restore**: No CronJob, VolumeSnapshot, or RDB export mechanism.
- **Requires Valkey 9.0+** for atomic slot migration. This limits backward compatibility.
- **ValkeyNode CR is internal**: Users should not create these, but they are visible in the API. Crossplane composition would need to expose only `ValkeyCluster`.

---

### 3.2 SAP/valkey-operator — Deep Dive

**API**: `cache.cs.sap.com/v1alpha1` — single `Valkey` CR

#### Architecture

The operator is a thin wrapper around the **Bitnami Valkey Helm chart** (v2.2.3). It uses `component-operator-runtime` to:
1. Transform `ValkeySpec` → Helm values via `parameters.yaml`
2. Render the Helm chart
3. Apply resulting Kubernetes objects
4. Run a post-reconcile hook to create a binding secret

This is essentially a "Helm operator" pattern — the operator's value-add is the typed CRD, validating webhook, and binding secret generation.

#### Correctness Against Valkey Internals

| Valkey Requirement | Operator Implementation | Verdict |
|---|---|---|
| Standalone: primary + read replicas | Two StatefulSets: `valkey-{name}-primary` (1 pod) + `valkey-{name}-replicas` (N-1 pods) | ✅ Correct for Valkey replication model (02-replication §1) |
| Sentinel: primary election via quorum | Single StatefulSet with valkey + sentinel sidecar per pod. Sentinel scripts handle election | ✅ Correct — aligns with Sentinel architecture |
| Sentinel pre-stop failover | `prestop-sentinel.sh` triggers `CLUSTER FAILOVER` before pod termination | ✅ Correct — graceful failover |
| Replication PSYNC/backlog | Handled by Bitnami chart's Valkey config. No operator-level control | ✅ Valkey handles natively |
| **Cluster mode (sharding)** | **Explicitly not supported** (per README) | ⚠️ **Major gap** — cannot serve sharded workloads |
| TLS cert-manager | ✅ Creates Certificate + Issuer automatically | ✅ Correct |
| AOF persistence | `appendonly yes` via Bitnami chart | ✅ Correct (01-architecture §6) |

#### Critical Observations

- **No cluster mode is the deal-breaker** for a managed service that needs horizontal scaling via sharding.
- The operator is **well-engineered**: immutable field validation, clean binding secret with Go template support, topology spread constraints auto-populated.
- **Binding secret** is a nice feature for Crossplane — it produces a ready-to-use connection secret.
- Bitnami chart dependency means you inherit Bitnami's bugs and release cadence.
- Sentinel mode is fully supported with graceful failover, making it a strong choice for non-sharded HA deployments.

---

### 3.3 Hyperspike/valkey-operator — Deep Dive

**API**: `hyperspike.io/v1` — single `Valkey` CR

#### Architecture

Single monolithic controller (`valkey_controller.go`, 2809 lines) that:
1. Creates a single StatefulSet with `shards * (replicas + 1)` pods
2. Builds an in-memory cluster model
3. Manually issues `CLUSTER MEET`, `CLUSTER ADDSLOTSRANGE`, `CLUSTER REPLICATE`
4. Handles external access (LoadBalancer or Envoy proxy)

#### Critical Flaws Against Valkey Internals

| Valkey Requirement | Operator Implementation | Verdict |
|---|---|---|
| Replica initialization via `CLUSTER REPLICATE` | `replicas` field is **broken** — creates extra primary nodes instead of replicas (tracked as GitHub issue #186) | 🔴 **Critical** — cluster has no redundancy |
| Failover via native election | No operator-orchestrated failover. Relies entirely on Valkey native failover. With broken replicas, there are no replicas to fail over. | 🔴 **Critical** |
| Rolling update safety | Updates StatefulSet image → triggers rolling update. No proactive failover before rolling a primary. | 🔴 **Risk** — primary can be rolled while it's the only node serving its slots |
| PDB MaxUnavailable=1 | ✅ Present, but with broken replicas this provides no protection | ⚠️ Present but ineffective |
| Cluster MEET sequencing | `initCluster` ensures all nodes meet each other | ✅ Correct |
| Slot assignment | `CLUSTER ADDSLOTSRANGE` assigned by shard | ✅ Correct |
| Node balancing | `balanceNodes` forgets removed nodes, adds new ones | ✅ Correct |
| TLS | ✅ cert-manager integration | ✅ Correct |
| External access | ✅ LoadBalancer per shard + Envoy proxy option | ✅ Unique feature |

#### Critical Observations

- **The replica bug makes this operator unusable for production**. A cluster with `nodes=3, replicas=1` should have 3 primaries + 3 replicas = 6 pods. Instead it creates 6 primaries with no replication.
- No roadmap item addresses this as urgent — it's noted but not fixed.
- The **external access proxy** (Envoy-based) is a unique and valuable feature that the other two operators lack.
- The single 2809-line controller is a maintenance risk — difficult to test, difficult to extend.

---

## 4. Mermaid Diagrams

### 4.1 valkey-io Operator — Component Diagram

```mermaid
flowchart TB
    subgraph "Kubernetes Control Plane"
        VC[ValkeyCluster CR]
        VN[ValkeyNode CR x N]
    end

    subgraph "valkey-operator-controller"
        VCR[ValkeyClusterReconciler]
        VNR[ValkeyNodeReconciler]
    end

    subgraph "Managed Resources"
        SVC[Headless Service]
        PDB[PodDisruptionBudget]
        CM[ConfigMap valkey.conf]
        ACL[ACL Secret]
    end

    subgraph "Per-Node Resources (each ValkeyNode)"
        STS[StatefulSet (1 rep)]
        PVC[PVC (optional)]
    end

    subgraph "Pod"
        VK[Valkey Server]
        EXP[redis_exporter sidecar]
    end

    VC --> VCR
    VCR -->|"owns"| SVC
    VCR -->|"owns"| PDB
    VCR -->|"owns"| CM
    VCR -->|"owns"| ACL
    VCR -->|"creates"| VN
    VN --> VNR
    VNR -->|"owns"| STS
    VNR -->|"owns"| PVC
    STS --> VK
    STS --> EXP

    VCR -. "Phase 1: MEET" .-> VK
    VCR -. "Phase 2: ADDSLOTSRANGE" .-> VK
    VCR -. "Phase 3: REPLICATE" .-> VK
    VCR -. "Rebalance: MIGRATESLOTS" .-> VK
```

### 4.2 valkey-io Operator — Failover Sequence (Proactive Failover Before Rolling Update)

```mermaid
sequenceDiagram
    participant O as ValkeyClusterReconciler
    participant P1 as Primary Pod (shard 0)
    participant R1 as Replica Pod (shard 0)
    participant P2 as Primary Pod (shard 1)
    participant P3 as Primary Pod (shard 2)

    Note over O: Spec change detected<br/>(image upgrade, config change)
    O->>O: Determine roll order:<br/>replicas first, primaries last

    Note over O,R1: Step 1: Roll replica
    O->>R1: Update ValkeyNode spec
    R1->>R1: StatefulSet rolling update
    R1-->>O: Replica ready

    Note over O,R1: Step 2: Roll next replica (if any)
    O->>R1: (additional replicas rolled same way)

    Note over O,P1,R1: Step 3: Proactive failover BEFORE rolling primary
    O->>O: findFailoverShard: P1 is primary with synced replica R1
    O->>R1: CLUSTER FAILOVER
    R1->>R1: Requests votes from P2, P3
    P2-->>R1: FAILOVER_AUTH_ACK
    P3-->>R1: FAILOVER_AUTH_ACK
    R1->>R1: Quorum reached — claims P1's slots
    R1->>O: role:master confirmed

    Note over O,P1,R1: Step 4: Now safe to roll old primary
    O->>P1: Update ValkeyNode spec
    P1->>P1: StatefulSet rolling update
    P1->>P1: Restarts as replica of new primary (R1)
    P1-->>O: Ready (now as replica)

    Note over O: Reconcile complete — cluster healthy
```

### 4.3 SAP Operator — Sentinel Failover (Alternative Approach)

```mermaid
sequenceDiagram
    participant S1 as Node 1 (Valkey + Sentinel)
    participant S2 as Node 2 (Valkey + Sentinel)
    participant S3 as Node 3 (Valkey + Sentinel)
    participant Pod as Pod being terminated

    Note over Pod: Rolling update / scale-down
    Pod->>Pod: preStop hook: prestop-sentinel.sh
    Pod->>S1: CLUSTER FAILOVER (if this node is primary)

    S1->>S1: Sentinel election
    S2-->>S1: Vote
    S3-->>S1: Vote

    Note over S1,S2,S3: New primary elected
    S1->>S1: update label isPrimary=true
    Pod->>Pod: Graceful shutdown completes

    Note over Kubernetes: New pod starts
    NewPod->>NewPod: start-node.sh
    NewPod->>S1: Query sentinels for current primary
    S1-->>NewPod: Current primary info
    NewPod->>NewPod: Start as replica of current primary
```

---

## 5. Crossplane Integration Strategy

### 5.1 Recommended Approach: valkey-io via `provider-kubernetes`

Given that the **valkey-io operator** has the most correct cluster implementation and is the official Valkey project operator, it is the best long-term choice despite its current gaps. The integration strategy uses Crossplane's `provider-kubernetes` to render the `ValkeyCluster` CR directly.

### 5.2 CompositeResource Definition

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: CompositeResourceDefinition
metadata:
  name: xvalkeyclusters.platform.example.com
spec:
  group: platform.example.com
  names:
    kind: XValkeyCluster
    plural: xvalkeyclusters
  versions:
    - name: v1alpha1
      served: true
      referenceable: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                shards:
                  type: integer
                  default: 3
                  minimum: 1
                  description: Number of primary shards
                replicas:
                  type: integer
                  default: 1
                  minimum: 0
                  description: Replicas per shard
                storageSize:
                  type: string
                  default: "8Gi"
                tlsEnabled:
                  type: boolean
                  default: false
                metricsEnabled:
                  type: boolean
                  default: true
                valkeyVersion:
                  type: string
                  default: "9.0.0"
                resourceClass:
                  type: string
                  default: "standard"
                  enum: ["standard", "high-memory", "high-cpu"]
              required:
                - shards
          status:
            type: object
            properties:
              endpoint:
                type: string
              clusterState:
                type: string
  claimNames:
    kind: ValkeyCluster
    plural: valkeyclusters
```

### 5.3 Composition

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: valkey-cluster-composition
  labels:
    provider: valkey-io
spec:
  compositeTypeRef:
    apiVersion: platform.example.com/v1alpha1
    kind: XValkeyCluster
  mode: Pipeline
  pipeline:
    - step: render-valkey-cluster
      functionRef:
        name: function-kubeval
      input:
        apiVersion: kubeval.fn.crossplane.io/v1beta1
        kind: Input
        spec:
          resources:
            - name: valkeyCluster
              base:
                apiVersion: valkey.io/v1alpha1
                kind: ValkeyCluster
                metadata:
                  namespace: valkey-instances
                spec:
                  shards: 3
                  replicas: 1
                  image: "valkey/valkey:9.0.0"
                  persistence:
                    size: "8Gi"
                    reclaimPolicy: Retain
                  podDisruptionBudget: Managed
                  exporter:
                    enabled: true
              patches:
                - fromFieldPath: spec.shards
                  toFieldPath: spec.shards
                - fromFieldPath: spec.replicas
                  toFieldPath: spec.replicas
                - fromFieldPath: spec.storageSize
                  toFieldPath: spec.persistence.size
                - fromFieldPath: spec.valkeyVersion
                  toFieldPath: spec.image
                  transforms:
                    - type: string
                      string:
                        fmt: "valkey/valkey:%s"
                - fromFieldPath: metadata.name
                  toFieldPath: metadata.name
                - fromFieldPath: metadata.namespace
                  toFieldPath: metadata.namespace
                - type: ToCompositeConnectionDetail
                  fromFieldPath: status.state
                  toFieldPath: status.clusterState
                - type: ToCompositeConnectionDetail
                  fromFieldPath: status.conditions
                  toFieldPath: status.conditions
```

### 5.4 Example Claim (Developer-Facing)

```yaml
apiVersion: platform.example.com/v1alpha1
kind: ValkeyCluster
metadata:
  name: my-cache
  namespace: team-alpha
spec:
  shards: 3
  replicas: 2
  storageSize: "16Gi"
  tlsEnabled: true
  resourceClass: "high-memory"
```

### 5.5 Why Not Direct CRD Composition?

The valkey-io operator's `ValkeyNode` CR is **internal** — it should not be exposed to users or Crossplane. By composing only the `ValkeyCluster` CR through `provider-kubernetes`, we:
1. Hide the internal `ValkeyNode` abstraction
2. Let the operator manage its own child resources
3. Get proper status propagation from `ValkeyCluster.status`

### 5.6 SAP Operator as Crossplane Alternative (for non-sharded workloads)

For workloads that do **not** need sharding, the SAP operator is a stronger Crossplane candidate because of its built-in **binding secret**:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
metadata:
  name: valkey-standalone-composition
spec:
  compositeTypeRef:
    apiVersion: platform.example.com/v1alpha1
    kind: XValkeyCache
  resources:
    - name: valkey
      base:
        apiVersion: cache.cs.sap.com/v1alpha1
        kind: Valkey
        spec:
          replicas: 3
          sentinel:
            enabled: true
          persistence:
            enabled: true
            size: 8Gi
          metrics:
            enabled: true
            monitor:
              enabled: true
          tls:
            enabled: true
            certManager: {}
      patches:
        - fromFieldPath: spec.replicas
          toFieldPath: spec.replicas
        - fromFieldPath: metadata.name
          toFieldPath: metadata.name
    - name: binding-secret
      base:
        apiVersion: v1
        kind: Secret
        metadata:
          name: placeholder
      patches:
        - fromFieldPath: metadata.name
          toFieldPath: metadata.name
          transforms:
            - type: string
              string:
                fmt: "%s-valkey-binding"
        - type: FromCompositeFieldPath
          fromFieldPath: status.bindingSecret
          toFieldPath: data
          policy:
            fromFieldPath: Required
```

---

## 6. Gaps and Manual Interventions

This section maps each operator's gaps against the admin-required scenarios from `05-self-healing-and-admin.md`.

### 6.1 Gap Matrix vs. Admin-Required Scenarios

| Scenario (from 05-self-healing) | Hyperspike | SAP | valkey-io |
|---|---|---|---|
| **Cluster DOWN — no majority** (05 §3, Scenario 3) | ❌ No operator intervention. Cannot execute `CLUSTER FAILOVER TAKEOVER`. Admin must exec into a replica pod. | ✅ Sentinel handles failover natively. For non-cluster, no quorum issue. | ❌ No operator intervention for mass failure. Admin must exec into pod for `TAKEOVER`. |
| **Single primary failure, no replica** (05 §3, Scenario 2) | 🔴 Broken replicas mean this is the **default state**. Admin must manually create nodes and assign slots. | N/A — standalone uses read replicas, not cluster primaries. | ⚠️ Operator can add new ValkeyNodes, but manual `CLUSTER ADDSLOTS` may be needed if slot state is inconsistent. |
| **nodes.conf corruption** (05 §3, Scenario 5) | ❌ No detection or recovery. | ❌ No detection or recovery. | ❌ No detection or recovery. |
| **Data inconsistency after split brain** (05 §4) | ❌ No detection. | ❌ No detection. | ❌ No detection. All three operators lack post-failover data consistency checks. |
| **Stuck failover** (05 §5) | ❌ No detection or recovery. | ✅ Sentinel `automateClusterRecovery` can retry. | ⚠️ Operator detects unhealthy state but cannot force `TAKEOVER`. |
| **Persistence corruption** (05 §7) | ❌ No backup, no restore. | ❌ No backup, no restore. | ❌ No backup, no restore. **All three operators lack backup/restore.** |
| **Replication breakage / full resync storm** (05 §8) | ❌ No monitoring or intervention. | ⚠️ Bitnami chart config can tune backlog, but no operator-level intervention. | ⚠️ Operator monitors node health but cannot prevent resync storms. |
| **Memory fragmentation emergency** (05 §6) | ❌ No operator awareness. | ❌ No operator awareness. | ❌ No operator awareness. |
| **Orphaned primary (lost all slots)** (05 §12.4) | ⚠️ `balanceNodes` handles pod lifecycle but doesn't handle slot-less nodes gracefully. | N/A | ⚠️ Scale-in drains slots then deletes ValkeyNodes — correct, but mid-operation failures leave orphans. |
| **TLS certificate rotation** | ✅ cert-manager handles rotation automatically. | ✅ cert-manager or self-signed with Helm hooks. | ❌ Manual Secret rotation required. |
| **Backup/restore** | ❌ No CronJob, no VolumeSnapshot, no RDB export. | ❌ Same. | ❌ Same. |
| **External access** (LoadBalancer/Ingress) | ✅ LoadBalancer per shard + Envoy proxy. | ❌ None. | ❌ None. |

### 6.2 Critical Missing Capabilities Across All Operators

1. **Backup/Restore** — No operator provides automated RDB/AOF backup, VolumeSnapshot integration, or point-in-time restore. For a managed service, this must be built as a sidecar CronJob or external operator.

2. **Post-Failover Data Consistency Check** — After a failover (especially `TAKEOVER`), none of the operators verify that the new primary has all expected data. Per 05-self-healing §4, split-brain can cause data divergence that requires manual reconciliation.

3. **Automated `CLUSTER FAILOVER TAKEOVER`** — When native failover fails (no quorum), no operator can escalate to `TAKEOVER`. This requires manual intervention per 05-self-healing §5.

4. **nodes.conf Corruption Detection** — No operator validates cluster configuration consistency. A corrupted `nodes.conf` requires stopping all nodes and manual repair (05-self-healing §3, Scenario 5).

5. **Memory Management** — No operator monitors `mem_fragmentation_ratio` or triggers `MEMORY PURGE` / restart on fragmentation emergencies (05-self-healing §6).

6. **Replication Backlog Sizing** — No operator dynamically sizes `repl-backlog-size` based on dataset size or disconnection patterns. This is critical for preventing full resync storms (05-self-healing §12.5).

### 6.3 Manual Runbook Items for the Managed Service

Based on the gap analysis, the platform team must provide:

| Runbook | Trigger | Action |
|---|---|---|
| **Emergency TAKEOVER** | `cluster_state:fail` for > 60s | `kubectl exec -it <replica-pod> -- valkey-cli CLUSTER FAILOVER TAKEOVER` |
| **Backup trigger** | Scheduled (daily) + pre-upgrade | `kubectl exec -it <pod> -- valkey-cli BGSAVE` + copy RDB to object storage |
| **Restore from backup** | Data corruption / accidental deletion | Stop pod, replace `/data` with backed-up RDB, restart |
| **Slot reassignment** | Orphaned primary after node loss | `kubectl exec -it <healthy-pod> -- valkey-cli CLUSTER ADDSLOTS <range>` |
| **TLS Secret rotation** (valkey-io only) | Certificate expiry | Update Kubernetes Secret, update `spec.tls.certificate.secretName`, trigger rolling restart |
| **nodes.conf rebuild** | Cluster topology corruption | Stop all pods, delete `nodes.conf` from PVCs, restart with `CLUSTER MEET` |

---

## Appendix A: Valkey Version Compatibility

| Operator | Default Version | Supports Valkey 9.0+ ASM? | Supports Multi-DB Cluster? |
|---|---|---|---|
| Hyperspike | 8.1.4 | ❌ No (needs 9.0+) | ❌ No |
| SAP | 8.0.2 | ❌ No (N/A, no cluster) | ❌ No |
| valkey-io | 9.0.0 | ✅ Yes | ✅ Yes |

The valkey-io operator's 9.0+ default is a significant advantage: it gets atomic slot migration (fast, consistent slot moves), multi-database cluster mode, and hash field expiration out of the box.

## Appendix B: Recommendation Summary

| Use Case | Recommended Operator | Rationale |
|---|---|---|
| **Non-sharded cache** (single instance + read replicas) | SAP | Mature, stable, sentinel failover, clean binding secret |
| **Sharded cluster** (horizontal scaling needed) | valkey-io | Only operator with correct cluster topology management |
| **External access needed** | Hyperspike (temporary) | Unique Envoy proxy support, but fix replicas first |
| **Long-term managed service** | valkey-io + custom extensions | Official project operator, cleanest architecture, most Valkey-9.0-aware |
