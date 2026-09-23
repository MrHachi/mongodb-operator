# Architecture

```text
           SingleTenantMongoDB CR
                     |
                     v
              MongoDB Operator
                     |
    +----------------+----------------+
    |                |                |
    v                v                v
MongoDB Pods      Secrets         ConfigMap
    |
    v
Replica Set
```

- Controller deployment exists in user-defined namespace
- CRDs are installed cluster-wide. Custom Resources may exist in namespaces managed by the operator.
    - Controller manages:
        - Pods
        - Svc (headless and ClusterIP)
        - CM (connection information for applications)
        - Secret (MongoDB keyfile)

## Reconciliation flows

```mermaid

flowchart LR
    Init[Initialization]
    Scaling[Scaling]
    Ready[Ready]
    Degraded[Degraded]

    Init -->|desiredCount > 1| Scaling
    Init -->|desiredCount == 1| Ready
    Ready --> |desiredCount changes| Scaling
    Scaling --> Ready
    Ready --> |unhealthy| Degraded
    Degraded --> Ready

```

### Initialization phase

```mermaid

flowchart TD
    EnsureKeyfileSecret["Ensure keyfile secret"]
    EnsureFirstPod["Ensure primary pod"]
    EnsureService["Ensure headless service"]
    InitRS["Initiate replica set<br>(pod exec)"]
    InitAdmin["Create admin user<br>(pod exec)"]
    InitDone["Ready"]

    EnsureKeyfileSecret --> EnsureFirstPod
    EnsureFirstPod --> EnsureService
    EnsureService --> InitRS
    InitRS --> InitAdmin
    InitAdmin --> InitDone
```

### Scaling phase

```mermaid

flowchart TD
    subgraph ScaleOut[Scale-out]
        direction TD;
        ReclaimPVC["Find jettisoned PVCs"]
        ReuseSuffices["Reuse past suffices"]
        NewSuffices["Make new suffices"]
        AddPods["Add member pods"]

        ReclaimPVC -->|"found"| ReuseSuffices
        ReclaimPVC -->|"none remain"| NewSuffices
        ReuseSuffices --> AddPods
        NewSuffices --> AddPods
    end

    subgraph ScaleIn[Scale-in]
        direction TD;
        FilterTargets["Find Secondary replicas"]
        GateScaleIn["Wait for sufficient viable targets"]
        SelectTargets["Select targets with least replication lag"]
        RemovePods["Remove selected member pods"]

        FilterTargets --> GateScaleIn
        GateScaleIn --> SelectTargets
        SelectTargets --> RemovePods
    end
    ScaleDone["Ready"]

    AddPods -->|"status: Primary/Secondary"| ScaleDone
    RemovePods --> ScaleDone
```

### Ready phase

```mermaid

flowchart TD
    EnsurePods["Ensure pods"]
    ValidateHealth["Check RS status"]
    CleanupPVC["Delete expired PVCs"]
    Done["Ready"]

    EnsurePods --> ValidateHealth
    ValidateHealth --> CleanupPVC
    CleanupPVC --> Done

```

### Degraded phase

```mermaid

flowchart TD
    EnsureMajority["Ensure majority"]
    EnsurePrimary["Ensure primary health"]
    EnsureDesiredCount["Ensure desired count"]
    EnsureMembersHealthy["Ensure members healthy"]

    EnsureMajority --> EnsurePrimary
    EnsurePrimary --> EnsureDesiredCount
    EnsureDesiredCount --> EnsureMembersHealthy

```

## Evolution

```mermaid

flowchart TD
    A["Kubernetes templates + single-replica manually managed MongoDB STS"]
    B["Kubernetes templates + multiple-replica manually managed MongoDB STS"]
    C["Kubernetes templates + MongoDB RS operator"]
    D["CRD + hand-written MongoDB controller + RS operator"]
    E["CRD + Kubebuilder MongoDB controller + RS operator"]
    F["StatefulSet -> explicit Pod management"]

    A -->|experimentation| B
    B -->|operational automation| C
    C -->|naive abstraction| D
    D -->|adoption of industry-standard tooling| E
    E -->|enable topology-aware HA| F

```

## Future work

- Tighten RBAC so operators only have access to Secrets owned by their managed resources, reducing blast radius in multi-tenant clusters
- Keyfile rotation

## Current limitations

- Only supports a single application database per MongoDB deployment
- Does not currently implement finalizers for external cleanup
- Does not currently support MongoDB version upgrades
- Does not currently support automated keyfile rotation
- Does not currently expose Prometheus metrics for database health
