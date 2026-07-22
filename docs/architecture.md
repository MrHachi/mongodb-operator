# Architecture

```text
CRD
↓
Controller
↓
StatefulSet
↓
Pods
↓
MongoDB Replica Set
```

- Controller deployment exists in `controller-system` namespace (Kubebuilder default)
- CRDs deployed throughout cluster
    - Controller manages:
        - STS
        - Svc (headless)
        - CM (connection information for applications)
        - Secret (MongoDB keyfile)

## Evolution

```mermaid

flowchart TD
  A[Kubernetes templates + single-replica manually managed MongoDB STS]
  B[Kubernetes templates + multiple-replica manually managed MongoDB STS]
  C[Kubernetes templates + MongoDB RS operator]
  D[CRD + hand-written MongoDB controller + RS operator]
  E[CRD + Kubebuilder MongoDB controller + RS operator]

  A -- experimentation -> B
  B -- operational automation -> C
  C -- naive abstraction -> D
  D -- adoption of industry-standard tooling -> E

```

## Future work

- Tighten RBAC so operators only have access to Secrets owned by their managed resources, reducing blast radius in multi-tenant clusters
- Horizontal autoscaling
    - TODO: Investigate autoscaling strategies while considering MongoDB replica set membership changes and stateful workload constraints
- Keyfile rotation
