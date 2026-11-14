# MongoDB Operator Architecture

This document outlines the design and architectural decisions for the MongoDB operator.

## Core Components

### MongoDB Operator

The central controller that manages the lifecycle of MongoDB clusters through a reconciliation loop. It transitions the custom resource (CR) through different phases (e.g., `Initializing`, `Scaling`, `Ready`, `Degraded`).

**Note on Pod Management**: The operator does **not** use `StatefulSets` for pod management. Instead, it manages Pods directly to allow granular control over which members are removed during scaling-in, ensuring they are not deleted while the database is in an unstable replication state.

### Instance-Manager Sidecar

A Go-based service (using the Chi router) deployed as a sidecar container within each MongoDB Pod.

- **Responsibility**: Performs sensitive operations that require interaction with the database itself, such as initiating the Replica Set (RS) and creating administrative users.
- **Communication**: The operator communicates with the sidecar via the Pod's raw IP address on port `8080`.
- **Authentication**: The sidecar validates the operator's identity by performing `TokenReview` and `SubjectAccessReview` against the Kubernetes API.

## Reconciliation Lifecycle

The reconciliation loop follows a state-machine approach to ensure the desired state is reached.

### 1. Initialization Phase (`status.phase = Initializing`)

This phase focuses on setting up the first primary node and the foundational cluster infrastructure.

- **Resource Provisioning**:
    - **Keyfile**: Generates a secure keyfile and stores it in a Kubernetes `Secret`.
    - **Storage**: Provisions a `PersistentVolumeClaim` (PVC) for database data.
    - **Primary Pod**: Deploys the initial primary pod (suffixed `-r-a`).
        - Uses a `SecurityContext` for best practices.
        - Uses an `initContainer` to set correct keyfile ownership and permissions (`999:999 0600`).
    - **Networking**: Creates a `Headless Service` for intra-cluster communication.
- **Cluster Bootstrapping**:
    - The operator waits for the Primary Pod to reach the `Ready` state (requeuing the reconciliation every 15 seconds if not ready).
    - **RS Initiation**: Once ready, the operator calls `POST /v1/initiate` on the sidecar.
    - **Admin User Creation**: The operator calls `POST /v1/admin` on the sidecar.
    - Both operations are designed to be **idempotent** to handle retries safely.
        - The instance manager returns a `409` response if each operation is already completed, and the operator handles this gracefully.
- **Transition**: Upon successful completion, the CR `status.phase` is updated to `Scaling`.

### 2. Scaling Phase (`status.phase = Scaling`)

Handles the addition or removal of members from the replica set.

## Security & RBAC

Security is implemented via Kubernetes RBAC and sidecar-based authentication.

### Authentication Flow

1. The operator sends an HTTP request with the ServiceAccount token in the `Authorization: Bearer {token}` header.
2. The `instance-manager` sidecar uses the `client-go` package to perform a `TokenReview`.
3. The sidecar verifies that the operator has the required permissions.

### RBAC Requirements

- **Operator ServiceAccount**: Requires `create` permission for `db.mrhachi.dev` group, `mongodbs` resource.
- **DB Pod ServiceAccount**: Requires `create` permission for `tokenreviews` and `subjectaccessreviews` (to allow the sidecar to perform identity verification).

## Naming & Labeling

### Naming Convention

Pods are named using the CR name followed by a suffix denoting their role and alphabetical order: `{CR-Name}-r-{suffix}` (e.g., `-r-a` for the first primary).

### Labeling Schema

Standardized labels are applied to all managed resources for discovery and management.

| Label Key                | Value        | Applied To                                 |
| :----------------------- | :----------- | :----------------------------------------- |
| `app.kubernetes.io/name` | `{CR-Name}`  | All resources                              |
| `db.mrhachi.dev/mongodb` | `{CR-Name}`  | All resources                              |
| `db.mrhachi.dev/role`    | `replica`    | Pods, ServiceAccounts                      |
| `db.mrhachi.dev/member`  | `r-{suffix}` | Pods, PVCs                                 |
| `db.mrhachi.dev/status`  | `in-use`     | PVCs (changes to `jettisoned` on deletion) |
