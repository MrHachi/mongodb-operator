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

- Before creating the initial `-r-a` member, the operator lists managed Pods and queries their instance-manager topology endpoints. If a primary is reported, initialization resumes against that Pod. If managed Pods exist but no primary is reported, the cluster moves to `Degraded` and recovery reconciliation retries discovery. A fresh `-r-a` member is created only when no managed Pods are found.

- **Resource Provisioning**:
    - **Keyfile**: Generates a secure keyfile and stores it in a Kubernetes `Secret`.
    - **Storage**: Provisions a `PersistentVolumeClaim` (PVC) for database data.
    - **Primary Pod**: Deploys the initial primary pod (suffixed `-r-a`).
        - Uses a `SecurityContext` for best practices.
        - Uses an `initContainer` to set correct keyfile ownership and permissions (`999:999 0600`).
    - **Networking**: Creates a `Headless Service` for intra-cluster communication.
- **Cluster Bootstrapping**:
    - The operator waits for the Primary Pod to reach the `Ready` state (requeuing the reconciliation every 15 seconds if not ready).
    - **RS Initiation**: once the Pod becomes ready, the operator does the following:
        - **RS Status Check**: the operator calls `GET /v1/topology` on the instance manager to check RS initiation status.
        - **Execute RS Initiation**: if `GET /v1/topology` returns a `412 not initialized` status, the operator calls `POST /v1/initiate`.
    - **Admin User Creation**: once the MongoDB primary is confirmed to be running in RS mode, the operator creates the admin user.
        - **Admin User Auth Attempt**: the operator attempts to authenticate by calling `POST /v1/authenticate` with the admin user's credentials on the instance manager.
        - **Execute Admin User Creation**: if `POST /v1/authenticate` returns a `401 unauthenticated` status, the operator calls `POST /v1/admin` on the instance manager with the admin user's credentials.
- **Transition**: Upon successful completion, the CR `status.phase` is updated to `Scaling`.

### 2. Scaling Phase (`status.phase = Scaling`)

Handles the addition or removal of members from the replica set.

## Security & RBAC

Security is implemented via Kubernetes RBAC and sidecar-based authentication.

### Authentication Flow

1. The operator requests a short-lived ServiceAccount token through the Kubernetes TokenRequest API with the `db.mrhachi.dev/mongodb/instance-manager` audience.
2. The operator sends the token in the `Authorization: Bearer {token}` header.
3. The `instance-manager` sidecar validates the token with a `TokenReview` for that audience.
4. The sidecar checks the operator's permissions with a `SubjectAccessReview` in the MongoDB resource's namespace.

### RBAC Requirements

- **Operator ServiceAccount**: Requires `create` permission on its own `serviceaccounts/token` resource, and `create` / `get` permission for `db.mrhachi.dev` group, `mongodbs` resource.
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
