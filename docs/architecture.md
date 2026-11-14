# MongoDB Operator Architecture

This document outlines the design and architectural decisions for the MongoDB operator.

## Core Components

### MongoDB Operator

The central controller that manages the lifecycle of MongoDB clusters through a reconciliation loop. It transitions the custom resource (CR) through different phases (`Initializing`, `Progressing`, or `Ready`). The `Progressing` condition reports the current operation through its reason and message, including the current initialization step.

**Note on Pod Management**: The operator does **not** use `StatefulSets` for pod management. Instead, it manages Pods directly to allow granular control over which members are removed during scaling-in, ensuring they are not deleted while the database is in an unstable replication state.

### Instance-Manager Sidecar

A Go-based service (using the Chi router) deployed as a sidecar container within each MongoDB Pod.

- **Responsibility**: Performs sensitive operations that require interaction with the database itself, such as initiating the Replica Set (RS) and creating administrative users.
- **Health Monitoring**: Monitors MongoDB cluster health and reports findings to the MongoDB CR status subresource. To avoid competing writers, instance managers use Kubernetes Leases to elect a single reporting leader.
- **Communication**: The operator communicates with the sidecar via the Pod's raw IP address on port `8080`.
- **Authentication**: The sidecar validates the operator's identity by performing `TokenReview` and `SubjectAccessReview` against the Kubernetes API.

## Reconciliation Lifecycle

The reconciliation loop follows a state-machine approach to ensure the desired state is reached.

The reconciliation flow follows these steps:

1.  **Degradation Recovery**: Attempt recovery based on status conditions found on the MongoDB CR.
2.  **Main Reconciliation**: If degradation recovery is successful, or if it is unsuccessful but the cluster is still eligible to proceed, the operator proceeds to the main reconciliation, which branches based on the current `status.phase`.
3.  **Status Evaluation**: Always evaluate and update operator-owned status conditions (presently `Ready` and `Progressing`) to reflect the current state of the cluster.

The MongoDB watch enqueues reconciliation for resource creation and deletion, spec generation changes, changes to `status.phase`, and transitions of non-lifecycle status conditions to `True`. Changes to `Ready` or `Progressing` alone do not enqueue reconciliation. Before writing status, the controller fetches the latest resource and retries on resource-version conflicts. It only applies the write if the resource's generation still matches the one observed at the start of reconciliation; otherwise it stops and immediately requeues to process the new spec.

### 1. Initialization Phase (`status.phase = Initializing`)

This phase focuses on setting up the first primary node and the foundational cluster infrastructure.

- Before contacting any instance-manager, the operator creates a ClusterRoleBinding granting the namespace's `default` ServiceAccount permission to create TokenReviews and SubjectAccessReviews. A finalizer removes this cluster-scoped binding when the MongoDB resource is deleted.
- Before creating the initial `-r-a` member, the operator lists managed Pods and queries their instance-manager topology endpoints. If a primary is reported, initialization resumes against that Pod. A fresh `-r-a` member is created only when no managed Pods are found.

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
        - **RS Status Check**: the operator calls `GET /v1/mongo/topology` on the instance manager to check RS initiation status.
        - **Execute RS Initiation**: if `GET /v1/mongo/topology` returns a `412 not initialized` status, the operator calls `POST /v1/mongo/initiate`.
    - **Admin User Creation**: once the MongoDB primary is confirmed to be running in RS mode, the operator creates the admin user.
        - **Admin User Auth Attempt**: the operator attempts to authenticate by calling `POST /v1/mongo/authenticate` with the admin user's credentials on the instance manager.
        - **Execute Admin User Creation**: if `POST /v1/mongo/authenticate` returns a `401 unauthenticated` status, the operator calls `POST /v1/mongo/admin` on the instance manager with the admin user's credentials.
- **Transition**: After cluster bootstrap, the CR `status.phase` is updated to `Progressing`. Its `Progressing` condition uses the `ReconcilingMembers` reason while replica set members are reconciled.

### 2. Progressing Phase (`status.phase = Progressing`)

Represents ongoing reconciliation after bootstrap. The operator ensures the cluster reaches the desired replica count and configuration.

**Reconciliation Workflow:**

1.  **Infrastructure Reconciliation**: Ensures all foundational Kubernetes resources exist and are correct:
    - Headless Service for intra-cluster discovery.
    - Keyfile Secret and connection ConfigMaps.
    - A ClusterRoleBinding that grants the MongoDB Pod's namespace `default` ServiceAccount `create` on TokenReviews and SubjectAccessReviews. A MongoDB finalizer removes the binding when the resource is deleted.
2.  **Scaling Logic**:
    - The operator compares the desired replica count with the number of managed MongoDB Pods.
        - **Important:** This is the initial scaling basis; using replica-set membership or healthy voting members reported by MongoDB may be more appropriate in the future, especially during network partitions or transient failures.
    - Each reconciliation performs at most one member scale operation, then requeues and observes the cluster again before taking another action. This limits quorum risk and avoids making several decisions from stale observations.
    - **Scale-In (Draining)**:
        1.  Select the healthy `SECONDARY` with the least recently applied optime, among members whose replication lag is within the configured `drainThreshold`. This preserves the most synchronized members as failover candidates.
        2.  Read replica-set status from `GET /v1/mongo/topology` and voting/election settings from `GET /v1/mongo/config`. Defer if the reported topology and configuration versions differ.

        3.  Mark the selected member as draining and ensure instance-manager leadership is not on that member.
        4.  Capture a fixed majority-commit checkpoint from the current primary and wait for the selected member's durable optime to reach it.
        5.  Remove the member from the MongoDB Replica Set configuration and wait for the configuration change to be recognized.
        6.  Shut down the drained member while retaining its PVC. Set the PVC label `db.mrhachi.dev/status=jettisoned`, annotation `db.mrhachi.dev/jettisoned-at` (UTC timestamp), and annotation `db.mrhachi.dev/data-time` to the captured majority-commit checkpoint time. Timestamps use UTC RFC3339 format.

        Defer scale-in if removing the candidate would violate voting quorum, survivor replication health, or replica-set configuration stability. The operator does not force reconfiguration or manage oplog rollbacks.

    - **Scale-Out (Expansion)**:
        1.  **Discover a PVC to reclaim**: Search managed PVCs marked `jettisoned`. When reclaiming PVCs, the PVC with the latest valid `db.mrhachi.dev/data-time` is always prioritized because it contains the most recent majority-committed checkpoint. Tie-breaking is done by choosing the lowest member ordinal in alphabetical order. PVCs with missing or invalid data-time annotations rank behind PVCs with valid timestamps.
        2.  **Provision**: If a PVC was selected, create a Pod using its member suffix and return the PVC to `in-use`. Otherwise, create a PVC and Pod using the lowest available member suffix, filling gaps in the sequence. Suffixes use bijective base 26: `a` through `z`, then `aa` through `az`, `ba`, and so on.
        3.  **Join**: Call the appropriate `instance-manager` API to add the new or revived member to the RS configuration.
        4.  Requeue and observe again before provisioning another member if the desired Pod count is still not met.
3.  **Internal Resource Reconciliation**:
    - Reconcile MongoDB-internal users defined in the CRD via the `instance-manager`.
    - Finalize topology settings.
4.  **Quorum Stability Check**:
    - The cluster is considered "Stable" when at least $n/2+1$ members are in a voting state (`PRIMARY` or `SECONDARY`).
    - The operator waits for this quorum to persist for a configured number of consecutive minutes.

**Transition**: Once the quorum is stable, `status.phase` transitions to `Ready`. The elected leader instance-manager reports `Degraded=True` with reason `ServiceIssuesPresent` when one or more cluster degradation conditions are set. A cluster may remain `Ready` while degraded.

**Note**: Container image version updates and scale-to-zero remain open problems for future development.

## Security & RBAC

Security is implemented via Kubernetes RBAC and sidecar-based authentication.

### Authentication Flow

1. The operator requests a short-lived ServiceAccount token through the Kubernetes TokenRequest API with the `db.mrhachi.dev/mongodb/instance-manager` audience.
2. The operator sends the token in the `Authorization: Bearer {token}` header.
3. The `instance-manager` sidecar validates the token with a `TokenReview` for that audience.
4. The sidecar checks the operator's permissions with a `SubjectAccessReview` in the MongoDB resource's namespace.

### RBAC Requirements

- **Operator ServiceAccount**: Requires `create` permission on its own `serviceaccounts/token` resource, `create` / `get` permission for `db.mrhachi.dev` group, `mongodbs` resource, and `get` / `create` / `update` / `delete` permission for the per-MongoDB ClusterRoleBindings it reconciles.
- **DB Pod ServiceAccount**: The operator binds the namespace `default` ServiceAccount to a ClusterRole with `create` permission for `tokenreviews` and `subjectaccessreviews`, allowing the sidecar to verify identity and authorization. This requires cluster-scoped RBAC.

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
