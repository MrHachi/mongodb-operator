# MongoDB API Specification (v0.1)

This document defines the proposed API surface for the `MongoDB` custom resource.

## API Group & Version

- **Group**: `db.mrhachi.dev`
- **Version**: `v1alpha1`
- **Kind**: `MongoDB`

## Spec (`spec`)

| Field                        | Type       | Description                                                                              |
| :--------------------------- | :--------- | :--------------------------------------------------------------------------------------- |
| `image`                      | `string`   | MongoDB container image. Planned default: `mongo:latest`.                                |
| `replicas`                   | `integer`  | Desired number of MongoDB members in the replica set.                                    |
| `scaling.drainThreshold`     | `duration` | Maximum replication lag allowed for a member to be eligible for culling during scale-in. |
| `scaling.pvcRetentionPeriod` | `duration` | Duration to retain `jettisoned` PVCs before they are cleaned up in the `Ready` phase.    |
| `users`                      | `list`     | List of administrative users to be managed by the operator.                              |

### MongoDB Container Image

The planned `spec.image` field defaults to `mongo:latest`. A custom image must be compatible with the operator's container configuration:

- The operator supplies MongoDB options through the container's Kubernetes `args`. Kubernetes passes these arguments to the image's `ENTRYPOINT` and uses them instead of the image's default `CMD`.
- Its `ENTRYPOINT` must start `mongod` with those supplied arguments. A wrapper script must forward all arguments to `mongod` and replace itself with the server process (for example, using `exec`). Do not put required MongoDB options only in the image's `CMD`.
- The image must provide `mongod` and support the operator's mounted data and keyfile paths, including `/data/db` and `/data/configdb/mongodb.key`.
- MongoDB must listen on port `27017` and accept the replica set and keyfile options supplied by the operator.

The MongoDB container has a TCP startup probe on port `27017` with a five-minute startup window, followed by a TCP liveness probe on the same port. The instance-manager's `/readyz` readiness probe pings MongoDB; the Pod becomes Ready when its application containers are ready.

### User Object

- `username`: `string` (Required)
- `password`: `string` (Optional, otherwise generated and surfaced in a new Secret in the same namespace as the cluster. Alternatively, provide the name of an existing Secret resource.)
- `roles`: `list[string]` (e.g., `userAdminAnyDatabase`, `clusterAdmin`)

## Status (`status`)

| Field        | Type     | Description                                                                      |
| :----------- | :------- | :------------------------------------------------------------------------------- |
| `phase`      | `string` | Current lifecycle phase: `Initializing`, `Progressing`, or `Ready`.              |
| `conditions` | `list`   | Standard `metav1.Condition` list reporting state (e.g., `Progressing`, `Ready`). |

### Status conditions (`status.conditions`)

| Condition           | Type   | Description                                                                                                                                                                          |
| :------------------ | :----- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ready`             | `bool` | Flag indicating whether the MongoDB cluster is eligible to serve applications.                                                                                                       |
| `Progressing`       | `bool` | Flag indicating whether reconciliation is currently proceeding.                                                                                                                      |
| `NeedsIntervention` | `bool` | Flag indicating that manual administrator intervention is required to alleviate service degradation.                                                                                 |
| `Degraded`          | `bool` | Flag indicating that the actual state cannot be reconciled with the desired state for the API surface owned by the operator. Note that a cluster can be both `Degraded` and `Ready`. |
