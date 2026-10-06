# MongoDB API Specification (v0.1)

This document defines the proposed API surface for the `MongoDB` custom resource.

## API Group & Version
- **Group**: `db.mrhachi.dev`
- **Version**: `v1alpha1`
- **Kind**: `MongoDB`

## Spec (`spec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `integer` | Desired number of MongoDB members in the replica set. |
| `scaling.drainThreshold` | `duration` | Maximum replication lag allowed for a member to be eligible for culling during scale-in. |
| `scaling.pvcRetentionPeriod` | `duration` | Duration to retain `jettisoned` PVCs before they are cleaned up in the `Ready` phase. |
| `users` | `list` | List of administrative users to be managed by the operator. |

### User Object
- `username`: `string` (Required)
- `password`: `string` (Optional, otherwise generated)
- `roles`: `list[string]` (e.g., `userAdminAnyDatabase`, `clusterAdmin`)

## Status (`status`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `phase` | `string` | Current lifecycle phase: `Initializing`, `Progressing`, `Ready`, or `Degraded`. |
| `conditions` | `list` | Standard `metav1.Condition` list reporting state (e.g., `Progressing`, `Ready`). |
| `members` | `list` | Detailed status of each member in the cluster. |

### Member Object
- `id`: `string` (The member suffix, e.g., `r-a`).
- `role`: `string` (`PRIMARY`, `SECONDARY`, `RECOVERING`, or `UNKNOWN`).
- `replicationLag`: `duration` (Observed lag behind the primary).
- `isVoting`: `boolean` (Whether the member is currently a voting member of the RS).
- `podName`: `string` (The name of the associated Kubernetes Pod).
