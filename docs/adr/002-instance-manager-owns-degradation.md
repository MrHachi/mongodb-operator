# ADR 002: Monitor MongoDB cluster health with instance manager

- Date: 2026-10-08
- Status: Pending implementation
- Scope: Cluster health monitoring, status reporting, and separation of responsibilities

## Context

The operator monitors cluster degradation via conditions on the MongoDB Custom Resource (CR).

Kubernetes provides mechanisms for monitoring Pod and container health, but these do not fully represent MongoDB replica-set health. Conditions such as primary unavailability, replication lag, and replica-set membership failures require MongoDB-specific observations.

### Problem

While the operator could periodically evaluate cluster health during reconciliation, this would couple health monitoring to the reconciliation schedule and introduce additional monitoring responsibilities into its resource-management logic.

Each MongoDB Pod already runs an instance manager responsible for MongoDB-specific operations and Pod health checks. These managers are therefore well positioned to observe database health independently of the operator.

We want to maintain a clear separation between observing cluster health and reconciling resources toward their desired state.

## Decision

We will delegate MongoDB cluster health evaluation and reporting to the instance managers. Resource reconciliation and recovery remain responsibilities of the operator.

The instance manager will monitor MongoDB cluster health and publish findings to the status subresource of the MongoDB CR. In turn, status condition reports by the instance manager will trigger the reconciliation by the operator, which will attempt to resolve them.

Once the instance manager observes that the degradation condition afflicting the cluster no longer holds, it will revoke the status condition. This should not trigger reconciliation, or it should trigger a no-op reconciliation.

Only the elected reporting manager will publish cluster-level health conditions.

Reporting leadership will be independent of MongoDB primary election. This ensures that the loss of the MongoDB primary does not inherently prevent another instance manager from reporting degradation.

The Kubernetes Lease will serve as the persistent coordination mechanism. Leadership state amongst the instance managers will be maintained independently of the operator.

It is important to recognize that lease-based election reduces competing writers but does not provide strict fencing. A former leader may briefly attempt to publish stale observations after leadership has transferred.

We accept this limitation because health reports are observational and idempotent, and the operator must independently verify current state before performing corrective actions.

### Status ownership

The instance manager will own the `Degraded` condition. The operator will retain ownership of lifecycle-related status phase and conditions.

Status updates will use the CR's /status subresource, with field ownership configured to prevent unrelated status fields from being overwritten.

Instance managers will receive only the Kubernetes API permissions necessary for health reporting and leader election, and these will be scoped as minimally as possible.

## Options Considered

### Allow all instance managers to report health conditions

Each instance manager could independently update the MongoDB CR's degradation condition. This would eliminate the need for reporting leader election but introduce competing writers and potentially conflicting observations.

Server-side apply can manage field ownership but does not resolve disagreements between multiple managers writing the same logical condition. At larger replica counts, simultaneous reporting during a widespread degradation event could also generate unnecessary Kubernetes API traffic and update contention.

We prefer a single elected reporting manager to coordinate aggregate health reporting.

### Evaluate cluster health during operator reconciliation

The operator could periodically query MongoDB and evaluate health during reconciliation. This would centralize CR status ownership and avoid a separate reporting leader.

However, it would couple health observation frequency to the controller's reconciliation behavior and require the operator to perform ongoing MongoDB-specific monitoring.

We prefer to keep health evaluation close to the database processes while allowing the operator to concentrate on resource management and recovery.

### Bind reporting leadership to the MongoDB primary

The MongoDB primary could be responsible for reporting cluster health.

However, primary unavailability is itself an important degradation condition. Coupling reporting leadership to database leadership would introduce an undesirable dependency on the component being monitored.

We therefore elect reporting leadership independently.

### Manage reporting leadership through the operator

The operator could select and track a reporting manager.

However, this would couple reporting leadership transitions to operator availability and require an additional mechanism for persisting leadership state.

Kubernetes Leases already provide persistent coordination and leader-election primitives. Introducing a separate mechanism would duplicate existing functionality without a clear benefit.

## Consequences

### Positive

- Separates database health observation from resource reconciliation.
- Allows MongoDB-specific monitoring to operate independently of reconciliation schedule.
- Establishes a single reporting authority during normal operation.
- Avoids competing status updates from multiple instance managers.
- Limits unnecessary Kubernetes API writes.
- Allows the operator to respond to degradation through its existing reconciliation model.
- Reuses Kubernetes-native coordination primitives without introducing an external persistence dependency.

### Negative / Trade-offs

- Introduces a separate leader-election domain and associated Lease management.
- Health reporting may be delayed during leadership transitions.
- Lease-based election does not strictly prevent stale-leader writes.
- Requires careful coordination of CR status field ownership between the operator and instance managers.
- Health reporting depends on the availability of an instance manager with Kubernetes API connectivity.
- The most recently reported condition may become stale when reporting is interrupted.
- The operator must tolerate duplicate, delayed, or obsolete health observations.

## Open questions

1. What MongoDB-specific criteria constitute cluster degradation, and when should health be reported as Unknown?
2. At what threshold should we report failures to filter out transient problems and prevent rapid condition flapping?
3. Should health reports include observation timestamps or other metadata to help identify stale observations?
4. Do a 15s Lease duration and 5s renewal interval provide an appropriate balance between reporting availability and Kubernetes API traffic?
