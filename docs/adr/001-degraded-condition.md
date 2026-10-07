# ADR 001: Independent degradation

- Date: 2026-10-07
- Status: Pending implementation
- Scope: Lifecycle dispatch, resource recovery, and prerequisites for safe reconciliation

## Context

Our lifecycle state machine (`Initializing` → `Progressing` → `Ready`) assumes a best-case deployment scenario:

1. **Initializing:** Create Kubernetes resources and perform one-time bootstrap tasks (ReplicaSet initiation, admin user creation).
2. **Progressing:** Reconcile observed cluster state against the desired custom resource template.
3. **Ready:** Steady state. Shifts back to `Progressing` when state drift is observed.
4. **Degraded:** Traps failures from any preceding phase to attempt recovery based on status reasons.

### Problem

`Degraded` lacks a clear return path. Once a failure is resolved, the operator must resume reconciliation from the cluster's pre-failure state.

Transitioning a recovered cluster directly to `Progressing` works for post-bootstrap failures, but breaks clusters that degraded during `Initializing`. Because `Progressing` skips one-time bootstrap steps, a cluster that degraded mid-initialization can never finish bootstrapping automatically—trapping it indefinitely or requiring manual operator intervention.

Preventing direct transitions from `Initializing` to `Degraded` isn't viable either. Many initialization failures share root causes with operational failures; special-casing `Initializing` duplicates error-handling logic and obscures recovery paths.

## Decision

We will demote `Degraded` from a standalone lifecycle phase to a status condition.

**Note:** A MongoDB can be `Degraded` and still serve traffic (`Degraded=True, Ready=True`).

## Options Considered

### Always transition `Degraded` to `Progressing`

In the event we enter `Degraded` from `Initializing`, we may skip essential one-time bootstrap steps.
This could result in a cluster becoming stuck in `Degraded` status.

### Prevent direct transitions from `Initializing` to `Degraded`

This would lead to repeating recovery logic and turn `Initializing` into a "special-case" reconciliation.

## Consequences

### Positive

- Cleaner state graph: `Initializing` -> `Progressing` <-> `Ready`
- Safer reconciliation from `Initializing` phase

### Negative / Trade-offs

- We need to clearly itemize which resource properties the operator owns
- We need to clearly define what constitutes "degraded service quality"
