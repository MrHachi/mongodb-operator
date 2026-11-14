# ADR 004: Selecting instance to drain on scale-in (normal conditions)

- Date: 2026-10-22
- Status: Pending implementation
- Scope: Replica drain safety, scaling behavior, availability

## Context

One goal of this operator is to implement MongoDB cluster topology and health-aware horizontal scale-in and scale-out.

The original proposed strategy was to drain the replica with the lowest replication lag (the most synchronized member). The rationale for this was to maximize the reusability of the replica's data when rejoined to the cluster, and in doing so reducing the risk and time cost of a full initial sync during reclamation.

## Problem

Prioritizing the most synchronized replica introduces two primary risks:

1.  **Quorum Fragility:** In a degraded topology where some nodes are unhealthy, blindly draining the "best" node may leave the cluster without enough healthy members to maintain quorum, turning a benign scaling event into an outage.
2.  **Degraded Failover Readiness:** By consistently removing the most caught-up secondaries, the operator systematically prunes the highest-quality candidates for Primary promotion, increasing the potential RTO during an actual failover.

## Decision

The operator will prioritize **Cluster Resilience** over **Recovery Efficiency**. During normal scale-in, the operator will select the **healthy secondary** with the **least recently applied optime**, provided that its removal preserves the required voting quorum and the replication health of the surviving set.

This approach ensures that the most synchronized replicas remain in the topology to provide the most robust failover pool.

We accept a higher likelihood of initial syncs upon PVC reuse as a trade-off for increased cluster stability.

### Execution Process

1. Identify the healthy secondary with the oldest applied optime that satisfies the safety constraints.
2. Mark the selected member as `draining`.
3. Ensure that instance-manager leadership is not on the selected member.
4. Wait for the member's durable optime to reach a fixed majority-commit checkpoint captured from the current primary. This ensures the retained PVC is consistent with the last known majority-committed state.
5. Remove the member from the replica set configuration (retaining its PVC).
6. Wait for the cluster to recognize the configuration change.
7. Shut down the drained member.

Scale-in is deferred if quorum, replication health, or configuration stability requirements would not be met by performing any step.
The operator does not perform forced reconfigurations or manage oplog rollbacks.

### Replication Health Definition

A replica is considered "healthy" if:

- It is voting-eligible.
- Its replication lag is within a defined threshold (ensuring it is not a "zombie" node whose removal would be trivial but whose presence is useless).

## Alternatives

### Drain the most caught-up member

This maximizes PVC reusability and minimizes initial syncs. However, it prioritizes efficiency over availability and fails to protect failover readiness.

## Open questions

1. What is the maximum allowable lag for a node to be considered "healthy" and eligible for draining?
2. Should this health threshold be configurable via the Custom Resource API to allow users to tune the trade-off between recovery speed and failover readiness?
