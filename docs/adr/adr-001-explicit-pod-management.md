# ADR: Explicit Pod Management

## Context

The MongoDB operator initially used a Kubernetes `StatefulSet` to manage MongoDB member Pods.

However, MongoDB replica-set membership does not necessarily map cleanly to StatefulSet ordinals. During scale-in, MongoDB may require removing a member that is **not the highest ordinal**. Deleting that Pod while retaining higher ordinals causes the StatefulSet controller to recreate the deleted ordinal and remove a different Pod when the replica count is reduced.

This makes StatefulSet semantics incompatible with the operator's requirement to select MongoDB members for removal based on MongoDB topology and safety state.

## Decision

The operator will manage MongoDB member Pods explicitly rather than using a StatefulSet.

Each MongoDB member will have a stable logical identity managed by the operator. Pods and PVCs will be associated with that identity and reconciled independently.

MongoDB topology and member safety will determine which members may be removed during scale-in.

## Consequences

**Benefits**

- The operator can remove an arbitrary MongoDB member safely.
- MongoDB topology becomes the source of truth for member identity.
- Pod lifecycle can directly reflect MongoDB membership rather than StatefulSet ordinal semantics.

**Costs**

- The operator must implement Pod lifecycle management itself.
- Stable Pod/PVC association must be maintained explicitly.
- The operator assumes responsibility for behavior that StatefulSet previously provided, including replacement and lifecycle reconciliation.
