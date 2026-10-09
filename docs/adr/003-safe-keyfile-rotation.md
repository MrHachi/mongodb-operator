# ADR 003: Safe keyfile rotation for replica Pods

- Date: 2026-10-09
- Status: Open question
- Scope: Replica drain safety, MongoRS keyfile rotation

## Context

The operator replaces an existing replica Pod when its keyfile init container command or Secret volume setup is detemined to be outdated. The current path deletes the Pod directly. It does not check whether MongoDB reports that member as `PRIMARY` or `SECONDARY`, nor does it check replication lag or replica-set health first.

The Secret is shared by all members. If its key changes, newly created Pods can use the new key while existing Pods continue using the old key. A rolling replacement could therefore leave the replica set with members using different keys. At the same time, a replacement Pod can remain Pending because of scheduling or storage constraints, potentially preventing a rollout from completing.

## Open questions

1. Should replacement be allowed only after confirming the target member is a healthy `SECONDARY` and below the configured drain threshold? How should the operator proceed if the member is `PRIMARY`, its role cannot be determined, or the replica set is unhealthy?
2. Does MongoDB support a safe overlap period in which members accept both old and new key material, or must key rotation use a different protocol to avoid mixed-key authentication failures?
3. How should replacement handle unavailable capacity, unschedulable Pods, and retries so a blocked replacement does not cause unsafe progress or deadlock the rollout?
4. Should this path use a general member-drain workflow shared with scale-in, and how should it behave when the replica's current key has already drifted and replication is failing?

## Decision

No safe replacement or key-rotation protocol has been decided on yet. The current direct deletion path should be treated as provisional until these questions are resolved.
