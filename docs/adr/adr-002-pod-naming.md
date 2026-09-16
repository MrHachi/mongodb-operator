# ADR: MongoDB Pod Naming

## Context

MongoDB members need stable Kubernetes identities so that Pods can be recreated without changing the logical MongoDB member they represent.

Replcia roles such as `PRIMARY` and `SECONDARY` are unsuitable as Pod identity because MongoDB roles are dynamic. A secondary can become primary following an election, and a primary can later become secondary.

Arbitrary Kubernetes-generated names would likewise make stable replica-to-Pod and replica-to-PVC association unnecessarily difficult.

## Decision

MongoDB member Pods will use deterministic names based on their stable member identity:

```text
<cluster>-r-<replica-id>
<cluster>-a-<arbiter-id>
```

For example:

```text
mydb-r-0
mydb-r-1
mydb-r-2
mydb-a-0
```

The `m-#` and `a-#` suffixes identify **member identity/type**, not MongoDB's current topology role.

MongoDB roles such as primary and secondary will be represented through observed MongoDB state and/or Kubernetes status rather than encoded in Pod names.

## Consequences

**Benefits**

- Member identity remains stable across MongoDB elections and Pod recreation.
- Pod and PVC association can be deterministic.
- Names remain meaningful without conflating identity with topology role.
- The operator can directly derive the expected Pod name from a member identity.

**Costs**

- The operator must maintain the member-to-resource association itself.
- Member IDs should not be casually reused if doing so could cause an old PVC or MongoDB identity to be mistaken for a new member.
