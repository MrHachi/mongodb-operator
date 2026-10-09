# Testing Strategy

This document outlines the testing strategy for the MongoDB Operator, organized by the lifecycle phases of a `MongoDB` resource.

## Overview

The testing strategy focuses on ensuring the operator is **idempotent**, **resilient**, and **self-healing**. We use [Ginkgo](https://ginkgo.dev/) and [Gomega](https://github.com/onsi/gomega) for BDD-style testing, primarily through e2e tests that run against an isolated [Kind](https://kind.sigs.k8s.io/) cluster.

## Phase: Initializing

The `Initializing` phase is the most critical for cluster bootstrapping. Tests in this phase ensure that the operator can transition a cluster from "nothing" to a "running replica set" even in the face of various failures.

### 1. Happy Path

- **Scenario**: A new `MongoDB` resource is created with valid configuration.
- **Expected Outcome**: The operator successfully provisions all resources (Secret, PVC, Pod, Service) and transitions the CR to the `Progressing` phase.

### 2. Operator Resiliency (Idempotency)

- **Scenario**: The operator process is killed (e.g., `kubectl delete pod <operator-pod>`) midway through the initialization process (e.g., after the Pod is created but before RS initiation).
- **Expected Outcome**: Upon restart, the operator discovers the existing resources and resumes the reconciliation loop without creating duplicates, eventually reaching the `Progressing` phase.

### 3. Resource Recovery (Self-Healing)

- **Scenario**: A managed resource (e.g., the Primary Pod or the Headless Service) is deleted manually via `kubectl delete`.
- **Expected Outcome**: The operator detects the missing resource during the next reconciliation loop and recreates it, eventually completing the initialization.

### 4. Error Handling & Status Conditions

- **Scenario**: A required resource is present but invalid (e.g., the Keyfile Secret exists but contains no data).
- **Expected Outcome**: The operator sets the `Degraded` status condition to `True` with a specific, actionable `Reason` and `Message` (e.g., `ReasonCredentialsIncomplete`).

#### 5. Service Type Mismatch (Drift)

- **Scenario**: The managed Headless Service is deleted and replaced with a standard ClusterIP Service out of band.
- **Expected Outcome**: The operator detects the mismatch between the desired state (Headless) and the actual state (ClusterIP) and fails reconciliation (recreating the Service could be unsafe).

---

## Phase: Progressing

_(Planned: Tests for scaling, member addition/removal, and configuration updates)_

## Phase: Ready

_(Planned: Tests for stable state maintenance and observation)_

## Degradation detection

Tests in this phase ensure that the operator correctly reacts to cluster health degradation reported by the instance managers.

### 1. Health Reporting (Instance Manager)

- **Scenario**: A cluster experiences a health issue (e.g., artificial replication lag introduced with `tc`).
- **Expected Outcomes**:
    - The elected instance-manager leader correctly detects the issue and updates the `Degraded` condition on the MongoDB CR.
    - The operator's reconciliation loop detects the change and attempts to resolve the underlying issue (e.g., through resource recovery or scaling).

### 2. Recovery from Degradation

- **Scenario**: The underlying health issue is resolved (e.g., replication lag subsides).
- **Expected Outcome**: The instance-manager leader revokes the `Degraded` condition, and the operator performs a no-op or a successful reconciliation to return to the `Ready` phase.

### 3. Leadership Transitions

- **Scenario**: A leadership transition occurs among the instance managers in event the leader fails to renew its Lease.
- **Expected Outcome**: The new leader takes over reporting without causing conflicting or duplicate `Degraded` conditions.
