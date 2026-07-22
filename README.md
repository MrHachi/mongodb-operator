# SingleTenantMongoDB Controller

A simple Kubernetes Controller for managing single-tenant MongoDB replica sets.

It automates deployment, replica set initialization, topology reconciliation, and application user management so that applications can consume MongoDB through a simple Custom Resource rather than manually managing StatefulSets and replica set administration.

## Features

- Deploys MongoDB StatefulSets
- Manages replica set topology
- Bootstraps MongoDB automatically (in container image)
- Reconciles application users
- Rotates passwords
- Generates MongoDB keyfile secrets
- Publishes connection information

## Reconciliation

```mermaid

flowchart TD
    subgraph C[DB Bootstrap]
        CA[Get pod ordinal zero]
        CB[Initiate RS via pod exec]
        CC[Create admin via pod exec]

        CA -- not found, retry --> CA
        CA -- found --> CB
        CB --> CC
    end

    subgraph D[DB state reconciliation]
        DA[Reconcile RS topology]
        DB[Reconcile app users]

        DA --> DB
    end

    A{s}
    B[Kubernetes resource reconciliation]

    E[DB user secret reconciliation]

    A --> B
    B -- database not initialized --> C
    B -- database initialized --> D
    C --> D
    D --> E

```
