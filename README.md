# SingleTenantMongoDB Controller

A simple Kubernetes Operator for managing single-tenant MongoDB replica sets.

It allows applications to consume MongoDB through a declarative Custom Resource rather than manually managing StatefulSets, Secrets, and replica set administration.

## Getting started

1. Install Helm chart (controller and CRD)

    ```bash
    helm install mongodb-operator oci://ghcr.io/mrhachi/charts/mongodb-operator --namespace mongodb-system
    ```

2. Deploy user secrets

    ```bash
    kubectl -n sample-app create secret generic singletenantmongodb-sample-admin-pass \
        --from-literal=password=Ar34l5ecureP@ssw0rd?
    kubectl -n sample-app create secret generic singletenantmongodb-sample-app-user-pass \
        --from-literal=password=An0th3r1.
    kubectl -n sample-app create secret generic singletenantmongodb-sample-operation-user-pass \
        --from-literal=password=4N0THER0N3.
    ```

3. Install MongoDB custom resource

    ```bash
    kubectl -n sample-app apply -f singletenantmongodb.yml
    ```

    singletenantmongodb.yml
    ```yml
    apiVersion: db.mrhachi.dev/v1alphav1
    kind: SingleTenantMongoDB
    metadata:
      labels:
        app.kubernetes.io/name: controller
        app.kubernetes.io/managed-by: kustomize
      name: singletenantmongodb-sample
    spec:
      databaseName: sample
      replicas: 2
    
      admin:
        username: admin
        secretRef:
          name: singletenantmongodb-sample-admin-pass
    
      users:
        - username: app
          secretRef:
            name: singletenantmongodb-sample-app-user-pass
          roles:
            - role: readWrite
              database: sample
    
        - username: operation
          secretRef:
            name: singletenantmongodb-sample-operation-user-pass
          roles:
            - role: read
              database: sample
    
      storage:
        size: "2Gi"
    
      resources:
        requests:
          cpu: "500m"
          memory: "256Mi"
        limits:
          cpu: "1"
          memory: "512Mi"
    
    ```

## SingleTenantMongoDB Custom Resource API

Represents a single MongoDB replica set managed by the operator.

Creating a `SingleTenantMongoDB` causes the operator to create and reconcile the following:

- MongoDB StatefulSets
- MongoDB Services
- Connection information ConfigMap
- Replica set topology
- Application user roles and passwords
- MongoDB replica set keyfile secrets

### spec.databaseName

The default MongoDB database used.

### spec.replicas

Number of MongoDB replica set members.

### spec.admin

Defines the MongoDB administrator account.

#### username

MongoDB administrator username

#### secretRef.name

Kubernetes Secret containing the administrator password

The referenced Secret must contain the secret under the `password` key.

### spec.users

Defines MongoDB application users managed by the operator.

#### [].username

MongoDB administrator username

#### [].secretRef.name

Kubernetes Secret containing the administrator password

The referenced Secret must contain the secret under the `password` key.

#### [].roles

**[].role**: MongoDB user role
**[].database**: Database against which the role applies

### spec.storage

Defines persistent storage allocated to each MongoDB replica.

Example:

```yaml
storage:
  size: "10Gi"
  ```
  
The value is applied to the StatefulSet volume claims.

### spec.resources

Defines Kubernetes resource requests and limits for MongoDB containers.

## Managed resources

A `SingleTenantMongoDB` resource creates:

- StatefulSet
- Headless Service
- PersistentVolumeClaims
- Connection ConfigMap
- MongoDB authentication Secrets

Resources are owned by the Custom Resource and are garbage collected when the resource is deleted.
