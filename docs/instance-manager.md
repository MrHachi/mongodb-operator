# Instance-Manager API Documentation

The instance-manager is a Go-based sidecar service (using the Chi router) running inside each MongoDB Pod. It provides a secure interface for the MongoDB Operator to perform sensitive administrative operations on the database.

GHCR repository tag: `ghcr.io/mrhachi/mongodb-instance-manager`

## General Information

- **Port**: `8080`
- **Protocol**: `HTTP/1.1`
- **Authentication**: All requests must include a short-lived Kubernetes ServiceAccount token in the header:
  `Authorization: Bearer <token>`. The operator requests this token for the `db.mrhachi.dev/mongodb/instance-manager` audience.
- **Authorization scope**: The sidecar checks permissions in the MongoDB namespace configured through `MONGODB_NAMESPACE`; callers do not provide a namespace header.

### Health Monitoring & Reporting

- **Responsibility**: The instance-manager monitors MongoDB cluster health (e.g., replication lag, primary availability) and reports findings to the MongoDB CR status subresource.
- **Leadership Election**: To ensure a single source of truth and prevent competing writes to the CR status, instance managers use Kubernetes Leases to elect a single reporting leader. Only the elected leader publishes cluster-level health conditions.

### Security Enforcement

The sidecar verifies caller tokens with a Kubernetes `TokenReview`. The `/v1/mongo` endpoints also check MongoDB-resource permissions with a `SubjectAccessReview`. The `/v1/manager` endpoints instead require the authenticated username to exactly match the configured operator ServiceAccount namespace and name.

The operator's ServiceAccount must have:

- `create` permission on `serviceaccounts/token` for its own ServiceAccount.
- `create` and `get` permissions for `mongodbs` in the `db.mrhachi.dev` API group.
- `get`, `create`, `update`, and `delete` permissions for the per-MongoDB ClusterRoleBindings it reconciles.

The manager endpoints accept tokens only for this operator ServiceAccount identity. The operator injects its ServiceAccount namespace and name into each instance-manager Pod as `OPERATOR_SERVICE_ACCOUNT_NAMESPACE` and `OPERATOR_SERVICE_ACCOUNT_NAME`.

MongoDB Pods currently use the namespace's `default` ServiceAccount. During initialization, the operator creates a ClusterRoleBinding from that ServiceAccount to the `instance-manager-auth-role` ClusterRole, which grants only `create` on `tokenreviews` and `subjectaccessreviews`. The binding is removed by a MongoDB finalizer when the resource is deleted. This requires cluster-scoped RBAC; Helm installs must use `rbac.namespaced=false`.

## API Endpoints

### 1. Replica Set Initiation

Initiates the MongoDB Replica Set for the cluster.

- **Endpoint**: `POST /v1/mongo/initiate`
- **Description**: Triggers the MongoDB `rs.initiate()` command.
- **Idempotency**: This endpoint must be idempotent. If the Replica Set is already initialized, it should return a `409` response.
- **Implementation Detail**: The sidecar should use the MongoDB Go driver to execute the command.

### 2. Admin User Creation

Creates the administrative user for the MongoDB instance.

- **Endpoint**: `POST /v1/mongo/admin`
- **Description**: Creates a dedicated administrative user with required privileges for cluster management.
- **Idempotency**: This endpoint must be idempotent. If the admin user already exists, it should return a `409` response.
- **Request Body**:
    ```json
    {
        "username": "string",
        "password": "string"
    }
    ```
- **Implementation Detail**: The sidecar should use the MongoDB Go driver to execute the command.

### 3. Get Cluster Topology

Retrieves the current MongoDB replica set status, including member states, applied and durable optimes, and replication lag derived from the primary's applied optime.

- **Endpoint**: `GET /v1/mongo/topology`
- **Description**: Returns the `replSetGetStatus` view. Configuration is returned separately by `GET /v1/mongo/config`.
- **Response Body**:
    ```json
    {
        "config_version": 3,
        "members": [
        {
            "id": 0,
            "host": "mongodb-0.example.com:27017",
            "state": "PRIMARY",
            "state_code": 1,
            "optimes": {
                "applied": {"timestamp": {"T": 1710000000, "I": 1}, "term": 2},
                "durable": {"timestamp": {"T": 1710000000, "I": 1}, "term": 2},
                "applied_at": "2024-03-09T16:00:00Z",
                "durable_at": "2024-03-09T16:00:00Z"
            },
            "replication_lag": 0,
            "lag_available": true
        },
        ...
        ]
    }
    ```
- **Errors**:
    - `412 Precondition Failed`: Returned if the cluster is not yet initialized.
    - `500 Internal Server Error`: For other server-side errors.

### 4. Get Replica Set Configuration

Retrieves the current replica set voting and election configuration.

- **Endpoint**: `GET /v1/mongo/config`
- **Description**: Returns the `replSetGetConfig` settings, including each member's voting eligibility, votes, priority, and arbiter/hidden flags. `replication_lag` in the topology response is an integer number of nanoseconds because it is serialized from Go's `time.Duration`.
- **Response Body**:
    ```json
    {
        "version": 3,
        "members": [
            {
                "id": 0,
                "config": {
                    "version": 3,
                    "votes": 1,
                    "priority": 1,
                    "voting_eligible": true
                }
            }
        ]
    }
    ```
- **Errors**:
    - `412 Precondition Failed`: Returned if the cluster is not yet initialized.
    - `500 Internal Server Error`: For other server-side errors.

### 5. Authenticate

Authenticates with the MongoDB instance and updates the connection credentials for subsequent requests.

- **Endpoint**: `POST /v1/mongo/authenticate`
- **Description**: Verifies the provided credentials and replaces the current MongoDB client with a new one using the given authentication information.
- **Request Body**:
    ```json
    {
        "username": "string",
        "password": "string",
        "auth_source": "string"
    }
    ```
- **Errors**:
    - `401 Unauthorized`: If authentication fails or credentials are incorrect.
    - `400 Bad Request`: If the request body is invalid.
    - `500 Internal Server Error`: For other server-side errors.

### 6. Get Instance-Manager Leadership

Reports the node currently holding instance-manager leadership.

- **Endpoint**: `GET /v1/manager/leadership`
- **Authentication**: Requires a token for the exact operator ServiceAccount configured on the instance-manager Pod.
- **Response Body**:
    ```json
    {
        "leader": "-r-0"
    }
    ```
- **Implementation Status**: Leadership state is not implemented yet. The endpoint logs a warning and returns `-r-0`, a placeholder that cannot match the operator's alphabetically suffixed replica Pod names.

### 7. Request Leadership Transfer

Requests that the instance-manager move leadership away from the node receiving the request.

- **Endpoint**: `POST /v1/manager/leadership/transfer`
- **Authentication**: Requires a token for the exact operator ServiceAccount configured on the instance-manager Pod.
- **Response**: `202 Accepted`.
- **Implementation Status**: Leadership transfer is not implemented yet. The endpoint logs a warning and accepts the request as a no-op.

## Configuration

The following environment variables must be provided to the instance-manager container. These are injected by the MongoDB Operator.

| Environment Variable   | Description                                                     | Default                     |
| ---------------------- | --------------------------------------------------------------- | --------------------------- |
| `MONGODB_RS_NAME`      | The name of the MongoDB Replica Set.                            | (Required)                  |
| `MONGODB_HOSTNAME`     | The hostname of the MongoDB instance.                           | (Required)                  |
| `MONGODB_SERVICE_NAME` | The Kubernetes service name for the MongoDB instance.           | (Required)                  |
| `MONGODB_NAMESPACE`    | The Kubernetes namespace where the MongoDB instance is running. | (Required)                  |
| `OPERATOR_SERVICE_ACCOUNT_NAMESPACE` | The namespace of the operator ServiceAccount authorized for manager endpoints. | (Required) |
| `OPERATOR_SERVICE_ACCOUNT_NAME` | The name of the operator ServiceAccount authorized for manager endpoints. | (Required) |
| `MONGODB_URI`          | The MongoDB connection URI.                                     | `mongodb://localhost:27017` |
| `PORT`                 | The port on which the server listens.                           | `8080`                      |
