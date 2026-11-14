# Instance-Manager API Documentation

The instance-manager is a Go-based sidecar service (using the Chi router) running inside each MongoDB Pod. It provides a secure interface for the MongoDB Operator to perform sensitive administrative operations on the database.

## General Information

- **Port**: `8080`
- **Protocol**: `HTTP/1.1`
- **Authentication**: All requests must include a Kubernetes ServiceAccount token in the header:
  `Authorization: Bearer <token>`

### Security Enforcement

The sidecar verifies the operator's identity using the Kubernetes API. It performs a `TokenReview` and `SubjectAccessReview` against the Kubernetes API.

Specifically, the operator's ServiceAccount must have:

- `create` permission for the `mongodbs` resource in the `db.mrhachi.dev` API group.

## API Endpoints

### 1. Replica Set Initiation

Initiates the MongoDB Replica Set for the cluster.

- **Endpoint**: `POST /v1/initiate`
- **Description**: Triggers the MongoDB `rs.initiate()` command.
- **Idempotency**: This endpoint must be idempotent. If the Replica Set is already initialized, it should return a `409` response.
- **Implementation Detail**: The sidecar should use the MongoDB Go driver to execute the command.

### 2. Admin User Creation

Creates the administrative user for the MongoDB instance.

- **Endpoint**: `POST /v1/admin`
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

## Configuration

The following environment variables must be provided to the instance-manager container. These are injected by the MongoDB Operator.

| Environment Variable | Description | Default |
|----------------------|-------------|---------|
| `MONGODB_RS_NAME` | The name of the MongoDB Replica Set. | (Required) |
| `MONGODB_HOSTNAME` | The hostname of the MongoDB instance. | (Required) |
| `MONGODB_SERVICE_NAME` | The Kubernetes service name for the MongoDB instance. | (Required) |
| `MONGODB_NAMESPACE` | The Kubernetes namespace where the MongoDB instance is running. | (Required) |
| `MONGODB_URI` | The MongoDB connection URI. | `mongodb://localhost:27017` |
| `PORT` | The port on which the server listens. | `8080` |
