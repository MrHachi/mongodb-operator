# Lessons learned

## Background

I started by writing my own operator from scratch, but it quickly became clear that proceeding with Kubebuilder was the superior option when considering maintainability and ease of iteration.

With that said, starting from scratch did teach me some lessons:
- Kubernetes object ownership (for garbage collection)
- CRD and API type registration against the runtime scheme
- Order of operations for DB reconciliation
- The Kubernetes API
- Kubernetes cross-namespace RBAC (should probably be running ClusterRoles for a real operator)

### Kubernetes object ownership (for garbage collection)

- When we delete a CRD, we want its associated resources to be cleaned up as well
- This can be accomplished via the Kubernetes API before resource creation

### CRD and API type registration against the runtime scheme

- Performed by registering the types against a runtime Scheme struct using Go
- As such, (and this should be fairly obvious, but) this only affects the controller and has no effect on Kubernetes itself

### Order of operations for DB reconciliation

1. Kubernetes resource reconciliation from CRD definition
2. DB bootstrapping (RS initiation, admin user creation)
    - For MongoDB this seems to either require an exec into the pod or an idempotent bootstrap step in the image's entrypoint script (can't connect over network pre-initiation, so we need to leverage the localhost exception)
    - At first, I'd attempted to implement this via pod exec from the controller, but this was cumbersome and didn't play well with RBAC, so I moved it to the container image's entrypoint
3. DB state management
    - Replica set topology reconciliation
    - App user reconciliation (password and roles)
    - The controller performs these with the MongoDB client using the admin user
4. Secret (MongoDB RS keyfile) lifecycle management

### Why I stopped generating YAML

- Before writing this operator I'd only interacted with Kubernetes through manifests and Helm
- Building directly against the Kubernetes API changed how I think about Kubernetes—from "configuration files" to an object-oriented API with desired state

### Kubernetes cross-namespace RBAC

- Kubebuilder-generated controller code uses ClusterRoles, but in my bespoke operator I'd been running Roles and RoleBindings
- The ServiceAccount exists in the controller's namespace, and the Role and RoleBinding exist in the managed namespace
- This doesn't scale very well, so ClusterRoles do make more sense for most use cases
  - Of course, some use cases may require adherence to the principle of least privilege, but for a general-use MongoDB CRD a ClusterRole is sufficient

## About

In SingleTenantMongoDB Controller, I'm took the lessons learned about and attempted to write a more potentially "production-worthy" MongoDB operator.
At present, the MongoDB itself only supports one app database (hence the name "SingleTenantMongoDB").
I attempted to design this controller such that multiple MongoDB deployments can be managed, and to be easily extensible to allow the implementation of a MultiTenantMongoDB in the future.
