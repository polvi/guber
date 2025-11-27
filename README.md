# Guber

Guber is a minimal Kubernetes-style API server running on Cloudflare Workers that implements Custom Resource Definitions (CRDs) to create custom APIs. It provides a kubectl-compatible API surface for managing custom resources in a serverless environment.

## What is Guber?

Guber mimics the Kubernetes API server's CRD functionality, allowing you to:
- Define custom resource types using CRDs
- Create, read, update, and delete custom resources
- Use standard kubectl commands to interact with your custom APIs
- Run entirely on Cloudflare's edge network with D1 database storage

## Quick Start

1. Install dependencies and start development server:
```bash
bun install
bun dev
```

2. Initialize the database:
```bash
bun db:init
```

3. Apply the example CRD and resource:
```bash
kubectl apply -f k8s/boardposts.bulletin.yaml --validate=false
kubectl apply -f k8s/boardpost.yaml --validate=false
```

## Example Usage

First, set up kubectl to use the local Guber instance:
```bash
export KUBECONFIG=k8s/kubeconfig
```

Note: The kubeconfig is pre-configured to work with the development server running on `http://localhost:8787`.

Once running and with KUBECONFIG set, you can use kubectl to interact with your custom resources:

```bash
# List all CRDs
kubectl get crds

# List BoardPost resources
kubectl get boardposts

# Get a specific BoardPost
kubectl get boardpost first-post

# Delete a BoardPost
kubectl delete boardpost first-post --validate=false
```

The example manifests in `k8s/` demonstrate:
- `boardposts.bulletin.yaml`: A CRD defining a BoardPost resource type
- `boardpost.yaml`: An instance of a BoardPost resource
