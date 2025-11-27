# Guber

Guber is a minimal Kubernetes-style API server running on Cloudflare Workers that implements Custom Resource Definitions (CRDs) to create custom APIs. It provides a kubectl-compatible API surface for managing custom resources in a serverless environment.

## What is Guber?

Guber mimics the Kubernetes API server's CRD functionality, allowing you to:
- Define custom resource types using CRDs
- Create, read, update, and delete custom resources
- Use standard kubectl commands to interact with your custom APIs
- Run entirely on Cloudflare's edge network with D1 database storage

## Configuration

Guber requires several environment variables to be configured:

### Required Environment Variables

- `CLOUDFLARE_ACCOUNT_ID` - Your Cloudflare account ID
- `CLOUDFLARE_API_TOKEN` - Cloudflare API token with Workers and D1 permissions
- `GUBER_NAME` - Instance name (e.g., "dev", "staging", "prod")
- `GUBER_DOMAIN` - Your domain name (e.g., "proc.io")

### Domain Setup

For Cloudflare Workers to work with custom domains, you need to ensure your domain is managed by Cloudflare. 

This allows workers to be deployed at URLs like `example-worker.dev.proc.io`.

### Setting Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
# Edit .env with your values
```

For production deployment, set these as Wrangler secrets:

```bash
wrangler secret put CLOUDFLARE_ACCOUNT_ID
wrangler secret put CLOUDFLARE_API_TOKEN
wrangler secret put GUBER_NAME
wrangler secret put GUBER_DOMAIN
```

## Quick Start

Install dependencies and start development server:

```bash
bun install
bun db:init
bun dev
```

## Example Usage

First, set up kubectl to use the local Guber instance:

```bash
export KUBECONFIG=k8s/kubeconfig
```

Note: The kubeconfig is pre-configured to work with the development server running on `http://localhost:8787`.

### Working with Cloudflare Resources

Guber includes built-in support for provisioning Cloudflare resources:

```bash
# Apply Cloudflare resource CRDs
kubectl apply -f k8s/d1s.cf.guber.proc.io.yaml --validate=false
kubectl apply -f k8s/qs.cf.guber.proc.io.yaml --validate=false
kubectl apply -f k8s/workers.cf.guber.proc.io.yaml --validate=false

# Create Cloudflare resources
kubectl apply -f k8s/d1-example-db.yaml --validate=false
kubectl apply -f k8s/q-example.yaml --validate=false
kubectl apply -f k8s/worker-example.yaml --validate=false

# Check resource status
kubectl get d1s
kubectl get queues
kubectl get workers

# Get detailed information about a specific resource
kubectl describe worker example-worker
kubectl describe d1 example-db
kubectl describe queue example-queue
```

Workers will be automatically deployed to `{worker-name}.{GUBER_NAME}.{GUBER_DOMAIN}`.

### Basic kubectl Operations

Once running and with KUBECONFIG set, you can use kubectl to interact with your custom resources:

```bash
# List all CRDs
kubectl get crds

# List all Cloudflare resources
kubectl get d1s,queues,workers

# Delete resources
kubectl delete worker example-worker --validate=false
kubectl delete d1 example-db --validate=false
kubectl delete queue example-queue --validate=false
```

## Testing Reconciliation

You can manually trigger the reconciliation process by hitting the scheduled handler endpoint:

```bash
curl http://127.0.0.1:8787/cdn-cgi/handler/scheduled
```

The example manifests in `k8s/` demonstrate:
- `d1s.cf.guber.proc.io.yaml`: CRD for Cloudflare D1 databases
- `qs.cf.guber.proc.io.yaml`: CRD for Cloudflare Queues
- `workers.cf.guber.proc.io.yaml`: CRD for Cloudflare Workers
- `d1-example-db.yaml`: Example D1 database resource
- `q-example.yaml`: Example Queue resource
- `worker-example.yaml`: Example Worker resource
