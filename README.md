# Orchestration Service

## Purpose
- Consumes `build.succeeded` events from RabbitMQ.
- Reconciles Kubernetes `Namespace`, `Deployment`, `Service`, and `Ingress` resources in a K3s cluster.
- Waits for deployment rollout completion and publishes `deploy.succeeded` or `deploy.failed`.
- Creates a namespace-local registry pull secret when private registry credentials are configured.

## Environment variables
- `PORT=8082`
- `RABBITMQ_URL=amqp://guest:guest@localhost:5672/`
- `RABBITMQ_EXCHANGE=shiply.services`
- `RABBITMQ_APP_DEPLOY_QUEUE=shiply.app.deploy`
- `RABBITMQ_BUILD_SUCCEEDED_ROUTING_KEY=build.succeeded`
- `RABBITMQ_DEPLOY_SUCCEEDED_ROUTING_KEY=deploy.succeeded`
- `RABBITMQ_DEPLOY_FAILED_ROUTING_KEY=deploy.failed`
- `KUBECONFIG`
- `PLATFORM_BASE_DOMAIN`
- `K8S_INGRESS_CLASS=traefik`
- `K8S_NAMESPACE_PREFIX=shiply-prj`
- `K8S_DEFAULT_REPLICAS=1`
- `K8S_CONTAINER_PORT_DEFAULT=8080`
- `K8S_ROLLOUT_TIMEOUT=5m`
- `K8S_ROLLOUT_POLL_INTERVAL=5s`
- `K8S_CPU_REQUEST=100m`
- `K8S_CPU_LIMIT=500m`
- `K8S_MEMORY_REQUEST=128Mi`
- `K8S_MEMORY_LIMIT=512Mi`
- `K8S_IMAGE_PULL_SECRET_NAME=shiply-registry`
- `K8S_TLS_SECRET_NAME`
- `CONTAINER_REGISTRY_HOST`
- `CONTAINER_REGISTRY_USERNAME`
- `CONTAINER_REGISTRY_PASSWORD`

## Event contracts
- Consumes the shared `ServiceEvent[BuildSucceededPayload]` envelope with a payload containing `imageTag`, `commitSha`, `projectSlug`, `serviceSlug`, and `containerPort`.
- Publishes `deploy.succeeded` with the reconciled namespace, deployment name, service name, ingress host, and container port.
- Publishes `deploy.failed` with the same deploy target metadata plus an error message.

## Runtime behavior
- Uses in-cluster Kubernetes configuration when running inside K3s and falls back to `KUBECONFIG` for local development.
- Creates one namespace per project using `shiply-prj-<project-slug>`.
- Exposes apps through a standard Kubernetes `Ingress` using the host pattern `<service-slug>-<project-slug>.<PLATFORM_BASE_DOMAIN>`.
- Leaves failed workloads in place for inspection; later deploys update them in place.
