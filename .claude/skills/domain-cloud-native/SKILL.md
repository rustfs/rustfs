---
name: domain-cloud-native
description: "Use when building cloud-native apps. Keywords: kubernetes, k8s, docker, container, grpc, tonic, microservice, service mesh, observability, tracing, metrics, health check, cloud, deployment, 云原生, 微服务, 容器"
---

# Cloud-Native Domain

> **Layer 3: Domain Constraints**

## Domain Constraints → Design Implications

| Domain Rule | Design Constraint | Rust Implication |
|-------------|-------------------|------------------|
| 12-Factor | Config from env | Environment-based config |
| Observability | Metrics + traces | tracing + opentelemetry |
| Health checks | Liveness/readiness | Dedicated endpoints |
| Graceful shutdown | Clean termination | Signal handling |
| Horizontal scale | Stateless design | No local state |
| Container-friendly | Small binaries | Release optimization |

---

## Critical Constraints

### Stateless Design

```
RULE: No local persistent state
WHY: Pods can be killed/rescheduled anytime
RUST: External state (Redis, DB), no static mut
```

### Graceful Shutdown

```
RULE: Handle SIGTERM, drain connections
WHY: Zero-downtime deployments
RUST: tokio::signal + graceful shutdown
```

### Observability

```
RULE: Every request must be traceable
WHY: Debugging distributed systems
RUST: tracing spans, opentelemetry export
```

---

## Trace Down ↓

From constraints to design (Layer 2):

```
"Need distributed tracing"
    ↓ m12-lifecycle: Span lifecycle
    ↓ tracing + opentelemetry

"Need graceful shutdown"
    ↓ m07-concurrency: Signal handling
    ↓ m12-lifecycle: Connection draining

"Need health checks"
    ↓ domain-web: HTTP endpoints
    ↓ m06-error-handling: Health status
```

---

## Key Crates

| Purpose | Crate |
|---------|-------|
| gRPC | tonic |
| Kubernetes | kube, kube-runtime |
| Docker | bollard |
| Tracing | tracing, opentelemetry |
| Metrics | prometheus, metrics |
| Config | config, figment |
| Health | HTTP endpoints |

## Design Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| gRPC services | Service mesh | tonic + tower |
| K8s operators | Custom resources | kube-runtime Controller |
| Observability | Debugging | tracing + OTEL |
| Health checks | Orchestration | `/health`, `/ready` |
| Config | 12-factor | Env vars + secrets |

## Code Pattern: Graceful Shutdown

```rust
use tokio::signal;

async fn run_server() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutdown signal received");
}
```

## Health Check Pattern

```rust
async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(db): State<Arc<DbPool>>) -> StatusCode {
    match db.ping().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
```

---

## Common Mistakes

| Mistake | Domain Violation | Fix |
|---------|-----------------|-----|
| Local file state | Not stateless | External storage |
| No SIGTERM handling | Hard kills | Graceful shutdown |
| No tracing | Can't debug | tracing spans |
| Static config | Not 12-factor | Env vars |

---

## Trace to Layer 1

| Constraint | Layer 2 Pattern | Layer 1 Implementation |
|------------|-----------------|------------------------|
| Stateless | External state | Arc<Client> for external |
| Graceful shutdown | Signal handling | tokio::signal |
| Tracing | Span lifecycle | tracing + OTEL |
| Health checks | HTTP endpoints | Dedicated routes |

---

## Related Skills

| When | See |
|------|-----|
| Async patterns | m07-concurrency |
| HTTP endpoints | domain-web |
| Error handling | m13-domain-error |
| Resource lifecycle | m12-lifecycle |
