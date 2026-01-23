---
name: domain-web
description: "Use when building web services. Keywords: web server, HTTP, REST API, GraphQL, WebSocket, axum, actix, warp, rocket, tower, hyper, reqwest, middleware, router, handler, extractor, state management, authentication, authorization, JWT, session, cookie, CORS, rate limiting, web 开发, HTTP 服务, API 设计, 中间件, 路由"
globs: ["**/Cargo.toml"]
---

# Web Domain

> **Layer 3: Domain Constraints**

## Domain Constraints → Design Implications

| Domain Rule | Design Constraint | Rust Implication |
|-------------|-------------------|------------------|
| Stateless HTTP | No request-local globals | State in extractors |
| Concurrency | Handle many connections | Async, Send + Sync |
| Latency SLA | Fast response | Efficient ownership |
| Security | Input validation | Type-safe extractors |
| Observability | Request tracing | tracing + tower layers |

---

## Critical Constraints

### Async by Default

```
RULE: Web handlers must not block
WHY: Block one task = block many requests
RUST: async/await, spawn_blocking for CPU work
```

### State Management

```
RULE: Shared state must be thread-safe
WHY: Handlers run on any thread
RUST: Arc<T>, Arc<RwLock<T>> for mutable
```

### Request Lifecycle

```
RULE: Resources live only for request duration
WHY: Memory management, no leaks
RUST: Extractors, proper ownership
```

---

## Trace Down ↓

From constraints to design (Layer 2):

```
"Need shared application state"
    ↓ m07-concurrency: Use Arc for thread-safe sharing
    ↓ m02-resource: Arc<RwLock<T>> for mutable state

"Need request validation"
    ↓ m05-type-driven: Validated extractors
    ↓ m06-error-handling: IntoResponse for errors

"Need middleware stack"
    ↓ m12-lifecycle: Tower layers
    ↓ m04-zero-cost: Trait-based composition
```

---

## Framework Comparison

| Framework | Style | Best For |
|-----------|-------|----------|
| axum | Functional, tower | Modern APIs |
| actix-web | Actor-based | High performance |
| warp | Filter composition | Composable APIs |
| rocket | Macro-driven | Rapid development |

## Key Crates

| Purpose | Crate |
|---------|-------|
| HTTP server | axum, actix-web |
| HTTP client | reqwest |
| JSON | serde_json |
| Auth/JWT | jsonwebtoken |
| Session | tower-sessions |
| Database | sqlx, diesel |
| Middleware | tower |

## Design Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| Extractors | Request parsing | `State(db)`, `Json(payload)` |
| Error response | Unified errors | `impl IntoResponse` |
| Middleware | Cross-cutting | Tower layers |
| Shared state | App config | `Arc<AppState>` |

## Code Pattern: Axum Handler

```rust
async fn handler(
    State(db): State<Arc<DbPool>>,
    Json(payload): Json<CreateUser>,
) -> Result<Json<User>, AppError> {
    let user = db.create_user(&payload).await?;
    Ok(Json(user))
}

// Error handling
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "Not found"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal error"),
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}
```

---

## Common Mistakes

| Mistake | Domain Violation | Fix |
|---------|-----------------|-----|
| Blocking in handler | Latency spike | spawn_blocking |
| Rc in state | Not Send + Sync | Use Arc |
| No validation | Security risk | Type-safe extractors |
| No error response | Bad UX | IntoResponse impl |

---

## Trace to Layer 1

| Constraint | Layer 2 Pattern | Layer 1 Implementation |
|------------|-----------------|------------------------|
| Async handlers | Async/await | tokio runtime |
| Thread-safe state | Shared state | Arc<T>, Arc<RwLock<T>> |
| Request lifecycle | Extractors | Ownership via From<Request> |
| Middleware | Tower layers | Trait-based composition |

---

## Related Skills

| When | See |
|------|-----|
| Async patterns | m07-concurrency |
| State management | m02-resource |
| Error handling | m06-error-handling |
| Middleware design | m12-lifecycle |
