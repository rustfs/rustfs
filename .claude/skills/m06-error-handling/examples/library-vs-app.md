# Error Handling: Library vs Application

## Library Error Design

### Principles
1. **Define specific error types** - Don't use `anyhow` in libraries
2. **Implement std::error::Error** - For compatibility
3. **Provide error variants** - Let users match on errors
4. **Include source errors** - Enable error chains
5. **Be `Send + Sync`** - For async compatibility

### Example: Library Error Type
```rust
// lib.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("connection failed: {host}:{port}")]
    ConnectionFailed {
        host: String,
        port: u16,
        #[source]
        source: std::io::Error,
    },

    #[error("query failed: {query}")]
    QueryFailed {
        query: String,
        #[source]
        source: SqlError,
    },

    #[error("record not found: {table}.{id}")]
    NotFound { table: String, id: String },

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),
}

// Public Result alias
pub type Result<T> = std::result::Result<T, DatabaseError>;

// Library functions
pub fn connect(host: &str, port: u16) -> Result<Connection> {
    // ...
}

pub fn query(conn: &Connection, sql: &str) -> Result<Rows> {
    // ...
}
```

### Library Usage of Errors
```rust
impl Database {
    pub fn get_user(&self, id: &str) -> Result<User> {
        let rows = self.query(&format!("SELECT * FROM users WHERE id = '{}'", id))?;

        rows.first()
            .cloned()
            .ok_or_else(|| DatabaseError::NotFound {
                table: "users".to_string(),
                id: id.to_string(),
            })
    }
}
```

---

## Application Error Design

### Principles
1. **Use anyhow for convenience** - Or custom unified error
2. **Add context liberally** - Help debugging
3. **Log at boundaries** - Don't log in libraries
4. **Convert to user-friendly messages** - For display

### Example: Application Error Handling
```rust
// main.rs
use anyhow::{Context, Result};
use tracing::{error, info};

async fn run_server() -> Result<()> {
    let config = load_config()
        .context("failed to load configuration")?;

    let db = Database::connect(&config.db_url)
        .await
        .context("failed to connect to database")?;

    let server = Server::new(config.port)
        .context("failed to create server")?;

    info!("Server starting on port {}", config.port);

    server.run(db).await
        .context("server error")?;

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::init();

    if let Err(e) = run_server().await {
        error!("Application error: {:#}", e);
        std::process::exit(1);
    }
}
```

### Converting Library Errors
```rust
use mylib::DatabaseError;

async fn get_user_handler(id: &str) -> Result<Response> {
    match db.get_user(id).await {
        Ok(user) => Ok(Response::json(user)),

        Err(DatabaseError::NotFound { .. }) => {
            Ok(Response::not_found("User not found"))
        }

        Err(DatabaseError::ConnectionFailed { .. }) => {
            error!("Database connection failed");
            Ok(Response::internal_error("Service unavailable"))
        }

        Err(e) => {
            error!("Database error: {}", e);
            Err(e.into())  // Convert to anyhow::Error
        }
    }
}
```

---

## Error Handling Layers

```
┌─────────────────────────────────────┐
│           Application Layer          │
│  - Use anyhow or unified error       │
│  - Add context at boundaries         │
│  - Log errors                        │
│  - Convert to user messages          │
└─────────────────────────────────────┘
                 │
                 │ calls
                 ▼
┌─────────────────────────────────────┐
│           Service Layer              │
│  - Map between error types           │
│  - Add business context              │
│  - Handle recoverable errors         │
└─────────────────────────────────────┘
                 │
                 │ calls
                 ▼
┌─────────────────────────────────────┐
│           Library Layer              │
│  - Define specific error types       │
│  - Use thiserror                     │
│  - Include source errors             │
│  - No logging                        │
└─────────────────────────────────────┘
```

---

## Practical Examples

### HTTP API Error Response
```rust
use axum::{response::IntoResponse, http::StatusCode};
use serde::Serialize;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    code: String,
}

enum AppError {
    NotFound(String),
    BadRequest(String),
    Internal(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error, code) = match self {
            AppError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, msg, "NOT_FOUND")
            }
            AppError::BadRequest(msg) => {
                (StatusCode::BAD_REQUEST, msg, "BAD_REQUEST")
            }
            AppError::Internal(e) => {
                tracing::error!("Internal error: {:#}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                    "INTERNAL_ERROR",
                )
            }
        };

        let body = ErrorResponse {
            error,
            code: code.to_string(),
        };

        (status, axum::Json(body)).into_response()
    }
}
```

### CLI Error Handling
```rust
use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    config: String,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    let config = std::fs::read_to_string(&args.config)
        .context(format!("Failed to read config file: {}", args.config))?;

    let parsed: Config = toml::from_str(&config)
        .context("Failed to parse config file")?;

    process(parsed)?;

    println!("Done!");
    Ok(())
}
```

---

## Testing Error Handling

### Testing Error Cases
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_error() {
        let result = db.get_user("nonexistent");

        assert!(matches!(
            result,
            Err(DatabaseError::NotFound { table, id })
            if table == "users" && id == "nonexistent"
        ));
    }

    #[test]
    fn test_error_message() {
        let err = DatabaseError::NotFound {
            table: "users".to_string(),
            id: "123".to_string(),
        };

        assert_eq!(err.to_string(), "record not found: users.123");
    }

    #[test]
    fn test_error_chain() {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused"
        );

        let err = DatabaseError::ConnectionFailed {
            host: "localhost".to_string(),
            port: 5432,
            source: io_err,
        };

        // Check source is preserved
        assert!(err.source().is_some());
    }
}
```

### Testing with anyhow
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_context() -> anyhow::Result<()> {
        let result = process("valid input")?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_error_context() {
        let err = process("invalid")
            .context("processing failed")
            .unwrap_err();

        // Check error chain contains expected text
        let chain = format!("{:#}", err);
        assert!(chain.contains("processing failed"));
    }
}
```
