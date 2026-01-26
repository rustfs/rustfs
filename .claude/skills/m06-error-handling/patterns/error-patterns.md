# Error Handling Patterns

## The ? Operator

### Basic Usage
```rust
fn read_config() -> Result<Config, io::Error> {
    let content = std::fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&content)?;  // needs From impl
    Ok(config)
}
```

### With Different Error Types
```rust
use std::error::Error;

// Box<dyn Error> for quick prototyping
fn process() -> Result<(), Box<dyn Error>> {
    let file = std::fs::read_to_string("data.txt")?;
    let num: i32 = file.trim().parse()?;  // different error type
    Ok(())
}
```

### Custom Conversion with From
```rust
#[derive(Debug)]
enum MyError {
    Io(std::io::Error),
    Parse(std::num::ParseIntError),
}

impl From<std::io::Error> for MyError {
    fn from(err: std::io::Error) -> Self {
        MyError::Io(err)
    }
}

impl From<std::num::ParseIntError> for MyError {
    fn from(err: std::num::ParseIntError) -> Self {
        MyError::Parse(err)
    }
}

fn process() -> Result<i32, MyError> {
    let content = std::fs::read_to_string("num.txt")?;  // auto-converts
    let num: i32 = content.trim().parse()?;  // auto-converts
    Ok(num)
}
```

---

## Error Type Design

### Simple Enum Error
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigError {
    NotFound,
    InvalidFormat,
    MissingField(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::NotFound => write!(f, "configuration file not found"),
            ConfigError::InvalidFormat => write!(f, "invalid configuration format"),
            ConfigError::MissingField(field) => write!(f, "missing field: {}", field),
        }
    }
}

impl std::error::Error for ConfigError {}
```

### Error with Source (Wrapping)
```rust
#[derive(Debug)]
pub struct AppError {
    kind: AppErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, Clone, Copy)]
pub enum AppErrorKind {
    Config,
    Database,
    Network,
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            AppErrorKind::Config => write!(f, "configuration error"),
            AppErrorKind::Database => write!(f, "database error"),
            AppErrorKind::Network => write!(f, "network error"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}
```

---

## Using thiserror

### Basic Usage
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataError {
    #[error("file not found: {path}")]
    NotFound { path: String },

    #[error("invalid data format")]
    InvalidFormat,

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("parse error: {0}")]
    Parse(#[from] std::num::ParseIntError),
}

// Usage
fn load_data(path: &str) -> Result<Data, DataError> {
    let content = std::fs::read_to_string(path)
        .map_err(|_| DataError::NotFound { path: path.to_string() })?;
    let num: i32 = content.trim().parse()?;  // auto-converts with #[from]
    Ok(Data { value: num })
}
```

### Transparent Wrapper
```rust
use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct MyError(#[from] InnerError);

// Useful for newtype error wrappers
```

---

## Using anyhow

### For Applications
```rust
use anyhow::{Context, Result, bail, ensure};

fn process_file(path: &str) -> Result<Data> {
    let content = std::fs::read_to_string(path)
        .context("failed to read config file")?;

    ensure!(!content.is_empty(), "config file is empty");

    let data: Data = serde_json::from_str(&content)
        .context("failed to parse JSON")?;

    if data.version < 1 {
        bail!("unsupported config version: {}", data.version);
    }

    Ok(data)
}

fn main() -> Result<()> {
    let data = process_file("config.json")
        .context("failed to load configuration")?;
    Ok(())
}
```

### Error Chain
```rust
use anyhow::{Context, Result};

fn deep_function() -> Result<()> {
    std::fs::read_to_string("missing.txt")
        .context("failed to read file")?;
    Ok(())
}

fn middle_function() -> Result<()> {
    deep_function()
        .context("failed in deep function")?;
    Ok(())
}

fn top_function() -> Result<()> {
    middle_function()
        .context("failed in middle function")?;
    Ok(())
}

// Error output shows full chain:
// Error: failed in middle function
// Caused by:
//     0: failed in deep function
//     1: failed to read file
//     2: No such file or directory (os error 2)
```

---

## Option Handling

### Converting Option to Result
```rust
fn find_user(id: u32) -> Option<User> { ... }

// Using ok_or for static error
fn get_user(id: u32) -> Result<User, &'static str> {
    find_user(id).ok_or("user not found")
}

// Using ok_or_else for dynamic error
fn get_user(id: u32) -> Result<User, String> {
    find_user(id).ok_or_else(|| format!("user {} not found", id))
}
```

### Chaining Options
```rust
fn get_nested_value(data: &Data) -> Option<&str> {
    data.config
        .as_ref()?
        .nested
        .as_ref()?
        .value
        .as_deref()
}

// Equivalent with and_then
fn get_nested_value(data: &Data) -> Option<&str> {
    data.config
        .as_ref()
        .and_then(|c| c.nested.as_ref())
        .and_then(|n| n.value.as_deref())
}
```

---

## Pattern: Result Combinators

### map and map_err
```rust
fn parse_port(s: &str) -> Result<u16, ParseError> {
    s.parse::<u16>()
        .map_err(|e| ParseError::InvalidPort(e))
}

fn get_url(config: &Config) -> Result<String, Error> {
    config.url()
        .map(|u| format!("https://{}", u))
}
```

### and_then (flatMap)
```rust
fn validate_and_save(input: &str) -> Result<(), Error> {
    validate(input)
        .and_then(|valid| save(valid))
        .and_then(|saved| notify(saved))
}
```

### unwrap_or and unwrap_or_else
```rust
// Default value
let port = config.port().unwrap_or(8080);

// Computed default
let port = config.port().unwrap_or_else(|| find_free_port());

// Default for Result
let data = load_data().unwrap_or_default();
```

---

## Pattern: Early Return vs Combinators

### Early Return Style
```rust
fn process(input: &str) -> Result<Output, Error> {
    let step1 = validate(input)?;
    if !step1.is_valid {
        return Err(Error::Invalid);
    }

    let step2 = transform(step1)?;
    let step3 = save(step2)?;

    Ok(step3)
}
```

### Combinator Style
```rust
fn process(input: &str) -> Result<Output, Error> {
    validate(input)
        .and_then(|s| {
            if s.is_valid {
                Ok(s)
            } else {
                Err(Error::Invalid)
            }
        })
        .and_then(transform)
        .and_then(save)
}
```

### When to Use Which

| Style | Best For |
|-------|----------|
| Early return (`?`) | Most cases, clearer flow |
| Combinators | Functional pipelines, one-liners |
| Match | Complex branching on errors |

---

## Panic vs Result

### When to Panic
```rust
// 1. Unrecoverable programmer error
fn get_config() -> &'static Config {
    CONFIG.get().expect("config must be initialized")
}

// 2. In tests
#[test]
fn test_parsing() {
    let result = parse("valid").unwrap();  // OK in tests
    assert_eq!(result, expected);
}

// 3. Prototype/examples
fn main() {
    let data = load().unwrap();  // OK for quick examples
}
```

### When to Return Result
```rust
// 1. Any I/O operation
fn read_file(path: &str) -> Result<String, io::Error>

// 2. User input validation
fn parse_port(s: &str) -> Result<u16, ParseError>

// 3. Network operations
async fn fetch(url: &str) -> Result<Response, Error>

// 4. Anything that can fail at runtime
fn connect(addr: &str) -> Result<Connection, Error>
```

---

## Error Context Best Practices

### Add Context at Boundaries
```rust
fn load_user_config(user_id: u64) -> Result<Config, Error> {
    let path = format!("/home/{}/config.toml", user_id);

    std::fs::read_to_string(&path)
        .context(format!("failed to read config for user {}", user_id))?
        // NOT: .context("failed to read file")  // too generic

    // ...
}
```

### Include Relevant Data
```rust
// Good: includes the problematic value
fn parse_age(s: &str) -> Result<u8, Error> {
    s.parse()
        .context(format!("invalid age value: '{}'", s))
}

// Bad: no context about what failed
fn parse_age(s: &str) -> Result<u8, Error> {
    s.parse()
        .context("parse error")
}
```
