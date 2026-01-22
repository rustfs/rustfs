# Common Rust Anti-Patterns & Mistakes

## Ownership Anti-Patterns

### 1. Clone Everything

```rust
// ANTI-PATTERN: clone to avoid borrow checker
fn process(data: Vec<String>) {
    for item in data.clone() {  // unnecessary clone
        println!("{}", item);
    }
    use_data(data);
}

// BETTER: borrow when you don't need ownership
fn process(data: Vec<String>) {
    for item in &data {  // borrow instead
        println!("{}", item);
    }
    use_data(data);
}
```

### 2. Unnecessary Box

```rust
// ANTI-PATTERN: boxing everything
fn get_value() -> Box<String> {
    Box::new(String::from("hello"))
}

// BETTER: return value directly
fn get_value() -> String {
    String::from("hello")
}
```

### 3. Holding References Too Long

```rust
// ANTI-PATTERN: borrow prevents mutation
let mut data = vec![1, 2, 3];
let first = &data[0];
data.push(4);  // ERROR: data is borrowed
println!("{}", first);

// BETTER: scope the borrow
let mut data = vec![1, 2, 3];
let first = data[0];  // copy the value
data.push(4);  // OK
println!("{}", first);
```

---

## Error Handling Anti-Patterns

### 4. Unwrap Everywhere

```rust
// ANTI-PATTERN: crashes on error
fn process_file(path: &str) {
    let content = std::fs::read_to_string(path).unwrap();
    let config: Config = toml::from_str(&content).unwrap();
}

// BETTER: propagate errors
fn process_file(path: &str) -> Result<Config, Error> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}
```

### 5. Ignoring Errors

```rust
// ANTI-PATTERN: silent failure
let _ = file.write_all(data);

// BETTER: handle or propagate
file.write_all(data)?;
// or at minimum, log the error
if let Err(e) = file.write_all(data) {
    eprintln!("Warning: failed to write: {}", e);
}
```

### 6. Panic in Library Code

```rust
// ANTI-PATTERN: library panics
pub fn parse(input: &str) -> Data {
    if input.is_empty() {
        panic!("input cannot be empty");
    }
    // ...
}

// BETTER: return Result
pub fn parse(input: &str) -> Result<Data, ParseError> {
    if input.is_empty() {
        return Err(ParseError::EmptyInput);
    }
    // ...
}
```

---

## String Anti-Patterns

### 7. String Instead of &str

```rust
// ANTI-PATTERN: forces allocation
fn greet(name: String) {
    println!("Hello, {}", name);
}

greet("world".to_string());  // allocation

// BETTER: accept &str
fn greet(name: &str) {
    println!("Hello, {}", name);
}

greet("world");  // no allocation
```

### 8. Format for Simple Concatenation

```rust
// ANTI-PATTERN: format overhead
let greeting = format!("{}{}", "Hello, ", name);

// BETTER for simple cases: push_str
let mut greeting = String::from("Hello, ");
greeting.push_str(name);

// Or use + for String + &str
let greeting = String::from("Hello, ") + name;
```

### 9. Repeated String Operations

```rust
// ANTI-PATTERN: O(n²) allocations
let mut result = String::new();
for word in words {
    result = result + word + " ";
}

// BETTER: join
let result = words.join(" ");

// Or with_capacity + push_str
let mut result = String::with_capacity(total_len);
for word in words {
    result.push_str(word);
    result.push(' ');
}
```

---

## Collection Anti-Patterns

### 10. Index Instead of Iterator

```rust
// ANTI-PATTERN: bounds checking overhead
for i in 0..vec.len() {
    process(vec[i]);
}

// BETTER: iterator
for item in &vec {
    process(item);
}
```

### 11. Collect Then Iterate

```rust
// ANTI-PATTERN: unnecessary allocation
let filtered: Vec<_> = items.iter().filter(|x| x.valid).collect();
for item in filtered {
    process(item);
}

// BETTER: chain iterators
for item in items.iter().filter(|x| x.valid) {
    process(item);
}
```

### 12. Wrong Collection Type

```rust
// ANTI-PATTERN: Vec for frequent membership checks
let allowed: Vec<&str> = vec!["a", "b", "c"];
if allowed.contains(&input) { ... }  // O(n)

// BETTER: HashSet for membership
use std::collections::HashSet;
let allowed: HashSet<&str> = ["a", "b", "c"].into();
if allowed.contains(input) { ... }  // O(1)
```

---

## Concurrency Anti-Patterns

### 13. Mutex for Read-Heavy Data

```rust
// ANTI-PATTERN: Mutex when mostly reading
let data = Arc::new(Mutex::new(config));
// All readers block each other

// BETTER: RwLock for read-heavy workloads
let data = Arc::new(RwLock::new(config));
// Multiple readers can proceed in parallel
```

### 14. Holding Lock Across Await

```rust
// ANTI-PATTERN: lock held across await
async fn bad() {
    let guard = mutex.lock().unwrap();
    some_async_op().await;  // lock held!
    use(guard);
}

// BETTER: scope the lock
async fn good() {
    let value = {
        let guard = mutex.lock().unwrap();
        guard.clone()
    };  // lock released
    some_async_op().await;
    use(value);
}
```

### 15. Blocking in Async

```rust
// ANTI-PATTERN: blocking call in async
async fn bad() {
    std::thread::sleep(Duration::from_secs(1));  // blocks executor!
}

// BETTER: async sleep
async fn good() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

// For CPU work: spawn_blocking
async fn compute() {
    tokio::task::spawn_blocking(|| heavy_work()).await
}
```

---

## Type System Anti-Patterns

### 16. Stringly Typed

```rust
// ANTI-PATTERN: strings for everything
fn connect(host: &str, port: &str, timeout: &str) { ... }
connect("8080", "localhost", "30");  // wrong order!

// BETTER: strong types
struct Host(String);
struct Port(u16);
struct Timeout(Duration);

fn connect(host: Host, port: Port, timeout: Timeout) { ... }
```

### 17. Boolean Parameters

```rust
// ANTI-PATTERN: what does true mean?
fn fetch(url: &str, use_cache: bool, validate_ssl: bool) { ... }
fetch("https://...", true, false);  // unclear

// BETTER: builder or named parameters
struct FetchOptions {
    use_cache: bool,
    validate_ssl: bool,
}

fn fetch(url: &str, options: FetchOptions) { ... }
fetch("https://...", FetchOptions {
    use_cache: true,
    validate_ssl: false,
});
```

### 18. Option<Option<T>>

```rust
// ANTI-PATTERN: nested Option
fn find(id: u32) -> Option<Option<User>> { ... }
// What does None vs Some(None) mean?

// BETTER: use Result or custom enum
enum FindResult {
    Found(User),
    NotFound,
    Error(String),
}
```

---

## API Design Anti-Patterns

### 19. Taking Ownership Unnecessarily

```rust
// ANTI-PATTERN: takes ownership but doesn't need it
fn validate(config: Config) -> bool {
    config.timeout > 0 && config.retries >= 0
}

// BETTER: borrow
fn validate(config: &Config) -> bool {
    config.timeout > 0 && config.retries >= 0
}
```

### 20. Returning References to Temporaries

```rust
// ANTI-PATTERN: impossible lifetime
fn get_default() -> &str {
    let s = String::from("default");
    &s  // ERROR: s is dropped
}

// BETTER: return owned
fn get_default() -> String {
    String::from("default")
}

// Or return static
fn get_default() -> &'static str {
    "default"
}
```

### 21. Overly Generic Functions

```rust
// ANTI-PATTERN: complex generics for simple function
fn process<T, U, V>(input: T) -> V
where
    T: Into<U>,
    U: AsRef<str> + Clone,
    V: From<String>,
{ ... }

// BETTER: concrete types if generics not needed
fn process(input: &str) -> String { ... }
```

---

## Macro Anti-Patterns

### 22. Macro When Function Works

```rust
// ANTI-PATTERN: macro for simple operation
macro_rules! add {
    ($a:expr, $b:expr) => { $a + $b };
}

// BETTER: just use a function
fn add(a: i32, b: i32) -> i32 { a + b }
```

### 23. Complex Macro Without Tests

```rust
// ANTI-PATTERN: complex macro with no tests
macro_rules! define_api {
    // ... 100 lines of macro code ...
}

// BETTER: test macro outputs
#[test]
fn test_macro_expansion() {
    // Use cargo-expand or trybuild
}
```

---

## Quick Reference

| Anti-Pattern | Better Alternative |
|--------------|-------------------|
| Clone everywhere | Borrow when possible |
| Unwrap everywhere | Propagate with `?` |
| `String` parameters | `&str` parameters |
| Index loops | Iterator loops |
| Collect then process | Chain iterators |
| Mutex for reads | RwLock for read-heavy |
| Lock across await | Scope the lock |
| Blocking in async | spawn_blocking |
| Stringly typed | Strong types |
| Boolean params | Builders or enums |
