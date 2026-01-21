# Ownership Best Practices

## API Design Patterns

### 1. Prefer Borrowing Over Ownership

```rust
// BAD: takes ownership unnecessarily
fn print_name(name: String) {
    println!("Name: {}", name);
}

// GOOD: borrows instead
fn print_name(name: &str) {
    println!("Name: {}", name);
}

// Caller benefits:
let name = String::from("Alice");
print_name(&name);  // can reuse name
print_name(&name);  // still valid
```

### 2. Return Owned Values from Constructors

```rust
// GOOD: return owned value
impl User {
    fn new(name: &str) -> Self {
        User {
            name: name.to_string(),
        }
    }
}

// GOOD: accept Into<String> for flexibility
impl User {
    fn new(name: impl Into<String>) -> Self {
        User {
            name: name.into(),
        }
    }
}

// Usage:
let u1 = User::new("Alice");        // &str
let u2 = User::new(String::from("Bob"));  // String
```

### 3. Use AsRef for Generic Borrowing

```rust
// GOOD: accepts both &str and String
fn process<S: AsRef<str>>(input: S) {
    let s = input.as_ref();
    println!("{}", s);
}

process("literal");           // &str
process(String::from("owned")); // String
process(&String::from("ref")); // &String
```

### 4. Cow for Clone-on-Write

```rust
use std::borrow::Cow;

// Return borrowed when possible, owned when needed
fn maybe_modify(s: &str, uppercase: bool) -> Cow<'_, str> {
    if uppercase {
        Cow::Owned(s.to_uppercase())  // allocates
    } else {
        Cow::Borrowed(s)  // zero-cost
    }
}

let input = "hello";
let result = maybe_modify(input, false);
// result is borrowed, no allocation
```

---

## Struct Design Patterns

### 1. Owned Fields vs References

```rust
// Use owned fields for most cases
struct User {
    name: String,
    email: String,
}

// Use references only when lifetime is clear
struct UserView<'a> {
    name: &'a str,
    email: &'a str,
}

// Pattern: owned data + view for efficiency
impl User {
    fn view(&self) -> UserView<'_> {
        UserView {
            name: &self.name,
            email: &self.email,
        }
    }
}
```

### 2. Builder Pattern with Ownership

```rust
#[derive(Default)]
struct RequestBuilder {
    url: Option<String>,
    method: Option<String>,
    body: Option<Vec<u8>>,
}

impl RequestBuilder {
    fn new() -> Self {
        Self::default()
    }

    // Take self by value for chaining
    fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    fn method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    fn build(self) -> Result<Request, Error> {
        Ok(Request {
            url: self.url.ok_or(Error::MissingUrl)?,
            method: self.method.unwrap_or_else(|| "GET".to_string()),
            body: self.body.unwrap_or_default(),
        })
    }
}

// Usage:
let req = RequestBuilder::new()
    .url("https://example.com")
    .method("POST")
    .build()?;
```

### 3. Interior Mutability When Needed

```rust
use std::cell::RefCell;
use std::rc::Rc;

// Shared mutable state in single-threaded context
struct Counter {
    value: Rc<RefCell<u32>>,
}

impl Counter {
    fn new() -> Self {
        Counter {
            value: Rc::new(RefCell::new(0)),
        }
    }

    fn increment(&self) {
        *self.value.borrow_mut() += 1;
    }

    fn get(&self) -> u32 {
        *self.value.borrow()
    }

    fn clone_handle(&self) -> Self {
        Counter {
            value: Rc::clone(&self.value),
        }
    }
}
```

---

## Collection Patterns

### 1. Efficient Iteration

```rust
let items = vec![1, 2, 3, 4, 5];

// Iterate by reference (no move)
for item in &items {
    println!("{}", item);
}

// Iterate by mutable reference
for item in &mut items.clone() {
    *item *= 2;
}

// Consume with into_iter when done
let sum: i32 = items.into_iter().sum();
```

### 2. Collecting Results

```rust
// Collect into owned collection
let strings: Vec<String> = (0..5)
    .map(|i| format!("item_{}", i))
    .collect();

// Collect references
let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();

// Collect with transformation
let result: Result<Vec<i32>, _> = ["1", "2", "3"]
    .iter()
    .map(|s| s.parse::<i32>())
    .collect();
```

### 3. Entry API for Maps

```rust
use std::collections::HashMap;

let mut map: HashMap<String, Vec<i32>> = HashMap::new();

// Efficient: don't search twice
map.entry("key".to_string())
   .or_insert_with(Vec::new)
   .push(42);

// With entry modification
map.entry("key".to_string())
   .and_modify(|v| v.push(43))
   .or_insert_with(|| vec![43]);
```

---

## Error Handling with Ownership

### 1. Preserve Context in Errors

```rust
use std::error::Error;
use std::fmt;

#[derive(Debug)]
struct ParseError {
    input: String,  // owns the problematic input
    message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to parse '{}': {}", self.input, self.message)
    }
}

fn parse(input: &str) -> Result<i32, ParseError> {
    input.parse().map_err(|_| ParseError {
        input: input.to_string(),  // clone for error context
        message: "not a valid integer".to_string(),
    })
}
```

### 2. Ownership in Result Chains

```rust
fn process_data(path: &str) -> Result<ProcessedData, Error> {
    let content = std::fs::read_to_string(path)?;  // owned String
    let parsed = parse_content(&content)?;          // borrow
    let processed = transform(parsed)?;             // ownership moves
    Ok(processed)                                   // return owned
}
```

---

## Performance Considerations

### 1. Avoid Unnecessary Clones

```rust
// BAD: cloning just to compare
fn contains_item(items: &[String], target: &str) -> bool {
    items.iter().any(|s| s.clone() == target)  // unnecessary clone
}

// GOOD: compare references
fn contains_item(items: &[String], target: &str) -> bool {
    items.iter().any(|s| s == target)  // String implements PartialEq<str>
}
```

### 2. Use Slices for Flexibility

```rust
// BAD: requires Vec
fn sum(numbers: &Vec<i32>) -> i32 {
    numbers.iter().sum()
}

// GOOD: accepts any slice
fn sum(numbers: &[i32]) -> i32 {
    numbers.iter().sum()
}

// Now works with:
sum(&vec![1, 2, 3]);     // Vec
sum(&[1, 2, 3]);         // array
sum(&array[1..3]);       // slice
```

### 3. In-Place Mutation

```rust
// BAD: allocates new String
fn make_uppercase(s: &str) -> String {
    s.to_uppercase()
}

// GOOD when you own the data: mutate in place
fn make_uppercase(mut s: String) -> String {
    s.make_ascii_uppercase();  // in-place for ASCII
    s
}
```
