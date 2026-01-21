# Thinking in Rust: Mental Models

## Core Mental Models

### 1. Ownership as Resource Management

```
Traditional: "Who has a pointer to this data?"
Rust:        "Who OWNS this data and is responsible for freeing it?"
```

Key insight: Every value has exactly one owner. When the owner goes out of scope, the value is dropped.

```rust
{
    let s = String::from("hello");  // s owns the String
    // use s...
}  // s goes out of scope, String is dropped (memory freed)
```

### 2. Borrowing as Temporary Access

```
Traditional: "I'll just read from this pointer"
Rust:        "I'm borrowing this value, owner still responsible for it"
```

Key insight: Borrows are like library books - you can read them, but must return them.

```rust
fn print_length(s: &String) {  // borrows s
    println!("{}", s.len());
}  // borrow ends, caller still owns s

let my_string = String::from("hello");
print_length(&my_string);  // lend to function
println!("{}", my_string);  // still have it
```

### 3. Lifetimes as Validity Scopes

```
Traditional: "Hope this pointer is still valid"
Rust:        "Compiler tracks exactly how long references are valid"
```

Key insight: A reference can't outlive the data it points to.

```rust
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    // 'a means: the returned reference is valid as long as BOTH inputs are valid
    if x.len() > y.len() { x } else { y }
}
```

---

## Shifting Perspectives

### From "Everything is a Reference" (Java/C#)

Java mental model:
```java
// Everything is implicitly a reference
User user = new User("Alice");  // user is a reference
List<User> users = new ArrayList<>();
users.add(user);  // shares the reference
user.setName("Bob");  // affects the list too!
```

Rust mental model:
```rust
// Values are owned, sharing is explicit
let user = User::new("Alice");  // user is owned
let mut users = vec![];
users.push(user);  // user moved into vec, can't use user anymore
// user.set_name("Bob");  // ERROR: user was moved

// If you need sharing:
use std::rc::Rc;
let user = Rc::new(User::new("Alice"));
let user2 = Rc::clone(&user);  // explicit shared ownership
```

### From "Manual Memory Management" (C/C++)

C mental model:
```c
char* s = malloc(100);
// ... must remember to free(s) ...
// ... what if we return early? ...
// ... what if an exception occurs? ...
free(s);
```

Rust mental model:
```rust
let s = String::with_capacity(100);
// ... use s ...
// No need to free - Rust drops s automatically when scope ends
// Even with early returns, panics, or any control flow
```

### From "Garbage Collection" (Go/Python)

GC mental model:
```python
# Create objects, GC will figure it out
users = []
for name in names:
    users.append(User(name))
# GC runs sometime later, when it feels like it
```

Rust mental model:
```rust
let users: Vec<User> = names
    .iter()
    .map(|name| User::new(name))
    .collect();
// Memory is freed EXACTLY when users goes out of scope
// Deterministic, no GC pauses, no unpredictable memory usage
```

---

## Key Questions to Ask

### When Designing Functions

1. **Does this function need to own the data, or just read it?**
   - Need to keep it: take ownership (`fn process(data: Vec<T>)`)
   - Just reading: borrow (`fn process(data: &[T])`)
   - Need to modify: mutable borrow (`fn process(data: &mut Vec<T>)`)

2. **Does the return value contain references to inputs?**
   - Yes: need lifetime annotations
   - No: lifetime elision usually works

### When Designing Structs

1. **Should this struct own its data or reference it?**
   - Long-lived, independent: own (`name: String`)
   - Short-lived view: reference (`name: &'a str`)

2. **Do multiple parts need to access the same data?**
   - Single-threaded: `Rc<T>` or `Rc<RefCell<T>>`
   - Multi-threaded: `Arc<T>` or `Arc<Mutex<T>>`

### When Hitting Borrow Checker Errors

1. **Am I trying to use a value after moving it?**
   - Clone it, borrow it, or restructure the code

2. **Am I trying to have multiple mutable references?**
   - Scope the mutations, use interior mutability, or redesign

3. **Does a reference outlive its source?**
   - Return owned data instead, or use `'static`

---

## Common Patterns

### The Clone Escape Hatch

When fighting the borrow checker, `.clone()` often works:

```rust
// Can't do this - double borrow
let mut map = HashMap::new();
for key in map.keys() {
    map.insert(key.clone(), process(key));  // ERROR: map borrowed twice
}

// Clone to escape
let keys: Vec<_> = map.keys().cloned().collect();
for key in keys {
    map.insert(key.clone(), process(&key));  // OK
}
```

But ask: "Is there a better design?" Often, restructuring is better than cloning.

### The "Make It Own" Pattern

When lifetimes get complex, make the struct own its data:

```rust
// Complex: struct with references
struct Parser<'a> {
    input: &'a str,
    current: &'a str,
}

// Simpler: struct owns data
struct Parser {
    input: String,
    position: usize,
}
```

### The "Split the Borrow" Pattern

```rust
struct Data {
    field_a: Vec<i32>,
    field_b: Vec<i32>,
}

// Can't borrow self mutably twice
fn process(&mut self) {
    // for a in &self.field_a {
    //     self.field_b.push(*a);  // ERROR
    // }

    // Split the borrow
    let Data { field_a, field_b } = self;
    for a in field_a.iter() {
        field_b.push(*a);  // OK: separate borrows
    }
}
```

---

## The Rust Way

### Embrace the Type System

```rust
// Don't: stringly-typed
fn connect(host: &str, port: &str) { ... }
connect("8080", "localhost");  // oops, wrong order

// Do: strongly-typed
struct Host(String);
struct Port(u16);
fn connect(host: Host, port: Port) { ... }
// connect(Port(8080), Host("localhost".into()));  // compile error!
```

### Make Invalid States Unrepresentable

```rust
// Don't: runtime checks
struct Connection {
    socket: Option<Socket>,
    connected: bool,
}

// Do: types enforce states
enum Connection {
    Disconnected,
    Connected { socket: Socket },
}
```

### Let the Compiler Guide You

```rust
// Start with what you want
fn process(data: ???) -> ???

// Let compiler errors tell you:
// - What types are needed
// - What lifetimes are needed
// - What bounds are needed

// The error messages are documentation!
```

---

## Summary: The Rust Mental Model

1. **Values have owners** - exactly one at a time
2. **Borrowing is lending** - temporary access, owner retains responsibility
3. **Lifetimes are scopes** - compiler tracks validity
4. **Types encode constraints** - use them to prevent bugs
5. **The compiler is your friend** - work with it, not against it

When stuck:
- Clone to make progress
- Restructure to own instead of borrow
- Ask: "What is the compiler trying to tell me?"
