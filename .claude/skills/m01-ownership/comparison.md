# Ownership: Comparison with Other Languages

## Rust vs C++

### Memory Management

| Aspect | Rust | C++ |
|--------|------|-----|
| Default | Move semantics | Copy semantics (pre-C++11) |
| Move | `let b = a;` (a invalidated) | `auto b = std::move(a);` (a valid but unspecified) |
| Copy | `let b = a.clone();` | `auto b = a;` |
| Safety | Compile-time enforcement | Runtime responsibility |

### Rust Move vs C++ Move

```rust
// Rust: after move, 'a' is INVALID
let a = String::from("hello");
let b = a;  // a moved
// println!("{}", a);  // COMPILE ERROR

// Equivalent in C++:
// std::string a = "hello";
// std::string b = std::move(a);
// std::cout << a;  // UNDEFINED (compiles but buggy)
```

### Smart Pointers

| Rust | C++ | Purpose |
|------|-----|---------|
| `Box<T>` | `std::unique_ptr<T>` | Unique ownership |
| `Rc<T>` | `std::shared_ptr<T>` | Shared ownership |
| `Arc<T>` | `std::shared_ptr<T>` + atomic | Thread-safe shared |
| `RefCell<T>` | (manual runtime checks) | Interior mutability |

---

## Rust vs Go

### Memory Model

| Aspect | Rust | Go |
|--------|------|-----|
| Memory | Stack + heap, explicit | GC manages all |
| Ownership | Enforced at compile-time | None (GC handles) |
| Null | `Option<T>` | `nil` for pointers |
| Concurrency | `Send`/`Sync` traits | Channels (less strict) |

### Sharing Data

```rust
// Rust: explicit about sharing
use std::sync::Arc;
let data = Arc::new(vec![1, 2, 3]);
let data_clone = Arc::clone(&data);
std::thread::spawn(move || {
    println!("{:?}", data_clone);
});

// Go: implicit sharing
// data := []int{1, 2, 3}
// go func() {
//     fmt.Println(data)  // potential race condition
// }()
```

### Why No GC in Rust

1. **Deterministic destruction**: Resources freed exactly when scope ends
2. **Zero-cost**: No GC pauses or overhead
3. **Embeddable**: Works in OS kernels, embedded systems
4. **Predictable latency**: Critical for real-time systems

---

## Rust vs Java/C#

### Reference Semantics

| Aspect | Rust | Java/C# |
|--------|------|---------|
| Objects | Owned by default | Reference by default |
| Null | `Option<T>` | `null` (nullable) |
| Immutability | Default | Must use `final`/`readonly` |
| Copy | Explicit `.clone()` | Reference copy (shallow) |

### Comparison

```rust
// Rust: clear ownership
fn process(data: Vec<i32>) {  // takes ownership
    // data is ours, will be freed at end
}

let numbers = vec![1, 2, 3];
process(numbers);
// numbers is invalid here

// Java: ambiguous ownership
// void process(List<Integer> data) {
//     // Who owns data? Caller? Callee? Both?
//     // Can caller still use it?
// }
```

---

## Rust vs Python

### Memory Model

| Aspect | Rust | Python |
|--------|------|--------|
| Typing | Static, compile-time | Dynamic, runtime |
| Memory | Ownership-based | Reference counting + GC |
| Mutability | Default immutable | Default mutable |
| Performance | Native, zero-cost | Interpreted, higher overhead |

### Common Pattern Translation

```rust
// Rust: borrowing iteration
let items = vec!["a", "b", "c"];
for item in &items {
    println!("{}", item);
}
// items still usable

// Python: iteration doesn't consume
// items = ["a", "b", "c"]
// for item in items:
//     print(item)
// items still usable (different reason - ref counting)
```

---

## Unique Rust Concepts

### Concepts Other Languages Lack

1. **Borrow Checker**: No other mainstream language has compile-time borrow checking
2. **Lifetimes**: Explicit annotation of reference validity
3. **Move by Default**: Values move, not copy
4. **No Null**: `Option<T>` instead of null pointers
5. **Affine Types**: Values can be used at most once

### Learning Curve Areas

| Concept | Coming From | Key Insight |
|---------|-------------|-------------|
| Ownership | GC languages | Think about who "owns" data |
| Borrowing | C/C++ | Like references but checked |
| Lifetimes | Any | Explicit scope of validity |
| Move | C++ | Move is default, not copy |

---

## Mental Model Shifts

### From GC Languages (Java, Go, Python)

```
Before: "Memory just works, GC handles it"
After:  "I explicitly decide who owns data and when it's freed"
```

Key shifts:
- Think about ownership at design time
- Returning references requires lifetime thinking
- No more `null` - use `Option<T>`

### From C/C++

```
Before: "I manually manage memory and hope I get it right"
After:  "Compiler enforces correctness, I fight the borrow checker"
```

Key shifts:
- Trust the compiler's errors
- Move is the default (unlike C++ copy)
- Smart pointers are idiomatic, not overhead

### From Functional Languages (Haskell, ML)

```
Before: "Everything is immutable, copying is fine"
After:  "Mutability is explicit, ownership prevents aliasing"
```

Key shifts:
- Mutability is safe because of ownership rules
- No persistent data structures needed (usually)
- Performance characteristics are explicit

---

## Performance Trade-offs

| Language | Memory Overhead | Latency | Throughput |
|----------|-----------------|---------|------------|
| Rust | Minimal (no GC) | Predictable | Excellent |
| C++ | Minimal | Predictable | Excellent |
| Go | GC overhead | GC pauses | Good |
| Java | GC overhead | GC pauses | Good |
| Python | High (ref counting + GC) | Variable | Lower |

### When Rust Ownership Wins

1. **Real-time systems**: No GC pauses
2. **Embedded**: No runtime overhead
3. **High-performance**: Zero-cost abstractions
4. **Concurrent**: Data races prevented at compile time

### When GC Might Be Preferable

1. **Rapid prototyping**: Less mental overhead
2. **Complex object graphs**: Cycles are tricky in Rust
3. **GUI applications**: Object lifetimes are dynamic
4. **Small programs**: Overhead doesn't matter
