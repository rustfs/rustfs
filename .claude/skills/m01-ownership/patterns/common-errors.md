# Common Ownership Errors & Fixes

## E0382: Use of Moved Value

### Error Pattern
```rust
let s = String::from("hello");
let s2 = s;          // s moved here
println!("{}", s);   // ERROR: value borrowed after move
```

### Fix Options

**Option 1: Clone (if ownership not needed)**
```rust
let s = String::from("hello");
let s2 = s.clone();  // s is cloned
println!("{}", s);   // OK: s still valid
```

**Option 2: Borrow (if modification not needed)**
```rust
let s = String::from("hello");
let s2 = &s;         // borrow, not move
println!("{}", s);   // OK
println!("{}", s2);  // OK
```

**Option 3: Use Rc/Arc (for shared ownership)**
```rust
use std::rc::Rc;
let s = Rc::new(String::from("hello"));
let s2 = Rc::clone(&s);  // shared ownership
println!("{}", s);       // OK
println!("{}", s2);      // OK
```

---

## E0597: Borrowed Value Does Not Live Long Enough

### Error Pattern
```rust
fn get_str() -> &str {
    let s = String::from("hello");
    &s  // ERROR: s dropped here, but reference returned
}
```

### Fix Options

**Option 1: Return owned value**
```rust
fn get_str() -> String {
    String::from("hello")  // return owned value
}
```

**Option 2: Use 'static lifetime**
```rust
fn get_str() -> &'static str {
    "hello"  // string literal has 'static lifetime
}
```

**Option 3: Accept reference parameter**
```rust
fn get_str<'a>(s: &'a str) -> &'a str {
    s  // return reference with same lifetime as input
}
```

---

## E0499: Cannot Borrow as Mutable More Than Once

### Error Pattern
```rust
let mut s = String::from("hello");
let r1 = &mut s;
let r2 = &mut s;  // ERROR: second mutable borrow
println!("{}, {}", r1, r2);
```

### Fix Options

**Option 1: Sequential borrows**
```rust
let mut s = String::from("hello");
{
    let r1 = &mut s;
    r1.push_str(" world");
}  // r1 goes out of scope
let r2 = &mut s;  // OK: r1 no longer exists
```

**Option 2: Use RefCell for interior mutability**
```rust
use std::cell::RefCell;
let s = RefCell::new(String::from("hello"));
let mut r1 = s.borrow_mut();
// drop r1 before borrowing again
drop(r1);
let mut r2 = s.borrow_mut();
```

---

## E0502: Cannot Borrow as Mutable While Immutable Borrow Exists

### Error Pattern
```rust
let mut v = vec![1, 2, 3];
let first = &v[0];      // immutable borrow
v.push(4);              // ERROR: mutable borrow while immutable exists
println!("{}", first);
```

### Fix Options

**Option 1: Finish using immutable borrow first**
```rust
let mut v = vec![1, 2, 3];
let first = v[0];       // copy value, not borrow
v.push(4);              // OK
println!("{}", first);  // OK: using copied value
```

**Option 2: Clone before mutating**
```rust
let mut v = vec![1, 2, 3];
let first = v[0].clone();  // if T: Clone
v.push(4);
println!("{}", first);
```

---

## E0507: Cannot Move Out of Borrowed Content

### Error Pattern
```rust
fn take_string(s: &String) {
    let moved = *s;  // ERROR: cannot move out of borrowed content
}
```

### Fix Options

**Option 1: Clone**
```rust
fn take_string(s: &String) {
    let cloned = s.clone();
}
```

**Option 2: Take ownership in function signature**
```rust
fn take_string(s: String) {  // take ownership
    let moved = s;
}
```

**Option 3: Use mem::take for Option/Default types**
```rust
fn take_from_option(opt: &mut Option<String>) -> Option<String> {
    std::mem::take(opt)  // replaces with None, returns owned value
}
```

---

## E0515: Return Local Reference

### Error Pattern
```rust
fn create_string() -> &String {
    let s = String::from("hello");
    &s  // ERROR: cannot return reference to local variable
}
```

### Fix Options

**Option 1: Return owned value**
```rust
fn create_string() -> String {
    String::from("hello")
}
```

**Option 2: Use static/const**
```rust
fn get_static_str() -> &'static str {
    "hello"
}
```

---

## E0716: Temporary Value Dropped While Borrowed

### Error Pattern
```rust
let r: &str = &String::from("hello");  // ERROR: temporary dropped
println!("{}", r);
```

### Fix Options

**Option 1: Bind to variable first**
```rust
let s = String::from("hello");
let r: &str = &s;
println!("{}", r);
```

**Option 2: Use let binding with reference**
```rust
let r: &str = {
    let s = String::from("hello");
    // s.as_str()  // ERROR: still temporary
    Box::leak(s.into_boxed_str())  // extreme: leak for 'static
};
```

---

## Pattern: Loop Ownership Issues

### Error Pattern
```rust
let strings = vec![String::from("a"), String::from("b")];
for s in strings {
    println!("{}", s);
}
// ERROR: strings moved into loop
println!("{:?}", strings);
```

### Fix Options

**Option 1: Iterate by reference**
```rust
let strings = vec![String::from("a"), String::from("b")];
for s in &strings {
    println!("{}", s);
}
println!("{:?}", strings);  // OK
```

**Option 2: Use iter()**
```rust
for s in strings.iter() {
    println!("{}", s);
}
```

**Option 3: Clone if needed**
```rust
for s in strings.clone() {
    // consumes cloned vec
}
println!("{:?}", strings);  // original still available
```
