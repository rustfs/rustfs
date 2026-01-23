---
id: union-02
original_id: P.UNS.UNI.02
level: P
impact: CRITICAL
---

# Do Not Use Union Variants Across Different Lifetimes

## Summary

Do not write to one union field and read from another field that has a different lifetime or references data with a different lifetime.

## Rationale

Union fields share the same memory. If one field stores a reference with lifetime `'a` and you read it as a reference with lifetime `'b`, you bypass lifetime checking and can create dangling references.

## Bad Example

```rust
// DON'T: Extend lifetime through union
union LifetimeBypass<'a, 'b> {
    short: &'a str,
    long: &'b str,
}

fn bad_lifetime_extension<'a, 'b>(short: &'a str) -> &'b str {
    let u = LifetimeBypass { short };
    // BAD: Reading with different lifetime is UB
    unsafe { u.long }
}

fn exploit() {
    let long_ref: &'static str;
    {
        let temp = String::from("temporary");
        // Extend local reference to 'static - dangling pointer!
        long_ref = bad_lifetime_extension(&temp);
    }
    // temp is dropped, long_ref is dangling
    println!("{}", long_ref);  // UB: use after free
}
```

## Good Example

```rust
// DO: Use same lifetime for all reference fields
union SafeUnion<'a> {
    str_ref: &'a str,
    bytes_ref: &'a [u8],
}

fn safe_conversion<'a>(s: &'a str) -> &'a [u8] {
    let u = SafeUnion { str_ref: s };
    // SAFETY: Both fields have same lifetime 'a
    // AND str and [u8] have compatible representations
    unsafe { u.bytes_ref }
}

// Better: Just use as_bytes()
fn better_conversion(s: &str) -> &[u8] {
    s.as_bytes()
}

// DO: Use MaybeUninit for delayed initialization, not lifetime tricks
use std::mem::MaybeUninit;

fn delayed_init<T>(init: impl FnOnce() -> T) -> T {
    let mut value: MaybeUninit<T> = MaybeUninit::uninit();
    value.write(init());
    unsafe { value.assume_init() }
}
```

## Why This Is Dangerous

The Rust lifetime system prevents use-after-free by tracking how long references are valid. Unions can subvert this:

```
Memory: [pointer to "hello"]

Union as 'short: points to stack memory (valid during function)
Union as 'long:  claims to point to valid memory forever

Reality: After function returns, pointer is dangling
```

## Safe Union Patterns

```rust
// Pattern 1: All fields have same lifetime
union SameLifetime<'a, T, U> {
    a: &'a T,
    b: &'a U,
}

// Pattern 2: No references at all
#[repr(C)]
union NoRefs {
    i: i32,
    f: f32,
}

// Pattern 3: Use ManuallyDrop for owned values (careful with Drop!)
union OwnedUnion {
    s: std::mem::ManuallyDrop<String>,
    v: std::mem::ManuallyDrop<Vec<u8>>,
}
```

## Checklist

- [ ] Do all reference fields have the same lifetime parameter?
- [ ] Am I trying to extend a lifetime through union? (If yes, stop!)
- [ ] For owned types, am I handling Drop correctly?

## Related Rules

- `union-01`: Avoid union except for C interop
- `safety-02`: Verify safety invariants
