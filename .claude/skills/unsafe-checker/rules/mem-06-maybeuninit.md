---
id: mem-06
original_id: G.UNS.MEM.01
level: G
impact: HIGH
clippy: uninit_assumed_init, uninit_vec
---

# Use MaybeUninit<T> for Uninitialized Memory

## Summary

Use `MaybeUninit<T>` instead of `mem::uninitialized()` or `mem::zeroed()` when working with uninitialized memory.

## Rationale

- `mem::uninitialized()` is deprecated and unsound
- `mem::zeroed()` is UB for types where zero is invalid (references, NonZero, bool)
- `MaybeUninit<T>` clearly marks memory as potentially uninitialized
- Compiler can optimize based on initialization state

## Bad Example

```rust
// DON'T: Use deprecated uninitialized
fn bad_uninit<T>() -> T {
    unsafe { std::mem::uninitialized() }  // Deprecated, UB
}

// DON'T: Use zeroed for types where zero is invalid
fn bad_zeroed() -> &'static str {
    unsafe { std::mem::zeroed() }  // UB: null reference
}

fn bad_zeroed_bool() -> bool {
    unsafe { std::mem::zeroed() }  // UB: 0 might not be valid bool
}

// DON'T: Transmute to "initialize"
fn bad_transmute() -> [String; 10] {
    unsafe { std::mem::transmute([0u8; std::mem::size_of::<[String; 10]>()]) }
}

// DON'T: Set Vec length without initializing
fn bad_vec() -> Vec<String> {
    let mut v = Vec::with_capacity(10);
    unsafe { v.set_len(10); }  // Elements are uninitialized!
    v
}
```

## Good Example

```rust
use std::mem::MaybeUninit;

// DO: Use MaybeUninit for delayed initialization
fn good_array() -> [String; 10] {
    let mut arr: [MaybeUninit<String>; 10] =
        unsafe { MaybeUninit::uninit().assume_init() };

    for (i, elem) in arr.iter_mut().enumerate() {
        elem.write(format!("item {}", i));
    }

    // SAFETY: All elements initialized above
    unsafe { std::mem::transmute::<_, [String; 10]>(arr) }
}

// DO: Use MaybeUninit with arrays (cleaner with array_assume_init)
fn good_array_nightly() -> [String; 10] {
    let mut arr: [MaybeUninit<String>; 10] =
        [const { MaybeUninit::uninit() }; 10];

    for (i, elem) in arr.iter_mut().enumerate() {
        elem.write(format!("item {}", i));
    }

    // On nightly: arr.map(|e| unsafe { e.assume_init() })
    unsafe { MaybeUninit::array_assume_init(arr) }
}

// DO: Use zeroed only for types where it's valid
fn good_zeroed() -> [u8; 1024] {
    // SAFETY: All-zero bytes is valid for u8
    unsafe { std::mem::zeroed() }
}

// DO: Initialize buffer properly
fn good_vec() -> Vec<u8> {
    let mut v = Vec::with_capacity(1024);

    // Option 1: Resize with default value
    v.resize(1024, 0);

    // Option 2: Use spare_capacity_mut
    let spare = v.spare_capacity_mut();
    for elem in spare.iter_mut().take(1024) {
        elem.write(0);
    }
    unsafe { v.set_len(1024); }

    v
}

// DO: Use MaybeUninit::uninit_array (nightly) or const array
fn good_uninit_array<const N: usize>() -> [MaybeUninit<u8>; N] {
    // Stable: create array of uninit
    [const { MaybeUninit::uninit() }; N]
}
```

## MaybeUninit API

```rust
use std::mem::MaybeUninit;

// Creation
let uninit: MaybeUninit<T> = MaybeUninit::uninit();
let zeroed: MaybeUninit<T> = MaybeUninit::zeroed();
let init: MaybeUninit<T> = MaybeUninit::new(value);

// Writing
uninit.write(value);  // Returns &mut T

// Reading (unsafe)
let value: T = unsafe { uninit.assume_init() };
let ref_: &T = unsafe { uninit.assume_init_ref() };
let mut_: &mut T = unsafe { uninit.assume_init_mut() };

// Pointer access
let ptr: *const T = uninit.as_ptr();
let mut_ptr: *mut T = uninit.as_mut_ptr();
```

## Checklist

- [ ] Am I using `mem::uninitialized()`? → Replace with `MaybeUninit`
- [ ] Am I using `mem::zeroed()` for non-POD types? → Use `MaybeUninit`
- [ ] Am I setting Vec length without initialization? → Use proper initialization
- [ ] Have I initialized all MaybeUninit before assume_init?

## Related Rules

- `safety-03`: Don't expose uninitialized memory in APIs
- `safety-01`: Panic safety with partial initialization
