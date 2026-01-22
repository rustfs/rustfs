---
id: union-01
original_id: P.UNS.UNI.01
level: P
impact: HIGH
---

# Avoid Union Except for C Interop

## Summary

Only use `union` for FFI with C code. For Rust-only code, use `enum` with explicit tags.

## Rationale

- Unions require unsafe to read (any field access is unsafe)
- Easy to read wrong field, causing undefined behavior
- Enums are type-safe and the compiler tracks the active variant
- Unions don't run destructors properly

## Bad Example

```rust
// DON'T: Use union for space optimization in Rust-only code
union IntOrFloat {
    i: i32,
    f: f32,
}

fn bad_usage() {
    let mut u = IntOrFloat { i: 42 };

    // BAD: Reading wrong field is UB
    let f = unsafe { u.f };  // UB if i was the last written field
}

// DON'T: Use union for variant types
union Variant {
    string: std::mem::ManuallyDrop<String>,
    number: i64,
}

// Problems:
// 1. Must manually track which variant is active
// 2. Must manually call drop on String variant
// 3. Easy to have memory leaks or double-free
```

## Good Example

```rust
// DO: Use enum for variant types in Rust
enum Variant {
    String(String),
    Number(i64),
}

// Compiler tracks active variant, runs correct destructor

// DO: Use union only for C FFI
#[repr(C)]
union CUnion {
    i: i32,
    f: f32,
}

// When interfacing with C code that uses this union
extern "C" {
    fn c_function_returns_union() -> CUnion;
    fn c_function_takes_union(u: CUnion);
}

// DO: Wrap in safe API with explicit variant tracking
#[repr(C)]
pub struct SafeUnion {
    tag: u8,
    data: CUnion,
}

impl SafeUnion {
    pub fn as_int(&self) -> Option<i32> {
        if self.tag == 0 {
            // SAFETY: Tag indicates integer variant is active
            Some(unsafe { self.data.i })
        } else {
            None
        }
    }
}
```

## When Union Is Appropriate

1. **C FFI**: Matching C union layout for interoperability
2. **MaybeUninit**: The standard library uses union internally
3. **Very low-level optimization**: Only after profiling and careful safety analysis

## Alternatives to Union

| Use Case | Instead of Union | Use |
|----------|-----------------|-----|
| Variant types | union + tag | `enum` |
| Optional value | union + bool | `Option<T>` |
| Type punning | union | `transmute` or `from_ne_bytes` |
| Uninitialized memory | union | `MaybeUninit<T>` |

## Checklist

- [ ] Is this for C FFI? If not, use enum
- [ ] If union is necessary, is there a tag tracking active variant?
- [ ] Are destructors handled correctly for Drop types?
- [ ] Is the union #[repr(C)] for FFI?

## Related Rules

- `union-02`: Don't use union variants across lifetimes
- `ffi-13`: Ensure consistent data layout
