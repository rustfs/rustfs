---
id: ptr-06
original_id: G.UNS.PTR.03
level: G
impact: LOW
clippy: ptr_as_ptr
---

# Prefer pointer::cast Over `as` for Pointer Casting

## Summary

Use the `cast()` method instead of `as` for pointer type conversions. It's clearer and prevents accidental provenance loss.

## Rationale

- `cast()` only changes the pointed-to type, not pointer properties
- `as` can accidentally convert to integer and back, losing provenance
- `cast()` is more explicit about intent
- Better tooling support (clippy, miri)

## Bad Example

```rust
// DON'T: Use `as` for pointer casts
fn bad_cast(ptr: *const u8) -> *const i32 {
    ptr as *const i32  // Works, but less clear
}

// DON'T: Accidental provenance loss
fn bad_roundtrip(ptr: *const u8) -> *const u8 {
    let addr = ptr as usize;   // Converts to integer
    addr as *const u8          // Loses provenance information!
}

// DON'T: Multiple `as` casts in chain
fn bad_chain(ptr: *const u8) -> *mut i32 {
    ptr as *mut u8 as *mut i32  // Hard to follow
}
```

## Good Example

```rust
// DO: Use cast() for pointer type changes
fn good_cast(ptr: *const u8) -> *const i32 {
    ptr.cast::<i32>()
}

// DO: Use cast_mut() for const-to-mut (when valid)
fn good_cast_mut(ptr: *const u8) -> *mut u8 {
    ptr.cast_mut()  // Only use when mutation is valid!
}

// DO: Use cast_const() for mut-to-const
fn good_cast_const(ptr: *mut u8) -> *const u8 {
    ptr.cast_const()
}

// DO: Chain casts clearly
fn good_chain(ptr: *const u8) -> *mut i32 {
    ptr.cast_mut().cast::<i32>()
}

// DO: Use with_addr() for address manipulation (nightly)
#[cfg(feature = "strict_provenance")]
fn good_provenance(ptr: *const u8, new_addr: usize) -> *const u8 {
    ptr.with_addr(new_addr)  // Preserves provenance
}
```

## Pointer Method Reference

| Method | From | To | Notes |
|--------|------|-----|-------|
| `.cast::<U>()` | `*T` | `*U` | Changes pointee type |
| `.cast_mut()` | `*const T` | `*mut T` | Removes const |
| `.cast_const()` | `*mut T` | `*const T` | Adds const |
| `.addr()` | `*T` | `usize` | Gets address (nightly) |
| `.with_addr(usize)` | `*T` | `*T` | Changes address, keeps provenance |
| `.map_addr(fn)` | `*T` | `*T` | Transforms address |

## Provenance Considerations

```rust
// Provenance = permission to access memory

// BAD: Loses provenance
let ptr: *const u8 = &data as *const u8;
let addr = ptr as usize;
let ptr2 = addr as *const u8;  // ptr2 has no provenance!

// GOOD: Preserves provenance (nightly strict_provenance)
let ptr2 = ptr.with_addr(addr);  // Still has permission

// GOOD: Use expose/from_exposed when provenance must cross integer
let addr = ptr.expose_addr();  // "Expose" the provenance
let ptr2 = std::ptr::from_exposed_addr(addr);  // Recover it
```

## Checklist

- [ ] Am I using `as` where `cast()` would be clearer?
- [ ] Am I accidentally converting through `usize`?
- [ ] Do I need to preserve provenance?

## Related Rules

- `ptr-04`: Alignment considerations when casting
- `ptr-05`: Don't convert const to mut improperly
