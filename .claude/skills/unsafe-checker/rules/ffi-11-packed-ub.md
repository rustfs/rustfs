---
id: ffi-11
original_id: P.UNS.FFI.11
level: P
impact: HIGH
clippy: unaligned_references
---

# Be Careful with UB When Referencing #[repr(packed)] Struct Fields

## Summary

Creating references to fields in `#[repr(packed)]` structs is undefined behavior if the field is misaligned. Use raw pointers and `read_unaligned`/`write_unaligned` instead.

## Rationale

- Packed structs have no padding, so fields may be misaligned
- References must be aligned; misaligned references are UB
- Even implicit references (method calls, match) can cause UB

## Bad Example

```rust
#[repr(C, packed)]
struct Packet {
    header: u8,
    value: u32,   // Misaligned! At offset 1, not 4
    data: u64,    // Misaligned! At offset 5, not 8
}

fn bad_reference(p: &Packet) -> &u32 {
    &p.value  // UB: Creates misaligned reference!
}

fn bad_match(p: &Packet) {
    match p.value {  // UB: Match creates a reference
        0 => {},
        _ => {},
    }
}

fn bad_method(p: &Packet) {
    p.value.to_string();  // UB: Method call creates reference
}

fn bad_borrow(p: &mut Packet) {
    let v = &mut p.value;  // UB: Misaligned mutable reference
    *v = 42;
}
```

## Good Example

```rust
#[repr(C, packed)]
struct Packet {
    header: u8,
    value: u32,
    data: u64,
}

// DO: Copy out the value
fn good_read(p: &Packet) -> u32 {
    p.value  // Copies the value, no reference created
}

// DO: Use addr_of! for raw pointer (Rust 2021+)
fn good_ptr_read(p: &Packet) -> u32 {
    // SAFETY: read_unaligned handles misalignment
    unsafe {
        std::ptr::addr_of!(p.value).read_unaligned()
    }
}

// DO: Use addr_of_mut! for writing
fn good_ptr_write(p: &mut Packet, value: u32) {
    // SAFETY: write_unaligned handles misalignment
    unsafe {
        std::ptr::addr_of_mut!(p.value).write_unaligned(value);
    }
}

// DO: Create accessor methods
impl Packet {
    fn value(&self) -> u32 {
        unsafe { std::ptr::addr_of!(self.value).read_unaligned() }
    }

    fn set_value(&mut self, value: u32) {
        unsafe { std::ptr::addr_of_mut!(self.value).write_unaligned(value); }
    }

    fn data(&self) -> u64 {
        unsafe { std::ptr::addr_of!(self.data).read_unaligned() }
    }
}

// DO: Consider using byte arrays + from_ne_bytes
#[repr(C, packed)]
struct PacketBytes {
    header: u8,
    value: [u8; 4],  // Store as bytes
    data: [u8; 8],
}

impl PacketBytes {
    fn value(&self) -> u32 {
        u32::from_ne_bytes(self.value)  // Safe, no alignment issue
    }
}
```

## Safe Alternatives

```rust
// Alternative 1: Don't use packed
#[repr(C)]
struct AlignedPacket {
    header: u8,
    _pad: [u8; 3],
    value: u32,
    data: u64,
}

// Alternative 2: Use zerocopy crate
// use zerocopy::{AsBytes, FromBytes};

// Alternative 3: Use bytemuck
// use bytemuck::{Pod, Zeroable};
```

## Checklist

- [ ] Am I creating references to packed struct fields?
- [ ] Am I using addr_of! / addr_of_mut! for field access?
- [ ] Am I using read_unaligned / write_unaligned?
- [ ] Would a byte array representation be safer?

## Related Rules

- `ptr-04`: Don't dereference misaligned pointers
- `mem-01`: Choose appropriate data layout
