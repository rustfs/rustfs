---
id: mem-05
original_id: P.UNS.MEM.05
level: P
impact: MEDIUM
---

# Use Third-Party Crates for Bitfields

## Summary

Use crates like `bitflags`, `bitvec`, or `modular-bitfield` instead of manual bit manipulation for complex bitfield operations.

## Rationale

- Manual bit manipulation is error-prone
- Easy to get offsets, masks, or endianness wrong
- Crates provide type-safe, tested abstractions
- Proc-macro crates generate efficient code

## Bad Example

```rust
// DON'T: Manual bitfield manipulation
struct Flags(u32);

impl Flags {
    const READ: u32 = 1 << 0;
    const WRITE: u32 = 1 << 1;
    const EXECUTE: u32 = 1 << 2;

    fn has_read(&self) -> bool {
        (self.0 & Self::READ) != 0
    }

    fn set_read(&mut self) {
        self.0 |= Self::READ;
    }

    fn clear_read(&mut self) {
        self.0 &= !Self::READ;  // Easy to forget the !
    }
}

// DON'T: Manual packed bitfields for FFI
#[repr(C)]
struct PackedHeader {
    data: u32,
}

impl PackedHeader {
    // Error-prone: wrong shift or mask values
    fn version(&self) -> u8 {
        ((self.data >> 24) & 0xFF) as u8
    }

    fn flags(&self) -> u16 {
        ((self.data >> 8) & 0xFFFF) as u16
    }

    fn tag(&self) -> u8 {
        (self.data & 0xFF) as u8
    }
}
```

## Good Example

```rust
// DO: Use bitflags for flag sets
use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Flags: u32 {
        const READ = 1 << 0;
        const WRITE = 1 << 1;
        const EXECUTE = 1 << 2;
        const RW = Self::READ.bits() | Self::WRITE.bits();
    }
}

fn use_flags() {
    let mut flags = Flags::READ | Flags::WRITE;
    flags.insert(Flags::EXECUTE);
    flags.remove(Flags::WRITE);

    if flags.contains(Flags::READ) {
        println!("Readable");
    }
}

// DO: Use modular-bitfield for packed structures
use modular_bitfield::prelude::*;

#[bitfield]
#[repr(C)]
struct PackedHeader {
    tag: B8,      // 8 bits
    flags: B16,   // 16 bits
    version: B8,  // 8 bits
}

fn use_packed() {
    let header = PackedHeader::new()
        .with_version(1)
        .with_flags(0x1234)
        .with_tag(0xAB);

    assert_eq!(header.version(), 1);
    assert_eq!(header.flags(), 0x1234);
}

// DO: Use bitvec for arbitrary bit manipulation
use bitvec::prelude::*;

fn use_bitvec() {
    let mut bits = bitvec![u8, Msb0; 0; 16];
    bits.set(0, true);
    bits.set(7, true);

    let byte: u8 = bits[0..8].load_be();
    assert_eq!(byte, 0b1000_0001);
}
```

## Recommended Crates

| Crate | Use Case | Features |
|-------|----------|----------|
| `bitflags` | Flag sets (like C enums) | Type-safe, const, derives |
| `modular-bitfield` | Packed struct fields | Proc macro, repr(C) |
| `bitvec` | Arbitrary bit arrays | Slicing, iteration |
| `packed_struct` | Binary protocol structs | Endianness, derive |
| `deku` | Binary parsing | Derive, read/write |

## Checklist

- [ ] Am I manipulating multiple bit flags? → Use `bitflags`
- [ ] Am I packing fields into bytes? → Use `modular-bitfield` or `packed_struct`
- [ ] Am I doing binary protocol work? → Consider `deku`
- [ ] Is the manual approach really simpler?

## Related Rules

- `mem-01`: Choose appropriate data layout
- `ffi-13`: Ensure consistent data layout
