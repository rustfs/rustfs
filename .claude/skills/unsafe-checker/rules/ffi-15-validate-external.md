---
id: ffi-15
original_id: P.UNS.FFI.15
level: P
impact: HIGH
---

# Validate Non-Robust External Values

## Summary

Data received from external sources (FFI, files, network) may be invalid. Validate before using it as Rust types with stricter invariants.

## Rationale

- External data can be malicious or corrupted
- Rust types have invariants (e.g., valid UTF-8 for str)
- Invalid data causes undefined behavior

## Bad Example

```rust
// DON'T: Trust external data
extern "C" {
    fn get_status() -> u8;
}

#[derive(Debug)]
enum Status { Active = 0, Inactive = 1, Pending = 2 }

fn bad_convert() -> Status {
    let raw = unsafe { get_status() };
    // BAD: Assumes C returns valid enum value
    unsafe { std::mem::transmute(raw) }  // UB if raw > 2
}

// DON'T: Trust strings from C
fn bad_string(ptr: *const c_char) -> &str {
    let cstr = unsafe { CStr::from_ptr(ptr) };
    // BAD: Assumes valid UTF-8
    cstr.to_str().unwrap()
}

// DON'T: Trust size values
fn bad_size(ptr: *const u8, len: usize) -> Vec<u8> {
    // BAD: len could be huge, causing OOM
    // BAD: len could exceed actual data
    unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}
```

## Good Example

```rust
// DO: Validate enum values
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum Status {
    Active = 0,
    Inactive = 1,
    Pending = 2,
}

impl TryFrom<u8> for Status {
    type Error = InvalidStatusError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Status::Active),
            1 => Ok(Status::Inactive),
            2 => Ok(Status::Pending),
            _ => Err(InvalidStatusError(value)),
        }
    }
}

fn good_convert() -> Result<Status, InvalidStatusError> {
    let raw = unsafe { get_status() };
    Status::try_from(raw)  // Returns error for invalid values
}

// DO: Handle invalid UTF-8
fn good_string(ptr: *const c_char) -> Result<String, std::str::Utf8Error> {
    if ptr.is_null() {
        return Ok(String::new());
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str().map(|s| s.to_owned())
}

fn good_string_lossy(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_string_lossy().into_owned()  // Replaces invalid UTF-8
}

// DO: Validate sizes
const MAX_REASONABLE_SIZE: usize = 100 * 1024 * 1024;  // 100 MB

fn good_size(ptr: *const u8, len: usize) -> Result<Vec<u8>, ValidationError> {
    if ptr.is_null() {
        return Err(ValidationError::NullPointer);
    }
    if len > MAX_REASONABLE_SIZE {
        return Err(ValidationError::SizeTooLarge);
    }

    // Still need to trust that ptr points to len valid bytes
    // Document this as a caller requirement
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    Ok(slice.to_vec())
}

// DO: Use num_enum for safe enum conversion
// use num_enum::TryFromPrimitive;
//
// #[derive(TryFromPrimitive)]
// #[repr(u8)]
// enum Status { Active = 0, Inactive = 1, Pending = 2 }
```

## Validation Patterns

| External Data | Validation |
|---------------|------------|
| Enum discriminant | Match against valid values |
| String | Check UTF-8 or use lossy conversion |
| Size/length | Check against maximum |
| Pointer | Check for null |
| Boolean | Explicit 0/1 check or treat any non-zero as true |
| Float | Check for NaN, infinity if problematic |

## Checklist

- [ ] Am I validating external enum values?
- [ ] Am I handling potential invalid UTF-8?
- [ ] Am I checking sizes against reasonable limits?
- [ ] Am I using TryFrom instead of transmute?

## Related Rules

- `ffi-12`: Document invariant assumptions
- `safety-02`: Verify safety invariants
