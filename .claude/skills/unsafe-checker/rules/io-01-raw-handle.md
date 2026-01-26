---
id: io-01
original_id: P.UNS.FIO.01
level: P
impact: HIGH
---

# Ensure I/O Safety When Using Raw Handles

## Summary

When working with raw file descriptors or handles, ensure they are valid for the duration of use and properly ownership-tracked.

## Rationale

- Raw handles can be closed by other code
- Using a closed handle is undefined behavior
- Handle reuse can cause data corruption
- Rust 1.63+ provides I/O safety traits

## Bad Example

```rust
#[cfg(unix)]
mod bad_example {
    use std::os::unix::io::RawFd;

    // DON'T: Accept raw handle without ownership
    fn bad_read(fd: RawFd) -> std::io::Result<Vec<u8>> {
        // What if fd was closed? What if it's reused?
        let mut buf = vec![0u8; 1024];
        let n = unsafe {
            libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
        };
        if n < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            buf.truncate(n as usize);
            Ok(buf)
        }
    }

    // DON'T: Store raw handle without tracking ownership
    struct BadFileRef {
        fd: RawFd,  // Who owns this? Who closes it?
    }
}
```

## Good Example

```rust
#[cfg(unix)]
mod good_example {
    use std::os::unix::io::{AsFd, BorrowedFd, OwnedFd, FromRawFd, AsRawFd};
    use std::fs::File;

    // DO: Use BorrowedFd for borrowed access (Rust 1.63+)
    fn good_read(fd: BorrowedFd<'_>) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; 1024];
        // BorrowedFd guarantees the fd is valid for this call
        let n = unsafe {
            libc::read(
                fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len()
            )
        };
        if n < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            buf.truncate(n as usize);
            Ok(buf)
        }
    }

    // DO: Use OwnedFd for owned handles
    struct GoodFileOwner {
        fd: OwnedFd,  // Clearly owns the handle
    }

    impl Drop for GoodFileOwner {
        fn drop(&mut self) {
            // OwnedFd closes automatically
        }
    }

    // DO: Use generic AsFd bound for flexibility
    fn generic_read<F: AsFd>(f: &F) -> std::io::Result<Vec<u8>> {
        good_read(f.as_fd())
    }

    // Usage
    fn example() -> std::io::Result<()> {
        let file = File::open("test.txt")?;

        // Pass as BorrowedFd
        let data = good_read(file.as_fd())?;

        // Or use generic function
        let data = generic_read(&file)?;

        Ok(())
    }

    // DO: Take ownership from raw fd
    fn from_raw(fd: i32) -> Option<GoodFileOwner> {
        if fd < 0 {
            return None;
        }
        // SAFETY: Caller guarantees fd is valid and ownership is transferred
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        Some(GoodFileOwner { fd: owned })
    }
}
```

## I/O Safety Types (Rust 1.63+)

| Type | Meaning |
|------|---------|
| `OwnedFd` | Owns a file descriptor, closes on drop |
| `BorrowedFd<'a>` | Borrows a fd for lifetime 'a |
| `RawFd` | Raw integer, no safety guarantees |
| `AsFd` | Trait for types that have a fd |
| `From<OwnedFd>` | Create from owned fd |
| `Into<OwnedFd>` | Convert to owned fd |

## Windows Equivalents

```rust
#[cfg(windows)]
use std::os::windows::io::{
    OwnedHandle, BorrowedHandle, RawHandle,
    AsHandle, FromRawHandle,
    OwnedSocket, BorrowedSocket, RawSocket,
    AsSocket, FromRawSocket,
};
```

## Checklist

- [ ] Am I using BorrowedFd/OwnedFd instead of RawFd?
- [ ] Is ownership of handles clear?
- [ ] Am I using the AsFd trait for generic code?
- [ ] Is the fd guaranteed valid for the duration of use?

## Related Rules

- `ffi-03`: Implement Drop for resource wrappers
- `safety-02`: Verify safety invariants
