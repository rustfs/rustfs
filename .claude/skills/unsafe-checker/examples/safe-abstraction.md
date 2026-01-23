# Safe Abstraction Examples

Examples of building safe APIs on top of unsafe code.

## Example 1: Simple Wrapper with Bounds Check

```rust
/// A slice wrapper that provides unchecked access internally
/// but safe access externally.
pub struct SafeSlice<'a, T> {
    ptr: *const T,
    len: usize,
    _marker: std::marker::PhantomData<&'a T>,
}

impl<'a, T> SafeSlice<'a, T> {
    /// Creates a SafeSlice from a regular slice.
    pub fn new(slice: &'a [T]) -> Self {
        Self {
            ptr: slice.as_ptr(),
            len: slice.len(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Safe get - returns Option.
    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.len {
            // SAFETY: We just verified index < len
            Some(unsafe { &*self.ptr.add(index) })
        } else {
            None
        }
    }

    /// Unsafe get - caller must ensure bounds.
    ///
    /// # Safety
    /// `index` must be less than `self.len()`.
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        debug_assert!(index < self.len);
        &*self.ptr.add(index)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}
```

## Example 2: Resource Wrapper with Drop

```rust
use std::ptr::NonNull;

/// Safe wrapper around a C-allocated buffer.
pub struct CBuffer {
    ptr: NonNull<u8>,
    len: usize,
}

extern "C" {
    fn c_alloc(size: usize) -> *mut u8;
    fn c_free(ptr: *mut u8);
}

impl CBuffer {
    /// Creates a new buffer. Returns None if allocation fails.
    pub fn new(size: usize) -> Option<Self> {
        let ptr = unsafe { c_alloc(size) };
        NonNull::new(ptr).map(|ptr| Self { ptr, len: size })
    }

    /// Returns a slice view of the buffer.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes (from c_alloc contract)
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns a mutable slice view.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: We have &mut self, so exclusive access
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for CBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated by c_alloc and not yet freed
        unsafe { c_free(self.ptr.as_ptr()); }
    }
}

// Prevent double-free
impl !Clone for CBuffer {}

// Safe to send between threads (assuming c_alloc is thread-safe)
unsafe impl Send for CBuffer {}
```

## Example 3: Interior Mutability with UnsafeCell

```rust
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// A simple spinlock demonstrating safe abstraction over UnsafeCell.
pub struct SpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

pub struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> SpinLock<T> {
    pub const fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        // Spin until we acquire the lock
        while self.locked.compare_exchange_weak(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ).is_err() {
            std::hint::spin_loop();
        }
        SpinLockGuard { lock: self }
    }
}

impl<T> std::ops::Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: We hold the lock, so we have exclusive access
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> std::ops::DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: We hold the lock, so we have exclusive access
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

// SAFETY: The lock ensures only one thread accesses data at a time
unsafe impl<T: Send> Sync for SpinLock<T> {}
unsafe impl<T: Send> Send for SpinLock<T> {}
```

## Example 4: Iterator with Lifetime Tracking

```rust
use std::marker::PhantomData;

/// An iterator over raw pointer range with proper lifetime tracking.
pub struct PtrIter<'a, T> {
    current: *const T,
    end: *const T,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PtrIter<'a, T> {
    /// Creates an iterator from a slice.
    pub fn new(slice: &'a [T]) -> Self {
        let ptr = slice.as_ptr();
        Self {
            current: ptr,
            // SAFETY: Adding len to slice pointer is always valid
            end: unsafe { ptr.add(slice.len()) },
            _marker: PhantomData,
        }
    }
}

impl<'a, T> Iterator for PtrIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            // SAFETY:
            // - current < end (checked above)
            // - PhantomData<&'a T> ensures the data lives for 'a
            let item = unsafe { &*self.current };
            self.current = unsafe { self.current.add(1) };
            Some(item)
        }
    }
}
```

## Example 5: Builder Pattern with Delayed Initialization

```rust
use std::mem::MaybeUninit;

/// A builder that collects exactly N items, then produces an array.
pub struct ArrayBuilder<T, const N: usize> {
    data: [MaybeUninit<T>; N],
    count: usize,
}

impl<T, const N: usize> ArrayBuilder<T, N> {
    pub fn new() -> Self {
        Self {
            // SAFETY: MaybeUninit doesn't require initialization
            data: unsafe { MaybeUninit::uninit().assume_init() },
            count: 0,
        }
    }

    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.count >= N {
            return Err(value);
        }
        self.data[self.count].write(value);
        self.count += 1;
        Ok(())
    }

    pub fn build(self) -> Option<[T; N]> {
        if self.count != N {
            return None;
        }

        // SAFETY: All N elements have been initialized
        let result = unsafe {
            // Prevent drop of self.data (we're moving out)
            let data = std::ptr::read(&self.data);
            std::mem::forget(self);
            // Transmute MaybeUninit array to initialized array
            std::mem::transmute_copy::<[MaybeUninit<T>; N], [T; N]>(&data)
        };
        Some(result)
    }
}

impl<T, const N: usize> Drop for ArrayBuilder<T, N> {
    fn drop(&mut self) {
        // Drop only initialized elements
        for i in 0..self.count {
            // SAFETY: Elements 0..count are initialized
            unsafe { self.data[i].assume_init_drop(); }
        }
    }
}
```

## Key Patterns

1. **Encapsulation**: Hide unsafe behind safe public API
2. **Invariant maintenance**: Use private fields to maintain invariants
3. **PhantomData**: Track lifetimes and ownership for pointers
4. **RAII**: Use Drop for cleanup
5. **Type state**: Use types to encode valid states
