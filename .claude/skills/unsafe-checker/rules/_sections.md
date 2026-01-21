# Unsafe Checker - Section Definitions

## Section Overview

| # | Section | Prefix | Level | Count | Impact |
|---|---------|--------|-------|-------|--------|
| 1 | General Principles | `general-` | CRITICAL | 3 | Foundational unsafe usage guidance |
| 2 | Safety Abstraction | `safety-` | CRITICAL | 11 | Building sound safe APIs |
| 3 | Raw Pointers | `ptr-` | HIGH | 6 | Pointer manipulation safety |
| 4 | Union | `union-` | HIGH | 2 | Union type safety |
| 5 | Memory Layout | `mem-` | HIGH | 6 | Data representation correctness |
| 6 | FFI | `ffi-` | CRITICAL | 18 | C interoperability safety |
| 7 | I/O Safety | `io-` | MEDIUM | 1 | Handle/resource safety |

## Section Details

### 1. General Principles (`general-`)

**Focus**: When and why to use unsafe

- P.UNS.01: Don't abuse unsafe to escape borrow checker
- P.UNS.02: Don't use unsafe blindly for performance
- G.UNS.01: Don't create aliases for "unsafe" named items

### 2. Safety Abstraction (`safety-`)

**Focus**: Building sound safe abstractions over unsafe code

Key invariants:
- Panic safety
- Memory initialization
- Send/Sync correctness
- API soundness

### 3. Raw Pointers (`ptr-`)

**Focus**: Safe pointer manipulation patterns

- Aliasing rules
- Alignment requirements
- Null/dangling prevention
- Type casting

### 4. Union (`union-`)

**Focus**: Safe union usage (primarily for C interop)

- Initialization rules
- Lifetime considerations
- Type punning dangers

### 5. Memory Layout (`mem-`)

**Focus**: Correct data representation

- `#[repr(C)]` usage
- Alignment and padding
- Uninitialized memory
- Cross-process memory

### 6. FFI (`ffi-`)

**Focus**: Safe C interoperability

Subcategories:
- String handling (CString, CStr)
- Type compatibility
- Error handling across FFI
- Thread safety
- Resource management

### 7. I/O Safety (`io-`)

**Focus**: Handle and resource ownership

- Raw file descriptor safety
- Handle validity guarantees
