# Unsafe Checker - Quick Reference

**Auto-generated from rules/**

## Rule Summary by Section

### General Principles (3 rules)
| ID | Level | Title |
|----|-------|-------|
| general-01 | P | Do Not Abuse Unsafe to Escape Compiler Safety Checks |
| general-02 | P | Do Not Blindly Use Unsafe for Performance |
| general-03 | G | Do Not Create Aliases for Types/Methods Named "Unsafe" |

### Safety Abstraction (11 rules)
| ID | Level | Title |
|----|-------|-------|
| safety-01 | P | Be Aware of Memory Safety Issues from Panics |
| safety-02 | P | Unsafe Code Authors Must Verify Safety Invariants |
| safety-03 | P | Do Not Expose Uninitialized Memory in Public APIs |
| safety-04 | P | Avoid Double-Free from Panic Safety Issues |
| safety-05 | P | Consider Safety When Manually Implementing Auto Traits |
| safety-06 | P | Do Not Expose Raw Pointers in Public APIs |
| safety-07 | P | Provide Unsafe Counterparts for Performance Alongside Safe Methods |
| safety-08 | P | Mutable Return from Immutable Parameter is Wrong |
| safety-09 | P | Add SAFETY Comment Before Any Unsafe Block |
| safety-10 | G | Add Safety Section in Docs for Public Unsafe Functions |
| safety-11 | G | Use assert! Instead of debug_assert! in Unsafe Functions |

### Raw Pointers (6 rules)
| ID | Level | Title |
|----|-------|-------|
| ptr-01 | P | Do Not Share Raw Pointers Across Threads |
| ptr-02 | P | Prefer NonNull<T> Over *mut T |
| ptr-03 | P | Use PhantomData<T> for Variance and Ownership |
| ptr-04 | G | Do Not Dereference Pointers Cast to Misaligned Types |
| ptr-05 | G | Do Not Manually Convert Immutable Pointer to Mutable |
| ptr-06 | G | Prefer pointer::cast Over `as` for Pointer Casting |

### Union (2 rules)
| ID | Level | Title |
|----|-------|-------|
| union-01 | P | Avoid Union Except for C Interop |
| union-02 | P | Do Not Use Union Variants Across Different Lifetimes |

### Memory Layout (6 rules)
| ID | Level | Title |
|----|-------|-------|
| mem-01 | P | Choose Appropriate Data Layout for Struct/Tuple/Enum |
| mem-02 | P | Do Not Modify Memory Variables of Other Processes |
| mem-03 | P | Do Not Let String/Vec Auto-Drop Other Process's Memory |
| mem-04 | P | Prefer Reentrant Versions of C-API or Syscalls |
| mem-05 | P | Use Third-Party Crates for Bitfields |
| mem-06 | G | Use MaybeUninit<T> for Uninitialized Memory |

### FFI (18 rules)
| ID | Level | Title |
|----|-------|-------|
| ffi-01 | P | Avoid Passing Strings Directly to C |
| ffi-02 | P | Read Documentation Carefully for std::ffi Types |
| ffi-03 | P | Implement Drop for Wrapped C Pointers |
| ffi-04 | P | Handle Panics When Crossing FFI Boundaries |
| ffi-05 | P | Use Portable Type Aliases from std or libc |
| ffi-06 | P | Ensure C-ABI String Compatibility |
| ffi-07 | P | Do Not Implement Drop for Types Passed to External Code |
| ffi-08 | P | Handle Errors Properly in FFI |
| ffi-09 | P | Use References Instead of Raw Pointers in Safe Wrappers |
| ffi-10 | P | Exported Functions Must Be Thread-Safe |
| ffi-11 | P | Be Careful with repr(packed) Field References |
| ffi-12 | P | Document Invariant Assumptions for C Parameters |
| ffi-13 | P | Ensure Consistent Data Layout for Custom Types |
| ffi-14 | P | Types in FFI Should Have Stable Layout |
| ffi-15 | P | Validate Non-Robust External Values |
| ffi-16 | P | Separate Data and Code for Closures to C |
| ffi-17 | P | Use Opaque Types Instead of c_void |
| ffi-18 | P | Avoid Passing Trait Objects to C |

### I/O Safety (1 rule)
| ID | Level | Title |
|----|-------|-------|
| io-01 | P | Ensure I/O Safety When Using Raw Handles |

## Clippy Lint Mapping

| Clippy Lint | Rule | Category |
|-------------|------|----------|
| `undocumented_unsafe_blocks` | safety-09 | SAFETY comments |
| `missing_safety_doc` | safety-10 | Safety docs |
| `panic_in_result_fn` | safety-01, ffi-04 | Panic safety |
| `non_send_fields_in_send_ty` | safety-05 | Send/Sync |
| `uninit_assumed_init` | safety-03 | Initialization |
| `uninit_vec` | mem-06 | Initialization |
| `mut_from_ref` | safety-08 | Aliasing |
| `cast_ptr_alignment` | ptr-04 | Alignment |
| `cast_ref_to_mut` | ptr-05 | Aliasing |
| `ptr_as_ptr` | ptr-06 | Pointer casting |
| `unaligned_references` | ffi-11 | Packed structs |
| `debug_assert_with_mut_call` | safety-11 | Assertions |

## Quick Decision Tree

```
Writing unsafe code?
    │
    ├─ FFI with C?
    │   └─ See ffi-* rules
    │
    ├─ Raw pointers?
    │   └─ See ptr-* rules
    │
    ├─ Manual Send/Sync?
    │   └─ See safety-05
    │
    ├─ MaybeUninit/uninitialized?
    │   └─ See safety-03, mem-06
    │
    └─ Performance optimization?
        └─ See general-02, safety-07
```

## Essential Checklist

Before every unsafe block:
- [ ] SAFETY comment present
- [ ] Invariants documented
- [ ] Pointer validity checked
- [ ] Aliasing rules followed
- [ ] Panic safety considered
- [ ] Tested with Miri

## Resources

- `checklists/before-unsafe.md` - Pre-writing checklist
- `checklists/review-unsafe.md` - Code review checklist
- `checklists/common-pitfalls.md` - Common bugs and fixes
- `examples/safe-abstraction.md` - Safe wrapper patterns
- `examples/ffi-patterns.md` - FFI best practices
