---
id: general-02
original_id: P.UNS.02
level: P
impact: CRITICAL
---

# Do Not Blindly Use Unsafe for Performance

## Summary

Do not assume that using `unsafe` will automatically improve performance. Always measure first and verify the safety invariants.

## Rationale

1. Modern Rust optimizers often eliminate bounds checks when they can prove safety
2. Unsafe code may prevent optimizations by breaking aliasing assumptions
3. Unmeasured "optimizations" often provide no real benefit while introducing risk

## Bad Example

```rust
// DON'T: Blind unsafe for "performance"
fn sum_bad(slice: &[i32]) -> i32 {
    let mut sum = 0;
    // Unnecessary unsafe - LLVM can optimize the safe version
    for i in 0..slice.len() {
        unsafe {
            sum += *slice.get_unchecked(i);
        }
    }
    sum
}
```

## Good Example

```rust
// DO: Use safe iteration - compiler optimizes bounds checks away
fn sum_good(slice: &[i32]) -> i32 {
    slice.iter().sum()
}

// DO: If unsafe is justified, document why
fn sum_justified(slice: &[i32]) -> i32 {
    let mut sum = 0;
    // This is actually slower than iter().sum() in most cases
    // Only use get_unchecked when:
    // 1. Profiler shows bounds checks as bottleneck
    // 2. Iterator patterns can't be used
    // 3. Safety is proven by other means
    for i in 0..slice.len() {
        // SAFETY: i is always < slice.len() due to loop condition
        unsafe {
            sum += *slice.get_unchecked(i);
        }
    }
    sum
}
```

## When Unsafe Might Be Justified for Performance

1. **Hot inner loops** where profiling shows bounds checks are a bottleneck
2. **SIMD operations** that require specific memory alignment
3. **Lock-free data structures** with carefully verified memory orderings

## Measurement Workflow

```bash
# 1. Benchmark the safe version first
cargo bench --bench my_bench

# 2. Profile to identify actual bottlenecks
cargo flamegraph --bench my_bench

# 3. Only then consider unsafe, with measurements
```

## Checklist

- [ ] Have I benchmarked the safe version?
- [ ] Does profiling show this specific code as a bottleneck?
- [ ] Have I measured the actual improvement from unsafe?
- [ ] Is the performance gain worth the safety risk?

## Related Rules

- `general-01`: Don't abuse unsafe to escape safety checks
- `safety-02`: Unsafe code authors must verify safety invariants
