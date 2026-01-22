---
name: m10-performance
description: "CRITICAL: Use for performance optimization. Triggers: performance, optimization, benchmark, profiling, flamegraph, criterion, slow, fast, allocation, cache, SIMD, make it faster, 性能优化, 基准测试"
---

# Performance Optimization

> **Layer 2: Design Choices**

## Core Question

**What's the bottleneck, and is optimization worth it?**

Before optimizing:
- Have you measured? (Don't guess)
- What's the acceptable performance?
- Will optimization add complexity?

---

## Performance Decision → Implementation

| Goal | Design Choice | Implementation |
|------|---------------|----------------|
| Reduce allocations | Pre-allocate, reuse | `with_capacity`, object pools |
| Improve cache | Contiguous data | `Vec`, `SmallVec` |
| Parallelize | Data parallelism | `rayon`, threads |
| Avoid copies | Zero-copy | References, `Cow<T>` |
| Reduce indirection | Inline data | `smallvec`, arrays |

---

## Thinking Prompt

Before optimizing:

1. **Have you measured?**
   - Profile first → flamegraph, perf
   - Benchmark → criterion, cargo bench
   - Identify actual hotspots

2. **What's the priority?**
   - Algorithm (10x-1000x improvement)
   - Data structure (2x-10x)
   - Allocation (2x-5x)
   - Cache (1.5x-3x)

3. **What's the trade-off?**
   - Complexity vs speed
   - Memory vs CPU
   - Latency vs throughput

---

## Trace Up ↑

To domain constraints (Layer 3):

```
"How fast does this need to be?"
    ↑ Ask: What's the performance SLA?
    ↑ Check: domain-* (latency requirements)
    ↑ Check: Business requirements (acceptable response time)
```

| Question | Trace To | Ask |
|----------|----------|-----|
| Latency requirements | domain-* | What's acceptable response time? |
| Throughput needs | domain-* | How many requests per second? |
| Memory constraints | domain-* | What's the memory budget? |

---

## Trace Down ↓

To implementation (Layer 1):

```
"Need to reduce allocations"
    ↓ m01-ownership: Use references, avoid clone
    ↓ m02-resource: Pre-allocate with_capacity

"Need to parallelize"
    ↓ m07-concurrency: Choose rayon or threads
    ↓ m07-concurrency: Consider async for I/O-bound

"Need cache efficiency"
    ↓ Data layout: Prefer Vec over HashMap when possible
    ↓ Access patterns: Sequential over random access
```

---

## Quick Reference

| Tool | Purpose |
|------|---------|
| `cargo bench` | Micro-benchmarks |
| `criterion` | Statistical benchmarks |
| `perf` / `flamegraph` | CPU profiling |
| `heaptrack` | Allocation tracking |
| `valgrind` / `cachegrind` | Cache analysis |

## Optimization Priority

```
1. Algorithm choice     (10x - 1000x)
2. Data structure       (2x - 10x)
3. Allocation reduction (2x - 5x)
4. Cache optimization   (1.5x - 3x)
5. SIMD/Parallelism     (2x - 8x)
```

## Common Techniques

| Technique | When | How |
|-----------|------|-----|
| Pre-allocation | Known size | `Vec::with_capacity(n)` |
| Avoid cloning | Hot paths | Use references or `Cow<T>` |
| Batch operations | Many small ops | Collect then process |
| SmallVec | Usually small | `smallvec::SmallVec<[T; N]>` |
| Inline buffers | Fixed-size data | Arrays over Vec |

---

## Common Mistakes

| Mistake | Why Wrong | Better |
|---------|-----------|--------|
| Optimize without profiling | Wrong target | Profile first |
| Benchmark in debug mode | Meaningless | Always `--release` |
| Use LinkedList | Cache unfriendly | `Vec` or `VecDeque` |
| Hidden `.clone()` | Unnecessary allocs | Use references |
| Premature optimization | Wasted effort | Make it work first |

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| Clone to avoid lifetimes | Performance cost | Proper ownership |
| Box everything | Indirection cost | Stack when possible |
| HashMap for small sets | Overhead | Vec with linear search |
| String concat in loop | O(n^2) | `String::with_capacity` or `format!` |

---

## Related Skills

| When | See |
|------|-----|
| Reducing clones | m01-ownership |
| Concurrency options | m07-concurrency |
| Smart pointer choice | m02-resource |
| Domain requirements | domain-* |
