# Rust Code Quality Checklist

Use this as a quick pre-merge checklist for every Rust code change.

## Critical (P0 — block merge)

| Check | Command |
|-------|---------|
| No `unwrap()` in request/storage hot path | `rg '\.unwrap\(\)' <files> \| grep -v test` |
| No `as` truncation on user input | `rg ' as (u32\|usize\|i32)' <files>` |
| Lock order consistent across call sites | Manual: trace all lock acquisitions |
| Recursive functions have depth limit | Manual: check for `max_depth` or iterative pattern |
| No `panic!`/`unwrap_or_else(panic!)` in production | `rg 'panic!\|unwrap_or_else.*panic' <files> \| grep -v test` |

## High (P1 — must fix)

| Check | Command |
|-------|---------|
| No `Result<_, String>` in public API | `rg 'Result<.*String>' <files> \| grep -v test` |
| No `Box<dyn Error>` in public trait | `rg 'Box<dyn.*Error' <files> \| grep -v test` |
| No unnecessary `.clone()` in hot path | Manual: check loops and per-request paths |
| `Error::source()` implemented when inner error stored | Manual: check `impl Error` |
| No `eprintln!`/`println!` in production | `rg 'println!\|eprintln!' <files> \| grep -v test` |

## Medium (P2 — should fix)

| Check | Command |
|-------|---------|
| Tests have assertions | Manual: check for `assert` in test functions |
| `HashMap`/`Vec` use `with_capacity` when size known | Manual: check `::new()` in loops |
| No `#![allow(dead_code)]` at crate root | `rg 'allow.dead_code' <files> \| grep 'lib.rs'` |
| Serde structs from untrusted input have `deny_unknown_fields` | Manual: check `#[derive(Deserialize)]` |

## Low (P3 — nice to fix)

| Check | Command |
|-------|---------|
| No camelCase statics | `rg 'static ref [a-z]' <files>` |
| `Arc::ptr_eq` instead of `as_ptr + ptr::eq` | `rg 'as_ptr\|ptr::eq' <files>` |
| Public functions have doc comments | `rg 'pub fn' <files> \| grep -v '///'` |

## Quick One-Liner

```bash
# Run all automated checks on changed files
CHANGED=$(git diff --name-only HEAD~1 -- '*.rs' | grep -v test | grep -v bench)
echo "=== unwrap/expect ===" && rg -c '\.unwrap\(\)|\.expect\(' $CHANGED 2>/dev/null
echo "=== as casts ===" && rg -c ' as (u8|u16|u32|u64|usize|i8|i16|i32|i64|isize)\b' $CHANGED 2>/dev/null
echo "=== String errors ===" && rg -c 'Result<.*String>' $CHANGED 2>/dev/null
echo "=== println ===" && rg -c 'println!|eprintln!' $CHANGED 2>/dev/null
echo "=== Ordering::Relaxed ===" && rg -c 'Ordering::Relaxed' $CHANGED 2>/dev/null
```
