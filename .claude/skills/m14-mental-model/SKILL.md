---
name: m14-mental-model
description: "Use when learning Rust concepts. Keywords: mental model, how to think about ownership, understanding borrow checker, visualizing memory layout, analogy, misconception, explaining ownership, why does Rust, help me understand, confused about, learning Rust, explain like I'm, ELI5, intuition for, coming from Java, coming from Python, 心智模型, 如何理解所有权, 学习 Rust, Rust 入门, 为什么 Rust"
---

# Mental Models

> **Layer 2: Design Choices**

## Core Question

**What's the right way to think about this Rust concept?**

When learning or explaining Rust:
- What's the correct mental model?
- What misconceptions should be avoided?
- What analogies help understanding?

---

## Key Mental Models

| Concept | Mental Model | Analogy |
|---------|--------------|---------|
| Ownership | Unique key | Only one person has the house key |
| Move | Key handover | Giving away your key |
| `&T` | Lending for reading | Lending a book |
| `&mut T` | Exclusive editing | Only you can edit the doc |
| Lifetime `'a` | Valid scope | "Ticket valid until..." |
| `Box<T>` | Heap pointer | Remote control to TV |
| `Rc<T>` | Shared ownership | Multiple remotes, last turns off |
| `Arc<T>` | Thread-safe Rc | Remotes from any room |

---

## Coming From Other Languages

| From | Key Shift |
|------|-----------|
| Java/C# | Values are owned, not references by default |
| C/C++ | Compiler enforces safety rules |
| Python/Go | No GC, deterministic destruction |
| Functional | Mutability is safe via ownership |
| JavaScript | No null, use Option instead |

---

## Thinking Prompt

When confused about Rust:

1. **What's the ownership model?**
   - Who owns this data?
   - How long does it live?
   - Who can access it?

2. **What guarantee is Rust providing?**
   - No data races
   - No dangling pointers
   - No use-after-free

3. **What's the compiler telling me?**
   - Error = violation of safety rule
   - Solution = work with the rules

---

## Trace Up ↑

To design understanding (Layer 2):

```
"Why can't I do X in Rust?"
    ↑ Ask: What safety guarantee would be violated?
    ↑ Check: m01-m07 for the rule being enforced
    ↑ Ask: What's the intended design pattern?
```

---

## Trace Down ↓

To implementation (Layer 1):

```
"I understand the concept, now how do I implement?"
    ↓ m01-ownership: Ownership patterns
    ↓ m02-resource: Smart pointer choice
    ↓ m07-concurrency: Thread safety
```

---

## Common Misconceptions

| Error | Wrong Model | Correct Model |
|-------|-------------|---------------|
| E0382 use after move | GC cleans up | Ownership = unique key transfer |
| E0502 borrow conflict | Multiple writers OK | Only one writer at a time |
| E0499 multiple mut borrows | Aliased mutation | Exclusive access for mutation |
| E0106 missing lifetime | Ignoring scope | References have validity scope |
| E0507 cannot move from `&T` | Implicit clone | References don't own data |

## Deprecated Thinking

| Deprecated | Better |
|------------|--------|
| "Rust is like C++" | Different ownership model |
| "Lifetimes are GC" | Compile-time validity scope |
| "Clone solves everything" | Restructure ownership |
| "Fight the borrow checker" | Work with the compiler |
| "`unsafe` to avoid rules" | Understand safe patterns first |

---

## Ownership Visualization

```
Stack                          Heap
+----------------+            +----------------+
| main()         |            |                |
|   s1 ─────────────────────> │ "hello"        |
|                |            |                |
| fn takes(s) {  |            |                |
|   s2 (moved) ─────────────> │ "hello"        |
| }              |            | (s1 invalid)   |
+----------------+            +----------------+

After move: s1 is no longer valid
```

## Reference Visualization

```
+----------------+
| data: String   |────────────> "hello"
+----------------+
       ↑
       │ &data (immutable borrow)
       │
+------+------+
| reader1    reader2    (multiple OK)
+------+------+

+----------------+
| data: String   |────────────> "hello"
+----------------+
       ↑
       │ &mut data (mutable borrow)
       │
+------+
| writer (only one)
+------+
```

---

## Learning Path

| Stage | Focus | Skills |
|-------|-------|--------|
| Beginner | Ownership basics | m01-ownership, m14-mental-model |
| Intermediate | Smart pointers, error handling | m02, m06 |
| Advanced | Concurrency, unsafe | m07, unsafe-checker |
| Expert | Design patterns | m09-m15, domain-* |

---

## Related Skills

| When | See |
|------|-----|
| Ownership errors | m01-ownership |
| Smart pointers | m02-resource |
| Concurrency | m07-concurrency |
| Anti-patterns | m15-anti-pattern |
