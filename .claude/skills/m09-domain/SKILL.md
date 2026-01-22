---
name: m09-domain
description: "CRITICAL: Use for domain modeling. Triggers: domain model, DDD, domain-driven design, entity, value object, aggregate, repository pattern, business rules, validation, invariant, 领域模型, 领域驱动设计, 业务规则"
---

# Domain Modeling

> **Layer 2: Design Choices**

## Core Question

**What is this concept's role in the domain?**

Before modeling in code, understand:
- Is it an Entity (identity matters) or Value Object (interchangeable)?
- What invariants must be maintained?
- Where are the aggregate boundaries?

---

## Domain Concept → Rust Pattern

| Domain Concept | Rust Pattern | Ownership Implication |
|----------------|--------------|----------------------|
| Entity | struct + Id | Owned, unique identity |
| Value Object | struct + Clone/Copy | Shareable, immutable |
| Aggregate Root | struct owns children | Clear ownership tree |
| Repository | trait | Abstracts persistence |
| Domain Event | enum | Captures state changes |
| Service | impl block / free fn | Stateless operations |

---

## Thinking Prompt

Before creating a domain type:

1. **What's the concept's identity?**
   - Needs unique identity → Entity (Id field)
   - Interchangeable by value → Value Object (Clone/Copy)

2. **What invariants must hold?**
   - Always valid → private fields + validated constructor
   - Transition rules → type state pattern

3. **Who owns this data?**
   - Single owner (parent) → owned field
   - Shared reference → Arc/Rc
   - Weak reference → Weak

---

## Trace Up ↑

To domain constraints (Layer 3):

```
"How should I model a Transaction?"
    ↑ Ask: What domain rules govern transactions?
    ↑ Check: domain-fintech (audit, precision requirements)
    ↑ Check: Business stakeholders (what invariants?)
```

| Design Question | Trace To | Ask |
|-----------------|----------|-----|
| Entity vs Value Object | domain-* | What makes two instances "the same"? |
| Aggregate boundaries | domain-* | What must be consistent together? |
| Validation rules | domain-* | What business rules apply? |

---

## Trace Down ↓

To implementation (Layer 1):

```
"Model as Entity"
    ↓ m01-ownership: Owned, unique
    ↓ m05-type-driven: Newtype for Id

"Model as Value Object"
    ↓ m01-ownership: Clone/Copy OK
    ↓ m05-type-driven: Validate at construction

"Model as Aggregate"
    ↓ m01-ownership: Parent owns children
    ↓ m02-resource: Consider Rc for shared within aggregate
```

---

## Quick Reference

| DDD Concept | Rust Pattern | Example |
|-------------|--------------|---------|
| Value Object | Newtype | `struct Email(String);` |
| Entity | Struct + ID | `struct User { id: UserId, ... }` |
| Aggregate | Module boundary | `mod order { ... }` |
| Repository | Trait | `trait UserRepo { fn find(...) }` |
| Domain Event | Enum | `enum OrderEvent { Created, ... }` |

## Pattern Templates

### Value Object

```rust
struct Email(String);

impl Email {
    pub fn new(s: &str) -> Result<Self, ValidationError> {
        validate_email(s)?;
        Ok(Self(s.to_string()))
    }
}
```

### Entity

```rust
struct UserId(Uuid);

struct User {
    id: UserId,
    email: Email,
    // ... other fields
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id  // Identity equality
    }
}
```

### Aggregate

```rust
mod order {
    pub struct Order {
        id: OrderId,
        items: Vec<OrderItem>,  // Owned children
        // ...
    }

    impl Order {
        pub fn add_item(&mut self, item: OrderItem) {
            // Enforce aggregate invariants
        }
    }
}
```

---

## Common Mistakes

| Mistake | Why Wrong | Better |
|---------|-----------|--------|
| Primitive obsession | No type safety | Newtype wrappers |
| Public fields with invariants | Invariants violated | Private + accessor |
| Leaked aggregate internals | Broken encapsulation | Methods on root |
| String for semantic types | No validation | Validated newtype |

---

## Related Skills

| When | See |
|------|-----|
| Type-driven implementation | m05-type-driven |
| Ownership for aggregates | m01-ownership |
| Domain error handling | m13-domain-error |
| Specific domain rules | domain-* |
