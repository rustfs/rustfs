---
name: rust-skill-creator
description: "Use when creating skills for Rust crates or std library documentation. Keywords: create rust skill, create crate skill, create std skill, 创建 rust skill, 创建 crate skill, 创建 std skill, 动态 rust skill, 动态 crate skill, skill for tokio, skill for serde, skill for axum, generate rust skill, rust 技能, crate 技能, 从文档创建skill, from docs create skill"
---

# Rust Skill Creator

> Create dynamic skills for Rust crates and std library documentation.

## When to Use

This skill handles requests to create skills for:
- Third-party crates (tokio, serde, axum, etc.)
- Rust standard library (std::sync, std::marker, etc.)
- Any Rust documentation URL

## Workflow

### 1. Identify the Target

| User Request | Target Type | URL Pattern |
|--------------|-------------|-------------|
| "create tokio skill" | Third-party crate | `docs.rs/tokio/latest/tokio/` |
| "create Send trait skill" | Std library | `doc.rust-lang.org/std/marker/trait.Send.html` |
| "create skill from URL" + URL | Custom URL | User-provided URL |

### 2. Execute the Command

Use the `/create-llms-for-skills` command:

```
/create-llms-for-skills <url> [requirements]
```

**Examples:**

```bash
# For third-party crate
/create-llms-for-skills https://docs.rs/tokio/latest/tokio/

# For std library
/create-llms-for-skills https://doc.rust-lang.org/std/marker/trait.Send.html

# With specific requirements
/create-llms-for-skills https://docs.rs/axum/latest/axum/ "Focus on routing and extractors"
```

### 3. Follow-up with Skill Creation

After llms.txt is generated, use:

```
/create-skills-via-llms <crate_name> <llms_path> [version]
```

## URL Construction Helper

| Target | URL Template |
|--------|--------------|
| Crate overview | `https://docs.rs/{crate}/latest/{crate}/` |
| Crate module | `https://docs.rs/{crate}/latest/{crate}/{module}/` |
| Std trait | `https://doc.rust-lang.org/std/{module}/trait.{Name}.html` |
| Std struct | `https://doc.rust-lang.org/std/{module}/struct.{Name}.html` |
| Std module | `https://doc.rust-lang.org/std/{module}/index.html` |

## Common Std Library Paths

| Item | Path |
|------|------|
| Send, Sync, Copy, Clone | `std/marker/trait.{Name}.html` |
| Arc, Mutex, RwLock | `std/sync/struct.{Name}.html` |
| Rc, Weak | `std/rc/struct.{Name}.html` |
| RefCell, Cell | `std/cell/struct.{Name}.html` |
| Box | `std/boxed/struct.Box.html` |
| Vec | `std/vec/struct.Vec.html` |
| String | `std/string/struct.String.html` |
| Option | `std/option/enum.Option.html` |
| Result | `std/result/enum.Result.html` |

## Example Interactions

### Example 1: Create Crate Skill

```
User: "Create a dynamic skill for tokio"

Claude:
1. Identify: Third-party crate "tokio"
2. Execute: /create-llms-for-skills https://docs.rs/tokio/latest/tokio/
3. Wait for llms.txt generation
4. Execute: /create-skills-via-llms tokio ~/tmp/{timestamp}-tokio-llms.txt
```

### Example 2: Create Std Library Skill

```
User: "Create a skill for Send and Sync traits"

Claude:
1. Identify: Std library traits
2. Execute: /create-llms-for-skills https://doc.rust-lang.org/std/marker/trait.Send.html https://doc.rust-lang.org/std/marker/trait.Sync.html
3. Wait for llms.txt generation
4. Execute: /create-skills-via-llms std-marker ~/tmp/{timestamp}-std-marker-llms.txt
```

### Example 3: Custom URL

```
User: "Create skill from https://docs.rs/sqlx/latest/sqlx/"

Claude:
1. Identify: User-provided URL
2. Execute: /create-llms-for-skills https://docs.rs/sqlx/latest/sqlx/
3. Follow standard workflow
```

## DO NOT

- Use `best-skill-creator` for Rust-related skill creation
- Skip the `/create-llms-for-skills` step
- Guess documentation URLs without verification

## Output Location

All generated skills are saved to: `~/.claude/skills/`
