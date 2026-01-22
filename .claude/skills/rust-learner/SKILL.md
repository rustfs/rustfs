---
name: rust-learner
description: "Use when asking about Rust versions or crate info. Keywords: latest version, what's new, changelog, Rust 1.x, Rust release, stable, nightly, crate info, crates.io, lib.rs, docs.rs, API documentation, crate features, dependencies, which crate, what version, Rust edition, edition 2021, edition 2024, cargo add, cargo update, 最新版本, 版本号, 稳定版, 最新, 哪个版本, crate 信息, 文档, 依赖, Rust 版本, 新特性, 有什么特性"
---

# Rust Learner

> **Version:** 1.1.0 | **Last Updated:** 2026-01-16

You are an expert at fetching Rust and crate information. Help users by:
- **Version queries**: Get latest Rust/crate versions via background agents
- **API documentation**: Fetch docs from docs.rs
- **Changelog**: Get Rust version features from releases.rs

**Primary skill for fetching Rust/crate information. All agents run in background.**

## CRITICAL: How to Launch Agents

**All agents MUST be launched via Task tool with these parameters:**

```
Task(
  subagent_type: "general-purpose",
  run_in_background: true,
  prompt: <read from ../../agents/*.md file>
)
```

**Workflow:**
1. Read the agent prompt file: `../../agents/<agent-name>.md` (relative to this skill)
2. Launch Task with `run_in_background: true`
3. Continue with other work or wait for completion
4. Read results when agent completes

## Agent Routing Table

| Query Type | Agent File | Source |
|------------|------------|--------|
| Rust version features | `../../agents/rust-changelog.md` | releases.rs |
| Crate info/version | `../../agents/crate-researcher.md` | lib.rs, crates.io |
| **Std library docs** (Send, Sync, Arc, etc.) | `../../agents/std-docs-researcher.md` | doc.rust-lang.org |
| Third-party crate docs (tokio, serde, etc.) | `../../agents/docs-researcher.md` | docs.rs |
| Clippy lints | `../../agents/clippy-researcher.md` | rust-clippy docs |
| **Rust news/daily report** | `../../agents/rust-daily-reporter.md` | Reddit, TWIR, blogs |

### Choosing docs-researcher vs std-docs-researcher

| Query Pattern | Use Agent |
|---------------|-----------|
| `std::*`, `Send`, `Sync`, `Arc`, `Rc`, `Box`, `Vec`, `String` | `std-docs-researcher` |
| `tokio::*`, `serde::*`, any third-party crate | `docs-researcher` |

## Tool Chain

All agents use this tool chain (in order):

1. **actionbook MCP** (first - get pre-computed selectors)
   - `mcp__actionbook__search_actions("site_name")` → get action ID
   - `mcp__actionbook__get_action_by_id(id)` → get URL + selectors

2. **agent-browser CLI** (PRIMARY execution tool)
   ```bash
   agent-browser open <url>
   agent-browser get text <selector_from_actionbook>
   agent-browser close
   ```

3. **WebFetch** (LAST RESORT only if agent-browser unavailable)

### Fallback Principle (CRITICAL)

```
actionbook → agent-browser → WebFetch (only if agent-browser unavailable)
```

**DO NOT:**
- Skip agent-browser because it's slower
- Use WebFetch as primary when agent-browser is available
- Block on WebFetch without trying agent-browser first

## Example: Crate Version Query

```
User: "tokio latest version"

Claude:
1. Read ../../agents/crate-researcher.md
2. Task(
     subagent_type: "general-purpose",
     run_in_background: true,
     prompt: "Fetch crate info for 'tokio'. Use actionbook MCP to get lib.rs selectors, then agent-browser to fetch. Return: name, version, description, features."
   )
3. Wait for agent or continue with other work
4. Summarize results to user
```

## Example: Third-Party Crate Documentation

```
User: "tokio::spawn documentation"

Claude:
1. Read ../../agents/docs-researcher.md
2. Task(
     subagent_type: "general-purpose",
     run_in_background: true,
     prompt: "Fetch API docs for tokio::spawn from docs.rs. Use agent-browser first. Return: signature, description, examples."
   )
3. Wait for agent
4. Summarize API to user
```

## Example: Std Library Documentation

```
User: "Send trait documentation"

Claude:
1. Read ../../agents/std-docs-researcher.md  (NOT docs-researcher!)
2. Task(
     subagent_type: "general-purpose",
     run_in_background: true,
     prompt: "Fetch std::marker::Send trait docs from doc.rust-lang.org. Use agent-browser first. Return: description, implementors, examples."
   )
3. Wait for agent
4. Summarize trait to user
```

## Example: Rust Changelog Query

```
User: "What's new in Rust 1.85?"

Claude:
1. Read ../../agents/rust-changelog.md
2. Task(
     subagent_type: "general-purpose",
     run_in_background: true,
     prompt: "Fetch Rust 1.85 changelog from releases.rs. Use actionbook MCP for selectors, agent-browser to fetch. Return: language features, library changes, stabilized APIs."
   )
3. Wait for agent
4. Summarize features to user
```

## Deprecated Patterns

| Deprecated | Use Instead | Reason |
|------------|-------------|--------|
| WebSearch for crate info | Task + crate-researcher | Structured data |
| Direct WebFetch | Task + actionbook | Pre-computed selectors |
| Foreground agent execution | `run_in_background: true` | Non-blocking |
| Guessing version numbers | Always use agents | Prevents misinformation |

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| actionbook unavailable | MCP not configured | Fall back to WebFetch |
| agent-browser not found | CLI not installed | Fall back to WebFetch |
| Agent timeout | Site slow/down | Retry or inform user |
| Empty results | Selector mismatch | Report and use WebFetch fallback |

## Proactive Triggering

This skill triggers AUTOMATICALLY when:
- Any Rust crate name mentioned (tokio, serde, axum, sqlx, etc.)
- Questions about "latest", "new", "version", "changelog"
- API documentation requests
- Dependency/feature questions

**DO NOT use WebSearch for Rust crate info. Launch background agents instead.**
