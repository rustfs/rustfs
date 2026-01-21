---
name: core-dynamic-skills
# Command-based tool - no description to prevent auto-triggering
# Triggered by: /sync-crate-skills, /clean-crate-skills, /update-crate-skill
---

# Dynamic Skills Manager

Orchestrates on-demand generation of crate-specific skills based on project dependencies.

## Concept

Dynamic skills are:
- Generated locally at `~/.claude/skills/`
- Based on Cargo.toml dependencies
- Created using llms.txt from docs.rs
- Versioned and updatable
- Not committed to the rust-skills repository

## Trigger Scenarios

### Prompt-on-Open

When entering a directory with Cargo.toml:
1. Detect Cargo.toml (single or workspace)
2. Parse dependencies list
3. Check which crates are missing skills
4. If missing: "Found X dependencies without skills. Sync now?"
5. If confirmed: run `/sync-crate-skills`

### Manual Commands

- `/sync-crate-skills` - Sync all dependencies
- `/clean-crate-skills [crate]` - Remove skills
- `/update-crate-skill <crate>` - Update specific skill

## Architecture

```
Cargo.toml
    ↓
Parse dependencies
    ↓
For each crate:
  ├─ Check ~/.claude/skills/{crate}/
  ├─ If missing: Check actionbook for llms.txt
  │     ├─ Found: /create-skills-via-llms
  │     └─ Not found: /create-llms-for-skills first
  └─ Load skill
```

## Local Skills Directory

```
~/.claude/skills/
├── tokio/
│   ├── SKILL.md
│   └── references/
├── serde/
│   ├── SKILL.md
│   └── references/
└── axum/
    ├── SKILL.md
    └── references/
```

## Workflow Priority

1. **actionbook MCP** - Check for pre-generated llms.txt
2. **/create-llms-for-skills** - Generate llms.txt from docs.rs
3. **/create-skills-via-llms** - Create skills from llms.txt

## Workspace Support

For Cargo workspace projects:
1. Parse root Cargo.toml for `[workspace] members`
2. Collect all member Cargo.toml paths
3. Aggregate all dependencies
4. Deduplicate before skill generation

## Related Commands

- `/sync-crate-skills` - Main sync command
- `/clean-crate-skills` - Cleanup command
- `/update-crate-skill` - Update command
- `/create-llms-for-skills` - Generate llms.txt
- `/create-skills-via-llms` - Create skills from llms.txt
