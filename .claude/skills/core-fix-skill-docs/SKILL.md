---
name: core-fix-skill-docs
# Internal maintenance tool - no description to prevent auto-triggering
# Triggered by: /fix-skill-docs command
---

# Fix Skill Documentation

Check and fix missing reference files in dynamic skills.

## Usage

```
/fix-skill-docs [crate_name] [--check-only] [--remove-invalid]
```

**Arguments:**
- `crate_name`: Specific crate to check (optional, defaults to all)
- `--check-only`: Only report issues, don't fix
- `--remove-invalid`: Remove invalid references instead of creating files

## Instructions

### 1. Scan Skills Directory

```bash
# If crate_name provided
skill_dir=~/.claude/skills/{crate_name}

# Otherwise scan all
for dir in ~/.claude/skills/*/; do
    # Process each skill
done
```

### 2. Parse SKILL.md for References

Extract referenced files from Documentation section:

```markdown
## Documentation
- `./references/file1.md` - Description
```

### 3. Check File Existence

```bash
if [ ! -f "{skill_dir}/references/{filename}" ]; then
    echo "MISSING: {filename}"
fi
```

### 4. Report Status

```
=== {crate_name} ===
SKILL.md: ✅
references/:
  - sync.md: ✅
  - runtime.md: ❌ MISSING

Action needed: 1 file missing
```

### 5. Fix Missing Files

**--check-only**: Only report, don't fix.

**--remove-invalid**: Update SKILL.md to remove invalid references.

**Default**: Generate missing files using agent-browser:

```bash
agent-browser "Navigate to docs.rs/{crate_name}/latest/{crate_name}/{module}/
Extract documentation for {topic}. Save as markdown."
```

### 6. Update SKILL.md

Ensure Documentation section matches actual files.

## Tool Priority

1. **agent-browser CLI** - Generate missing documentation
2. **WebFetch** - Fallback if agent-browser unavailable
3. **Edit SKILL.md** - Remove invalid references (--remove-invalid)

## Example

```bash
# Check all skills
/fix-skill-docs --check-only

# Fix specific crate
/fix-skill-docs tokio

# Remove invalid references
/fix-skill-docs tokio --remove-invalid
```
