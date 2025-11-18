# Migration Guide: Phase 2 to Phase 3

## Overview

Phase 3 of the adaptive buffer sizing feature makes workload profiles **enabled by default**. This document helps you understand the changes and how to migrate smoothly.

## What Changed

### Phase 2 (Opt-In)
- Buffer profiling was **disabled by default**
- Required explicit enabling via `--buffer-profile-enable` or `RUSTFS_BUFFER_PROFILE_ENABLE=true`
- Used legacy PR #869 behavior unless explicitly enabled

### Phase 3 (Default Enablement)
- Buffer profiling is **enabled by default** with `GeneralPurpose` profile
- No configuration needed for default behavior
- Can opt-out via `--buffer-profile-disable` or `RUSTFS_BUFFER_PROFILE_DISABLE=true`
- Maintains full backward compatibility

## Impact Analysis

### For Most Users (No Action Required)

The `GeneralPurpose` profile (default in Phase 3) provides the **same buffer sizes** as PR #869 for most file sizes:
- Small files (< 1MB): 64KB buffer
- Medium files (1MB-100MB): 256KB buffer
- Large files (≥ 100MB): 1MB buffer

**Result:** Your existing deployments will work exactly as before, with no performance changes.

### For Users Who Explicitly Enabled Profiles in Phase 2

If you were using:
```bash
# Phase 2
export RUSTFS_BUFFER_PROFILE_ENABLE=true
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data
```

You can simplify to:
```bash
# Phase 3
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data
```

The `RUSTFS_BUFFER_PROFILE_ENABLE` variable is no longer needed (but still respected for compatibility).

### For Users Who Want Exact Legacy Behavior

If you need the guaranteed exact behavior from PR #869 (before any profiling):

```bash
# Phase 3 - Opt out to legacy behavior
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data

# Or via command-line
./rustfs --buffer-profile-disable /data
```

## Migration Scenarios

### Scenario 1: Default Deployment (No Changes Needed)

**Phase 2:**
```bash
./rustfs /data
# Used PR #869 fixed algorithm
```

**Phase 3:**
```bash
./rustfs /data
# Uses GeneralPurpose profile (same buffer sizes as PR #869 for most cases)
```

**Action:** None required. Behavior is essentially identical.

### Scenario 2: Using Custom Profile in Phase 2

**Phase 2:**
```bash
export RUSTFS_BUFFER_PROFILE_ENABLE=true
export RUSTFS_BUFFER_PROFILE=WebWorkload
./rustfs /data
```

**Phase 3 (Simplified):**
```bash
export RUSTFS_BUFFER_PROFILE=WebWorkload
./rustfs /data
# RUSTFS_BUFFER_PROFILE_ENABLE no longer needed
```

**Action:** Remove `RUSTFS_BUFFER_PROFILE_ENABLE=true` from your configuration.

### Scenario 3: Explicitly Disabled in Phase 2

**Phase 2:**
```bash
# Or just not setting RUSTFS_BUFFER_PROFILE_ENABLE
./rustfs /data
```

**Phase 3 (If you want to keep legacy behavior):**
```bash
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data
```

**Action:** Set `RUSTFS_BUFFER_PROFILE_DISABLE=true` if you want to guarantee exact PR #869 behavior.

### Scenario 4: AI/ML Workloads

**Phase 2:**
```bash
export RUSTFS_BUFFER_PROFILE_ENABLE=true
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data
```

**Phase 3 (Simplified):**
```bash
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data
```

**Action:** Remove `RUSTFS_BUFFER_PROFILE_ENABLE=true`.

## Configuration Reference

### Phase 3 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTFS_BUFFER_PROFILE` | `GeneralPurpose` | The workload profile to use |
| `RUSTFS_BUFFER_PROFILE_DISABLE` | `false` | Disable profiling and use legacy behavior |

### Phase 3 Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--buffer-profile <PROFILE>` | `GeneralPurpose` | Set the workload profile |
| `--buffer-profile-disable` | disabled | Disable profiling (opt-out) |

### Deprecated (Still Supported for Compatibility)

| Variable | Status | Replacement |
|----------|--------|-------------|
| `RUSTFS_BUFFER_PROFILE_ENABLE` | Deprecated | Profiling is enabled by default; use `RUSTFS_BUFFER_PROFILE_DISABLE` to opt-out |

## Performance Expectations

### GeneralPurpose Profile (Default)

Same performance as PR #869 for most workloads:
- Small files: Same 64KB buffer
- Medium files: Same 256KB buffer
- Large files: Same 1MB buffer

### Specialized Profiles

When you switch to a specialized profile, you get optimized buffer sizes:

| Profile | Performance Benefit | Use Case |
|---------|-------------------|----------|
| `AiTraining` | Up to 4x throughput on large files | ML model files, training datasets |
| `WebWorkload` | Lower memory, higher concurrency | Static assets, CDN |
| `DataAnalytics` | Balanced for mixed patterns | Data warehouses, BI |
| `IndustrialIoT` | Low latency, memory-efficient | Sensor data, telemetry |
| `SecureStorage` | Compliance-focused, minimal memory | Government, healthcare |

## Testing Your Migration

### Step 1: Test Default Behavior

```bash
# Start with default configuration
./rustfs /data

# Verify it works as expected
# Check logs for: "Using buffer profile: GeneralPurpose"
```

### Step 2: Test Your Workload Profile (If Using)

```bash
# Set your specific profile
export RUSTFS_BUFFER_PROFILE=AiTraining
./rustfs /data

# Verify in logs: "Using buffer profile: AiTraining"
```

### Step 3: Test Opt-Out (If Needed)

```bash
# Disable profiling
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data

# Verify in logs: "using legacy adaptive buffer sizing"
```

## Rollback Plan

If you encounter any issues with Phase 3, you can easily roll back:

### Option 1: Disable Profiling

```bash
export RUSTFS_BUFFER_PROFILE_DISABLE=true
./rustfs /data
```

This gives you the exact PR #869 behavior.

### Option 2: Use GeneralPurpose Profile Explicitly

```bash
export RUSTFS_BUFFER_PROFILE=GeneralPurpose
./rustfs /data
```

This uses profiling but with conservative buffer sizes.

## FAQ

### Q: Will Phase 3 break my existing deployment?

**A:** No. The default `GeneralPurpose` profile uses the same buffer sizes as PR #869 for most scenarios. Your deployment will work exactly as before.

### Q: Do I need to change my configuration?

**A:** Only if you were explicitly using profiles in Phase 2. You can simplify by removing `RUSTFS_BUFFER_PROFILE_ENABLE=true`.

### Q: What if I want the exact legacy behavior?

**A:** Set `RUSTFS_BUFFER_PROFILE_DISABLE=true` to use the exact PR #869 algorithm.

### Q: Can I still use RUSTFS_BUFFER_PROFILE_ENABLE?

**A:** Yes, it's still supported for backward compatibility, but it's no longer necessary.

### Q: How do I know which profile is active?

**A:** Check the startup logs for messages like:
- "Using buffer profile: GeneralPurpose"
- "Buffer profiling is disabled, using legacy adaptive buffer sizing"

### Q: Should I switch to a specialized profile?

**A:** Only if you have specific workload characteristics:
- AI/ML with large files → `AiTraining`
- Web applications → `WebWorkload`
- Secure/compliance environments → `SecureStorage`
- Default is fine for most general-purpose workloads

## Support

If you encounter issues during migration:

1. Check logs for buffer profile information
2. Try disabling profiling with `--buffer-profile-disable`
3. Report issues with:
   - Your workload type
   - File sizes you're working with
   - Performance observations
   - Log excerpts showing buffer profile initialization

## Timeline

- **Phase 1:** Infrastructure (✅ Complete)
- **Phase 2:** Opt-In Usage (✅ Complete)
- **Phase 3:** Default Enablement (✅ Current - You are here)
- **Phase 4:** Full Integration (Future)

## Conclusion

Phase 3 represents a smooth evolution of the adaptive buffer sizing feature. The default behavior remains compatible with PR #869, while providing an easy path to optimize for specific workloads when needed.

Most users can migrate without any changes, and those who need the exact legacy behavior can easily opt-out.
