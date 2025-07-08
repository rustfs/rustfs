# Delete __XLDIR__ Directory Scripts

This directory contains scripts for deleting all directories ending with `__XLDIR__` in the specified path.

## Script Description

### 1. delete_xldir.sh (Full Version)

A feature-rich version with multiple options and safety checks.

**Usage:**
```bash
./scripts/delete_xldir.sh <path> [options]
```

**Options:**
- `-f, --force`    Force deletion without confirmation
- `-v, --verbose`  Show verbose information
- `-d, --dry-run`  Show directories to be deleted without actually deleting
- `-h, --help`     Show help information

**Examples:**
```bash
# Preview directories to be deleted (without actually deleting)
./scripts/delete_xldir.sh /path/to/search --dry-run

# Interactive deletion (will ask for confirmation)
./scripts/delete_xldir.sh /path/to/search

# Force deletion (without confirmation)
./scripts/delete_xldir.sh /path/to/search --force

# Verbose mode deletion
./scripts/delete_xldir.sh /path/to/search --verbose
```

### 2. delete_xldir_simple.sh (Simple Version)

A streamlined version that directly deletes found directories.

**Usage:**
```bash
./scripts/delete_xldir_simple.sh <path>
```

**Example:**
```bash
# Delete all directories ending with __XLDIR__ in the specified path
./scripts/delete_xldir_simple.sh /path/to/search
```

## How It Works

Both scripts use the `find` command to locate directories:
```bash
find "$SEARCH_PATH" -type d -name "*__XLDIR__"
```

- `-type d`: Only search for directories
- `-name "*__XLDIR__"`: Find directories ending with `__XLDIR__`

## Safety Notes

⚠️ **Important Reminders:**
- Deletion operations are irreversible, please confirm the path is correct before use
- It's recommended to use the `--dry-run` option first to preview directories to be deleted
- For important data, please backup first

## Use Cases

These scripts are typically used for cleaning up temporary directories or metadata directories in storage systems, especially in distributed storage systems where `__XLDIR__` is commonly used as a specific directory identifier. 