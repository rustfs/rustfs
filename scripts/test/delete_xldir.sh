#!/usr/bin/env bash

# Delete all directories ending with __XLDIR__ in the specified path

# Check parameters
if [ $# -eq 0 ]; then
    echo "Usage: $0 <path> [options]"
    echo "Options:"
    echo "  -f, --force    Force deletion without confirmation"
    echo "  -v, --verbose  Show verbose information"
    echo "  -d, --dry-run  Show directories to be deleted without actually deleting"
    echo ""
    echo "Examples:"
    echo "  $0 /path/to/search"
    echo "  $0 /path/to/search --dry-run"
    echo "  $0 /path/to/search --force"
    exit 1
fi

# Parse parameters
SEARCH_PATH=""
FORCE=false
VERBOSE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 <path> [options]"
            echo "Delete all directories ending with __XLDIR__ in the specified path"
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            exit 1
            ;;
        *)
            if [ -z "$SEARCH_PATH" ]; then
                SEARCH_PATH="$1"
            else
                echo "Error: Only one path can be specified"
                exit 1
            fi
            shift
            ;;
    esac
done

# Check if path is provided
if [ -z "$SEARCH_PATH" ]; then
    echo "Error: Search path must be specified"
    exit 1
fi

# Check if path exists
if [ ! -d "$SEARCH_PATH" ]; then
    echo "Error: Path '$SEARCH_PATH' does not exist or is not a directory"
    exit 1
fi

# Find all directories ending with __XLDIR__
echo "Searching in path: $SEARCH_PATH"
echo "Looking for directories ending with __XLDIR__..."

# Use find command to locate directories
DIRS_TO_DELETE=$(find "$SEARCH_PATH" -type d -name "*__XLDIR__" 2>/dev/null)

if [ -z "$DIRS_TO_DELETE" ]; then
    echo "No directories ending with __XLDIR__ found"
    exit 0
fi

# Display found directories
echo "Found the following directories:"
echo "$DIRS_TO_DELETE"
echo ""

# Count directories
DIR_COUNT=$(echo "$DIRS_TO_DELETE" | wc -l)
echo "Total found: $DIR_COUNT directories"

# If dry-run mode, only show without deleting
if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "This is dry-run mode, no directories will be actually deleted"
    echo "To actually delete these directories, remove the --dry-run option"
    exit 0
fi

# If not force mode, ask for confirmation
if [ "$FORCE" = false ]; then
    echo ""
    read -p "Are you sure you want to delete these directories? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Operation cancelled"
        exit 0
    fi
fi

# Delete directories
echo ""
echo "Starting to delete directories..."

deleted_count=0
failed_count=0

while IFS= read -r dir; do
    if [ -d "$dir" ]; then
        if [ "$VERBOSE" = true ]; then
            echo "Deleting: $dir"
        fi
        
        if rm -rf "$dir" 2>/dev/null; then
            ((deleted_count++))
            if [ "$VERBOSE" = true ]; then
                echo "  ✓ Deleted successfully"
            fi
        else
            ((failed_count++))
            echo "  ✗ Failed to delete: $dir"
        fi
    fi
done <<< "$DIRS_TO_DELETE"

echo ""
echo "Deletion completed!"
echo "Successfully deleted: $deleted_count directories"
if [ $failed_count -gt 0 ]; then
    echo "Failed to delete: $failed_count directories"
    exit 1
else
    echo "All directories have been successfully deleted"
    exit 0
fi 