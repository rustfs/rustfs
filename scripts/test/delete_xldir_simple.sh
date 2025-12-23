#!/usr/bin/env bash

# Simple version: Delete all directories ending with __XLDIR__ in the specified path

if [ $# -eq 0 ]; then
    echo "Usage: $0 <path>"
    echo "Example: $0 /path/to/search"
    exit 1
fi

SEARCH_PATH="$1"

# Check if path exists
if [ ! -d "$SEARCH_PATH" ]; then
    echo "Error: Path '$SEARCH_PATH' does not exist or is not a directory"
    exit 1
fi

echo "Searching in path: $SEARCH_PATH"

# Find and delete all directories ending with __XLDIR__
find "$SEARCH_PATH" -type d -name "*__XLDIR__" -exec rm -rf {} \; 2>/dev/null

echo "Deletion completed!" 