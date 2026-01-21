#!/bin/bash
# Generate SSE encryption key (32 bytes base64 encoded)
# For testing with __RUSTFS_SSE_SIMPLE_CMK environment variable
# Usage: ./generate-sse-keys.sh

set -euo pipefail

# Function to generate a random 256-bit key and encode it in base64
generate_base64_key() {
    # Generate 32 bytes (256 bits) of random data and base64 encode it
    openssl rand -base64 32 | tr -d '\n'
}

# Generate key
base64_key=$(generate_base64_key)

# Output result
echo "Generated SSE encryption key (32 bytes, base64 encoded):"
echo ""
echo "$base64_key"
echo ""
echo "You can use this in your environment variable:"
echo "export __RUSTFS_SSE_SIMPLE_CMK=\"$base64_key\""

