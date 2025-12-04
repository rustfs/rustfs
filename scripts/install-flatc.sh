#!/bin/bash
# Install flatc 25.9.23 on macOS

set -e

FLATC_VERSION="25.9.23"
ARCH=$(uname -m)
INSTALL_DIR="${HOME}/.local/bin"
FLATC_BIN="${INSTALL_DIR}/flatc"

# Select download URL based on architecture
if [ "$ARCH" = "arm64" ]; then
    # Apple Silicon (M1/M2/M3)
    FLATC_URL="https://github.com/google/flatbuffers/releases/download/v${FLATC_VERSION}/Mac.flatc.binary.zip"
elif [ "$ARCH" = "x86_64" ]; then
    # Intel Mac
    FLATC_URL="https://github.com/google/flatbuffers/releases/download/v${FLATC_VERSION}/MacIntel.flatc.binary.zip"
else
    echo "Error: Unsupported architecture $ARCH"
    exit 1
fi

echo "Downloading flatc ${FLATC_VERSION} for ${ARCH}..."
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

# Download and extract
curl -L -o flatc.zip "$FLATC_URL"
unzip -q flatc.zip

# Create install directory
mkdir -p "$INSTALL_DIR"

# Backup existing version if present
if [ -f "$FLATC_BIN" ]; then
    echo "Backing up existing flatc to ${FLATC_BIN}.backup"
    mv "$FLATC_BIN" "${FLATC_BIN}.backup"
fi

# Install flatc
cp flatc "$FLATC_BIN"
chmod +x "$FLATC_BIN"

# Clean up temporary files
cd -
rm -rf "$TEMP_DIR"

# Verify installation
echo ""
echo "Verifying installation..."
"$FLATC_BIN" --version

# Check PATH
if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
    echo ""
    echo "⚠️  Warning: ${INSTALL_DIR} is not in PATH"
    echo "Please add the following to ~/.zshrc or ~/.bash_profile:"
    echo ""
    echo "  export PATH=\"\${HOME}/.local/bin:\$PATH\""
    echo ""
    echo "Then run: source ~/.zshrc"
else
    echo ""
    echo "✅ flatc ${FLATC_VERSION} installed successfully!"
    echo "Location: $FLATC_BIN"
fi

