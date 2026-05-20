#!/usr/bin/env bash
# Install protoc 33.1 on macOS and Linux

set -e

PROTOC_VERSION="33.1"
ARCH=$(uname -m)
INSTALL_DIR="${HOME}/.local/bin"
PROTOC_BIN="${INSTALL_DIR}/protoc"

# Detect OS
OS="$(uname -s)"

# Select download URL based on OS and architecture
if [ "$OS" = "Darwin" ]; then
    if [ "$ARCH" = "arm64" ]; then
        # Apple Silicon (M1/M2/M3)
        PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-aarch_64.zip"
    elif [ "$ARCH" = "x86_64" ]; then
        # Intel Mac
        PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-x86_64.zip"
    else
        echo "Error: Unsupported macOS architecture $ARCH"
        exit 1
    fi
elif [ "$OS" = "Linux" ]; then
    if [ "$ARCH" = "x86_64" ]; then
        PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"
    elif [ "$ARCH" = "aarch64" ]; then
        PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-aarch_64.zip"
    else
        echo "Error: Unsupported Linux architecture $ARCH"
        exit 1
    fi
else
    echo "Error: Unsupported OS $OS"
    echo "On Windows, install protoc via:"
    echo "  choco install protoc --version=${PROTOC_VERSION}"
    echo "  or download from https://github.com/protocolbuffers/protobuf/releases"
    exit 1
fi

echo "Downloading protoc ${PROTOC_VERSION} for ${ARCH}..."
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

# Download and extract
curl -L -o protoc.zip "$PROTOC_URL"
unzip -q protoc.zip

# Create install directory
mkdir -p "$INSTALL_DIR"

# Backup existing version if present
if [ -f "$PROTOC_BIN" ]; then
    echo "Backing up existing protoc to ${PROTOC_BIN}.backup"
    mv "$PROTOC_BIN" "${PROTOC_BIN}.backup"
fi

# Install protoc
cp bin/protoc "$PROTOC_BIN"
chmod +x "$PROTOC_BIN"

# Clean up temporary files
cd -
rm -rf "$TEMP_DIR"

# Verify installation
echo ""
echo "Verifying installation..."
"$PROTOC_BIN" --version

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
    echo "✅ protoc ${PROTOC_VERSION} installed successfully!"
    echo "Location: $PROTOC_BIN"
fi

