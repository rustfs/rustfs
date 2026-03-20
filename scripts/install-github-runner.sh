#!/usr/bin/env bash
# Install and configure a GitHub Actions self-hosted runner on Ubuntu.
# Repo: https://github.com/wasabi/rustfs
# Prompts for runner token and runner name; adds labels 'ubicloud-standard-2' and 'ubicloud-standard-4  '; uses default group.

set -e

RUNNER_VERSION="2.332.0"
RUNNER_TAR="actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
RUNNER_SHA256="f2094522a6b9afeab07ffb586d1eb3f190b6457074282796c497ce7dce9e0f2a"
REPO_URL="https://github.com/wasabi/rustfs"
RUNNER_LABELS="ubicloud-standard-2,ubicloud-standard-4"
INSTALL_DIR="${INSTALL_DIR:-$HOME/actions-runner}"

echo "GitHub self-hosted runner installer for ${REPO_URL}"
echo "Runner version: ${RUNNER_VERSION}"
echo "Install directory: ${INSTALL_DIR}"
echo ""

# System packages and pipx CLI tools used by CI workflows
echo "Installing dependencies (apt: unzip, pipx)..."
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y unzip pipx
echo "Installing awscurl via pipx..."
pipx install awscurl
pipx ensurepath

# Prompt for token
read -r -p "Runner token: " RUNNER_TOKEN
if [ -z "$RUNNER_TOKEN" ]; then
  echo "Error: Runner token is required."
  exit 1
fi

# Prompt for runner name
read -r -p "Runner name: " RUNNER_NAME
if [ -z "$RUNNER_NAME" ]; then
  echo "Error: Runner name is required."
  exit 1
fi

# Create install directory
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Download
echo "Downloading runner package..."
curl -o "$RUNNER_TAR" -L "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${RUNNER_TAR}"

# Validate hash
echo "Validating checksum..."
echo "${RUNNER_SHA256}  ${RUNNER_TAR}" | shasum -a 256 -c

# Extract
echo "Extracting..."
tar xzf "$RUNNER_TAR"
rm -f "$RUNNER_TAR"

# Configure: default group (omit --runnergroup), labels x,y
echo "Configuring runner..."
./config.sh \
  --url "$REPO_URL" \
  --token "$RUNNER_TOKEN" \
  --name "$RUNNER_NAME" \
  --labels "$RUNNER_LABELS" \
  --unattended

# Install and start service (runs in background, survives reboot)
echo "Installing runner as a systemd service..."
sudo ./svc.sh install
sudo ./svc.sh start

echo ""
echo "Runner installed and running. To use in workflows:"
echo "  runs-on: self-hosted"
echo ""
echo "Useful commands (from ${INSTALL_DIR}):"
echo "  sudo ./svc.sh status   # check status"
echo "  sudo ./svc.sh stop     # stop"
echo "  sudo ./svc.sh start    # start"
echo "  sudo ./svc.sh uninstall  # remove service (then delete directory to fully remove)"
