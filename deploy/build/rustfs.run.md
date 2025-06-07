# RustFS Service Installation Guide

## 1. Prerequisites

### 1.1 Create System User

```bash
# Create rustfs system user and group without login shell
sudo useradd -r -s /sbin/nologin rustfs
```

### 1.2 Create Required Directories

```bash
# Create program directory
sudo mkdir -p /opt/rustfs

# Create data directories
sudo mkdir -p /data/rustfs/{vol1,vol2}

# Create configuration directory
sudo mkdir -p /etc/rustfs

# Set directory permissions
sudo chown -R rustfs:rustfs /opt/rustfs /data/rustfs
sudo chmod 755 /opt/rustfs /data/rustfs
```

## 2. Install RustFS

```bash
# Copy RustFS binary
sudo cp rustfs /usr/local/bin/
sudo chmod +x /usr/local/bin/rustfs

# Copy configuration file
sudo cp obs.yaml /etc/rustfs/
sudo chown -R rustfs:rustfs /etc/rustfs
```

## 3. Configure Systemd Service

```bash
# Copy service unit file
sudo cp rustfs.service /etc/systemd/system/

# Reload systemd configuration
sudo systemctl daemon-reload
```

## 4. Service Management

### 4.1 Start Service

```bash
sudo systemctl start rustfs
```

### 4.2 Check Service Status

```bash
sudo systemctl status rustfs
```

### 4.3 Enable Auto-start

```bash
sudo systemctl enable rustfs
```

### 4.4 View Service Logs

```bash
# View real-time logs
sudo journalctl -u rustfs -f

# View today's logs
sudo journalctl -u rustfs --since today
```

## 5. Verify Installation

```bash
# Check service ports
ss -tunlp | grep 9000
ss -tunlp | grep 9001

# Test service availability
curl -I http://localhost:9000
```
