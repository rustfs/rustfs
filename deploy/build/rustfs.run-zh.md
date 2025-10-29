# RustFS Service Installation Guide

## 1. Preparation

### 1.1 Create a system user

```bash
# Create the rustfs system user and group without shell access
sudo useradd -r -s /sbin/nologin rustfs
```

### 1.2 Create required directories

```bash
# Application directory
sudo mkdir -p /opt/rustfs

# Data directories
sudo mkdir -p /data/rustfs/{vol1,vol2}

# Configuration directory
sudo mkdir -p /etc/rustfs

# Assign ownership and permissions
sudo chown -R rustfs:rustfs /opt/rustfs /data/rustfs
sudo chmod 755 /opt/rustfs /data/rustfs
```

## 2. Install RustFS

```bash
# Copy the RustFS binary
sudo cp rustfs /usr/local/bin/
sudo chmod +x /usr/local/bin/rustfs

# Copy configuration files
sudo cp obs.yaml /etc/rustfs/
sudo chown -R rustfs:rustfs /etc/rustfs
```

## 3. Configure the systemd service

```bash
# Install the service unit
sudo cp rustfs.service /etc/systemd/system/

# Reload systemd units
sudo systemctl daemon-reload
```

## 4. Service management

### 4.1 Start the service

```bash
sudo systemctl start rustfs
```

### 4.2 Check service status

```bash
sudo systemctl status rustfs
```

### 4.3 Enable auto-start on boot

```bash
sudo systemctl enable rustfs
```

### 4.4 Inspect logs

```bash
# Follow live logs
sudo journalctl -u rustfs -f

# View today's logs
sudo journalctl -u rustfs --since today
```

## 5. Validate the installation

```bash
# Confirm the service port
ss -tunlp | grep 9000

# Verify availability
curl -I http://localhost:9000
```
