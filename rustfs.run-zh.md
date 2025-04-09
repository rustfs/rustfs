# RustFS 服务安装配置教程

## 1. 准备工作

### 1.1 创建系统用户

```bash
# 创建 rustfs 系统用户和用户组，禁止登录shell
sudo useradd -r -s /sbin/nologin rustfs
```

### 1.2 创建必要目录

```bash
# 创建程序目录
sudo mkdir -p /opt/rustfs

# 创建数据目录
sudo mkdir -p /data/rustfs/{vol1,vol2}

# 创建配置目录
sudo mkdir -p /etc/rustfs

# 设置目录权限
sudo chown -R rustfs:rustfs /opt/rustfs /data/rustfs
sudo chmod 755 /opt/rustfs /data/rustfs
```

## 2. 安装 RustFS

```bash
# 复制 RustFS 二进制文件
sudo cp rustfs /usr/local/bin/
sudo chmod +x /usr/local/bin/rustfs

# 复制配置文件
sudo cp obs.yaml /etc/rustfs/
sudo chown -R rustfs:rustfs /etc/rustfs
```

## 3. 配置 Systemd 服务

```bash
# 复制服务单元文件
sudo cp rustfs.service /etc/systemd/system/

# 重新加载 systemd 配置
sudo systemctl daemon-reload
```

## 4. 服务管理

### 4.1 启动服务

```bash
sudo systemctl start rustfs
```

### 4.2 查看服务状态

```bash
sudo systemctl status rustfs
```

### 4.3 启用开机自启

```bash
sudo systemctl enable rustfs
```

### 4.4 查看服务日志

```bash
# 查看实时日志
sudo journalctl -u rustfs -f

# 查看今天的日志
sudo journalctl -u rustfs --since today
```

## 5. 验证安装

```bash
# 检查服务端口
ss -tunlp | grep 9000
ss -tunlp | grep 9002

# 测试服务可用性
curl -I http://localhost:9000
```
