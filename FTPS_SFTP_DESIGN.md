# RustFS FTPS/SFTP 实现设计文档

## 1. 总体架构

RustFS的FTPS/SFTP实现遵循与MinIO类似的模块化架构：

```
rustfs/rustfs/protocols/
├── ftps/
│   ├── server.rs      # FTPS服务器实现
│   ├── driver.rs      # FTP存储驱动
│   └── mod.rs         # 模块声明
├── sftp/
│   ├── server.rs      # SFTP服务器实现
│   ├── handler.rs     # SFTP请求处理器
│   └── mod.rs         # 模块声明
|---webdav（未来）
├── client/
│   └── s3.rs          # 统一S3客户端
├── session/
│   ├── principal.rs   # 协议主体信息
│   └── context.rs     # 会话上下文
├── gateway/
│   ├── adapter.rs     # 协议适配器
│   ├── authorize.rs   # 授权模块
│   ├── error.rs       # 错误处理(都映射为S3标准错误类型)
│   └── restrictions.rs # 功能限制（提供请求方法检查，是否支持这种类型操作）
└── mod.rs             # 协议模块声明
```

## 2. 核心组件设计

### 2.1 FTPS服务器 (ftps/server.rs)

**主要功能：**
- 使用libunftp作为FTP服务器框架
- 支持FTPS加密传输
- 集成RustFS IAM认证系统
- 管理服务器生命周期

### 2.2 FTPS驱动 (ftps/driver.rs)

**主要功能：**
- 实现libunftp的StorageBackend trait
- 将FTP操作转换为S3操作
- 集成网关层进行权限检查

### 2.3 SFTP服务器 (sftp/server.rs)

**主要功能：**
- 使用russh作为SSH/SFTP服务器框架
- 支持SSH密钥认证
- 集成RustFS IAM认证系统
- 管理服务器生命周期

### 2.4 SFTP处理器 (sftp/handler.rs)

**主要功能：**
- 实现russh_sftp的Handler trait
- 将SFTP操作转换为S3操作
- 集成网关层进行权限检查

### 2.5 统一S3客户端 (client/s3.rs)

**主要功能：**
- 为协议实现提供统一的S3客户端
- 确保所有操作都通过标准S3 API路径
- 遵循MinIO约束：只能提供外部S3客户端能做的功能

## 3. 认证和授权

### 3.1 用户类型

**FTPS用户：**
```rust
pub struct FtpsUser {
    pub username: String,
    pub name: Option<String>,
    pub principal: Arc<UserIdentity>,
}
```

**SFTP用户：**
通过SSH会话上下文获取用户身份信息。

### 3.2 认证流程

1. FTPS: 通过libunftp认证器集成RustFS IAM系统
2. SFTP: 通过russh认证回调集成RustFS IAM系统
3. 验证凭据有效性
4. 创建协议主体信息

### 3.3 授权流程

1. 每个操作前检查权限
2. 使用网关层进行访问控制
3. 验证操作是否符合S3语义

## 4. 存储后端接口

### 4.1 FTPS存储操作

- `metadata`: 对应S3 HeadObject/HeadBucket
- `list`: 对应S3 ListObjectsV2
- `get`: 对应S3 GetObject
- `put`: 对应S3 PutObject
- `del`: 对应S3 DeleteObject
- `mkd`: 对应S3 PutObject (目录标记)
- `rmd`: 对应S3 DeleteObject (目录标记)
- `rename`: 对应S3 CopyObject + DeleteObject

### 4.2 SFTP存储操作

- `Fileread`: 对应S3 GetObject
- `Filewrite`: 对应S3 PutObject
- `Filecmd`: 对应S3 DeleteObject/CopyObject等
- `Filelist`: 对应S3 ListObjectsV2

## 5. 网关层集成

### 5.1 适配器模块
- 将协议操作映射到S3操作
- 验证操作支持性

### 5.2 授权模块
- 检查用户权限
- 验证操作合法性

### 5.3 限制模块
- 定义协议功能限制
- 确保与S3语义一致性

## 6. 错误处理

- 统一错误类型定义
- S3错误到协议错误的映射
- 适当的日志记录

## 7. 配置集成

### 7.1 命令行参数
```
--ftps-enable                启用FTPS服务器
--ftps-address               FTPS服务器绑定地址
--ftps-certs-file            FTPS证书文件路径
--ftps-key-file              FTPS私钥文件路径

--sftp-enable                启用SFTP服务器
--sftp-address               SFTP服务器绑定地址
--sftp-host-key              SFTP主机密钥文件路径
```

### 7.2 环境变量
```
RUSTFS_FTPS_ENABLE
RUSTFS_FTPS_ADDRESS
RUSTFS_FTPS_CERTS_FILE
RUSTFS_FTPS_KEY_FILE

RUSTFS_SFTP_ENABLE
RUSTFS_SFTP_ADDRESS
RUSTFS_SFTP_HOST_KEY
```
