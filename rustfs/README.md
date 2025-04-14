rustfs/
├── Cargo.toml
├── src/
│ ├── main.rs # 主入口
│ ├── admin/
│ │ └── mod.rs # 管理接口
│ ├── auth/
│ │ └── mod.rs # 认证模块
│ ├── config/
│ │ ├── mod.rs # 配置模块
│ │ └── options.rs # 命令行参数
│ ├── console/
│ │ ├── mod.rs # 控制台模块
│ │ └── server.rs # 控制台服务器
│ ├── grpc/
│ │ └── mod.rs # gRPC 服务
│ ├── license/
│ │ └── mod.rs # 许可证管理
│ ├── logging/
│ │ └── mod.rs # 日志管理
│ ├── server/
│ │ ├── mod.rs # 服务器实现
│ │ ├── connection.rs # 连接处理
│ │ ├── service.rs # 服务实现
│ │ └── state.rs # 状态管理
│ ├── storage/
│ │ ├── mod.rs # 存储模块
│ │ └── fs.rs # 文件系统实现
│ └── utils/
│ └── mod.rs # 工具函数