# MinIO的优点你们会继续保持吗？

我们会继续保持MinIO的优点来实现RustFS。

1. 简单；
2. 快速；
3. 无元数据中心风险；
4. 方便扩容；
5. 方便退役；
6. S3兼容；
7. 数据湖和AI兼容；
8. 生命周期管理；
9. 桶复制；
... ...

| RustFS | MinIO | Ceph |
| - | - | - |
| 简单部署 | 简单部署| 部署困难|
|一键启动|一键启动| 复杂的启动流程|
|扩容简单| 扩容简单| 扩容实再平衡IO压力非常大|
|无元数据中心风险| 无元数据中心风险| 有元数据中心风险|
|速度极快|速度极快|对象存储没有RustFS和MinIO快|
|S3协议| S3协议兼容| S3、NFS、Swift、POSIX、iSCSI等|
|多云支持| 多云支持|多云支持能力一般|
|物联网非常友好|物联网支持相对友好|物联网设备支持较差|
|资源占用极低|资源占用低|资源占用高|
|使用简单|使用简单|使用很难|
|学习简单|学习简单|学习很难|



