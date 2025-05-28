# TODO LIST

## 基础存储

- [x] EC 可用读写数量判断 Read/WriteQuorum
- [ ] 优化后台并发执行，可中断，传引用？
- [x] 小文件存储到 metafile, inlinedata
- [x] 完善 bucketmeta
- [x] 对象锁
- [x] 边读写边 hash，实现 reader 嵌套
- [x] 远程 rpc
- [x] 错误类型判断，程序中判断错误类型，如何统一错误
- [x] 优化 xlmeta, 自定义 msg 数据结构
- [ ] 优化 io.reader 参考 GetObjectNInfo 方便 io copy 如果 异步写，再平衡
- [ ] 代码优化 使用范型？
- [ ] 抽象出 metafile 存储

## 基础功能

- [ ] 桶操作
  - [x] 创建 CreateBucket
  - [x] 列表 ListBuckets
    - [ ] 桶下面的文件列表 ListObjects
      - [x] 简单实现功能
      - [ ] 优化并发读取
  - [ ] 删除
  - [x] 详情 HeadBucket
- [ ] 文件操作
  - [x] 上传 PutObject
  - [x] 大文件上传
    - [x] 创建分片上传 CreateMultipartUpload
    - [x] 上传分片 PubObjectPart
    - [x] 提交完成 CompleteMultipartUpload
    - [x] 取消上传 AbortMultipartUpload
  - [x] 下载 GetObject
  - [x] 删除 DeleteObjects
  - [ ] 版本控制
  - [ ] 对象锁
  - [ ] 复制 CopyObject
  - [ ] 详情 HeadObject
  - [ ] 对象预先签名（get、put、head、post）

## 扩展功能

- [ ] 用户管理
- [ ] Policy 管理
- [ ] AK/SK分配管理
- [ ] data scanner 统计和对象修复
- [ ] 桶配额
- [ ] 桶只读
- [ ] 桶复制
- [ ] 桶事件通知
- [ ] 桶公开、桶私有
- [ ] 对象生命周期管理
- [ ] prometheus 对接
- [ ] 日志收集和日志外发
- [ ] 对象压缩
- [ ] STS
- [ ] 分层（阿里云、腾讯云、S3 远程对接）



## 性能优化
- [ ] bitrot impl AsyncRead/AsyncWrite
- [ ] erasure 并发读写
- [x] 完善删除逻辑，并发处理，先移动到回收站，
- [ ] 空间不足时清空回收站
- [ ] list_object 使用 reader 传输