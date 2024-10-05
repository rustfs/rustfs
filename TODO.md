# TODO LIST

## 基础存储

- [x] EC可用读写数量判断 Read/WriteQuorum
- [ ] 优化后台并发执行，可中断
- [ ] 小文件存储到metafile, inlinedata
- [ ] 完善bucketmeta
- [x] 对象锁
- [ ] 边读写边hash，实现reader嵌套
- [x] 远程rpc
- [x] 错误类型判断,程序中判断错误类型，如何统一错误
- [x] 优化xlmeta, 自定义msg数据结构
- [ ] 优化io.reader 参考 GetObjectNInfo 方便io copy 如果 异步写，再平衡
- [ ] 代码优化 使用范型？
- [ ] 抽象出metafile存储

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
- [ ] Policy管理
- [ ] AK/SK分配管理
- [ ] data scanner统计和对象修复
- [ ] 桶配额
- [ ] 桶只读
- [ ] 桶复制
- [ ] 桶事件通知
- [ ] 桶公开、桶私有
- [ ] 对象生命周期管理
- [ ] prometheus对接
- [ ] 日志收集和日志外发
- [ ] 对象压缩
- [ ] STS
- [ ] 分层（阿里云、腾讯云、S3远程对接）
