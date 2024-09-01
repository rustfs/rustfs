# TODO LIST

## 基础存储

- [ ] EC可用读写数量判断 Read/WriteQuorum
- [ ] 优化并发执行，边读边取，可中断
- [ ] 小文件存储到metafile, inlinedata
- [ ] 代码优化 使用范型？
- [ ] 抽象出metafile存储
- [x] 错误类型判断,程序中判断错误类型，如何统一错误
- [x] 上传同名文件时，删除旧版本文件
- [x] 优化xlmeta, 自定义msg数据结构

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
  - [ ] 删除 DeleteObjects
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
