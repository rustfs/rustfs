# TODO LIST

## 基础存储

- [ ] 上传同名文件时，删除旧版本文件
- [ ] EC可用读写数量判断
- [ ] 小文件存储到metafile, inlinedata
- [ ] 错误类型判断,程序中判断错误类型，如何统一错误
- [ ] 优化并发执行
- [ ] 抽象出metafile存储
- [ ] 代码优化

## 基础功能

- [ ] 桶操作
  - [x] 创建 CreateBucket
  - [x] 列表 ListBuckets
    - [ ] 桶下面的文件列表 ListObjects
- [x] 详情 HeadBucket
- [ ] 删除
- [ ] 文件操作
  - [x] 上传 PutObject
  - [ ] 大文件上传
    - [x] 创建分片上传 CreateMultipartUpload
    - [x] 上传分片 PubObjectPart
    - [x] 提交完成 CompleteMultipartUpload
    - [ ] 取消上传
  - [x] 下载 GetObject
  - [ ] 复制 CopyObject
  - [ ] 详情 HeadObject
  - [ ] 删除

## 扩展功能

- [ ] 版本控制
- [ ] 对象锁
- [ ] 修复
