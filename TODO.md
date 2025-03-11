# TODO LIST

## 基础存储

- [x] EC可用读写数量判断 Read/WriteQuorum
- [ ] 优化后台并发执行，可中断, 传引用？
- [x] 小文件存储到metafile, inlinedata
- [x] 完善bucketmeta
- [x] 对象锁
- [x] 边读写边hash，实现reader嵌套
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



scp ./target/ubuntu22.04/release/rustfs.zip root@8.130.183.154:~/
scp ./target/ubuntu22.04/release/rustfs.zip root@8.130.177.182:~/ 
scp ./target/ubuntu22.04/release/rustfs.zip root@8.130.91.189:~/
scp ./target/ubuntu22.04/release/rustfs.zip root@8.130.182.114:~/ 

scp ./target/x86_64-unknown-linux-musl/release/rustfs root@8.130.183.154:~/
scp ./target/x86_64-unknown-linux-musl/release/rustfs root@8.130.177.182:~/
scp ./target/x86_64-unknown-linux-musl/release/rustfs root@8.130.91.189:~/
scp ./target/x86_64-unknown-linux-musl/release/rustfs root@8.130.182.114:~/





  2025-03-11T06:18:50.011565Z DEBUG s3s::service: req: Request { method: PUT, uri: /rustfs/rpc/put_file_stream?disk=http://node2:9000/data/rustfs2&volume=.rustfs.sys/tmp&path=a45ade1a-e09b-4eb4-bac1-8b5f55f7d438/235da61f-a705-4f9a-aa21-7801d2eaf61d/part.1&append=false, version: HTTP/1.1, headers: {"accept": "*/*", "host": "node2:9000", "transfer-encoding": "chunked"}, body: Body { hyper: Body(Streaming) } }
    at /Users/weisd/.cargo/git/checkouts/s3s-58426f2d17c34859/ab139f7/crates/s3s/src/service.rs:81
    in s3s::service::call with start_time: 2025-03-11 6:18:50.011550933 +00:00:00

  2025-03-11T06:18:50.011603Z DEBUG s3s::ops: parsing path-style request, decoded_uri_path: "/rustfs/rpc/put_file_stream"
    at /Users/weisd/.cargo/git/checkouts/s3s-58426f2d17c34859/ab139f7/crates/s3s/src/ops/mod.rs:266
    in s3s::service::call with start_time: 2025-03-11 6:18:50.011550933 +00:00:00

  2025-03-11T06:18:50.011651Z DEBUG s3s::ops: body_changed: false, decoded_content_length: None, has_multipart: false
    at /Users/weisd/.cargo/git/checkouts/s3s-58426f2d17c34859/ab139f7/crates/s3s/src/ops/mod.rs:342
    in s3s::service::call with start_time: 2025-03-11 6:18:50.011550933 +00:00:00

  2025-03-11T06:18:50.011687Z  WARN rustfs::admin::rpc: handle PutFile
    at rustfs/src/admin/rpc.rs:120
    in s3s::service::call with start_time: 2025-03-11 6:18:50.011550933 +00:00:00

  2025-03-11T06:18:50.011716Z DEBUG s3s::ops: custom route returns error, err: S3Error(Inner { code: InvalidArgument, message: Some("get query failed1 Error(\"missing field `size`\")"), request_id: None, status_code: None, source: None, headers: None })
    at /Users/weisd/.cargo/git/checkouts/s3s-58426f2d17c34859/ab139f7/crates/s3s/src/ops/mod.rs:227
    in s3s::service::call with start_time: 2025-03-11 6:18:50.011550933 +00:00:00

  2025-03-11T06:18:50.011751Z DEBUG s3s::service: res: Response { status: 400, version: HTTP/1.1, headers: {"content-type": "application/xml"}, body: Body { once: b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>get query failed1 Error(&quot;missing field `size`&quot;)</Message></Error>" } }