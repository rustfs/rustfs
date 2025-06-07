启动两个rustfs
rustfs --address 0.0.0.0:9000 /rustfs-data9000
rustfs --address 0.0.0.0:9001 /rustfs-data9001


### 使用 minio mc 设置 alias 分别为 rustfs 和 rustfs2


### 创建 bucket
mc mb rustfs/srcbucket

### 创建 desc bucket

mc mb rustfs2/destbucket



### 开启版本控制

mc version enable rustfs/srcbucket
mc version enable rustfs2/destbucket

#### 使用修改过的 mc 才能 add bucket replication

./mc replication add rustfs/srcbucket --remote-bucket rustfs2/destbucket



###### 复制一个小文件；
mc cp ./1.txt rustfs/srcbucket

###### 查看是否成功
mc ls --versions rustfs/srcbucket/1.txt
mc ls --versions rustfs/destbucket/1.txt


##### 复制一个大文件
1 创建一个大文件
dd if=/dev/zero of=./dd.out bs=4096000 count=1000

mc cp ./dd.out rustfs/srcbucket/

##### 查看是否成功
mc ls --versions rustfs/srcbucket/dd.out
mc ls --versions rustfs2/destbucket/dd.out
