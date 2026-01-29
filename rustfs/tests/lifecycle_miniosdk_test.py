# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta
import uuid
from fastapi import UploadFile
from minio import Minio
from minio.commonconfig import Filter
from minio.error import S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule
import os
from collections import namedtuple

Settings = namedtuple("Settings", ['oss_endpoint',
    'oss_access_key',
    'oss_secret_key',
    'oss_bucket_name',
    'oss_lifecycle_days',
    'oss_secure',
    'oss_region'])
settings = Settings(
    oss_endpoint=os.getenv("TEST_RUSTFS_SERVER", "localhost:9000"),
    oss_access_key = "rustfsadmin",
    oss_secret_key = "rustfsadmin",
    oss_bucket_name = "mblock99",
    oss_lifecycle_days = 1,
    oss_secure = False,
    oss_region = ""
)

class OSS:
    def __init__(self):
        self.bucket_name = settings.oss_bucket_name
        self.lifecycle_days = settings.oss_lifecycle_days
        self.client = Minio(
            endpoint=settings.oss_endpoint,
            access_key=settings.oss_access_key,
            secret_key=settings.oss_secret_key,
            secure=settings.oss_secure,
            region=settings.oss_region,
        )

    def new_uuid(self):
        return str(uuid.uuid4())

    def create_bucket(self):
        found = self.client.bucket_exists(self.bucket_name)
        if not found:
            try:
                self.client.make_bucket(self.bucket_name)
                self.set_lifecycle_expiration(days=self.lifecycle_days)
                print(f"Bucket {self.bucket_name} created successfully")
            except S3Error as exc:
                if exc.code == "BucketAlreadyOwnedByYou":
                    print(f"Bucket {self.bucket_name} already owned by you; continuing")
                else:
                    raise
        else:
            print(f"Bucket {self.bucket_name} already exists")

    def set_lifecycle_expiration(self,days: int = 1, prefix: str = "") -> None:
        """
        设置按天自动过期删除（MinIO 按天、每天巡检一次；<1天不生效）
        """
        rule_filter = Filter(prefix=prefix or "")
        rule = Rule(
            rule_id=f"expire-{prefix or 'all'}-{days}d",
            status="Enabled",
            rule_filter=rule_filter,
            expiration=Expiration(days=int(days)),
        )
        cfg = LifecycleConfig([rule])
        self.client.set_bucket_lifecycle(self.bucket_name, cfg)

    def upload_file(self, file: UploadFile):
        """
        上传文件到OSS，返回文件的UUID
        """
        ext = os.path.splitext(file.filename)[1]
        uuid = self.new_uuid()
        filename = f'{uuid}{ext}'
        file.file.seek(0)
        self.client.put_object(
            self.bucket_name, 
            filename, 
            file.file, 
            length=-1, 
            part_size=10*1024*1024,
        )
        return filename

    def get_presigned_url(self, filename: str):
        """
        获取文件的预签名URL，用于下载文件
        """
        return self.client.presigned_get_object(
            self.bucket_name, filename, expires=timedelta(days=self.lifecycle_days)
        )
        

def get_oss():
    """
    获取OSS实例
    """
    return OSS()

if __name__ == "__main__":
    oss = get_oss()
    oss.create_bucket()
