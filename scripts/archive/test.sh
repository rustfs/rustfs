#!/usr/bin/env bash
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


mkdir test

cd test

# make_bucket
mc mb rustfs/mbmb
mc ls rustfs
mc rb rustfs/mbmb

mc mb rustfs/rb-force
mc ls rustfs
echo "123525" >> test.txt
mc cp test.txt rustfs/rb-force

mc rb --force rustfs/rb-force
rm test.txt


mc mb rustfs/dada

echo "123525" >> test.txt
mc cp test.txt rustfs/dada

mc get rustfs/dada/test.txt test2.txt

diff test.txt test2.txt

echo "33333" >> test2.txt

mc cp test2.txt rustfs/dada

mc get rustfs/dada/test2.txt test3.txt

diff test2.txt test3.txt

# list_buckets
mc ls rustfs/dada

mc mb rustfs/dada2

mc ls rustfs

mc ls rustfs/dada

mc rm rustfs/dada/test2.txt

mc ls rustfs/dada

dd if=/dev/urandom of=50M.file bs=1m count=50

mc cp 50M.file rustfs/dada

mc ls rustfs/dada


mc get rustfs/dada/50M.file 50m.file.d

diff 50M.file 50m.file.d

mc rm rustfs/dada/50M.file
mc ls rustfs/dada

rm test.txt  test2.txt test3.txt
rm 50M.file 50m.file.d


# object_tags
echo "33333" >> tags.txt
mc cp tags.txt rustfs/dada
mc tag list rustfs/dada/tags.txt
mc tag set rustfs/dada/tags.txt "key1=value1&key2=value2"
mc tag list rustfs/dada/tags.txt
mc tag remove rustfs/dada/tags.txt
mc tag list rustfs/dada/tags.txt
rm tags.txt

# bucket_tags
mc tag list rustfs/dada
mc tag set rustfs/dada "a=b&b=c&dada=yes&yy=11&77=99&99=23&11=11"
mc tag list rustfs/dada
mc tag remove rustfs/dada
mc tag list rustfs/dada


# bucket_versioning
mc version info rustfs/dada
mc version enable rustfs/dada
mc version info rustfs/dada
mc version suspend rustfs/dada
mc version info rustfs/dada

# bucket_policy
mc anonymous get  rustfs/dada
mc anonymous set public rustfs/dada
mc anonymous set upload rustfs/dada
mc anonymous set download rustfs/dada
mc anonymous list rustfs/dada

# lifecycle
mc ilm ls rustfs/dada
mc ilm rule add --expire-days 90 --noncurrent-expire-days 30  rustfs/dada
mc ilm ls rustfs/dada

# bucket_encryption
mc encrypt info rustfs/dada
mc encrypt set sse-kms rustfs-encryption-key rustfs/dada
mc encrypt info rustfs/dada
mc encrypt clear rustfs/dada

# object_lock_config
mc mb --with-lock rustfs/lock
mc retention info --default rustfs/lock
mc retention set --default GOVERNANCE "30d" rustfs/lock
mc retention info --default rustfs/lock
mc retention clear --default rustfs/lock
mc rb rustfs/lock

# bucket_notification
mc event list rustfs/dada
mc event add --event "put,delete" rustfs/dada arn:aws:sqs::primary:target
mc event list rustfs/dada
mc event rm --event "put,delete" rustfs/dada arn:aws:sqs::primary:target
mc event list rustfs/dada

# bucket_quota ? admin/v3/get-bucket-quota
# mc quota info rustfs/dada
# bucket_target ?
