// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(feature = "test-util")]

mod storage_api;

use std::sync::Arc;
use storage_api::contract_compat::StorageListOperations;
use storage_api::metadata_lock::{
    BucketOperations, Error, MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader, init_bucket_metadata_sys,
    isolated_store_over_temp_disks,
};

#[tokio::test]
async fn double_slash_list_prefix_is_literal_and_empty() {
    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
    let bucket = "list-double-slash-prefix";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let key = "foo/foo/base/backup";
    let mut data = PutObjReader::from_vec(b"backup".to_vec());
    store
        .put_object(bucket, key, &mut data, &ObjectOptions::default())
        .await
        .expect("write control object");

    for prefix in ["foo//foo/base/", "zzz//yyy", "foo///", "//foo/", "/foo/"] {
        for delimiter in [None, Some("/".to_owned())] {
            for max in [0, 1, 100] {
                for marker in [None, Some(key.to_owned())] {
                    let page = store
                        .clone()
                        .list_objects_v2(bucket, prefix, marker.clone(), delimiter.clone(), max, false, None, false)
                        .await
                        .expect("a literal prefix with no stored matches is a valid list request");
                    assert!(page.objects.is_empty(), "{prefix} must not alias {key}");
                    assert!(page.prefixes.is_empty(), "{prefix} must not publish normalized prefixes");
                    assert!(!page.is_truncated);
                    assert!(page.next_continuation_token.is_none());
                    assert_eq!(page.continuation_token, marker);
                }
            }
        }
    }
    let oversized = format!("foo//{}", "x".repeat(32 << 10));
    for prefix in ["foo//../secret", "foo//./secret", "foo//bad\0name", &oversized] {
        let err = store
            .clone()
            .list_objects_v2(bucket, prefix, None, None, 100, false, None, false)
            .await
            .expect_err("invalid prefixes must still be rejected");
        assert!(matches!(err, Error::ObjectNameInvalid(_, _)), "{err:?}");
    }
    let err = store
        .clone()
        .list_objects_v2("invalid_bucket", "foo//bar", None, None, 100, false, None, false)
        .await
        .expect_err("invalid buckets must still be rejected");
    assert!(matches!(err, Error::BucketNameInvalid(_)), "{err:?}");

    let control = store
        .clone()
        .list_objects_v2(bucket, "foo/", None, None, 100, false, None, false)
        .await
        .expect("normal prefix still lists the control object");
    assert_eq!(control.objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), [key]);
}
