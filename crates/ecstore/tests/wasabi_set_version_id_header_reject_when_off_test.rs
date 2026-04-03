// Copyright 2024 RustFS Team
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

//! Separate binary: `LazyLock` for `RUSTFS_WASABI_VERSION_IDS` must read `false` on first use.

use http::{HeaderMap, HeaderValue};
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::{WASABI_SET_VERSION_ID_HEADER, ensure_wasabi_set_version_id_header_allowed};
use temp_env::with_var;

#[test]
fn rejects_non_empty_wasabi_set_version_id_when_mode_off() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("false"), || {
        let mut headers = HeaderMap::new();
        headers.insert(WASABI_SET_VERSION_ID_HEADER, HeaderValue::from_static("01234567890123456789012345678901"));
        let err = ensure_wasabi_set_version_id_header_allowed(&headers, "b", "k").unwrap_err();
        match err {
            StorageError::InvalidArgument(bucket, object, msg) => {
                assert_eq!(bucket, "b");
                assert_eq!(object, "k");
                assert!(msg.contains("X-Wasabi-Set-Version-Id"), "message should mention header: {msg}");
            }
            _ => panic!("expected InvalidArgument, got {err:?}"),
        }
    });
}

#[test]
fn allows_empty_or_whitespace_wasabi_set_version_id_when_mode_off() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("false"), || {
        let mut headers = HeaderMap::new();
        headers.insert(WASABI_SET_VERSION_ID_HEADER, HeaderValue::from_static("   "));
        ensure_wasabi_set_version_id_header_allowed(&headers, "b", "k").unwrap();
    });
}

#[test]
fn allows_absent_header_when_mode_off() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("false"), || {
        let headers = HeaderMap::new();
        ensure_wasabi_set_version_id_header_allowed(&headers, "b", "k").unwrap();
    });
}
