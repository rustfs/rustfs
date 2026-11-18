// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use rustfs_storage_api::multipart::{ListPartsInfo, ListPartsOwner, MultipartInfo, MultipartOwner};
use std::any::TypeId;
use std::collections::HashMap;

trait DownstreamOwner {
    fn kind(&self) -> &'static str;
}

impl DownstreamOwner for ListPartsOwner {
    fn kind(&self) -> &'static str {
        "list-parts"
    }
}

impl DownstreamOwner for MultipartOwner {
    fn kind(&self) -> &'static str {
        "multipart"
    }
}

#[test]
fn multipart_owner_types_remain_distinct_for_downstream_implementations() {
    let list_owner = ListPartsOwner {
        bucket: "bucket".to_owned(),
        object: "key".to_owned(),
    };
    let multipart_owner = MultipartOwner {
        bucket: "bucket".to_owned(),
        object: "key".to_owned(),
    };
    assert_eq!(list_owner.kind(), "list-parts");
    assert_eq!(multipart_owner.kind(), "multipart");
    assert_ne!(TypeId::of::<ListPartsOwner>(), TypeId::of::<MultipartOwner>());
    let page = ListPartsInfo {
        recorded_owner: Some(list_owner.clone()),
        ..Default::default()
    };
    assert_eq!(page.recorded_owner, Some(list_owner));
}

#[test]
fn multipart_info_retains_exhaustive_literal_and_pattern() {
    let info = MultipartInfo {
        bucket: "bucket".to_owned(),
        object: "key".to_owned(),
        upload_id: "upload".to_owned(),
        initiated: None,
        user_defined: HashMap::new(),
    };
    let MultipartInfo {
        bucket,
        object,
        upload_id,
        initiated,
        user_defined,
    } = info;
    assert_eq!((bucket.as_str(), object.as_str(), upload_id.as_str()), ("bucket", "key", "upload"));
    assert!(initiated.is_none());
    assert!(user_defined.is_empty());
}
