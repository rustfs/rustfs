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

//! Differential cases of the `object_meta` batch (see `super`).

use super::super::{Recorder, grants, none, tags};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{AccessControlPolicy, Grant, Grantee, Owner, Permission, Type};

const BUCKET: &str = "diff-object-meta";
const TAGGED: &str = "tagged";

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    s.step("CreateBucket", "object-meta", c.create_bucket().bucket(BUCKET).send(), none)
        .await;
    let put = c
        .put_object()
        .bucket(BUCKET)
        .key(TAGGED)
        .tagging("k=&odd%20key=v%2Fw")
        .body(ByteStream::from_static(b"object meta"));
    s.step("PutObject", "object-meta-tagged", put.send(), none).await;

    let get_tags = |key: &str, version: Option<&str>| {
        c.get_object_tagging()
            .bucket(BUCKET)
            .key(key)
            .set_version_id(version.map(str::to_owned))
    };
    s.step("GetObjectTagging", "empty-and-encoded-tags", get_tags(TAGGED, None).send(), |o| {
        vec![("tags", tags(o.tag_set())), ("version", format!("{:?}", o.version_id()))]
    })
    .await;
    s.step(
        "GetObjectTagging",
        "malformed-version",
        get_tags(TAGGED, Some("not-a-version")).send(),
        none,
    )
    .await;
    let absent_version = "00000000-0000-0000-0000-000000000001";
    s.step("GetObjectTagging", "absent-version", get_tags(TAGGED, Some(absent_version)).send(), none)
        .await;
    s.step("GetObjectTagging", "nul-key", get_tags("a\0b", None).send(), none)
        .await;
    s.step(
        "DeleteObjectTagging",
        "nul-key",
        c.delete_object_tagging().bucket(BUCKET).key("a\0b").send(),
        none,
    )
    .await;
    s.step(
        "DeleteObjectTagging",
        "then-empty",
        c.delete_object_tagging().bucket(BUCKET).key(TAGGED).send(),
        none,
    )
    .await;
    s.step("GetObjectTagging", "after-delete", get_tags(TAGGED, None).send(), |o| {
        vec![("tags", tags(o.tag_set()))]
    })
    .await;

    let grantee = Grantee::builder()
        .r#type(Type::CanonicalUser)
        .id("owner")
        .build()
        .expect("grantee");
    let policy = AccessControlPolicy::builder()
        .owner(Owner::builder().id("owner").build())
        .grants(Grant::builder().grantee(grantee).permission(Permission::Read).build())
        .build();
    let xml_grants = c.put_object_acl().bucket(BUCKET).key(TAGGED).access_control_policy(policy);
    s.step("PutObjectAcl", "xml-grants", xml_grants.send(), none).await;
    s.step(
        "GetObjectAcl",
        "after-xml-grants",
        c.get_object_acl().bucket(BUCKET).key(TAGGED).send(),
        |o| vec![("grants", grants(o.grants()))],
    )
    .await;
    s.step("GetObjectAcl", "nul-key", c.get_object_acl().bucket(BUCKET).key("a\0b").send(), none)
        .await;

    s.step(
        "GetObjectLegalHold",
        "no-object-lock",
        c.get_object_legal_hold().bucket(BUCKET).key(TAGGED).send(),
        none,
    )
    .await;
    s.step(
        "GetObjectRetention",
        "no-object-lock",
        c.get_object_retention().bucket(BUCKET).key(TAGGED).send(),
        none,
    )
    .await;
    s.step(
        "GetObjectTorrent",
        "nul-key",
        c.get_object_torrent().bucket(BUCKET).key("a\0b").send(),
        none,
    )
    .await;
}
