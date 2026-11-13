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

//! Differential cases of the `bucket_core` batch (see `super`).

use super::super::{Recorder, none};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};

/// Names RustFS creates although the AWS grammar reserves them, and one it refuses.
const RESERVED_NAMES: [(&str, &str); 5] = [
    ("reserved-sthree", "sthree-dual-stack"),
    ("reserved-s3alias", "dual-stack-s3alias"),
    ("reserved-ol-s3", "dual-stack--ol-s3"),
    ("reserved-x-s3", "dual-stack--x-s3"),
    ("reserved-xn", "xn--dual-stack"),
];

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    let located = |bucket: &str, location: &str| {
        let configuration = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(location))
            .build();
        c.create_bucket().bucket(bucket).create_bucket_configuration(configuration)
    };
    s.step(
        "CreateBucket",
        "foreign-location",
        located("dual-stack-foreign", "ap-south-1").send(),
        none,
    )
    .await;
    s.step("CreateBucket", "eu-alias", located("dual-stack-eu", "EU").send(), none)
        .await;
    for (case, name) in RESERVED_NAMES {
        s.step("CreateBucket", case, c.create_bucket().bucket(name).send(), none)
            .await;
    }
}
