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

//! Raw HTTP regression coverage for the Select request root alias (backlog#1626).

use crate::common::{RustFSTestEnvironment, signed_s3_request};
use aws_sdk_s3::primitives::ByteStream;
use http::Method;
use std::error::Error;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CSV_BODY: &[u8] = b"name\nGatewayJ-root-alias\nignored\n";
const EXPECTED_RECORD: &[u8] = b"GatewayJ-root-alias";

fn select_request(root: &str) -> String {
    format!(
        r#"<{root} xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Expression>SELECT s.name FROM S3Object s WHERE s.name = 'GatewayJ-root-alias'</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</{root}>"#
    )
}

async fn raw_select(env: &RustFSTestEnvironment, bucket: &str, object: &str, root: &str) -> TestResult {
    let response = signed_s3_request(
        Method::POST,
        &format!("{}/{bucket}/{object}?select&select-type=2", env.url),
        Some(select_request(root)),
        Some("application/xml"),
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    let status = response.status();
    let body = response.bytes().await?.to_vec();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "{root} root was rejected: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        body.windows(EXPECTED_RECORD.len()).any(|window| window == EXPECTED_RECORD),
        "{root} root did not return the projected record"
    );
    Ok(())
}

#[tokio::test]
async fn select_request_root_alias_reaches_select_endpoint() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let client = env.create_s3_client();
    let bucket = format!("select-root-{}", Uuid::new_v4().simple());
    let object = "input.csv";

    client.create_bucket().bucket(&bucket).send().await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(object)
        .body(ByteStream::from_static(CSV_BODY))
        .send()
        .await?;

    raw_select(&env, &bucket, object, "SelectObjectContentRequest").await?;
    raw_select(&env, &bucket, object, "SelectRequest").await?;
    Ok(())
}
