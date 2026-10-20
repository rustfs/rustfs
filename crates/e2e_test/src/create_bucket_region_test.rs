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

//! Regression test for rustfs/backlog#629(b): region-aware `CreateBucket`.
//!
//! `minio-go`'s `MakeBucket(bucket, "us-east-1")` was reported to fail SigV4
//! validation. The SigV4 signature is verified by `s3s` over the raw request
//! body bytes (compared against `x-amz-content-sha256`); a
//! `CreateBucketConfiguration`/`LocationConstraint` body therefore does not
//! change signature canonicalization. This test proves that both a
//! region-body `CreateBucket` and a plain `CreateBucket` succeed under SigV4.

#[cfg(test)]
mod tests {
    use crate::common::{
        AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
        build_test_s3_config, build_test_sts_client, init_logging, local_http_client, signed_s3_request_with_headers,
    };
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::Region;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
    use std::error::Error;

    /// `CreateBucket` with a `LocationConstraint` body must pass SigV4 validation
    /// and create the bucket, mirroring `minio-go` `MakeBucket(bucket, "us-east-1")`.
    #[tokio::test]
    async fn test_create_bucket_with_us_east_1_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        let client = env.create_s3_client();

        client
            .create_bucket()
            .bucket("region-aware-bucket")
            .create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from("us-east-1"))
                    .build(),
            )
            .send()
            .await
            .expect("region-aware CreateBucket must pass SigV4 validation and succeed");

        // The bucket exists and is listed.
        let listed = client.list_buckets().send().await?;
        let names: Vec<&str> = listed.buckets().iter().filter_map(|b| b.name()).collect();
        assert!(names.contains(&"region-aware-bucket"), "created bucket must be listed: {names:?}");

        env.stop_server();
        Ok(())
    }

    /// A plain `CreateBucket` (no body) must also succeed; guards against a
    /// regression where an empty body would be hashed incorrectly during SigV4.
    #[tokio::test]
    async fn test_create_bucket_without_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        let client = env.create_s3_client();

        client
            .create_bucket()
            .bucket("plain-region-bucket")
            .send()
            .await
            .expect("plain CreateBucket must succeed");

        env.stop_server();
        Ok(())
    }

    async fn location_policy_client(
        env: &RustFSTestEnvironment,
        user: &str,
        statements: serde_json::Value,
    ) -> Result<Client, Box<dyn Error + Send + Sync>> {
        let secret = "location-policy-test-secret";
        admin_create_user(env, user, secret).await?;
        let policy = serde_json::json!({"Version":"2012-10-17", "Statement":statements});
        admin_add_canned_policy_via(
            AdminTransport::Signed,
            &env.url,
            &env.access_key,
            &env.secret_key,
            user,
            &policy.to_string(),
        )
        .await?;
        admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, user, user).await?;
        Ok(Client::from_conf(
            build_test_s3_config(&env.url, user, secret, None, "location-policy")
                .to_builder()
                .region(Region::new("eu-west-1"))
                .build(),
        ))
    }

    #[tokio::test]
    async fn test_create_bucket_location_constraint_policy_allow() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec!["--region", "eu-west-1"]).await?;
        let root = Client::from_conf(
            env.create_s3_client()
                .config()
                .to_builder()
                .region(Region::new("eu-west-1"))
                .build(),
        );
        let configuration = |region: &str| {
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region))
                .build()
        };

        for (user, condition, accepts_region, accepts_alias, accepts_missing) in [
            (
                "location-allow",
                serde_json::json!({"StringEquals":{"s3:LocationConstraint":"eu-west-1"}}),
                true,
                false,
                false,
            ),
            (
                "location-alias",
                serde_json::json!({"StringEquals":{"s3:LocationConstraint":"EU"}}),
                false,
                true,
                false,
            ),
            (
                "location-null",
                serde_json::json!({"Null":{"s3:LocationConstraint":"true"}}),
                false,
                false,
                true,
            ),
        ] {
            let client = location_policy_client(
                &env,
                user,
                serde_json::json!([{
                    "Effect":"Allow", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*", "Condition":condition,
                }]),
            )
            .await?;
            for (index, (config, allowed)) in [
                (Some(configuration("eu-west-1")), accepts_region),
                (Some(configuration("EU")), accepts_alias),
                (None, accepts_missing),
                (Some(CreateBucketConfiguration::builder().build()), accepts_missing),
                (Some(configuration("")), accepts_missing),
            ]
            .into_iter()
            .enumerate()
            {
                let bucket = format!("{user}-{index}");
                let result = client
                    .create_bucket()
                    .bucket(&bucket)
                    .set_create_bucket_configuration(config)
                    .send()
                    .await;
                if allowed {
                    result.unwrap_or_else(|error| panic!("{bucket} should be allowed: {error:?}"));
                    root.head_bucket().bucket(&bucket).send().await?;
                } else {
                    let denied = result.expect_err("the requested XML constraint must satisfy the conditional Allow");
                    assert_eq!(denied.raw_response().map(|response| response.status().as_u16()), Some(403), "{bucket}");
                    assert_eq!(
                        denied.as_service_error().and_then(ProvideErrorMetadata::code),
                        Some("AccessDenied"),
                        "{bucket}"
                    );
                    let absent = root
                        .head_bucket()
                        .bucket(&bucket)
                        .send()
                        .await
                        .expect_err("denied bucket must not be created");
                    assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
                }
            }
        }
        let user = "location-session";
        location_policy_client(
            &env,
            user,
            serde_json::json!([{
                "Effect":"Allow", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*"
            }]),
        )
        .await?;
        let session_policy = serde_json::json!({"Version":"2012-10-17", "Statement":[
            {"Effect":"Allow", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*"},
            {"Effect":"Deny", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*",
             "Condition":{"StringEquals":{"s3:LocationConstraint":"eu-west-1"}}}
        ]});
        let assumed = build_test_sts_client(&env.url, user, "location-policy-test-secret", None, "location-policy-sts")
            .assume_role()
            .role_arn("arn:aws:iam::123456789012:role/location-policy")
            .role_session_name("location-policy-session")
            .policy(session_policy.to_string())
            .send()
            .await?;
        let credentials = assumed.credentials().expect("AssumeRole returns temporary credentials");
        let session = Client::from_conf(
            build_test_s3_config(
                &env.url,
                credentials.access_key_id(),
                credentials.secret_access_key(),
                Some(credentials.session_token()),
                "location-policy-session",
            )
            .to_builder()
            .region(Region::new("eu-west-1"))
            .build(),
        );
        let denied = session
            .create_bucket()
            .bucket("location-session-denied")
            .create_bucket_configuration(configuration("eu-west-1"))
            .send()
            .await
            .expect_err("the STS session's conditional Deny must be enforced");
        assert_eq!(denied.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));
        let absent = root
            .head_bucket()
            .bucket("location-session-denied")
            .send()
            .await
            .expect_err("session denial must not create a bucket");
        assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
        session.create_bucket().bucket("location-session-allowed").send().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_bucket_location_constraint_policy_deny() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec!["--region", "eu-west-1"]).await?;
        let root = Client::from_conf(
            env.create_s3_client()
                .config()
                .to_builder()
                .region(Region::new("eu-west-1"))
                .build(),
        );
        let user = "location-deny";
        let client = location_policy_client(
            &env,
            user,
            serde_json::json!([
                {"Effect":"Allow", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*"},
                {"Effect":"Deny", "Action":"s3:CreateBucket", "Resource":"arn:aws:s3:::location-*",
                 "Condition":{"StringEquals":{"s3:LocationConstraint":"eu-west-1"}}},
            ]),
        )
        .await?;
        let denied = client
            .create_bucket()
            .bucket("location-deny-matching")
            .create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from("eu-west-1"))
                    .build(),
            )
            .send()
            .await
            .expect_err("a matching explicit Deny must override the unconditional Allow");
        assert_eq!(denied.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));
        assert_eq!(denied.raw_response().map(|response| response.status().as_u16()), Some(403));
        let absent = root
            .head_bucket()
            .bucket("location-deny-matching")
            .send()
            .await
            .expect_err("denied bucket must not exist");
        assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
        client.create_bucket().bucket("location-deny-missing").send().await?;

        let xml = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>"#;
        let mut headers = http::HeaderMap::new();
        headers.insert("locationconstraint", http::HeaderValue::from_static("us-east-1"));
        for (index, body) in [xml.to_owned(), xml.replace("eu-west-1", "eu-west-&#49;")]
            .into_iter()
            .enumerate()
        {
            let bucket = format!("location-deny-spoofed-{index}");
            let spoofed = signed_s3_request_with_headers(
                http::Method::PUT,
                &format!("{}/{bucket}?LocationConstraint=us-east-1", env.url),
                Some(body),
                Some("application/xml"),
                user,
                "location-policy-test-secret",
                &headers,
            )
            .await?;
            assert_eq!(spoofed.status(), reqwest::StatusCode::FORBIDDEN);
            assert!(spoofed.text().await?.contains("<Code>AccessDenied</Code>"));
            let absent = root
                .head_bucket()
                .bucket(&bucket)
                .send()
                .await
                .expect_err("spoofed conditions must not create a bucket");
            assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
        }

        let bucket = "location-anonymous";
        root.create_bucket().bucket(bucket).send().await?;
        for (effect, match_status, missing_status) in [
            ("Allow", reqwest::StatusCode::CONFLICT, reqwest::StatusCode::FORBIDDEN),
            ("Deny", reqwest::StatusCode::FORBIDDEN, reqwest::StatusCode::CONFLICT),
        ] {
            let mut statements = vec![];
            if effect == "Deny" {
                statements.push(serde_json::json!({"Effect":"Allow", "Principal":"*", "Action":"s3:CreateBucket", "Resource":format!("arn:aws:s3:::{bucket}")}));
            }
            statements.push(serde_json::json!({"Effect":effect, "Principal":"*", "Action":"s3:CreateBucket", "Resource":format!("arn:aws:s3:::{bucket}"),
                "Condition":{"StringEquals":{"s3:LocationConstraint":"eu-west-1"}}}));
            root.put_bucket_policy()
                .bucket(bucket)
                .policy(serde_json::json!({"Version":"2012-10-17", "Statement":statements}).to_string())
                .send()
                .await?;
            let unsigned = local_http_client();
            for (body, expected) in [(xml, match_status), ("", missing_status)] {
                let response = unsigned
                    .put(format!("{}/{bucket}", env.url))
                    .header("content-type", "application/xml")
                    .body(body)
                    .send()
                    .await?;
                assert_eq!(response.status(), expected, "anonymous {effect}, XML present={}", !body.is_empty());
                let response = response.text().await?;
                assert!(response.contains(if expected == reqwest::StatusCode::FORBIDDEN {
                    "<Code>AccessDenied</Code>"
                } else {
                    "<Code>BucketAlreadyExists</Code>"
                }));
            }
        }
        Ok(())
    }
}
