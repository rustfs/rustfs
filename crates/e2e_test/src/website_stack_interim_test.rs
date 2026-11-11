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

//! Website hosting and the `?metadata=true` listings on the gateway stack (rustfs/backlog#2734
//! T2.11, PR "W"; release blocker R-064).
//!
//! Responsible for: proving that a server on `RUSTFS_S3_STACK=gateway` answers website-host
//! requests and the two metadata listings exactly as the legacy stack does. One data directory,
//! one binary, two processes in turn: the legacy stack seeds the buckets (the gateway stack cannot
//! write them yet) and answers a fixed set of requests, then the same directory is restarted on
//! the gateway stack and answers the same requests. Each answer is compared on status, `Location`,
//! `Content-Type`, `Vary`, the CORS headers and the body bytes. The legacy answers are also held
//! to what a website must answer, so that two stacks failing the same way cannot pass, and one
//! request the gateway stack must NOT hand to legacy is held to differ, so that a restart that
//! silently kept the legacy stack cannot pass either.
//! NOT responsible for: the comparison against the released 1.0.5 binary
//! (`.config/reference-binary.toml`); this harness has no reference side yet, so what is proved
//! here is gateway == legacy of the candidate, not candidate == 1.0.5.
//! Upstream: the routes under test are `rustfs/src/server/stack/{website,metadata}.rs`.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, admin_ok, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CorsConfiguration, CorsRule, ErrorDocument, IndexDocument, WebsiteConfiguration};
    use http::Method;
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use std::time::Duration;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    const WEBSITE_DOMAIN: &str = "website.test";
    const PUBLIC_BUCKET: &str = "interim-site-public";
    const PRIVATE_BUCKET: &str = "interim-site-private";
    const NO_LIST_USER: &str = "interimsitenolist";
    const NO_LIST_SECRET: &str = "interim-site-no-list-secret";
    const ORIGIN: &str = "https://app.example";
    /// The response headers compared besides status, `Location` and body.
    const COMPARED_HEADERS: [&str; 8] = [
        "content-type",
        "vary",
        "access-control-allow-origin",
        "access-control-allow-methods",
        "access-control-allow-headers",
        "access-control-expose-headers",
        "access-control-allow-credentials",
        "access-control-max-age",
    ];

    /// What one request was answered with, reduced to the compared fields.
    #[derive(Debug, PartialEq, Eq)]
    struct Answer {
        status: u16,
        location: Option<String>,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    }

    impl Answer {
        async fn of(response: reqwest::Response) -> TestResult<Self> {
            let status = response.status().as_u16();
            let location = header_text(response.headers(), "location");
            let headers = COMPARED_HEADERS
                .iter()
                .filter_map(|name| header_text(response.headers(), name).map(|value| (*name, value)))
                .collect();
            let body = response.bytes().await?.to_vec();
            Ok(Self {
                status,
                location,
                headers,
                body,
            })
        }

        fn body_text(&self) -> String {
            String::from_utf8_lossy(&self.body).into_owned()
        }
    }

    /// Every value of `name`, joined, so a second `Vary` line is a difference too.
    fn header_text(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
        let values: Vec<&str> = headers
            .get_all(name)
            .iter()
            .map(|value| value.to_str().unwrap_or("<not UTF-8>"))
            .collect();
        (!values.is_empty()).then(|| values.join(" | "))
    }

    /// A local client that reports a redirect instead of following it: the 302 and its
    /// `Location` are what is compared.
    fn no_redirect_client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("local http client")
    }

    fn website_host(bucket: &str) -> String {
        format!("{bucket}.{WEBSITE_DOMAIN}")
    }

    /// An unsigned request to `target` on this server, addressed to `host`.
    async fn anonymous(
        env: &RustFSTestEnvironment,
        method: Method,
        host: &str,
        target: &str,
        extra: &[(&str, &str)],
    ) -> TestResult<Answer> {
        let mut request = no_redirect_client()
            .request(method, format!("{}{target}", env.url))
            .header(HOST, host)
            .header("origin", ORIGIN);
        for (name, value) in extra {
            request = request.header(*name, *value);
        }
        Answer::of(request.send().await?).await
    }

    /// A SigV4 request to `target` on this server, addressed and signed for `host`.
    async fn signed(
        env: &RustFSTestEnvironment,
        host: &str,
        target: &str,
        access_key: &str,
        secret_key: &str,
    ) -> TestResult<Answer> {
        let url = format!("{}{target}", env.url);
        let request = http::Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header(HOST, host)
            .header("origin", ORIGIN)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())?;
        let signed = sign_v4(request, 0, access_key, secret_key, "", "us-east-1");
        let mut request = no_redirect_client().get(url);
        for (name, value) in signed.headers() {
            request = request.header(name, value);
        }
        Answer::of(request.send().await?).await
    }

    /// The server's own address: a path-style request, not a website one.
    fn server_host(env: &RustFSTestEnvironment) -> &str {
        &env.address
    }

    /// Writes everything the comparison reads, through the legacy stack.
    async fn seed(env: &RustFSTestEnvironment) -> TestResult {
        let s3 = env.create_s3_client();
        for bucket in [PUBLIC_BUCKET, PRIVATE_BUCKET] {
            s3.create_bucket().bucket(bucket).send().await?;
            for (key, body) in [
                ("index.html", "home"),
                ("404.html", "not here"),
                ("section/index.html", "section page"),
            ] {
                s3.put_object()
                    .bucket(bucket)
                    .key(key)
                    .content_type("text/html")
                    .metadata("colour", "blue")
                    .body(ByteStream::from(body.as_bytes().to_vec()))
                    .send()
                    .await?;
            }
            s3.put_bucket_website()
                .bucket(bucket)
                .website_configuration(
                    WebsiteConfiguration::builder()
                        .index_document(IndexDocument::builder().suffix("index.html").build()?)
                        .error_document(ErrorDocument::builder().key("404.html").build()?)
                        .build(),
                )
                .send()
                .await?;
        }
        // Only the public bucket is readable anonymously, and only by GetObject: no listing.
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{PUBLIC_BUCKET}/*")
            }]
        });
        s3.put_bucket_policy()
            .bucket(PUBLIC_BUCKET)
            .policy(policy.to_string())
            .send()
            .await?;
        s3.put_bucket_cors()
            .bucket(PUBLIC_BUCKET)
            .cors_configuration(
                CorsConfiguration::builder()
                    .cors_rules(
                        CorsRule::builder()
                            .allowed_origins(ORIGIN)
                            .allowed_methods("GET")
                            .allowed_methods("HEAD")
                            .expose_headers("ETag")
                            .build()?,
                    )
                    .build()?,
            )
            .send()
            .await?;
        // A real user whose policy grants nothing, so it may not list either bucket.
        admin_ok(
            env,
            Method::PUT,
            &format!("/rustfs/admin/v3/add-user?accessKey={NO_LIST_USER}"),
            Some(serde_json::json!({ "secretKey": NO_LIST_SECRET, "status": "enabled" }).to_string()),
        )
        .await?;
        Ok(())
    }

    /// IAM loads its users in the background after a start, so a user unknown for a moment is
    /// answered `InvalidAccessKeyId`; wait until the user without a policy is known again.
    async fn wait_until_the_user_is_loaded(env: &RustFSTestEnvironment) -> TestResult {
        let target = format!("/{PUBLIC_BUCKET}?list-type=2&metadata=true");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let answer = signed(env, server_host(env), &target, NO_LIST_USER, NO_LIST_SECRET).await?;
            if !answer.body_text().contains("InvalidAccessKeyId") {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("{NO_LIST_USER} is still unknown: {answer:?}").into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// The fixed requests, in order, each with the name the comparison reports it under.
    async fn observe(env: &RustFSTestEnvironment) -> TestResult<Vec<(&'static str, Answer)>> {
        let public = website_host(PUBLIC_BUCKET);
        let private = website_host(PRIVATE_BUCKET);
        let mut answers = Vec::new();
        wait_until_the_user_is_loaded(env).await?;

        let head = anonymous(env, Method::HEAD, &public, "/", &[]).await?;
        let etag = no_redirect_client()
            .head(format!("{}/", env.url))
            .header(HOST, &public)
            .send()
            .await?
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .ok_or("the website index document has no ETag")?;
        answers.push(("website GET /", anonymous(env, Method::GET, &public, "/", &[]).await?));
        answers.push(("website HEAD /", head));
        answers.push(("website GET directory", anonymous(env, Method::GET, &public, "/section", &[]).await?));
        answers.push(("website GET directory/", anonymous(env, Method::GET, &public, "/section/", &[]).await?));
        answers.push((
            "website GET missing key",
            anonymous(env, Method::GET, &public, "/missing.html", &[]).await?,
        ));
        answers.push((
            "website GET Range",
            anonymous(env, Method::GET, &public, "/index.html", &[("range", "bytes=0-1")]).await?,
        ));
        answers.push((
            "website GET If-None-Match",
            anonymous(env, Method::GET, &public, "/", &[("if-none-match", etag.as_str())]).await?,
        ));
        answers.push(("website POST", anonymous(env, Method::POST, &public, "/", &[]).await?));
        answers.push(("website GET bare domain", anonymous(env, Method::GET, WEBSITE_DOMAIN, "/", &[]).await?));
        answers.push(("website GET private bucket", anonymous(env, Method::GET, &private, "/", &[]).await?));
        answers.push((
            "website GET private bucket, owner-signed",
            signed(env, &private, "/", &env.access_key, &env.secret_key).await?,
        ));
        answers.push((
            "website GET public bucket, owner-signed",
            signed(env, &public, "/", &env.access_key, &env.secret_key).await?,
        ));
        let host = server_host(env);
        answers.push((
            "ListObjectsV2 metadata=true",
            signed(
                env,
                host,
                &format!("/{PUBLIC_BUCKET}?list-type=2&metadata=true"),
                &env.access_key,
                &env.secret_key,
            )
            .await?,
        ));
        answers.push((
            "ListObjectVersions metadata=true",
            signed(
                env,
                host,
                &format!("/{PUBLIC_BUCKET}?versions&metadata=true"),
                &env.access_key,
                &env.secret_key,
            )
            .await?,
        ));
        answers.push((
            "ListObjectsV2 metadata=true, no list permission",
            signed(
                env,
                host,
                &format!("/{PUBLIC_BUCKET}?list-type=2&metadata=true"),
                NO_LIST_USER,
                NO_LIST_SECRET,
            )
            .await?,
        ));
        answers.push((
            "ListObjectsV2 metadata=true, anonymous",
            anonymous(env, Method::GET, host, &format!("/{PUBLIC_BUCKET}?list-type=2&metadata=true"), &[]).await?,
        ));
        Ok(answers)
    }

    /// A plain ListObjectsV2: not a metadata listing, so the gateway stack must answer it itself.
    async fn plain_listing(env: &RustFSTestEnvironment) -> TestResult<Answer> {
        signed(
            env,
            server_host(env),
            &format!("/{PUBLIC_BUCKET}?list-type=2"),
            &env.access_key,
            &env.secret_key,
        )
        .await
    }

    fn answer<'a>(answers: &'a [(&'static str, Answer)], name: &str) -> &'a Answer {
        &answers
            .iter()
            .find(|(named, _)| *named == name)
            .unwrap_or_else(|| panic!("no answer named {name}"))
            .1
    }

    /// What a website and the metadata listings must answer, whichever stack serves them.
    fn assert_is_a_website(answers: &[(&'static str, Answer)]) {
        let get = answer(answers, "website GET /");
        assert_eq!((get.status, get.body_text().as_str()), (200, "home"), "{get:?}");
        let head = answer(answers, "website HEAD /");
        assert_eq!((head.status, head.body.len()), (200, 0), "{head:?}");
        let directory = answer(answers, "website GET directory");
        assert_eq!(
            (directory.status, directory.location.as_deref()),
            (302, Some("/section/")),
            "{directory:?}"
        );
        let section = answer(answers, "website GET directory/");
        assert_eq!((section.status, section.body_text().as_str()), (200, "section page"), "{section:?}");
        let missing = answer(answers, "website GET missing key");
        assert_eq!((missing.status, missing.body_text().as_str()), (404, "not here"), "{missing:?}");
        let range = answer(answers, "website GET Range");
        assert_eq!((range.status, range.body_text().as_str()), (206, "ho"), "{range:?}");
        let not_modified = answer(answers, "website GET If-None-Match");
        assert_eq!((not_modified.status, not_modified.body.len()), (304, 0), "{not_modified:?}");
        assert_eq!(answer(answers, "website POST").status, 405);
        assert_eq!(answer(answers, "website GET bare domain").status, 404);
        for name in ["website GET private bucket", "website GET private bucket, owner-signed"] {
            let refused = answer(answers, name);
            assert_eq!(refused.status, 403, "{name}: {refused:?}");
            assert!(!refused.body_text().contains("home"), "{name} leaked the index: {refused:?}");
        }
        let signed_public = answer(answers, "website GET public bucket, owner-signed");
        assert_eq!((signed_public.status, signed_public.body_text().as_str()), (200, "home"));
        for name in ["ListObjectsV2 metadata=true", "ListObjectVersions metadata=true"] {
            let listing = answer(answers, name);
            let body = listing.body_text();
            assert_eq!(listing.status, 200, "{name}: {body}");
            assert!(body.contains("<UserMetadata>"), "{name} lost the extension: {body}");
            assert!(body.contains("blue"), "{name} lost the user metadata value: {body}");
        }
        for name in [
            "ListObjectsV2 metadata=true, no list permission",
            "ListObjectsV2 metadata=true, anonymous",
        ] {
            let refused = answer(answers, name);
            assert_eq!(refused.status, 403, "{name}: {refused:?}");
            assert!(refused.body_text().contains("<Code>AccessDenied</Code>"), "{name}: {refused:?}");
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_answers_website_and_metadata_listing_requests_as_the_legacy_stack_does() -> TestResult {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        let legacy_env = [("RUSTFS_S3_STACK", "legacy"), ("RUSTFS_WEBSITE_DOMAINS", WEBSITE_DOMAIN)];
        let gateway_env = [("RUSTFS_S3_STACK", "gateway"), ("RUSTFS_WEBSITE_DOMAINS", WEBSITE_DOMAIN)];

        env.start_rustfs_server_with_env(vec![], &legacy_env).await?;
        seed(&env).await?;
        let legacy = observe(&env).await?;
        let legacy_plain = plain_listing(&env).await?;

        env.restart_server_preserving_data(vec![], &gateway_env).await?;
        let gateway = observe(&env).await?;
        let gateway_plain = plain_listing(&env).await?;
        env.stop_server();

        // The control: a request that is neither family is the gateway's own. Legacy lists the
        // bucket; the gateway stack does not serve ListObjectsV2 yet. Equal answers here would
        // mean the restart never reached the gateway stack, and every comparison below is void.
        assert_eq!(legacy_plain.status, 200, "{legacy_plain:?}");
        assert_eq!(gateway_plain.status, 501, "{gateway_plain:?}");

        assert_is_a_website(&legacy);
        // Every difference, not the first one: a family that regressed alone must name itself.
        let differences: Vec<String> = legacy
            .iter()
            .zip(&gateway)
            .filter(|((_, on_legacy), (_, on_gateway))| on_legacy != on_gateway)
            .map(|((name, on_legacy), (_, on_gateway))| format!("{name}:\n  legacy:  {on_legacy:?}\n  gateway: {on_gateway:?}"))
            .collect();
        assert!(
            differences.is_empty(),
            "the gateway stack answered {} of {} requests differently from the legacy stack:\n{}",
            differences.len(),
            legacy.len(),
            differences.join("\n")
        );
        assert_eq!(gateway.len(), legacy.len());
        Ok(())
    }

    /// The metadata listing on a bucket that does not exist: answered by the legacy route on both
    /// stacks, so `NoSuchBucket`, never the gateway's `NotImplemented`.
    #[tokio::test]
    async fn a_metadata_listing_of_a_missing_bucket_is_refused_by_legacy_on_the_gateway_stack() -> TestResult {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_S3_STACK", "gateway")])
            .await?;
        let host = env.address.clone();
        let missing = signed(
            &env,
            &host,
            "/interim-no-such-bucket?list-type=2&metadata=true",
            &env.access_key,
            &env.secret_key,
        )
        .await?;
        let plain = signed(&env, &host, "/interim-no-such-bucket?list-type=2", &env.access_key, &env.secret_key).await?;
        env.stop_server();

        assert_eq!(missing.status, 404, "{missing:?}");
        assert!(missing.body_text().contains("<Code>NoSuchBucket</Code>"), "{missing:?}");
        assert_eq!(plain.status, 501, "{plain:?}");
        assert!(plain.body_text().contains("<Code>NotImplemented</Code>"), "{plain:?}");
        Ok(())
    }
}
