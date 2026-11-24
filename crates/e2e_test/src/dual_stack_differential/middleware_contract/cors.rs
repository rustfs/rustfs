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

//! Browser preflight and actual CORS responses from a bucket with a stored rule.
use super::{BoxError, RustFSTestEnvironment};
use crate::common::signed_request_with_client;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CorsConfiguration, CorsRule},
};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode,
    header::{ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN},
};
use std::{net::SocketAddr, time::Duration};

const ORIGIN_VALUE: &str = "https://app.example.com";
const KEY: &str = "cors-object.txt";
const BODY: &[u8] = b"middleware cors object";

fn assert_allowed_origin(headers: &HeaderMap, expected: Option<&str>) -> Result<(), BoxError> {
    let mut origins = headers.get_all(ACCESS_CONTROL_ALLOW_ORIGIN).iter();
    assert_eq!(origins.next().map(HeaderValue::to_str).transpose()?, expected, "CORS allow-origin");
    assert!(origins.next().is_none(), "duplicate CORS allow-origin header");
    Ok(())
}

pub(super) async fn check(env: &RustFSTestEnvironment, bucket: &str) -> Result<(), BoxError> {
    let s3 = env.create_s3_client();
    let rule = CorsRule::builder()
        .allowed_origins(ORIGIN_VALUE)
        .allowed_methods("GET")
        .allowed_methods("HEAD")
        .build()?;
    s3.put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(CorsConfiguration::builder().cors_rules(rule).build()?)
        .send()
        .await?;
    s3.put_object()
        .bucket(bucket)
        .key(KEY)
        .body(ByteStream::from_static(BODY))
        .send()
        .await?;

    let address: SocketAddr = env.address.parse()?;
    assert!(address.ip().is_loopback(), "CORS fixture must run on loopback");
    let host = format!("{bucket}.s3.test");
    let client = || {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .resolve(&host, address)
    };
    let http = client().build()?;
    let preflight = http
        .request(Method::OPTIONS, format!("http://{host}:{}/{KEY}", address.port()))
        .header(ORIGIN, ORIGIN_VALUE)
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .send()
        .await?;
    assert_eq!(preflight.status(), StatusCode::OK, "virtual-hosted CORS preflight");
    assert_allowed_origin(preflight.headers(), Some(ORIGIN_VALUE))?;
    assert!(
        preflight
            .headers()
            .get_all(ACCESS_CONTROL_ALLOW_METHODS)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|method| method.trim() == "GET"),
        "preflight must allow GET"
    );

    let mut headers = HeaderMap::new();
    headers.insert(ORIGIN, HeaderValue::from_static(ORIGIN_VALUE));
    let browser = client().default_headers(headers).build()?;
    let url = format!("{}/{bucket}/{KEY}", env.url);
    for (client, origin) in [(&browser, Some(ORIGIN_VALUE)), (&http, None)] {
        let response =
            signed_request_with_client(client, Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;
        assert_eq!(response.status(), StatusCode::OK, "CORS object GET");
        assert_allowed_origin(response.headers(), origin)?;
        assert_eq!(response.bytes().await?.as_ref(), BODY, "CORS object GET payload");
    }
    Ok(())
}
