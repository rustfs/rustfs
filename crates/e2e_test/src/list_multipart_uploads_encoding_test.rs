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

//! Raw HTTP/XML regression coverage for ListMultipartUploads response encoding.

use crate::common::{RustFSTestEnvironment, signed_s3_request};
use http::{Method, StatusCode};
use std::error::Error;
use std::time::Duration;
use tokio::time::timeout;

async fn list_uploads_xml(
    env: &RustFSTestEnvironment,
    bucket: &str,
    query: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?uploads&{query}", env.url);
    let response = timeout(
        Duration::from_secs(30),
        signed_s3_request(Method::GET, &url, None, None, &env.access_key, &env.secret_key),
    )
    .await??;
    let status = response.status();
    let xml = timeout(Duration::from_secs(30), response.text()).await??;
    assert_eq!(status, StatusCode::OK, "ListMultipartUploads response: {xml}");
    Ok(xml)
}

fn assert_xml_element(xml: &str, name: &str, value: &str) {
    let element = format!("<{name}>{value}</{name}>");
    assert!(xml.contains(&element), "missing {element} in response: {xml}");
}

#[tokio::test]
async fn test_list_multipart_uploads_url_encoding() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let client = env.create_s3_client();
    let bucket = "multipart-url-encoding";
    client.create_bucket().bucket(bucket).send().await?;
    let upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("dir a/file+b")
        .send()
        .await?;
    let upload_id = upload.upload_id().expect("created multipart upload has an ID");

    let query = "prefix=dir%20a%2F&key-marker=dir%20a%2F";
    let plain = list_uploads_xml(&env, bucket, query).await?;
    assert!(!plain.contains("<EncodingType>"), "unexpected encoding in response: {plain}");
    assert_xml_element(&plain, "Prefix", "dir a/");
    assert_xml_element(&plain, "KeyMarker", "dir a/");
    assert_xml_element(&plain, "Key", "dir a/file+b");
    assert_xml_element(&plain, "UploadId", upload_id);

    let encoded = list_uploads_xml(&env, bucket, &format!("{query}&encoding-type=url")).await?;
    assert_xml_element(&encoded, "EncodingType", "url");
    assert_xml_element(&encoded, "Prefix", "dir%20a/");
    assert_xml_element(&encoded, "KeyMarker", "dir%20a/");
    assert_xml_element(&encoded, "Key", "dir%20a/file%2Bb");
    assert_xml_element(&encoded, "UploadId", upload_id);

    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("dir a/file+b")
        .upload_id(upload_id)
        .send()
        .await?;
    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

fn xml_element_value<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let (_, content) = xml.split_once(&format!("<{name}>"))?;
    let (value, _) = content.split_once(&format!("</{name}>"))?;
    Some(value)
}

fn next_page_query(xml: &str, url_encoded: bool) -> String {
    let marker = xml_element_value(xml, "NextKeyMarker").expect("truncated response has a next key marker");
    let marker = if url_encoded {
        urlencoding::decode(marker)
            .expect("response marker is valid URL encoding")
            .into_owned()
    } else {
        marker.to_owned()
    };
    let mut query = format!("key-marker={}", urlencoding::encode(&marker));
    if let Some(upload_id) = xml_element_value(xml, "NextUploadIdMarker") {
        query.push_str(&format!("&upload-id-marker={}", urlencoding::encode(upload_id)));
    }
    query
}

#[tokio::test]
async fn test_list_multipart_uploads_encoded_pagination() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let client = env.create_s3_client();
    let bucket = "multipart-encoded-pagination";
    client.create_bucket().bucket(bucket).send().await?;
    let key = "dir a/file+b%20(é)";
    let encoded_key = "dir%20a/file%2Bb%2520%28%C3%A9%29";
    let mut uploads = Vec::new();
    for object in [key, key, "dir a/z", "outside/prefix"] {
        let upload = client.create_multipart_upload().bucket(bucket).key(object).send().await?;
        uploads.push((object, upload.upload_id().expect("created multipart upload has an ID").to_owned()));
    }
    uploads.sort();
    let expected_keys = [key, key, "dir a/z"];
    let expected_encoded_keys = [encoded_key, encoded_key, "dir%20a/z"];

    for url_encoded in [false, true] {
        let base_query = if url_encoded {
            "prefix=dir%20a%2F&max-uploads=1&encoding-type=url"
        } else {
            "prefix=dir%20a%2F&max-uploads=1"
        };
        let mut query = base_query.to_owned();
        let response_keys = if url_encoded { expected_encoded_keys } else { expected_keys };
        for (index, expected_key) in response_keys.iter().enumerate() {
            let xml = list_uploads_xml(&env, bucket, &query).await?;
            assert_eq!(xml_element_value(&xml, "EncodingType"), url_encoded.then_some("url"));
            assert_xml_element(&xml, "Bucket", bucket);
            assert_xml_element(&xml, "Prefix", if url_encoded { "dir%20a/" } else { "dir a/" });
            assert_xml_element(&xml, "MaxUploads", "1");
            assert_eq!(xml.matches("<Upload>").count(), 1, "response: {xml}");
            assert_xml_element(&xml, "Key", expected_key);
            assert_xml_element(&xml, "UploadId", &uploads[index].1);
            if index > 0 {
                assert_xml_element(&xml, "KeyMarker", response_keys[index - 1]);
                assert_xml_element(&xml, "UploadIdMarker", &uploads[index - 1].1);
            }
            if index + 1 < response_keys.len() {
                assert_xml_element(&xml, "IsTruncated", "true");
                assert_xml_element(&xml, "NextKeyMarker", expected_key);
                assert_xml_element(&xml, "NextUploadIdMarker", &uploads[index].1);
                query = format!("{base_query}&{}", next_page_query(&xml, url_encoded));
            } else {
                assert_xml_element(&xml, "IsTruncated", "false");
                assert!(xml_element_value(&xml, "NextKeyMarker").is_none(), "response: {xml}");
                assert!(xml_element_value(&xml, "NextUploadIdMarker").is_none(), "response: {xml}");
            }
        }
    }

    for (object, upload_id) in uploads {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .send()
            .await?;
    }
    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

#[tokio::test]
async fn test_list_multipart_uploads_encoded_delimiter_pagination() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let client = env.create_s3_client();
    let bucket = "multipart-encoded-delimiter";
    client.create_bucket().bucket(bucket).send().await?;
    let mut uploads = Vec::new();
    for key in ["dir a/a%é+sub/file", "dir a/b%é+sub/file"] {
        let upload = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
        uploads.push((key, upload.upload_id().expect("created multipart upload has an ID").to_owned()));
    }

    for (delimiter, encoded_delimiter, prefixes, encoded_prefixes) in [
        (
            "/",
            "/",
            ["dir a/a%é+sub/", "dir a/b%é+sub/"],
            ["dir%20a/a%25%C3%A9%2Bsub/", "dir%20a/b%25%C3%A9%2Bsub/"],
        ),
        (
            "+",
            "%2B",
            ["dir a/a%é+", "dir a/b%é+"],
            ["dir%20a/a%25%C3%A9%2B", "dir%20a/b%25%C3%A9%2B"],
        ),
    ] {
        for url_encoded in [false, true] {
            let base_query = format!(
                "prefix=dir%20a%2F&max-uploads=1&delimiter={}{}",
                urlencoding::encode(delimiter),
                if url_encoded { "&encoding-type=url" } else { "" },
            );
            let mut query = base_query.clone();
            let response_prefixes = if url_encoded { encoded_prefixes } else { prefixes };
            for (index, prefix) in response_prefixes.iter().enumerate() {
                let xml = list_uploads_xml(&env, bucket, &query).await?;
                assert_eq!(xml_element_value(&xml, "EncodingType"), url_encoded.then_some("url"));
                assert_xml_element(&xml, "Delimiter", if url_encoded { encoded_delimiter } else { delimiter });
                assert_xml_element(&xml, "Prefix", if url_encoded { "dir%20a/" } else { "dir a/" });
                assert_xml_element(&xml, "CommonPrefixes", &format!("<Prefix>{prefix}</Prefix>"));
                assert_eq!(xml.matches("<CommonPrefixes>").count(), 1, "response: {xml}");
                assert!(!xml.contains("<Upload>"), "grouped uploads must not also be listed: {xml}");
                assert!(xml_element_value(&xml, "NextUploadIdMarker").is_none(), "response: {xml}");
                assert!(xml_element_value(&xml, "UploadIdMarker").is_none(), "response: {xml}");
                if index > 0 {
                    assert_xml_element(&xml, "KeyMarker", response_prefixes[index - 1]);
                }
                if index + 1 < response_prefixes.len() {
                    assert_xml_element(&xml, "IsTruncated", "true");
                    assert_xml_element(&xml, "NextKeyMarker", prefix);
                    query = format!("{base_query}&{}", next_page_query(&xml, url_encoded));
                } else {
                    assert_xml_element(&xml, "IsTruncated", "false");
                    assert!(xml_element_value(&xml, "NextKeyMarker").is_none(), "response: {xml}");
                }
            }
        }
    }

    for (key, upload_id) in uploads {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
    }
    client.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}
