#![allow(clippy::map_entry)]
use std::collections::HashMap;
use http::{HeaderMap, HeaderName, HeaderValue};
use time::OffsetDateTime;
use tracing::warn;

use crate::client::api_error_response::err_invalid_argument;

#[derive(Default)]
pub struct AdvancedGetOptions {
    replication_deletemarker:              bool,
    is_replication_ready_for_deletemarker: bool,
    replication_proxy_request:             String,
}

pub struct GetObjectOptions {
    pub headers: HashMap<String, String>,
    pub req_params: HashMap<String, String>,
    //pub server_side_encryption: encrypt.ServerSide,
    pub version_id: String,
    pub part_number: i64,
    pub checksum: bool,
    pub internal: AdvancedGetOptions,
}

type StatObjectOptions = GetObjectOptions;

impl Default for GetObjectOptions {
    fn default() -> Self {
        Self {
            headers: HashMap::new(),
            req_params: HashMap::new(),
            //server_side_encryption: encrypt.ServerSide::default(),
            version_id: "".to_string(),
            part_number: 0,
            checksum: false,
            internal: AdvancedGetOptions::default(),
        }
    }
}

impl GetObjectOptions {
    pub fn header(&self) -> HeaderMap {
        let mut headers: HeaderMap = HeaderMap::with_capacity(self.headers.len());
        for (k, v) in &self.headers {
            if let Ok(header_name) = HeaderName::from_bytes(k.as_bytes()) {
                headers.insert(header_name, v.parse().expect("err"));
            } else {
                warn!("Invalid header name: {}", k);
            }
        }
        if self.checksum {
           headers.insert("x-amz-checksum-mode", "ENABLED".parse().expect("err"));
        }
        headers
    }

    pub fn set(&self, key: &str, value: &str) {
        //self.headers[http.CanonicalHeaderKey(key)] = value;
    }

    pub fn set_req_param(&mut self, key: &str, value: &str) {
        self.req_params.insert(key.to_string(), value.to_string());
    }

    pub fn add_req_param(&mut self, key: &str, value: &str) {
        self.req_params.insert(key.to_string(), value.to_string());
    }

    pub fn set_match_etag(&mut self, etag: &str) -> Result<(), std::io::Error> {
        self.set("If-Match", &format!("\"{etag}\""));
        Ok(())
    }

    pub fn set_match_etag_except(&mut self, etag: &str) -> Result<(), std::io::Error> {
        self.set("If-None-Match", &format!("\"{etag}\""));
        Ok(())
    }

    pub fn set_unmodified(&mut self, mod_time: OffsetDateTime) -> Result<(), std::io::Error> {
        if mod_time.unix_timestamp() == 0 {
            return Err(std::io::Error::other(err_invalid_argument("Modified since cannot be empty.")));
        }
        self.set("If-Unmodified-Since", &mod_time.to_string());
        Ok(())
    }

    pub fn set_modified(&mut self, mod_time: OffsetDateTime) -> Result<(), std::io::Error> {
        if mod_time.unix_timestamp() == 0 {
            return Err(std::io::Error::other(err_invalid_argument("Modified since cannot be empty.")));
        }
        self.set("If-Modified-Since", &mod_time.to_string());
        Ok(())
    }

    pub fn set_range(&mut self, start: i64, end: i64) -> Result<(), std::io::Error> {
        if start == 0 && end < 0 {
            self.set("Range", &format!("bytes={}", end));
        }
        else if 0 < start && end == 0 {
            self.set("Range", &format!("bytes={}-", start));
        }
        else if 0 <= start && start <= end {
            self.set("Range", &format!("bytes={}-{}", start, end));
        }
        else {
            return Err(std::io::Error::other(err_invalid_argument(&format!("Invalid range specified: start={} end={}", start, end))));
        }
        Ok(())
    }

    pub fn to_query_values(&self) -> HashMap<String, String> {
        let mut url_values = HashMap::new();
        if self.version_id != "" {
            url_values.insert("versionId".to_string(), self.version_id.clone());
        }
        if self.part_number > 0 {
            url_values.insert("partNumber".to_string(), self.part_number.to_string());
        }

        for (key, value) in self.req_params.iter() {
            url_values.insert(key.to_string(), value.to_string());
        }

        url_values
    }
}