use std::collections::HashMap;
use http::request;
use hyper::Uri;
use bytes::{Bytes, BytesMut};
use std::fmt::Write;
use time::{OffsetDateTime, macros::format_description, format_description};

use rustfs_utils::crypto::{base64_encode, hex, hmac_sha1};

use super::utils::get_host_addr;

const SIGN_V4_ALGORITHM: &str   = "AWS4-HMAC-SHA256";
const SIGN_V2_ALGORITHM: &str = "AWS";

fn encode_url2path(req: &request::Builder, virtual_host: bool) -> String {
    let mut path = "".to_string();

    //path = serde_urlencoded::to_string(req.uri_ref().unwrap().path().unwrap()).unwrap();
    path = req.uri_ref().unwrap().path().to_string();
    path
}

pub fn pre_sign_v2(mut req: request::Builder, access_key_id: &str, secret_access_key: &str, expires: i64, virtual_host: bool) -> request::Builder {
    if access_key_id == "" || secret_access_key == "" {
        return req;
    }

    let d = OffsetDateTime::now_utc();
    let d = d.replace_time(time::Time::from_hms(0, 0, 0).unwrap());
    let epoch_expires = d.unix_timestamp() + expires;

    let mut headers = req.headers_mut().expect("err");
    let expires_str = headers.get("Expires");
    if expires_str.is_none() {
        headers.insert("Expires", format!("{:010}", epoch_expires).parse().unwrap());
    }

    let string_to_sign = pre_string_to_sign_v2(&req, virtual_host);
    let signature = hex(hmac_sha1(secret_access_key, string_to_sign));

    let result = serde_urlencoded::from_str::<HashMap<String, String>>(req.uri_ref().unwrap().query().unwrap());
    let mut query = result.unwrap_or_default();
    if get_host_addr(&req).contains(".storage.googleapis.com") {
        query.insert("GoogleAccessId".to_string(), access_key_id.to_string());
    } else {
        query.insert("AWSAccessKeyId".to_string(), access_key_id.to_string());
    }

    query.insert("Expires".to_string(), format!("{:010}", epoch_expires));

    let uri = req.uri_ref().unwrap().clone();
    let mut parts = req.uri_ref().unwrap().clone().into_parts();
    parts.path_and_query = Some(format!("{}?{}&Signature={}", uri.path(), serde_urlencoded::to_string(&query).unwrap(), signature).parse().unwrap());
    let req = req.uri(Uri::from_parts(parts).unwrap());

    req
}

fn post_pre_sign_signature_v2(policy_base64: &str, secret_access_key: &str) -> String {
    let signature = hex(hmac_sha1(secret_access_key, policy_base64));
    signature
}

pub fn sign_v2(mut req: request::Builder, content_len: i64, access_key_id: &str, secret_access_key: &str, virtual_host: bool) -> request::Builder {
    if access_key_id == "" || secret_access_key == "" {
        return req;
    }

    let d = OffsetDateTime::now_utc();
    let d2 = d.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let string_to_sign = string_to_sign_v2(&req, virtual_host);
    let mut headers = req.headers_mut().expect("err");

    let date = headers.get("Date").unwrap();
    if date.to_str().unwrap() == "" {
        headers.insert("Date", d2.format(&format_description::well_known::Rfc2822).unwrap().to_string().parse().unwrap());
    }

    let mut auth_header = format!("{} {}:", SIGN_V2_ALGORITHM, access_key_id);
    let auth_header = format!("{}{}", auth_header, base64_encode(&hmac_sha1(secret_access_key, string_to_sign)));

    headers.insert("Authorization", auth_header.parse().unwrap());

    req
}

fn pre_string_to_sign_v2(req: &request::Builder, virtual_host: bool) -> String {
    let mut buf = BytesMut::new();
    write_pre_sign_v2_headers(&mut buf, &req);
    write_canonicalized_headers(&mut buf, &req);
    write_canonicalized_resource(&mut buf, &req, virtual_host);
    String::from_utf8(buf.to_vec()).unwrap()
}

fn write_pre_sign_v2_headers(buf: &mut BytesMut, req: &request::Builder) {
    buf.write_str(req.method_ref().unwrap().as_str());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Content-Md5").unwrap().to_str().unwrap());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Content-Type").unwrap().to_str().unwrap());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Expires").unwrap().to_str().unwrap());
    buf.write_char('\n');
}

fn string_to_sign_v2(req: &request::Builder, virtual_host: bool) -> String {
    let mut buf = BytesMut::new();
    write_sign_v2_headers(&mut buf, &req);
    write_canonicalized_headers(&mut buf, &req);
    write_canonicalized_resource(&mut buf, &req, virtual_host);
    String::from_utf8(buf.to_vec()).unwrap()
}

fn write_sign_v2_headers(buf: &mut BytesMut, req: &request::Builder) {
    buf.write_str(req.method_ref().unwrap().as_str());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Content-Md5").unwrap().to_str().unwrap());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Content-Type").unwrap().to_str().unwrap());
    buf.write_char('\n');
    buf.write_str(req.headers_ref().unwrap().get("Date").unwrap().to_str().unwrap());
    buf.write_char('\n');
}

fn write_canonicalized_headers(buf: &mut BytesMut, req: &request::Builder) {
    let mut proto_headers = Vec::<String>::new();
    let mut vals = HashMap::<String, Vec<String>>::new();
    for k in req.headers_ref().expect("err").keys() {
        let lk = k.as_str().to_lowercase();
        if lk.starts_with("x-amz") {
            proto_headers.push(lk.clone());
            let vv = req.headers_ref().expect("err").get_all(k).iter().map(|e| e.to_str().unwrap().to_string()).collect();
            vals.insert(lk, vv);
        }
    }
    proto_headers.sort();
    for k in proto_headers {
      buf.write_str(&k);
      buf.write_char(':');
      for (idx, v) in vals[&k].iter().enumerate() {
          if idx > 0 {
              buf.write_char(',');
          }
          buf.write_str(v);
      }
      buf.write_char('\n');
    }
}

const INCLUDED_QUERY: &[&str] = &[
    "acl",
    "delete",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

fn write_canonicalized_resource(buf: &mut BytesMut, req: &request::Builder, virtual_host: bool) {
    let request_url = req.uri_ref().unwrap();
    buf.write_str(&encode_url2path(req, virtual_host));
    if request_url.query().unwrap() != "" {
        let mut n: i64 = 0;
        let result = serde_urlencoded::from_str::<HashMap<String, Vec<String>>>(req.uri_ref().unwrap().query().unwrap());
        let mut vals = result.unwrap_or_default();
        for resource in INCLUDED_QUERY {
            let vv = &vals[*resource];
            if  vv.len() > 0 {
                n += 1;
                match n {
                    1 => {
                        buf.write_char('?');
                    }
                    _ => {
                        buf.write_char('&');
                        buf.write_str(resource);
                        if vv[0].len() > 0 {
                            buf.write_char('=');
                            buf.write_str(&vv[0]);
                        }
                    }
                }
            }
        }
    }
}