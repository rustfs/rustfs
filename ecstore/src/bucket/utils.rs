use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use s3s::xml;

pub fn is_meta_bucketname(name: &str) -> bool {
    name.starts_with(RUSTFS_META_BUCKET)
}

use regex::Regex;

lazy_static::lazy_static! {
    static ref VALID_BUCKET_NAME: Regex = Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$").unwrap();
    static ref VALID_BUCKET_NAME_STRICT: Regex = Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap();
    static ref IP_ADDRESS: Regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
}

pub fn check_bucket_name_common(bucket_name: &str, strict: bool) -> Result<()> {
    let bucket_name_trimmed = bucket_name.trim();

    if bucket_name_trimmed.is_empty() {
        return Err(Error::other("Bucket name cannot be empty"));
    }
    if bucket_name_trimmed.len() < 3 {
        return Err(Error::other("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name_trimmed.len() > 63 {
        return Err(Error::other("Bucket name cannot be longer than 63 characters"));
    }

    if bucket_name_trimmed == "rustfs" {
        return Err(Error::other("Bucket name cannot be rustfs"));
    }

    if IP_ADDRESS.is_match(bucket_name_trimmed) {
        return Err(Error::other("Bucket name cannot be an IP address"));
    }
    if bucket_name_trimmed.contains("..") || bucket_name_trimmed.contains(".-") || bucket_name_trimmed.contains("-.") {
        return Err(Error::other("Bucket name contains invalid characters"));
    }
    if strict {
        if !VALID_BUCKET_NAME_STRICT.is_match(bucket_name_trimmed) {
            return Err(Error::other("Bucket name contains invalid characters"));
        }
    } else if !VALID_BUCKET_NAME.is_match(bucket_name_trimmed) {
        return Err(Error::other("Bucket name contains invalid characters"));
    }
    Ok(())
}

pub fn check_valid_bucket_name(bucket_name: &str) -> Result<()> {
    check_bucket_name_common(bucket_name, false)
}

pub fn check_valid_bucket_name_strict(bucket_name: &str) -> Result<()> {
    check_bucket_name_common(bucket_name, true)
}

pub fn check_valid_object_name_prefix(object_name: &str) -> Result<()> {
    if object_name.len() > 1024 {
        return Err(Error::other("Object name cannot be longer than 1024 characters"));
    }
    if !object_name.is_ascii() {
        return Err(Error::other("Object name with non-UTF-8 strings are not supported"));
    }
    Ok(())
}

pub fn check_valid_object_name(object_name: &str) -> Result<()> {
    if object_name.trim().is_empty() {
        return Err(Error::other("Object name cannot be empty"));
    }
    check_valid_object_name_prefix(object_name)
}

pub fn deserialize<T>(input: &[u8]) -> xml::DeResult<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let mut d = xml::Deserializer::new(input);
    let ans = T::deserialize(&mut d)?;
    d.expect_eof()?;
    Ok(ans)
}

pub fn serialize_content<T: xml::SerializeContent>(val: &T) -> xml::SerResult<String> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize_content(&mut ser)?;
    }
    Ok(String::from_utf8(buf).unwrap())
}

pub fn serialize<T: xml::Serialize>(val: &T) -> xml::SerResult<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize(&mut ser)?;
    }
    Ok(buf)
}
