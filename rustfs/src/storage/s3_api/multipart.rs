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

use crate::storage::storage_api::s3_api_consumer::multipart::contract::multipart::MAX_MULTIPART_PART_NUMBER;
use rustfs_s3_types::{S3Error, S3ErrorCode};

const MAX_MULTIPART_UPLOADS_LIST: i32 = 1000;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListPartsParams {
    pub part_number_marker: Option<usize>,
    pub max_parts: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListMultipartUploadsParams {
    pub prefix: String,
    pub key_marker: Option<String>,
    pub max_uploads: usize,
}

pub(crate) fn parse_list_parts_params(
    part_number_marker: Option<i32>,
    max_parts: Option<i32>,
) -> Result<ListPartsParams, S3Error> {
    let part_number_marker = match part_number_marker {
        Some(marker) => {
            if marker < 0 {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "part-number-marker must be non-negative".to_string(),
                ));
            }
            Some(marker as usize)
        }
        None => None,
    };
    let max_parts = match max_parts {
        Some(parts) => {
            if !(1..=1000).contains(&parts) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "max-parts must be between 1 and 1000".to_string(),
                ));
            }
            parts as usize
        }
        None => 1000,
    };

    Ok(ListPartsParams {
        part_number_marker,
        max_parts,
    })
}

pub(crate) fn parse_upload_part_number(part_number: i32) -> Result<usize, S3Error> {
    if !(1..=MAX_MULTIPART_PART_NUMBER).contains(&part_number) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("partNumber must be between 1 and {MAX_MULTIPART_PART_NUMBER}"),
        ));
    }

    Ok(part_number as usize)
}

pub(crate) fn parse_list_multipart_uploads_params(
    prefix: Option<String>,
    key_marker: Option<String>,
    max_uploads: Option<i32>,
) -> Result<ListMultipartUploadsParams, S3Error> {
    let prefix = prefix.unwrap_or_default();
    let max_uploads = match max_uploads {
        Some(value) => {
            if !(1..=MAX_MULTIPART_UPLOADS_LIST).contains(&value) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    format!("max-uploads must be between 1 and {}", MAX_MULTIPART_UPLOADS_LIST),
                ));
            }

            value as usize
        }
        None => MAX_MULTIPART_UPLOADS_LIST as usize,
    };

    if let Some(key_marker) = &key_marker
        && !key_marker.starts_with(prefix.as_str())
    {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid key marker".to_string()));
    }

    Ok(ListMultipartUploadsParams {
        prefix,
        key_marker,
        max_uploads,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_MULTIPART_UPLOADS_LIST, parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
    };
    use rustfs_s3_types::S3ErrorCode;

    #[test]
    fn test_parse_list_parts_params_defaults_and_valid_values() {
        let parsed = parse_list_parts_params(Some(5), Some(100)).expect("expected valid params");
        assert_eq!(parsed.part_number_marker, Some(5));
        assert_eq!(parsed.max_parts, 100);

        let parsed = parse_list_parts_params(None, None).expect("expected default params");
        assert_eq!(parsed.part_number_marker, None);
        assert_eq!(parsed.max_parts, 1000);
    }

    #[test]
    fn test_parse_list_parts_params_rejects_invalid_max_parts() {
        let err = parse_list_parts_params(None, Some(0)).expect_err("expected invalid max_parts");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_parts_params(None, Some(1001)).expect_err("expected invalid max_parts");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_parse_list_parts_params_rejects_negative_part_number_marker() {
        let err = parse_list_parts_params(Some(-1), None).expect_err("expected invalid part_number_marker");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("part-number-marker must be non-negative"));
    }

    #[test]
    fn test_parse_upload_part_number_accepts_s3_range() {
        assert_eq!(parse_upload_part_number(1).expect("part 1 should be valid"), 1);
        assert_eq!(parse_upload_part_number(10000).expect("part 10000 should be valid"), 10000);
    }

    #[test]
    fn test_parse_upload_part_number_rejects_out_of_s3_range() {
        for part_number in [-1, 0, 10001] {
            let err = parse_upload_part_number(part_number).expect_err("expected invalid part number");
            assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
            assert_eq!(err.message(), Some("partNumber must be between 1 and 10000"));
        }
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_defaults_and_valid_values() {
        let parsed =
            parse_list_multipart_uploads_params(Some("prefix/".to_string()), Some("prefix/key-marker".to_string()), Some(100))
                .expect("expected valid params");
        assert_eq!(parsed.prefix, "prefix/");
        assert_eq!(parsed.key_marker.as_deref(), Some("prefix/key-marker"));
        assert_eq!(parsed.max_uploads, 100);

        let parsed = parse_list_multipart_uploads_params(None, None, None).expect("expected default params");
        assert_eq!(parsed.prefix, "");
        assert_eq!(parsed.key_marker, None);
        assert_eq!(parsed.max_uploads, MAX_MULTIPART_UPLOADS_LIST as usize);
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_rejects_invalid_key_marker() {
        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), Some("other/key-marker".to_string()), None)
            .expect_err("expected invalid key marker");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid key marker"));
    }

    #[test]
    fn test_parse_list_multipart_uploads_params_rejects_invalid_max_uploads() {
        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some(-1))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some(0))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = parse_list_multipart_uploads_params(Some("prefix/".to_string()), None, Some(MAX_MULTIPART_UPLOADS_LIST + 1))
            .expect_err("expected invalid max_uploads");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }
}
