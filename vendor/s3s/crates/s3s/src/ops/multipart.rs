use super::CompleteMultipartUpload;

use crate::dto::CompleteMultipartUploadOutput;
use crate::error::S3Result;
use crate::header::*;
use crate::http;

use sync_wrapper::SyncFuture;

impl CompleteMultipartUpload {
    pub fn serialize_http(x: CompleteMultipartUploadOutput) -> S3Result<http::Response> {
        let mut res = http::Response::with_status(http::StatusCode::OK);

        if let Some(future) = x.future {
            let future = SyncFuture::new(async move {
                let result = future.await;
                match result {
                    Ok(val) => {
                        let mut res = http::Response::default();
                        http::set_xml_body_no_decl(&mut res, &val)?;
                        Ok(res)
                    }
                    Err(err) => super::serialize_error(err, false).map_err(Into::into),
                }
            });
            let duration = std::time::Duration::from_millis(100);
            http::set_keep_alive_xml_body(&mut res, future, duration)?;
        } else {
            http::set_xml_body(&mut res, &x)?;
        }

        http::add_opt_header(&mut res, X_AMZ_SERVER_SIDE_ENCRYPTION_BUCKET_KEY_ENABLED, x.bucket_key_enabled)?;
        http::add_opt_header(&mut res, X_AMZ_EXPIRATION, x.expiration)?;
        http::add_opt_header(&mut res, X_AMZ_REQUEST_CHARGED, x.request_charged)?;
        http::add_opt_header(&mut res, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, x.ssekms_key_id)?;
        http::add_opt_header(&mut res, X_AMZ_SERVER_SIDE_ENCRYPTION, x.server_side_encryption)?;
        http::add_opt_header(&mut res, X_AMZ_VERSION_ID, x.version_id)?;
        Ok(res)
    }
}
