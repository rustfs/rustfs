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

use rustfs_iam::error::Error as IamError;
use s3s::{S3Error, S3ErrorCode};

pub(super) fn iam_error_to_s3_error(err: IamError) -> S3Error {
    let code = match &err {
        IamError::NoSuchUser(_)
        | IamError::NoSuchAccount(_)
        | IamError::NoSuchServiceAccount(_)
        | IamError::NoSuchTempAccount(_)
        | IamError::NoSuchGroup(_)
        | IamError::NoSuchPolicy => S3ErrorCode::NoSuchResource,
        _ => S3ErrorCode::InternalError,
    };

    let message = err.to_string();
    let mut s3_error = S3Error::with_message(code, message);
    s3_error.set_source(Box::new(err));
    s3_error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iam_not_found_errors_map_to_not_found_status_class() {
        let errors = [
            IamError::NoSuchUser("user".to_string()),
            IamError::NoSuchAccount("account".to_string()),
            IamError::NoSuchServiceAccount("service".to_string()),
            IamError::NoSuchTempAccount("temp".to_string()),
            IamError::NoSuchGroup("group".to_string()),
            IamError::NoSuchPolicy,
        ];

        for err in errors {
            let s3_error = iam_error_to_s3_error(err);
            assert_eq!(s3_error.code(), &S3ErrorCode::NoSuchResource);
        }
    }

    #[test]
    fn non_not_found_iam_errors_remain_internal_errors() {
        let s3_error = iam_error_to_s3_error(IamError::IamSysNotInitialized);

        assert_eq!(s3_error.code(), &S3ErrorCode::InternalError);
    }
}
