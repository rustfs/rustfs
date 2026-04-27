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

use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io::{Error, ErrorKind};

pub(crate) const SIGNER_HEADER_ERROR_MARKER: &str = "rustfs_signer_header_error";

#[derive(Debug)]
struct SignerHeaderError {
    scope: String,
    header_name: String,
}

impl SignerHeaderError {
    fn new(scope: &str, header_name: &str) -> Self {
        Self {
            scope: scope.to_string(),
            header_name: header_name.to_string(),
        }
    }
}

impl Display for SignerHeaderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: invalid UTF-8 header value for `{}` [{}]",
            self.scope, self.header_name, SIGNER_HEADER_ERROR_MARKER
        )
    }
}

impl StdError for SignerHeaderError {}

pub(crate) fn invalid_utf8_header_error(scope: &str, header_name: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, SignerHeaderError::new(scope, header_name))
}

pub(crate) fn signer_error_to_io_error(scope: &str, error: rustfs_signer::SignV4Error) -> Error {
    match error {
        rustfs_signer::SignV4Error::InvalidHeaderValue { name } => invalid_utf8_header_error(scope, &name),
        other => Error::other(format!("{scope}: {other}")),
    }
}

pub(crate) fn error_chain_contains_signer_header_marker(err: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(source) = current {
        if source.downcast_ref::<SignerHeaderError>().is_some() {
            return true;
        }

        if source.to_string().contains(SIGNER_HEADER_ERROR_MARKER) {
            return true;
        }

        current = source.source();
    }

    false
}
