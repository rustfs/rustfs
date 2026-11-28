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

use crate::vector::protocol::gateway::replace_error_response;
use crate::vector::protocol::{ErrorCode, ProtocolError};
use rustfs_gateway::{ResponseView, S3Error, StageFilter, response_filter};

/// Install only on the independently configured S3 Vectors service.
/// The mapper converts only refusals with an established service classification.
/// The S3Error extension records the original Gateway refusal after conversion.
pub(crate) fn typed_error_filter<F>(map: F) -> impl StageFilter
where
    F: Fn(&ResponseView<'_>, &S3Error) -> Option<ProtocolError> + Send + Sync + 'static,
{
    response_filter(move |view, response| {
        let mapped = response.extensions().get::<S3Error>().and_then(|error| map(view, error));
        if let Some(error) = mapped {
            replace_error_response(response, error);
        }
        Ok(())
    })
}

/// Maps the service classifications established by the Vector protocol boundary.
/// Unclassified typed refusals retain their original Gateway response.
pub(crate) fn map_gateway_error(_: &ResponseView<'_>, error: &S3Error) -> Option<ProtocolError> {
    let (code, message) = match error.code()?.as_str() {
        "AccessDenied" => (ErrorCode::AccessDenied, "Access denied"),
        "EntityTooLarge" => return Some(ProtocolError::validation("request", "Request body exceeds 20 MiB")),
        "RequestTimeout" => (ErrorCode::RequestTimeout, "Request timed out"),
        "InternalError" => (ErrorCode::InternalServer, "Internal server error"),
        "ServiceUnavailable" => (ErrorCode::ServiceUnavailable, "Service is unavailable"),
        "SlowDown" => (ErrorCode::TooManyRequests, "Too many requests"),
        _ => return None,
    };
    Some(ProtocolError::new(code, message))
}

#[cfg(test)]
#[path = "gateway_error_bridge_tests.rs"]
mod tests;
