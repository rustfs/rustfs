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

//! SigV2 and SigV4 signing for the requests RustFS sends.
//!
//! Every signing function is generic over the request body type: signing reads
//! and writes only the request head (method, URI, headers), and the payload is
//! committed through `x-amz-content-sha256`, never by reading the body. The body
//! passes through unchanged, so callers sign whatever body type they send.

pub mod constants;
pub mod request_signature_streaming;
pub mod request_signature_streaming_unsigned_trailer;
pub mod request_signature_v2;
pub mod request_signature_v4;
pub mod utils;

pub use request_signature_streaming::streaming_sign_v4;
pub use request_signature_streaming::try_streaming_sign_v4;
pub use request_signature_v2::SignV2Error;
pub use request_signature_v2::pre_sign_v2;
pub use request_signature_v2::sign_v2;
pub use request_signature_v2::try_pre_sign_v2;
pub use request_signature_v2::try_sign_v2;
pub use request_signature_v4::SignV4Error;
pub use request_signature_v4::pre_sign_v4;
pub use request_signature_v4::sign_v4;
pub use request_signature_v4::sign_v4_trailer;
pub use request_signature_v4::try_pre_sign_v4;
pub use request_signature_v4::try_sign_v4;
pub use request_signature_v4::try_sign_v4_headers;
pub use request_signature_v4::try_sign_v4_trailer;
