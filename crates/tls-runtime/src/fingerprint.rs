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

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsFingerprint {
    pub server_sha256: Option<[u8; 32]>,
    pub public_ca_sha256: Option<[u8; 32]>,
    pub client_ca_sha256: Option<[u8; 32]>,
    pub client_cert_sha256: Option<[u8; 32]>,
    pub client_key_sha256: Option<[u8; 32]>,
}

fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

impl TlsFingerprint {
    pub fn from_optional_bytes(
        server: Option<&[u8]>,
        public_ca: Option<&[u8]>,
        client_ca: Option<&[u8]>,
        client_cert: Option<&[u8]>,
        client_key: Option<&[u8]>,
    ) -> Self {
        Self {
            server_sha256: server.map(digest_bytes),
            public_ca_sha256: public_ca.map(digest_bytes),
            client_ca_sha256: client_ca.map(digest_bytes),
            client_cert_sha256: client_cert.map(digest_bytes),
            client_key_sha256: client_key.map(digest_bytes),
        }
    }
}
