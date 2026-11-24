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

#[cfg(feature = "fips")]
use aws_lc_rs::digest;
#[cfg(not(feature = "fips"))]
use sha2::{Digest, Sha256};

pub(super) fn hash(data: &[u8]) -> [u8; 32] {
    #[cfg(feature = "fips")]
    {
        digest_bytes(digest::digest(&digest::SHA256, data))
    }
    #[cfg(not(feature = "fips"))]
    {
        Sha256::digest(data).into()
    }
}

pub(super) fn hash_slices<'a>(slices: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    #[cfg(feature = "fips")]
    let mut context = digest::Context::new(&digest::SHA256);
    #[cfg(not(feature = "fips"))]
    let mut context = Sha256::new();

    for slice in slices {
        context.update(slice);
    }

    #[cfg(feature = "fips")]
    {
        digest_bytes(context.finish())
    }
    #[cfg(not(feature = "fips"))]
    {
        context.finalize().into()
    }
}

#[cfg(feature = "fips")]
fn digest_bytes(digest: digest::Digest) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes.copy_from_slice(digest.as_ref());
    bytes
}
