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

//! The internal keying modes: how an object's key is formed and how it is stored.

pub(crate) mod context;
pub(crate) mod direct;
pub(crate) mod material;
pub(crate) mod metadata;
#[cfg(feature = "rio-v2")]
pub(crate) mod object;
pub(crate) mod stored;

use crate::core::keying::material::{EncryptionKeyKind, ManagedSealedKey};

/// The key a new object is encrypted with, in the mode this build writes.
pub(crate) struct NewObjectKey {
    /// Key handed to the stream cipher.
    pub(crate) key_bytes: [u8; 32],
    /// Base nonce the stream cipher derives its block nonces from.
    pub(crate) base_nonce: [u8; 12],
    /// The mode `key_bytes` belongs to.
    pub(crate) key_kind: EncryptionKeyKind,
    /// The sealed form to store, in a mode that seals a separate object key.
    pub(crate) sealed_key: Option<ManagedSealedKey>,
}

/// A managed-SSE data key as a stored object recorded it, still wrapped.
pub(crate) struct StoredDataKey {
    /// The data key, wrapped by the provider that issued it.
    pub(crate) wrapped: Vec<u8>,
    /// The cipher the object recorded.
    pub(crate) algorithm: String,
    /// What the data key is to the object once it is unwrapped.
    pub(crate) mode: StoredKeyMode,
}

/// The mode a stored key was recorded in.
pub(crate) enum StoredKeyMode {
    /// The data key is the object's key, under the nonce stored beside it.
    Direct { base_nonce: [u8; 12] },
    /// The data key unseals the object's key.
    #[cfg(feature = "rio-v2")]
    Object { sealed_key: ManagedSealedKey },
}

// The mode new objects are written in. Every build reads every mode it can
// recognize; writing picks exactly one, and this is the only place that says which.
#[cfg(not(feature = "rio-v2"))]
pub(crate) use direct::{
    SESSION_KEY_NEEDS_CUSTOMER_KEY, new_key_under_customer_key, new_key_under_data_key, new_session_key_under_customer_key,
};
#[cfg(feature = "rio-v2")]
pub(crate) use object::{
    SESSION_KEY_NEEDS_CUSTOMER_KEY, new_key_under_customer_key, new_key_under_data_key, new_session_key_under_customer_key,
};
