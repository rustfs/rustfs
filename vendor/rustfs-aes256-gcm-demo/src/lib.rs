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

//! Placeholder AEAD cipher that exercises the RustFS multi-cipher extension
//! wiring (write-side cipher selection, frame types 0x03/0x04, read-side
//! dispatch). The core reuses the audited AES-256-GCM primitive so the demo
//! emits real 12-byte nonce / 16-byte tag frames without introducing new
//! cryptography; a real algorithm replaces the inner core while keeping this
//! struct's [`Aead`] contract.

use aes_gcm::aead::{Aead, AeadCore, Nonce, Payload, TagPosition};
use aes_gcm::{Aes256Gcm, KeyInit};

/// Demo AEAD primitive with the same on-disk frame parameters as AES-256-GCM
/// (12-byte nonce, 16-byte tag), exposed under a distinct type and key
/// derivation path so the cipher wiring can be exercised end to end.
pub struct Aes256GcmDemo {
    inner: Aes256Gcm,
}

impl Aes256GcmDemo {
    /// Construct from the 32-byte key. A 16-byte-key cipher would derive the
    /// key to 32 bytes here at the cipher boundary.
    pub fn new_from_key(key: &[u8; 32]) -> Self {
        Self {
            inner: Aes256Gcm::new_from_slice(key).expect("32-byte key is a valid AES-256 key"),
        }
    }
}

impl AeadCore for Aes256GcmDemo {
    type NonceSize = <Aes256Gcm as AeadCore>::NonceSize;
    type TagSize = <Aes256Gcm as AeadCore>::TagSize;
    const TAG_POSITION: TagPosition = TagPosition::Postfix;
}

impl Aead for Aes256GcmDemo {
    fn encrypt<'msg, 'aad>(
        &self,
        nonce: &Nonce<Self>,
        plaintext: impl Into<Payload<'msg, 'aad>>,
    ) -> aes_gcm::aead::Result<Vec<u8>> {
        self.inner.encrypt(nonce, plaintext)
    }

    fn decrypt<'msg, 'aad>(
        &self,
        nonce: &Nonce<Self>,
        ciphertext: impl Into<Payload<'msg, 'aad>>,
    ) -> aes_gcm::aead::Result<Vec<u8>> {
        self.inner.decrypt(nonce, ciphertext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::Tag;

    const KEY: [u8; 32] = [0x5a; 32];
    const NONCE_BYTES: [u8; 12] = [0x3c; 12];
    const AAD: &[u8] = b"rustfs-aes256-gcm-demo-aad";

    fn nonce() -> Nonce<Aes256GcmDemo> {
        Nonce::<Aes256GcmDemo>::try_from(NONCE_BYTES.as_slice()).expect("12-byte nonce")
    }

    #[test]
    fn parameter_contract_locks_12_byte_nonce_and_16_byte_tag() {
        assert_eq!(std::mem::size_of::<Nonce<Aes256GcmDemo>>(), 12);
        assert_eq!(std::mem::size_of::<Tag<Aes256GcmDemo>>(), 16);
        assert_eq!(Aes256GcmDemo::TAG_POSITION, TagPosition::Postfix);
    }

    #[test]
    fn roundtrip_encrypts_decrypts_and_appends_tag() {
        let cipher = Aes256GcmDemo::new_from_key(&KEY);
        let plaintext = b"the quick brown fox jumps over the lazy dog";
        let ct = cipher
            .encrypt(&nonce(), Payload { msg: plaintext, aad: AAD })
            .expect("encrypt");
        assert_eq!(ct.len(), plaintext.len() + 16);
        let pt = cipher
            .decrypt(&nonce(), Payload { msg: ct.as_slice(), aad: AAD })
            .expect("decrypt");
        assert_eq!(pt, plaintext);
    }

    #[test]
    fn tampered_tag_fails_authentication() {
        let cipher = Aes256GcmDemo::new_from_key(&KEY);
        let mut ct = cipher
            .encrypt(&nonce(), Payload { msg: b"payload", aad: AAD })
            .expect("encrypt");
        let last = ct.len() - 1;
        ct[last] ^= 0x01;
        assert!(cipher
            .decrypt(&nonce(), Payload { msg: ct.as_slice(), aad: AAD })
            .is_err());
    }

    #[test]
    fn wrong_aad_fails_authentication() {
        let cipher = Aes256GcmDemo::new_from_key(&KEY);
        let ct = cipher
            .encrypt(&nonce(), Payload { msg: b"payload", aad: AAD })
            .expect("encrypt");
        assert!(cipher
            .decrypt(&nonce(), Payload { msg: ct.as_slice(), aad: b"other-aad" })
            .is_err());
    }
}