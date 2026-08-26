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

//! Object encryption service implementation

pub mod ciphers;
pub mod dek;

pub use dek::{
    AesDekCrypto, CONTEXT_BINDING_AAD_V1, DataKeyEnvelope, DekCrypto, context_aad, desired_context_binding,
    envelope_aad_write_enabled, envelope_wrap_aad, generate_key_material, is_data_key_envelope,
};
