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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedSseScheme {
    SseS3,
    SseKms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedDekProvider {
    LocalSseS3,
    Kms,
}

pub fn managed_dek_provider(scheme: ManagedSseScheme, has_kms_envelope: bool) -> ManagedDekProvider {
    match scheme {
        ManagedSseScheme::SseKms => ManagedDekProvider::Kms,
        ManagedSseScheme::SseS3 if has_kms_envelope => ManagedDekProvider::Kms,
        ManagedSseScheme::SseS3 => ManagedDekProvider::LocalSseS3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_sse_routing_is_determined_by_scheme_and_envelope() {
        assert_eq!(managed_dek_provider(ManagedSseScheme::SseS3, false), ManagedDekProvider::LocalSseS3);
        assert_eq!(managed_dek_provider(ManagedSseScheme::SseS3, true), ManagedDekProvider::Kms);
        assert_eq!(managed_dek_provider(ManagedSseScheme::SseKms, false), ManagedDekProvider::Kms);
        assert_eq!(managed_dek_provider(ManagedSseScheme::SseKms, true), ManagedDekProvider::Kms);
    }
}
