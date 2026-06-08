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

use std::collections::BTreeSet;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactSourceKind {
    WorkspaceBuild,
    ThirdPartyDownload,
    GeneratedReleaseAsset,
}

impl ArtifactSourceKind {
    pub const fn requires_digest(self) -> bool {
        matches!(self, Self::ThirdPartyDownload | Self::GeneratedReleaseAsset)
    }

    pub const fn requires_provenance(self) -> bool {
        matches!(self, Self::GeneratedReleaseAsset)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactIntegrityPolicy {
    artifact: &'static str,
    source: ArtifactSourceKind,
    digest_required: bool,
    signature_required: bool,
    provenance_required: bool,
}

impl ArtifactIntegrityPolicy {
    pub const fn new(
        artifact: &'static str,
        source: ArtifactSourceKind,
        digest_required: bool,
        signature_required: bool,
        provenance_required: bool,
    ) -> Self {
        Self {
            artifact,
            source,
            digest_required,
            signature_required,
            provenance_required,
        }
    }

    pub const fn artifact(self) -> &'static str {
        self.artifact
    }

    pub const fn source(self) -> ArtifactSourceKind {
        self.source
    }

    pub const fn digest_required(self) -> bool {
        self.digest_required
    }

    pub const fn signature_required(self) -> bool {
        self.signature_required
    }

    pub const fn provenance_required(self) -> bool {
        self.provenance_required
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SupplyChainPolicyError {
    #[error("artifact integrity policy at index {index} has an empty artifact")]
    EmptyArtifact { index: usize },

    #[error("artifact integrity policy for {artifact} must require a digest")]
    DigestRequired { artifact: &'static str },

    #[error("artifact integrity policy for {artifact} must require provenance")]
    ProvenanceRequired { artifact: &'static str },

    #[error("duplicate artifact integrity policy for {artifact}")]
    DuplicateArtifact { artifact: &'static str },
}

pub fn validate_artifact_integrity_policies(policies: &[ArtifactIntegrityPolicy]) -> Result<(), SupplyChainPolicyError> {
    let mut artifacts = BTreeSet::new();

    for (index, policy) in policies.iter().copied().enumerate() {
        if policy.artifact.trim().is_empty() {
            return Err(SupplyChainPolicyError::EmptyArtifact { index });
        }

        if policy.source.requires_digest() && !policy.digest_required {
            return Err(SupplyChainPolicyError::DigestRequired {
                artifact: policy.artifact,
            });
        }

        if policy.source.requires_provenance() && !policy.provenance_required {
            return Err(SupplyChainPolicyError::ProvenanceRequired {
                artifact: policy.artifact,
            });
        }

        if !artifacts.insert(policy.artifact) {
            return Err(SupplyChainPolicyError::DuplicateArtifact {
                artifact: policy.artifact,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_artifact_integrity_policies() {
        let policies = [
            ArtifactIntegrityPolicy::new("rustfs-server", ArtifactSourceKind::WorkspaceBuild, false, true, false),
            ArtifactIntegrityPolicy::new(
                "rustfs-cli-windows-amd64.zip",
                ArtifactSourceKind::GeneratedReleaseAsset,
                true,
                true,
                true,
            ),
        ];

        assert!(validate_artifact_integrity_policies(&policies).is_ok());
        assert_eq!(policies[0].artifact(), "rustfs-server");
        assert_eq!(policies[0].source(), ArtifactSourceKind::WorkspaceBuild);
        assert!(policies[1].digest_required());
        assert!(policies[1].signature_required());
        assert!(policies[1].provenance_required());
    }

    #[test]
    fn rejects_empty_artifacts() {
        let policies = [ArtifactIntegrityPolicy::new(
            " ",
            ArtifactSourceKind::WorkspaceBuild,
            false,
            false,
            false,
        )];

        let err = validate_artifact_integrity_policies(&policies).expect_err("empty artifact should fail validation");

        assert_eq!(err, SupplyChainPolicyError::EmptyArtifact { index: 0 });
    }

    #[test]
    fn rejects_external_artifacts_without_digest() {
        let policies = [ArtifactIntegrityPolicy::new(
            "third-party-tool",
            ArtifactSourceKind::ThirdPartyDownload,
            false,
            false,
            false,
        )];

        let err = validate_artifact_integrity_policies(&policies).expect_err("third-party artifact should require digest");

        assert_eq!(
            err,
            SupplyChainPolicyError::DigestRequired {
                artifact: "third-party-tool"
            }
        );
    }

    #[test]
    fn rejects_generated_artifacts_without_digest() {
        let policies = [ArtifactIntegrityPolicy::new(
            "rustfs-cli-windows-amd64.zip",
            ArtifactSourceKind::GeneratedReleaseAsset,
            false,
            true,
            true,
        )];

        let err = validate_artifact_integrity_policies(&policies).expect_err("generated artifact should require digest");

        assert_eq!(
            err,
            SupplyChainPolicyError::DigestRequired {
                artifact: "rustfs-cli-windows-amd64.zip"
            }
        );
    }

    #[test]
    fn rejects_generated_artifacts_without_provenance() {
        let policies = [ArtifactIntegrityPolicy::new(
            "rustfs-cli-windows-amd64.zip",
            ArtifactSourceKind::GeneratedReleaseAsset,
            true,
            true,
            false,
        )];

        let err = validate_artifact_integrity_policies(&policies).expect_err("generated artifact should require provenance");

        assert_eq!(
            err,
            SupplyChainPolicyError::ProvenanceRequired {
                artifact: "rustfs-cli-windows-amd64.zip"
            }
        );
    }

    #[test]
    fn rejects_duplicate_artifacts() {
        let policies = [
            ArtifactIntegrityPolicy::new("rustfs-server", ArtifactSourceKind::WorkspaceBuild, false, true, false),
            ArtifactIntegrityPolicy::new("rustfs-server", ArtifactSourceKind::WorkspaceBuild, false, false, false),
        ];

        let err = validate_artifact_integrity_policies(&policies).expect_err("duplicate artifact should fail validation");

        assert_eq!(
            err,
            SupplyChainPolicyError::DuplicateArtifact {
                artifact: "rustfs-server"
            }
        );
    }
}
