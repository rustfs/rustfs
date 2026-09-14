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

use std::collections::HashMap;

use base64_simd::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer as _, SigningKey};
use rustfs::connect::relay;
use rustfs::connect::relay::{
    RelayDirection, RelayEnvelope, RelayError, RelayMaterialKind, RelayParty, RelayReceiptOutcome, RelayReceiptPayload,
    RelayReplayKey, RelayTransport, TrustedReceiptSigner, relay_approved_artifact,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

const TRANSFER_UID: &str = "0198f3a1-a200-7b20-8b22-112233445566";

struct Destination {
    signing_key: SigningKey,
    transfers: HashMap<String, RelayReplayKey>,
    applied: HashMap<RelayReplayKey, String>,
    interrupted: bool,
    side_effects: usize,
}

impl Destination {
    fn new(signing_key: SigningKey, interrupted: bool) -> Self {
        Self {
            signing_key,
            transfers: HashMap::new(),
            applied: HashMap::new(),
            interrupted,
            side_effects: 0,
        }
    }

    fn receipt(&self, envelope: &RelayEnvelope, outcome: RelayReceiptOutcome) -> Vec<u8> {
        let payload = RelayReceiptPayload {
            format_version: relay::RELAY_RECEIPT_FORMAT.to_owned(),
            protocol_version: "v1".to_owned(),
            transfer_uid: envelope.transfer_uid.clone(),
            material_kind: envelope.material_kind,
            direction: envelope.direction,
            artifact_sha256: envelope.artifact.sha256.clone(),
            producer: envelope.asserted_producer.clone(),
            destination: envelope.destination.clone(),
            outcome,
            received_at: "2026-08-20T09:30:01Z".to_owned(),
        };
        let payload = serde_json::to_vec(&payload).unwrap();
        let encoded_payload = URL_SAFE_NO_PAD.encode_to_string(&payload);
        let mut signed = b"rustfs-connect-relay-receipt-v1\0".to_vec();
        signed.extend_from_slice(&payload);
        let signature = self.signing_key.sign(&signed).to_bytes();
        let key_id = hex_lower(&Sha256::digest(self.signing_key.verifying_key().as_bytes()));

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Signature<'a> {
            algorithm: &'static str,
            key_id: String,
            value: String,
            #[serde(skip)]
            _marker: std::marker::PhantomData<&'a ()>,
        }
        #[derive(Serialize)]
        struct Receipt<'a> {
            payload: String,
            signature: Signature<'a>,
        }
        serde_json::to_vec(&Receipt {
            payload: encoded_payload,
            signature: Signature {
                algorithm: "Ed25519",
                key_id,
                value: URL_SAFE_NO_PAD.encode_to_string(signature),
                _marker: std::marker::PhantomData,
            },
        })
        .unwrap()
    }
}

impl RelayTransport for Destination {
    fn deliver(&mut self, bytes: &[u8]) -> Result<Option<Vec<u8>>, RelayError> {
        let envelope: RelayEnvelope = serde_json::from_slice(bytes).unwrap();
        let key = envelope.replay_key();
        if self
            .transfers
            .get(&envelope.transfer_uid)
            .is_some_and(|existing| existing != &key)
        {
            return Err(RelayError::Transport);
        }
        self.transfers
            .entry(envelope.transfer_uid.clone())
            .or_insert_with(|| key.clone());
        let duplicate = self.applied.contains_key(&key);
        if !duplicate {
            self.side_effects += 1;
            self.applied.insert(key, envelope.transfer_uid.clone());
        }
        if self.interrupted {
            self.interrupted = false;
            return Ok(None);
        }
        let outcome = if duplicate {
            RelayReceiptOutcome::Duplicate
        } else {
            RelayReceiptOutcome::Applied
        };
        Ok(Some(self.receipt(&envelope, outcome)))
    }
}

fn party(party_type: &str, name: &str, key_id: bool) -> RelayParty {
    RelayParty {
        party_type: party_type.to_owned(),
        name: name.to_owned(),
        key_id: key_id.then(|| "39ca24c8b02a559fd9beb2b1f5d18ced20c4bb246577b92914ae6814c3f70acf".to_owned()),
    }
}

fn trusted(key: &SigningKey) -> TrustedReceiptSigner {
    let public_key = key.verifying_key().to_bytes();
    TrustedReceiptSigner::new(hex_lower(&Sha256::digest(public_key)), public_key).unwrap()
}

#[test]
fn interrupted_transfer_retries_exact_bytes_and_applies_once() {
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let mut destination = Destination::new(signing_key.clone(), true);
    let artifact = b"opaque signed diagnostic manifest";
    let delivery = relay_approved_artifact(
        TRANSFER_UID,
        RelayMaterialKind::DiagnosticBundleManifest,
        artifact,
        party("DEVICE", "organizations/o/clusters/c/clusterDevices/d", true),
        party("CONNECT", "organizations/o", false),
        &trusted(&signing_key),
        |review| review.artifact_sha256 == hex_lower(&Sha256::digest(artifact)),
        &mut destination,
    )
    .unwrap();

    assert_eq!(delivery.attempts, 2);
    assert_eq!(delivery.receipt.outcome, RelayReceiptOutcome::Duplicate);
    assert_eq!(destination.side_effects, 1);
    assert_eq!(delivery.envelope.direction, RelayDirection::ClusterToConnect);
    assert_eq!(
        BASE64_STANDARD
            .decode_to_vec(delivery.envelope.artifact.bytes.as_bytes())
            .unwrap(),
        artifact
    );
}

#[test]
fn customer_rejection_prevents_delivery() {
    let signing_key = SigningKey::from_bytes(&[8; 32]);
    let mut destination = Destination::new(signing_key.clone(), false);
    let result = relay_approved_artifact(
        TRANSFER_UID,
        RelayMaterialKind::OfflineEnrollmentResponse,
        b"signed enrollment response",
        party("DEVICE", "organizations/o/clusters/c/candidateDevices/d", true),
        party("CONNECT", "organizations/o", false),
        &trusted(&signing_key),
        |_| false,
        &mut destination,
    );
    assert_eq!(result.unwrap_err(), RelayError::ApprovalRequired);
    assert_eq!(destination.side_effects, 0);
}

#[test]
fn receipt_from_an_untrusted_destination_is_rejected() {
    let trusted_key = SigningKey::from_bytes(&[9; 32]);
    let relay_key = SigningKey::from_bytes(&[10; 32]);
    let mut destination = Destination::new(relay_key, false);
    let result = relay_approved_artifact(
        TRANSFER_UID,
        RelayMaterialKind::ServiceLicense,
        b"signed license",
        party("CONNECT_LICENSE_ISSUER", "issuer", true),
        party("CLUSTER", "organizations/o/clusters/c", false),
        &trusted(&trusted_key),
        |_| true,
        &mut destination,
    );
    assert_eq!(result.unwrap_err(), RelayError::ReceiptSignerUntrusted);
    assert_eq!(destination.side_effects, 1);
}

#[test]
fn transfer_uid_reuse_with_different_material_conflicts() {
    let producer = party("DEVICE", "organizations/o/clusters/c/clusterDevices/d", true);
    let destination = party("CONNECT", "organizations/o", false);
    let signing_key = SigningKey::from_bytes(&[12; 32]);
    let mut relay_destination = Destination::new(signing_key, false);
    let first = envelope(b"first", producer, destination);
    assert!(relay_destination.deliver(&serde_json::to_vec(&first).unwrap()).is_ok());
    assert!(relay_destination.deliver(&serde_json::to_vec(&first).unwrap()).is_ok());
    let mut conflicting = first;
    conflicting.artifact.bytes = BASE64_STANDARD.encode_to_string(b"second");
    conflicting.artifact.sha256 = hex_lower(&Sha256::digest(b"second"));
    assert!(relay_destination.deliver(&serde_json::to_vec(&conflicting).unwrap()).is_err());
    assert_eq!(relay_destination.side_effects, 1);
}

#[test]
fn unknown_material_and_changed_receipt_are_fail_closed() {
    let json = br#"{"formatVersion":"rustfs.connect.relayEnvelope/1","protocolVersion":"v1","transferUid":"0198f3a1-a200-7b20-8b22-112233445566","materialKind":"OBJECT_DATA","direction":"CLUSTER_TO_CONNECT","artifact":{"encoding":"base64","bytes":"eA","sha256":"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"},"assertedProducer":{"type":"DEVICE","name":"d","keyId":"39ca24c8b02a559fd9beb2b1f5d18ced20c4bb246577b92914ae6814c3f70acf"},"destination":{"type":"CONNECT","name":"o"}}"#;
    assert!(serde_json::from_slice::<RelayEnvelope>(json).is_err());

    let signing_key = SigningKey::from_bytes(&[11; 32]);
    let destination = Destination::new(signing_key.clone(), false);
    let envelope = envelope(
        b"signed",
        party("DEVICE", "organizations/o/clusters/c/clusterDevices/d", true),
        party("CONNECT", "organizations/o", false),
    );
    let receipt = destination.receipt(&envelope, RelayReceiptOutcome::Applied);
    let mut receipt: serde_json::Value = serde_json::from_slice(&receipt).unwrap();
    receipt["payload"] = serde_json::Value::String("e30".to_owned());
    assert_eq!(
        relay::verify_receipt(&serde_json::to_vec(&receipt).unwrap(), &envelope, &trusted(&signing_key)).unwrap_err(),
        RelayError::ReceiptSignatureInvalid
    );
}

#[test]
fn verifies_the_frozen_connect_diagnostic_receipt() {
    let envelope = RelayEnvelope {
        format_version: relay::RELAY_ENVELOPE_FORMAT.to_owned(),
        protocol_version: "v1".to_owned(),
        transfer_uid: TRANSFER_UID.to_owned(),
        material_kind: RelayMaterialKind::DiagnosticBundleManifest,
        direction: RelayDirection::ClusterToConnect,
        artifact: relay::RelayArtifact {
            encoding: "base64".to_owned(),
            bytes: "unused by receipt verification".to_owned(),
            sha256: "3e16c840167f7ea3344e9d2cbd36b2e3f78fc505ba17fed3368f88fdc72b5c7e".to_owned(),
        },
        asserted_producer: RelayParty {
            party_type: "DEVICE".to_owned(),
            name: "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50/clusters/0198f3a1-5d00-7b20-9c31-1d2e3f4a5b61/clusterDevices/0198f3a1-6e00-7c30-ad41-2e3f4a5b6c72".to_owned(),
            key_id: Some("39ca24c8b02a559fd9beb2b1f5d18ced20c4bb246577b92914ae6814c3f70acf".to_owned()),
        },
        destination: RelayParty {
            party_type: "CONNECT".to_owned(),
            name: "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50".to_owned(),
            key_id: None,
        },
    };
    let public_key = URL_SAFE_NO_PAD
        .decode_to_vec(b"ClL2RdbJCGTGikXEDuSFpivrCjV0aT6EibHPecUkGB0")
        .unwrap()
        .try_into()
        .unwrap();
    let trust =
        TrustedReceiptSigner::new("aef7765496addd64bb9fcdd7b61682148622aed4856a7315326faea0aa86d53b".to_owned(), public_key)
            .unwrap();
    let receipt = br#"{"payload":"eyJmb3JtYXRWZXJzaW9uIjoicnVzdGZzLmNvbm5lY3QucmVsYXlSZWNlaXB0LzEiLCJwcm90b2NvbFZlcnNpb24iOiJ2MSIsInRyYW5zZmVyVWlkIjoiMDE5OGYzYTEtYTIwMC03YjIwLThiMjItMTEyMjMzNDQ1NTY2IiwibWF0ZXJpYWxLaW5kIjoiRElBR05PU1RJQ19CVU5ETEVfTUFOSUZFU1QiLCJkaXJlY3Rpb24iOiJDTFVTVEVSX1RPX0NPTk5FQ1QiLCJhcnRpZmFjdFNoYTI1NiI6IjNlMTZjODQwMTY3ZjdlYTMzNDRlOWQyY2JkMzZiMmUzZjc4ZmM1MDViYTE3ZmVkMzM2OGY4OGZkYzcyYjVjN2UiLCJwcm9kdWNlciI6eyJ0eXBlIjoiREVWSUNFIiwibmFtZSI6Im9yZ2FuaXphdGlvbnMvMDE5OGYzYTEtNGMwMC03YTEwLThiMjEtMGMxZDJlM2Y0YTUwL2NsdXN0ZXJzLzAxOThmM2ExLTVkMDAtN2IyMC05YzMxLTFkMmUzZjRhNWI2MS9jbHVzdGVyRGV2aWNlcy8wMTk4ZjNhMS02ZTAwLTdjMzAtYWQ0MS0yZTNmNGE1YjZjNzIiLCJrZXlJZCI6IjM5Y2EyNGM4YjAyYTU1OWZkOWJlYjJiMWY1ZDE4Y2VkMjBjNGJiMjQ2NTc3YjkyOTE0YWU2ODE0YzNmNzBhY2YifSwiZGVzdGluYXRpb24iOnsidHlwZSI6IkNPTk5FQ1QiLCJuYW1lIjoib3JnYW5pemF0aW9ucy8wMTk4ZjNhMS00YzAwLTdhMTAtOGIyMS0wYzFkMmUzZjRhNTAifSwib3V0Y29tZSI6IkFQUExJRUQiLCJyZWNlaXZlZEF0IjoiMjAyNi0wOC0yMFQwOTozMDowMVoifQ","signature":{"algorithm":"Ed25519","keyId":"aef7765496addd64bb9fcdd7b61682148622aed4856a7315326faea0aa86d53b","value":"wA8ruum_X9g2Z3ZPdyimS5mbPclsWc7YD-7ZQrN2pNrcrJwBmwhOLIAHmcPevZ__RC-sJcIPixOa0n9B3ygYDA"}}"#;

    let verified = relay::verify_receipt(receipt, &envelope, &trust).unwrap();
    assert_eq!(verified.outcome, RelayReceiptOutcome::Applied);
}

fn envelope(bytes: &[u8], producer: RelayParty, destination: RelayParty) -> RelayEnvelope {
    RelayEnvelope {
        format_version: relay::RELAY_ENVELOPE_FORMAT.to_owned(),
        protocol_version: "v1".to_owned(),
        transfer_uid: TRANSFER_UID.to_owned(),
        material_kind: RelayMaterialKind::DiagnosticBundleManifest,
        direction: RelayDirection::ClusterToConnect,
        artifact: relay::RelayArtifact {
            encoding: "base64".to_owned(),
            bytes: BASE64_STANDARD.encode_to_string(bytes),
            sha256: hex_lower(&Sha256::digest(bytes)),
        },
        asserted_producer: producer,
        destination,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
