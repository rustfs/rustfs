// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn session() -> PaginationSession {
    PaginationSession::new(SessionState {
        binding: SessionBinding {
            resource: [1; 32],
            principal: [2; 32],
            request: [3; 32],
        },
        snapshot_version: 42,
        snapshot_digest: [4; 32],
        issued_at: 1000,
        expires_at: 1900,
        results: SessionResults::List,
    })
    .unwrap()
}

#[test]
fn persisted_session_verifies_on_another_instance_and_keeps_original_expiry() {
    let first = session();
    let token = ParsedToken::parse(&first.token(123).unwrap()).unwrap();
    let second = PaginationSession::decode(&first.encode().unwrap(), &first.id()).unwrap();
    assert_eq!(second.verify(&token, &first.state.binding, 1200).unwrap(), 123);
    let next = ParsedToken::parse(&second.token(456).unwrap()).unwrap();
    assert_eq!(next.claims.expires_at, 1900);
    assert_eq!(first.verify(&next, &first.state.binding, 1899).unwrap(), 456);
    assert!(first.verify(&next, &first.state.binding, 1900).is_err());
    assert!(first.verify(&next, &first.state.binding, 939).is_err());
}

#[test]
fn edited_claims_cannot_be_signed_with_the_public_session_identifier() {
    let session = session();
    let original = ParsedToken::parse(&session.token(100).unwrap()).unwrap();
    let mut claims = original.claims;
    claims.expires_at += 3600;
    let payload = serde_json::to_vec(&claims).unwrap();
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(session.id().as_bytes()).unwrap();
    mac.update(TOKEN_DOMAIN);
    mac.update(&payload);
    let forged = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode_to_string(payload),
        URL_SAFE_NO_PAD.encode_to_string(mac.finalize().into_bytes())
    );
    assert!(
        session
            .verify(&ParsedToken::parse(&forged).unwrap(), &session.state.binding, 1100)
            .is_err()
    );
}

#[test]
fn session_and_all_identity_bindings_are_required() {
    let first = session();
    let token = ParsedToken::parse(&first.token(1).unwrap()).unwrap();
    let second = session();
    assert_ne!(first.id(), second.id());
    assert!(second.verify(&token, &first.state.binding, 1000).is_err());
    for field in 0..3 {
        let mut binding = first.state.binding.clone();
        match field {
            0 => binding.resource[0] ^= 1,
            1 => binding.principal[0] ^= 1,
            _ => binding.request[0] ^= 1,
        }
        assert!(first.verify(&token, &binding, 1000).is_err());
    }
    assert!(PaginationSession::decode(&first.encode().unwrap(), &second.id()).is_err());
}

#[test]
fn malformed_tokens_and_session_records_are_rejected_without_exposing_secret() {
    let session = session();
    assert!(ParsedToken::parse(&"x".repeat(4097)).is_err());
    assert!(ParsedToken::parse("../../dataset.abc").is_err());
    let mut record = session.encode().unwrap();
    record[0] ^= 1;
    assert!(PaginationSession::decode(&record, &session.id()).is_err());
    let token = session.token(100).unwrap();
    assert!(!token.contains(&URL_SAFE_NO_PAD.encode_to_string(session.session_signing_key.as_ref())));
    assert!(token.len() <= 2048);
}

#[test]
fn changed_result_state_cannot_reuse_an_existing_token() {
    let first = session();
    let token = ParsedToken::parse(&first.token(100).unwrap()).unwrap();
    let mut second = PaginationSession::decode(&first.encode().unwrap(), &first.id()).unwrap();
    second.state.results = SessionResults::Query {
        matches: (0..101)
            .map(|index| StoredMatch {
                key: format!("key-{index}"),
                row_id: index,
                distance_bits: (index as f64).to_bits(),
            })
            .collect(),
    };
    assert!(second.verify(&token, &first.state.binding, 1100).is_err());
}

#[test]
fn stored_query_distances_preserve_every_bit() {
    let mut state = session().state;
    let distance = 2.0_f64.sqrt();
    state.results = SessionResults::Query {
        matches: (0..101)
            .map(|index| StoredMatch {
                key: format!("key-{index}"),
                row_id: index,
                distance_bits: distance.to_bits(),
            })
            .collect(),
    };
    let first = PaginationSession::new(state).unwrap();
    let second = PaginationSession::decode(&first.encode().unwrap(), &first.id()).unwrap();
    let SessionResults::Query { matches } = &second.state.results else {
        panic!("ranked results");
    };
    assert!(matches.iter().all(|row| f64::from_bits(row.distance_bits) == distance));
    let token = ParsedToken::parse(&first.token(100).unwrap()).unwrap();
    assert_eq!(second.verify(&token, &first.state.binding, 1100).unwrap(), 100);
}

#[test]
fn token_decoder_rejects_duplicate_and_unknown_claims() {
    let session = session();
    let original = ParsedToken::parse(&session.token(100).unwrap()).unwrap();
    let payload = String::from_utf8(original.payload).unwrap();
    for prefix in ["\"version\":1,", "\"extra\":true,"] {
        let payload = format!("{{{prefix}{}", &payload[1..]);
        let token = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode_to_string(payload),
            URL_SAFE_NO_PAD.encode_to_string(&original.tag)
        );
        assert!(ParsedToken::parse(&token).is_err());
    }
}
