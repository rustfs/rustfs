use crate::core::{CoreGrantType, CoreJwsSigningAlgorithm};

#[test]
fn test_grant_type_serialize() {
    let serialized_implicit = serde_json::to_string(&CoreGrantType::Implicit).unwrap();
    assert_eq!("\"implicit\"", serialized_implicit);
    assert_eq!(
        CoreGrantType::Implicit,
        serde_json::from_str::<CoreGrantType>(&serialized_implicit).unwrap()
    );
}

#[test]
fn test_signature_alg_serde_plain() {
    assert_eq!(
        serde_plain::to_string(&CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256).unwrap(),
        "RS256"
    );
    assert_eq!(
        serde_plain::from_str::<CoreJwsSigningAlgorithm>("RS256").unwrap(),
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256
    );
}
