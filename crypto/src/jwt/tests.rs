use time::OffsetDateTime;

use super::{decode::decode, encode::encode};

#[test]
fn test() {
    let claims = serde_json::json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "aaa": 1,
        "bbb": "bbb"
    });

    let jwt_token = encode(b"aaaa", &claims).unwrap();
    let new_claims = decode(&jwt_token, b"aaaa").unwrap();
    assert_eq!(new_claims.claims, claims);
}
