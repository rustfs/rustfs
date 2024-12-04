use time::OffsetDateTime;

use super::{decode::decode, encode::encode};

#[test]
fn test() {
    let claims = serde_json::json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "aaa": 1,
        "bbb": "bbb"
    });

    let jwt_token = encode(b"aaaa", &claims).unwrap_or_default();
    let new_claims = match decode(&jwt_token, b"aaaa") {
        Ok(res) => Some(res.claims),
        Err(_errr) => None,
    };
    assert_eq!(new_claims, Some(claims));
}
