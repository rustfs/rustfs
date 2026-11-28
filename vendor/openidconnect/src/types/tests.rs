use crate::IssuerUrl;

#[test]
fn test_issuer_url_append() {
    assert_eq!(
        "http://example.com/.well-known/openid-configuration",
        IssuerUrl::new("http://example.com".to_string())
            .unwrap()
            .join(".well-known/openid-configuration")
            .unwrap()
            .to_string()
    );
    assert_eq!(
        "http://example.com/.well-known/openid-configuration",
        IssuerUrl::new("http://example.com/".to_string())
            .unwrap()
            .join(".well-known/openid-configuration")
            .unwrap()
            .to_string()
    );
    assert_eq!(
        "http://example.com/x/.well-known/openid-configuration",
        IssuerUrl::new("http://example.com/x".to_string())
            .unwrap()
            .join(".well-known/openid-configuration")
            .unwrap()
            .to_string()
    );
    assert_eq!(
        "http://example.com/x/.well-known/openid-configuration",
        IssuerUrl::new("http://example.com/x/".to_string())
            .unwrap()
            .join(".well-known/openid-configuration")
            .unwrap()
            .to_string()
    );
}

#[test]
fn test_url_serialize() {
    let issuer_url =
        IssuerUrl::new("http://example.com/.well-known/openid-configuration".to_string()).unwrap();
    let serialized_url = serde_json::to_string(&issuer_url).unwrap();

    assert_eq!(
        "\"http://example.com/.well-known/openid-configuration\"",
        serialized_url
    );

    let deserialized_url = serde_json::from_str(&serialized_url).unwrap();
    assert_eq!(issuer_url, deserialized_url);

    assert_eq!(
        serde_json::to_string(&IssuerUrl::new("http://example.com".to_string()).unwrap()).unwrap(),
        "\"http://example.com\"",
    );
}

#[cfg(feature = "accept-string-booleans")]
#[test]
fn test_string_bool_parse() {
    use crate::helpers::Boolean;

    fn test_case(input: &str, expect: bool) {
        let value: Boolean = serde_json::from_str(input).unwrap();
        assert_eq!(value.0, expect);
    }
    test_case("true", true);
    test_case("false", false);
    test_case("\"true\"", true);
    test_case("\"false\"", false);
    assert!(serde_json::from_str::<Boolean>("\"maybe\"").is_err());
}
