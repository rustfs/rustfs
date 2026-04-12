use crate::http::Multipart;

pub struct PostSignatureV2<'a> {
    pub policy: &'a str,
    pub access_key_id: &'a str,
    pub signature: &'a str,
}

impl<'a> PostSignatureV2<'a> {
    pub fn extract(m: &'a Multipart) -> Option<Self> {
        let policy = m.find_field_value("policy")?;
        let access_key_id = m.find_field_value("awsaccesskeyid")?;
        let signature = m.find_field_value("signature")?;
        Some(Self {
            policy,
            access_key_id,
            signature,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::File;

    fn make_multipart(fields: Vec<(&str, &str)>) -> Multipart {
        let fields = fields.into_iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect();
        let file = File {
            name: String::new(),
            content_type: None,
            stream: None,
        };
        Multipart::new_for_test(fields, file)
    }

    #[test]
    fn extract_success() {
        let m = make_multipart(vec![("policy", "test-policy"), ("AWSAccessKeyId", "AKID"), ("signature", "sig123")]);
        let v = PostSignatureV2::extract(&m).unwrap();
        assert_eq!(v.policy, "test-policy");
        assert_eq!(v.access_key_id, "AKID");
        assert_eq!(v.signature, "sig123");
    }

    #[test]
    fn extract_missing_policy() {
        let m = make_multipart(vec![("AWSAccessKeyId", "AKID"), ("signature", "sig123")]);
        assert!(PostSignatureV2::extract(&m).is_none());
    }

    #[test]
    fn extract_missing_access_key() {
        let m = make_multipart(vec![("policy", "test-policy"), ("signature", "sig123")]);
        assert!(PostSignatureV2::extract(&m).is_none());
    }

    #[test]
    fn extract_missing_signature() {
        let m = make_multipart(vec![("policy", "test-policy"), ("AWSAccessKeyId", "AKID")]);
        assert!(PostSignatureV2::extract(&m).is_none());
    }
}
