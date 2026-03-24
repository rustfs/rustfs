use crate::http::Multipart;

pub struct PostSignatureV4<'a> {
    pub policy: &'a str,
    pub x_amz_algorithm: &'a str,
    pub x_amz_credential: &'a str,
    pub x_amz_date: &'a str,
    pub x_amz_signature: &'a str,
}

impl<'a> PostSignatureV4<'a> {
    pub fn extract(m: &'a Multipart) -> Option<Self> {
        let policy = m.find_field_value("policy")?;
        let x_amz_algorithm = m.find_field_value("x-amz-algorithm")?;
        let x_amz_credential = m.find_field_value("x-amz-credential")?;
        let x_amz_date = m.find_field_value("x-amz-date")?;
        let x_amz_signature = m.find_field_value("x-amz-signature")?;
        Some(Self {
            policy,
            x_amz_algorithm,
            x_amz_credential,
            x_amz_date,
            x_amz_signature,
        })
    }
}
