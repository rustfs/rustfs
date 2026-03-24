//! Authorization
//!
//! See [sigv4-auth-using-authorization-header](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html)
//!

use serde::{Deserialize, Serialize};

/// Authorization
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorizationV4<'a> {
    /// The algorithm that was used to calculate the signature.
    pub algorithm: &'a str,

    /// Access key ID and the scope information, which includes the date, Region, and service that were used to calculate the signature.
    pub credential: CredentialV4<'a>,

    /// A semicolon-separated list of request headers that you used to compute `Signature`.
    pub signed_headers: Vec<&'a str>,

    /// The 256-bit signature expressed as 64 lowercase hexadecimal characters.
    pub signature: &'a str,
}

/// Access key ID and the scope information, which includes the date, Region, and service that were used to calculate the signature.
///
/// This string has the following form:
/// `<your-access-key-id>/<date>/<aws-region>/<aws-service>/aws4_request`
///
/// See [sigv4-auth-using-authorization-header](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html)
#[derive(Debug, Serialize, Deserialize)]
pub struct CredentialV4<'a> {
    /// access key id
    pub access_key_id: &'a str,
    /// \<date\> value is specified using YYYYMMDD format.
    pub date: &'a str,
    /// region
    pub aws_region: &'a str,
    /// \<aws-service\> value is `s3` when sending request to Amazon S3.
    pub aws_service: &'a str,
}

/// [`AuthorizationV4`]
#[derive(Debug, thiserror::Error)]
#[error("ParseAuthorizationError")]
pub struct ParseAuthorizationError {
    /// priv place holder
    _priv: (),
}

/// [`CredentialV4`]
#[derive(Debug, thiserror::Error)]
#[error("ParseAuthorizationError")]
pub struct ParseCredentialError {
    /// priv place holder
    _priv: (),
}

impl<'a> CredentialV4<'a> {
    pub fn parse(input: &'a str) -> Result<Self, ParseCredentialError> {
        match parser::parse_credential(input) {
            Ok(("", ans)) => Ok(ans),
            Ok(_) | Err(_) => Err(ParseCredentialError { _priv: () }),
        }
    }
}

impl<'a> AuthorizationV4<'a> {
    /// Parses `AuthorizationV4` from `Authorization` header
    /// # Errors
    /// Returns an `Err` if the header is invalid
    pub fn parse(header: &'a str) -> Result<Self, ParseAuthorizationError> {
        match parser::parse_authorization(header) {
            Ok(("", ans)) => Ok(ans),
            Ok(_) | Err(_) => Err(ParseAuthorizationError { _priv: () }),
        }
    }
}

mod parser {
    use super::*;

    use crate::utils::parser::{Error, consume, digit2, digit4};

    use nom::IResult;
    use nom::Parser;
    use nom::bytes::complete::{tag, take, take_till, take_till1};
    use nom::character::complete::{multispace0, multispace1};
    use nom::combinator::verify;
    use nom::multi::separated_list1;
    use nom::sequence::{delimited, preceded, terminated};

    pub fn parse_authorization(mut input: &str) -> IResult<&str, AuthorizationV4<'_>> {
        let s = &mut input;

        let algorithm = consume(s, till_space1)?;

        consume(s, multispace1)?;
        let credential = consume(s, |i| delimited(tag("Credential="), parse_credential, tag(",")).parse(i))?;

        consume(s, multispace0)?;
        let parse_headers = separated_list1(tag(";"), take_till(|c| c == ';' || c == ','));
        let signed_headers = consume(s, |i| delimited(tag("SignedHeaders="), parse_headers, tag(",")).parse(i))?;

        consume(s, multispace0)?;
        let signature = consume(s, |i| preceded(tag("Signature="), till_space0).parse(i))?;

        consume(s, multispace0)?;

        let ans = AuthorizationV4 {
            algorithm,
            credential,
            signed_headers,
            signature,
        };

        Ok((input, ans))
    }

    fn till_space0(input: &str) -> IResult<&str, &str> {
        take_till(|c: char| c.is_ascii_whitespace())(input)
    }

    fn till_space1(input: &str) -> IResult<&str, &str> {
        take_till1(|c: char| c.is_ascii_whitespace())(input)
    }

    pub fn parse_credential(mut input: &str) -> IResult<&str, CredentialV4<'_>> {
        let s = &mut input;

        let access_key_id = consume(s, until_slash0)?;
        let date = consume(s, |i| verify(until_slash1, |s| verify_date(s).is_ok()).parse(i))?;
        let aws_region = consume(s, until_slash0)?;
        let aws_service = consume(s, until_slash1)?;
        consume(s, tag("aws4_request"))?;

        let ans = CredentialV4 {
            access_key_id,
            date,
            aws_region,
            aws_service,
        };

        Ok((input, ans))
    }

    fn until_slash0(input: &str) -> IResult<&str, &str> {
        terminated(take_till(|c| c == '/'), take(1_usize)).parse(input)
    }

    fn until_slash1(input: &str) -> IResult<&str, &str> {
        terminated(take_till1(|c| c == '/'), take(1_usize)).parse(input)
    }

    fn verify_date(s: &str) -> Result<(), Error> {
        let x = s.as_bytes();
        if x.len() != 8 {
            return Err(Error);
        }

        let yyyy = digit4([x[0], x[1], x[2], x[3]])?.into();
        let mm = digit2([x[4], x[5]])?.into();
        let dd = digit2([x[6], x[7]])?.into();

        match chrono::NaiveDate::from_ymd_opt(yyyy, mm, dd) {
            Some(_) => Ok(()),
            None => Err(Error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_header() {
        {
            let auth = "AWS4-HMAC-SHA256 
                Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, 
                SignedHeaders=host;range;x-amz-date,
                Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024
            ";
            let ans = AuthorizationV4::parse(auth).unwrap();

            assert_eq!(ans.algorithm, "AWS4-HMAC-SHA256");
            assert_eq!(ans.credential.access_key_id, "AKIAIOSFODNN7EXAMPLE");
            assert_eq!(ans.credential.date, "20130524");
            assert_eq!(ans.credential.aws_region, "us-east-1");
            assert_eq!(ans.credential.aws_service, "s3");
            assert_eq!(ans.signed_headers, &["host", "range", "x-amz-date"]);
            assert_eq!(ans.signature, "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024");
        }
        {
            let auth = "AWS4-HMAC-SHA256 
                Credential=AKIAIOSFODNN7EXAMPLE/20200931/us-east-1/s3/aws4_request, 
                SignedHeaders=host;range;x-amz-date,
                Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024
            ";

            assert!(AuthorizationV4::parse(auth).is_err());
        }
    }

    #[test]
    fn special_20200921() {
        let auth = concat!(
            "AWS4-HMAC-SHA256 ",
            "Credential=AKIAIOSFODNN7EXAMPLE/20200921/us-east-1/s3/aws4_request,",
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,",
            "Signature=7a7f7778618cadc05f112b44cca218e001a0a020c5c512d8aa2bca2afb713fad",
        );

        let ans = AuthorizationV4::parse(auth).unwrap();

        assert_eq!(ans.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(ans.credential.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(ans.credential.date, "20200921");
        assert_eq!(ans.credential.aws_region, "us-east-1");
        assert_eq!(ans.credential.aws_service, "s3");
        assert_eq!(
            ans.signed_headers,
            &["host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"]
        );
        assert_eq!(ans.signature, "7a7f7778618cadc05f112b44cca218e001a0a020c5c512d8aa2bca2afb713fad");
    }

    #[test]
    fn special_20230204() {
        let auth = concat!(
            "AWS4-HMAC-SHA256 ",
            "Credential=/20230204/us-east-1/s3/aws4_request, ",
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-user-agent, ",
            "Signature=d2ff90c5a29855fd7c56251aa4c02c49a1bc258a8cc9c191ba3cfc037c5dab80"
        );
        let ans = AuthorizationV4::parse(auth).unwrap();

        assert_eq!(ans.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(ans.credential.access_key_id, "");
        assert_eq!(ans.credential.date, "20230204");
        assert_eq!(ans.credential.aws_region, "us-east-1");
        assert_eq!(ans.credential.aws_service, "s3");
        assert_eq!(ans.signed_headers, &["host", "x-amz-content-sha256", "x-amz-date", "x-amz-user-agent"]);
        assert_eq!(ans.signature, "d2ff90c5a29855fd7c56251aa4c02c49a1bc258a8cc9c191ba3cfc037c5dab80");
    }
}
