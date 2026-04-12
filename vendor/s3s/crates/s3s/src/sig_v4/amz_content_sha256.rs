use crate::utils::crypto::is_sha256_checksum;

/// [x-amz-content-sha256](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html)
///
/// See also [Common Request Headers](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonRequestHeaders.html)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmzContentSha256<'a> {
    /// Actual payload checksum value
    SingleChunk(&'a str),

    /// `UNSIGNED-PAYLOAD`
    UnsignedPayload,

    /// `STREAMING-UNSIGNED-PAYLOAD-TRAILER`
    StreamingUnsignedPayloadTrailer,

    /// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
    StreamingAws4HmacSha256Payload,

    /// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`
    StreamingAws4HmacSha256PayloadTrailer,

    /// `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD`
    StreamingAws4EcdsaP256Sha256Payload,

    /// `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER`
    StreamingAws4EcdsaP256Sha256PayloadTrailer,
}

/// [`AmzContentSha256`]
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum ParseAmzContentSha256Error {
    /// unknown variant
    #[error("ParseAmzContentSha256Error: UnknownVariant")]
    UnknownVariant,
}

impl<'a> AmzContentSha256<'a> {
    /// Parses `AmzContentSha256` from `x-amz-content-sha256` header
    ///
    /// # Errors
    /// Returns an `Err` if the header is invalid
    pub fn parse(header: &'a str) -> Result<Self, ParseAmzContentSha256Error> {
        if is_sha256_checksum(header) {
            return Ok(Self::SingleChunk(header));
        }

        match header {
            "UNSIGNED-PAYLOAD" => Ok(Self::UnsignedPayload),
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Ok(Self::StreamingUnsignedPayloadTrailer),
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Ok(Self::StreamingAws4HmacSha256Payload),
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => Ok(Self::StreamingAws4HmacSha256PayloadTrailer),
            "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD" => Ok(Self::StreamingAws4EcdsaP256Sha256Payload),
            "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER" => Ok(Self::StreamingAws4EcdsaP256Sha256PayloadTrailer),
            _ => Err(ParseAmzContentSha256Error::UnknownVariant),
        }
    }

    // pub fn to_str(self) -> &'a str {
    //     match self {
    //         AmzContentSha256::SingleChunk(checksum) => checksum,
    //         AmzContentSha256::UnsignedPayload => "UNSIGNED-PAYLOAD",
    //         AmzContentSha256::StreamingUnsignedPayloadTrailer => "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
    //         AmzContentSha256::StreamingAws4HmacSha256Payload => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    //         AmzContentSha256::StreamingAws4HmacSha256PayloadTrailer => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
    //         AmzContentSha256::StreamingAws4EcdsaP256Sha256Payload => "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
    //         AmzContentSha256::StreamingAws4EcdsaP256Sha256PayloadTrailer => "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER",
    //     }
    // }

    pub fn is_streaming(&self) -> bool {
        match self {
            AmzContentSha256::SingleChunk(_) | AmzContentSha256::UnsignedPayload => false,
            AmzContentSha256::StreamingUnsignedPayloadTrailer
            | AmzContentSha256::StreamingAws4HmacSha256Payload
            | AmzContentSha256::StreamingAws4HmacSha256PayloadTrailer
            | AmzContentSha256::StreamingAws4EcdsaP256Sha256Payload
            | AmzContentSha256::StreamingAws4EcdsaP256Sha256PayloadTrailer => true,
        }
    }

    pub fn has_trailer(&self) -> bool {
        match self {
            AmzContentSha256::SingleChunk(_)
            | AmzContentSha256::UnsignedPayload
            | AmzContentSha256::StreamingAws4HmacSha256Payload
            | AmzContentSha256::StreamingAws4EcdsaP256Sha256Payload => false,
            AmzContentSha256::StreamingUnsignedPayloadTrailer
            | AmzContentSha256::StreamingAws4HmacSha256PayloadTrailer
            | AmzContentSha256::StreamingAws4EcdsaP256Sha256PayloadTrailer => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_chunk() {
        // Valid SHA-256 hex (64 lowercase hex chars)
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let v = AmzContentSha256::parse(hash).unwrap();
        assert_eq!(v, AmzContentSha256::SingleChunk(hash));
        assert!(!v.is_streaming());
        assert!(!v.has_trailer());
    }

    #[test]
    fn parse_unsigned_payload() {
        let v = AmzContentSha256::parse("UNSIGNED-PAYLOAD").unwrap();
        assert_eq!(v, AmzContentSha256::UnsignedPayload);
        assert!(!v.is_streaming());
        assert!(!v.has_trailer());
    }

    #[test]
    fn parse_streaming_unsigned_payload_trailer() {
        let v = AmzContentSha256::parse("STREAMING-UNSIGNED-PAYLOAD-TRAILER").unwrap();
        assert_eq!(v, AmzContentSha256::StreamingUnsignedPayloadTrailer);
        assert!(v.is_streaming());
        assert!(v.has_trailer());
    }

    #[test]
    fn parse_streaming_aws4_hmac_sha256_payload() {
        let v = AmzContentSha256::parse("STREAMING-AWS4-HMAC-SHA256-PAYLOAD").unwrap();
        assert_eq!(v, AmzContentSha256::StreamingAws4HmacSha256Payload);
        assert!(v.is_streaming());
        assert!(!v.has_trailer());
    }

    #[test]
    fn parse_streaming_aws4_hmac_sha256_payload_trailer() {
        let v = AmzContentSha256::parse("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER").unwrap();
        assert_eq!(v, AmzContentSha256::StreamingAws4HmacSha256PayloadTrailer);
        assert!(v.is_streaming());
        assert!(v.has_trailer());
    }

    #[test]
    fn parse_streaming_aws4_ecdsa_p256_sha256_payload() {
        let v = AmzContentSha256::parse("STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD").unwrap();
        assert_eq!(v, AmzContentSha256::StreamingAws4EcdsaP256Sha256Payload);
        assert!(v.is_streaming());
        assert!(!v.has_trailer());
    }

    #[test]
    fn parse_streaming_aws4_ecdsa_p256_sha256_payload_trailer() {
        let v = AmzContentSha256::parse("STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER").unwrap();
        assert_eq!(v, AmzContentSha256::StreamingAws4EcdsaP256Sha256PayloadTrailer);
        assert!(v.is_streaming());
        assert!(v.has_trailer());
    }

    #[test]
    fn parse_unknown_variant() {
        let err = AmzContentSha256::parse("INVALID-VALUE").unwrap_err();
        assert!(matches!(err, ParseAmzContentSha256Error::UnknownVariant));
        // Verify error Display impl
        let _ = format!("{err}");
    }
}
