use s3s::xml;

use std::fmt;

use stdx::default::default;

fn deserialize_content<T>(input: &[u8]) -> xml::DeResult<T>
where
    T: for<'xml> xml::DeserializeContent<'xml>,
{
    let mut d = xml::Deserializer::new(input);
    let ans = T::deserialize_content(&mut d)?;
    d.expect_eof()?;
    Ok(ans)
}

fn deserialize<T>(input: &[u8]) -> xml::DeResult<T>
where
    T: for<'xml> xml::Deserialize<'xml>,
{
    let mut d = xml::Deserializer::new(input);
    let ans = T::deserialize(&mut d)?;
    d.expect_eof()?;
    Ok(ans)
}

fn serialize_content<T: xml::SerializeContent>(val: &T) -> xml::SerResult<String> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize_content(&mut ser)?;
    }
    Ok(String::from_utf8(buf).unwrap())
}

fn serialize<T: xml::Serialize>(val: &T) -> xml::SerResult<String> {
    let mut buf = Vec::with_capacity(256);
    {
        let mut ser = xml::Serializer::new(&mut buf);
        val.serialize(&mut ser)?;
    }
    Ok(String::from_utf8(buf).unwrap())
}

fn test_serde<T>(val: &T)
where
    T: for<'xml> xml::Deserialize<'xml>,
    T: xml::Serialize,
    T: fmt::Debug + PartialEq,
{
    let xml = serialize(val).unwrap();
    let ans = deserialize::<T>(xml.as_bytes()).unwrap();
    assert_eq!(*val, ans);
}

fn test_serde_content<T>(val: &T)
where
    T: for<'xml> xml::DeserializeContent<'xml>,
    T: xml::SerializeContent,
    T: fmt::Debug + PartialEq,
{
    let xml = serialize_content(val).unwrap();
    let ans = deserialize_content::<T>(xml.as_bytes()).unwrap();
    assert_eq!(*val, ans);
}

/// See <https://github.com/Nugine/s3s/issues/2>
#[test]
fn completed_multipart_upload() {
    let input = r#"
        <CompleteMultipartUpload>
            <Part>
                <PartNumber>1</PartNumber>
                <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
            </Part>
            <Part>
                <PartNumber>2</PartNumber>
                <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
            </Part>
            <Part>
                <PartNumber>3</PartNumber>
                <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
            </Part>
        </CompleteMultipartUpload>
    "#;

    let ans = deserialize::<s3s::dto::CompletedMultipartUpload>(input.as_bytes()).unwrap();

    let parts = ans.parts.as_deref().unwrap();
    assert_eq!(parts.len(), 3);

    assert_eq!(parts[0].part_number, Some(1));
    assert_eq!(
        parts[0].e_tag.as_ref().map(s3s::dto::ETag::value),
        Some("a54357aff0632cce46d942af68356b38")
    );

    assert_eq!(parts[1].part_number, Some(2));
    assert_eq!(
        parts[1].e_tag.as_ref().map(s3s::dto::ETag::value),
        Some("0c78aef83f66abc1fa1e8477f296d394")
    );

    assert_eq!(parts[2].part_number, Some(3));
    assert_eq!(
        parts[2].e_tag.as_ref().map(s3s::dto::ETag::value),
        Some("acbd18db4cc2f85cedef654fccc4a4d8")
    );

    test_serde(&ans);
}

#[test]
fn select_object_content_request() {
    {
        let input = r#"
        <SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Expression>select * from s3object</Expression>
            <ExpressionType>SQL</ExpressionType>
            <InputSerialization>
                <CompressionType>NONE</CompressionType>
                <CSV>
                    <AllowQuotedRecordDelimiter>FALSE</AllowQuotedRecordDelimiter>
                    <Comments>#</Comments>
                    <FieldDelimiter>,</FieldDelimiter>
                    <FileHeaderInfo>NONE</FileHeaderInfo>
                    <QuoteCharacter>""</QuoteCharacter>
                    <QuoteEscapeCharacter>"</QuoteEscapeCharacter>
                    <RecordDelimiter>\n</RecordDelimiter>
                </CSV>
            </InputSerialization>
            <OutputSerialization>
                <JSON>
                    <RecordDelimiter>\n</RecordDelimiter>
                </JSON>
            </OutputSerialization>
        </SelectObjectContentRequest>
    "#;

        let ans = deserialize::<s3s::dto::SelectObjectContentRequest>(input.as_bytes()).unwrap();

        {
            let csv = ans.input_serialization.csv.as_ref().unwrap();
            assert_eq!(csv.allow_quoted_record_delimiter, Some(false));
        }

        test_serde(&ans);
    }

    {
        let input = r#"
    <SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Expression>select * from s3object</Expression>
        <ExpressionType>SQL</ExpressionType>
        <InputSerialization>
            <CSV />
        </InputSerialization>
        <OutputSerialization>
            <CSV />
        </OutputSerialization>
        <RequestProgress>
            <Enabled>true</Enabled>
        </RequestProgress>
    </SelectObjectContentRequest>
"#;

        let ans = deserialize::<s3s::dto::SelectObjectContentRequest>(input.as_bytes()).unwrap();

        assert!(ans.input_serialization.csv.is_some());
        assert!(ans.output_serialization.csv.is_some());

        test_serde(&ans);
    }
}

#[test]
fn tagging() {
    let input = r#"
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>Key4</Key>
                    <Value>Value4</Value>
                </Tag>
            </TagSet>
        </Tagging>
    "#;

    let ans = deserialize::<s3s::dto::Tagging>(input.as_bytes()).unwrap();

    assert_eq!(ans.tag_set.len(), 1);
    let tag = &ans.tag_set[0];
    assert_eq!(tag.key.as_deref(), Some("Key4"));
    assert_eq!(tag.value.as_deref(), Some("Value4"));

    test_serde(&ans);
}

#[test]
fn lifecycle_expiration() {
    let val = s3s::dto::LifecycleExpiration {
        days: Some(365),
        ..Default::default()
    };

    let ans = serialize_content(&val).unwrap();
    let expected = "<Days>365</Days>";

    assert_eq!(ans, expected);

    test_serde_content(&val);
}

#[test]
fn get_bucket_location_output() {
    {
        let us_west_2 = s3s::dto::BucketLocationConstraint::from_static(s3s::dto::BucketLocationConstraint::US_WEST_2);
        let val = s3s::dto::GetBucketLocationOutput {
            location_constraint: Some(us_west_2),
        };

        let ans = serialize(&val).unwrap();
        let expected = "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-west-2</LocationConstraint>";

        assert_eq!(ans, expected);

        test_serde(&val);
    }
    {
        let val = s3s::dto::GetBucketLocationOutput {
            location_constraint: None,
        };

        let ans = serialize(&val).unwrap();
        let expected = "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>";

        assert_eq!(ans, expected);

        test_serde(&val);
    }
}

#[test]
fn get_bucket_notification_configuration_output() {
    let xml = "<NotificationConfiguration></NotificationConfiguration>";

    let val = deserialize::<s3s::dto::NotificationConfiguration>(xml.as_bytes()).unwrap();
    assert_eq!(val, default());
    test_serde(&val);

    let val = deserialize::<s3s::dto::GetBucketNotificationConfigurationOutput>(xml.as_bytes()).unwrap();
    assert_eq!(val, default());
    test_serde(&val);
}

#[test]
fn assume_role_output() {
    let xml = r#"
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <SourceIdentity>Alice</SourceIdentity>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/demo/TestAR</Arn>
      <AssumedRoleId>ARO123EXAMPLE123:TestAR</AssumedRoleId>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
      <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
      <SessionToken>
       AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQW
       LWsKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGd
       QrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU
       9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz
       +scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==
      </SessionToken>
      <Expiration>2019-11-09T13:34:41Z</Expiration>
    </Credentials>
    <PackedPolicySize>6</PackedPolicySize>
  </AssumeRoleResult>
</AssumeRoleResponse>
    "#;

    let val = deserialize::<s3s::dto::AssumeRoleOutput>(xml.as_bytes()).unwrap();
    test_serde(&val);
}

#[cfg(feature = "minio")]
#[test]
fn minio_bucket_lifecycle_configuration_root() {
    // MinIO compatibility: accept both LifecycleConfiguration and BucketLifecycleConfiguration
    let xml_bucket = r"
<BucketLifecycleConfiguration>
  <Rule>
    <ID>r1</ID>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</BucketLifecycleConfiguration>
    ";
    let val = deserialize::<s3s::dto::BucketLifecycleConfiguration>(xml_bucket.as_bytes()).unwrap();
    assert_eq!(val.rules.len(), 1);
    assert_eq!(val.rules[0].id.as_deref(), Some("r1"));

    let xml_std = r"
<LifecycleConfiguration>
  <Rule>
    <ID>r2</ID>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>
    ";
    let val2 = deserialize::<s3s::dto::BucketLifecycleConfiguration>(xml_std.as_bytes()).unwrap();
    assert_eq!(val2.rules.len(), 1);
    assert_eq!(val2.rules[0].id.as_deref(), Some("r2"));
}

#[cfg(feature = "minio")]
#[test]
fn minio_versioning_configuration() {
    let xml = r"
<VersioningConfiguration>
    <Status>Enabled</Status>
    <ExcludedPrefixes>
        <Prefix>a</Prefix>
    </ExcludedPrefixes>
    <ExcludedPrefixes>
        <Prefix>b</Prefix>
    </ExcludedPrefixes>
    <ExcludeFolders>true</ExcludeFolders>
</VersioningConfiguration>
    ";
    let val = deserialize::<s3s::dto::VersioningConfiguration>(xml.as_bytes()).unwrap();
    test_serde(&val);
}

#[cfg(feature = "minio")]
#[test]
fn minio_delete_replication() {
    let xml = r"
<ReplicationConfiguration>
    <Rule>
        <ID>cte4oalu3vqltovlh28g</ID>
        <Status>Enabled</Status>
        <Priority>0</Priority>
        <DeleteMarkerReplication>
            <Status>Enabled</Status>
        </DeleteMarkerReplication>
        <DeleteReplication>
            <Status>Enabled</Status>
        </DeleteReplication>
        <Destination>
            <Bucket>arn:minio:replication:us-east-1:e02ce029-7459-4be2-8267-064712b0ead4:buc2</Bucket>
        </Destination>
        <Filter>
            <Prefix></Prefix>
            <And></And>
            <Tag></Tag>
        </Filter>
        <SourceSelectionCriteria>
            <ReplicaModifications>
                <Status>Enabled</Status>
            </ReplicaModifications>
        </SourceSelectionCriteria>
        <ExistingObjectReplication>
            <Status>Enabled</Status>
        </ExistingObjectReplication>
    </Rule>
    <Role>
    </Role>
</ReplicationConfiguration>
    ";
    let val = deserialize::<s3s::dto::ReplicationConfiguration>(xml.as_bytes()).unwrap();
    test_serde(&val);
}

#[test]
fn xmlns_xsi() {
    let xml = r#"
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>
    <DisplayName>OwnerDisplayName</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>
        <DisplayName>OwnerDisplayName</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI xmlns="">http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission xmlns="">READ</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI xmlns="">http://acs.amazonaws.com/groups/s3/LogDelivery</URI>
      </Grantee>
      <Permission xmlns="">WRITE</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">
        <EmailAddress xmlns="">xyz@amazon.com</EmailAddress>
      </Grantee>
      <Permission xmlns="">WRITE_ACP</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID xmlns="">f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID</ID>
      </Grantee>
      <Permission xmlns="">READ_ACP</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
    "#;
    let val = deserialize::<s3s::dto::AccessControlPolicy>(xml.as_bytes()).unwrap();
    test_serde(&val);

    assert_eq!(
        val.owner.unwrap().id.as_deref(),
        Some("852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID")
    );

    let grants = val.grants.as_deref().unwrap();
    assert_eq!(grants.len(), 5);

    assert_eq!(grants[0].permission.as_ref().unwrap().as_str(), "FULL_CONTROL");
    assert_eq!(grants[1].permission.as_ref().unwrap().as_str(), "READ");
    assert_eq!(grants[2].permission.as_ref().unwrap().as_str(), "WRITE");
    assert_eq!(grants[3].permission.as_ref().unwrap().as_str(), "WRITE_ACP");
    assert_eq!(grants[4].permission.as_ref().unwrap().as_str(), "READ_ACP");

    assert_eq!(
        grants[0].grantee.as_ref().unwrap().id.as_deref(),
        Some("852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID")
    );
    assert_eq!(
        grants[0].grantee.as_ref().unwrap().display_name.as_deref(), //
        Some("OwnerDisplayName")
    );
    assert_eq!(
        grants[1].grantee.as_ref().unwrap().uri.as_deref(),
        Some("http://acs.amazonaws.com/groups/global/AllUsers")
    );
    assert_eq!(
        grants[2].grantee.as_ref().unwrap().uri.as_deref(),
        Some("http://acs.amazonaws.com/groups/s3/LogDelivery")
    );
    assert_eq!(
        grants[3].grantee.as_ref().unwrap().email_address.as_deref(), //
        Some("xyz@amazon.com"),
    );
    assert_eq!(
        grants[4].grantee.as_ref().unwrap().id.as_deref(),
        Some("f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID")
    );
}

#[test]
fn test_str_enum_optimization_functional() {
    use s3s::dto::BucketVersioningStatus;
    use s3s::xml::{DeserializeContent, Deserializer};

    // Test case 1: Known static value "Enabled"
    let xml_enabled = br"Enabled";
    let mut deserializer = Deserializer::new(xml_enabled);
    let status = BucketVersioningStatus::deserialize_content(&mut deserializer).unwrap();

    assert_eq!(status.as_str(), "Enabled");
    assert_eq!(status.as_str(), BucketVersioningStatus::ENABLED);

    // Test case 2: Another known static value "Suspended"
    let xml_suspended = br"Suspended";
    let mut deserializer2 = Deserializer::new(xml_suspended);
    let status2 = BucketVersioningStatus::deserialize_content(&mut deserializer2).unwrap();

    assert_eq!(status2.as_str(), "Suspended");
    assert_eq!(status2.as_str(), BucketVersioningStatus::SUSPENDED);

    // Test case 3: Unknown value should still work correctly
    let xml_unknown = br"Unknown";
    let mut deserializer3 = Deserializer::new(xml_unknown);
    let status3 = BucketVersioningStatus::deserialize_content(&mut deserializer3).unwrap();

    assert_eq!(status3.as_str(), "Unknown");
}

#[test]
fn test_static_vs_from_static() {
    use s3s::dto::BucketVersioningStatus;

    // Create values using different methods and verify they're equivalent
    let static_enabled = BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED);
    let from_string_enabled = BucketVersioningStatus::from("Enabled".to_owned());

    assert_eq!(static_enabled.as_str(), from_string_enabled.as_str());

    // Verify pointer equality for static values (indirect test for optimization)
    assert_eq!(static_enabled.as_str().as_ptr(), BucketVersioningStatus::ENABLED.as_ptr());
}

#[test]
fn test_xml_deserialization_with_various_enum_types() {
    use s3s::dto::{BucketAccelerateStatus, ChecksumAlgorithm};
    use s3s::xml::{DeserializeContent, Deserializer};

    // Test BucketAccelerateStatus optimization
    let xml_enabled = br"Enabled";
    let mut deserializer = Deserializer::new(xml_enabled);
    let accel_status = BucketAccelerateStatus::deserialize_content(&mut deserializer).unwrap();
    assert_eq!(accel_status.as_str(), BucketAccelerateStatus::ENABLED);

    // Test ChecksumAlgorithm optimization
    let xml_crc32 = br"CRC32";
    let mut deserializer2 = Deserializer::new(xml_crc32);
    let checksum_algo = ChecksumAlgorithm::deserialize_content(&mut deserializer2).unwrap();
    assert_eq!(checksum_algo.as_str(), ChecksumAlgorithm::CRC32);
}
