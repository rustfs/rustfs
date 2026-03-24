use s3s::dto::{
    AnalyticsConfiguration, BucketLifecycleConfiguration, GetObjectInput, IntelligentTieringConfiguration,
    InventoryConfiguration, LambdaFunctionConfiguration, ListObjectsV2Input, ListObjectsV2Output, MetadataTableConfiguration,
    MetricsConfiguration, PutObjectOutput, QueueConfiguration, ReplicationConfiguration, RequestPaymentConfiguration,
    TopicConfiguration,
};

#[test]
fn builder() {
    let input = {
        let mut b = GetObjectInput::builder();
        b.set_bucket("hello".to_owned());
        b.set_key("world".to_owned());
        b.build().unwrap()
    };

    assert_eq!(input.bucket, "hello");
    assert_eq!(input.key, "world");
}

#[test]
fn configuration_types_have_default() {
    // Test the two types mentioned in the issue
    let _ = BucketLifecycleConfiguration::default();
    let _ = ReplicationConfiguration::default();

    // Test a few more Configuration types
    let _ = AnalyticsConfiguration::default();
    let _ = IntelligentTieringConfiguration::default();
    let _ = InventoryConfiguration::default();
    let _ = LambdaFunctionConfiguration::default();
    let _ = MetadataTableConfiguration::default();
    let _ = MetricsConfiguration::default();
    let _ = QueueConfiguration::default();
    let _ = RequestPaymentConfiguration::default();
    let _ = TopicConfiguration::default();
}

#[test]
fn configuration_serialization() {
    // Test that Configuration types can be serialized and deserialized
    let config = BucketLifecycleConfiguration::default();
    let json = serde_json::to_string(&config).expect("should serialize");
    let _: BucketLifecycleConfiguration = serde_json::from_str(&json).expect("should deserialize");

    let config = ReplicationConfiguration::default();
    let json = serde_json::to_string(&config).expect("should serialize");
    let _: ReplicationConfiguration = serde_json::from_str(&json).expect("should deserialize");
}

fn require_clone<T: Clone>() {}

#[test]
fn configuration_types_have_clone() {
    require_clone::<BucketLifecycleConfiguration>();
    require_clone::<ReplicationConfiguration>();

    require_clone::<AnalyticsConfiguration>();
    require_clone::<IntelligentTieringConfiguration>();
    require_clone::<InventoryConfiguration>();
    require_clone::<LambdaFunctionConfiguration>();
    require_clone::<MetadataTableConfiguration>();
    require_clone::<MetricsConfiguration>();
    require_clone::<QueueConfiguration>();
    require_clone::<RequestPaymentConfiguration>();
    require_clone::<TopicConfiguration>();
}

#[test]
fn operation_types_have_clone() {
    require_clone::<GetObjectInput>();
    require_clone::<ListObjectsV2Input>();
    require_clone::<ListObjectsV2Output>();
    require_clone::<PutObjectOutput>();
}
