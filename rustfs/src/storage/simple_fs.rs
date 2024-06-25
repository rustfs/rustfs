use s3s::S3;

pub struct SimpleFS {}

#[async_trait::async_trait]
impl S3 for SimpleFS {}
