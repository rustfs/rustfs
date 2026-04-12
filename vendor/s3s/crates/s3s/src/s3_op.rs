pub struct S3Operation {
    pub(crate) name: &'static str,
}

impl S3Operation {
    /// Returns the name of the operation.
    ///
    /// # Example
    /// ```
    /// use s3s::S3Operation;
    /// fn is_basic_list_op(op: &S3Operation) -> bool {
    ///     matches!(op.name(), "ListBuckets" | "ListObjects" | "ListObjectsV2")
    /// }
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_name() {
        let op = S3Operation { name: "GetObject" };
        assert_eq!(op.name(), "GetObject");
    }
}
