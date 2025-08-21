use prometheus::{Counter, CounterVec, Histogram, HistogramVec, Registry};
use std::sync::Arc;
use tokio::time::Instant;

#[derive(Clone)]
pub struct EncryptionMetrics {
    /// Counter for total encryption operations
    pub encryption_operations: Counter,
    /// Counter for total decryption operations
    pub decryption_operations: Counter,
    /// Counter for encryption failures
    pub encryption_failures: Counter,
    /// Counter for decryption failures
    pub decryption_failures: Counter,
    /// Counter for KMS operations
    pub kms_operations: CounterVec,
    /// Histogram for encryption operation duration
    pub encryption_duration: Histogram,
    /// Histogram for decryption operation duration
    pub decryption_duration: Histogram,
    /// Histogram for KMS operation duration
    pub kms_duration: HistogramVec,
    /// Counter for data encrypted (bytes)
    pub data_encrypted_bytes: Counter,
    /// Counter for data decrypted (bytes)
    pub data_decrypted_bytes: Counter,
}

impl EncryptionMetrics {
    pub fn new(registry: &Registry) -> Result<Self, Box<dyn std::error::Error>> {
        let encryption_operations = Counter::new(
            "rustfs_encryption_operations_total",
            "Total number of encryption operations",
        )?;
        registry.register(Box::new(encryption_operations.clone()))?;

        let decryption_operations = Counter::new(
            "rustfs_decryption_operations_total",
            "Total number of decryption operations",
        )?;
        registry.register(Box::new(decryption_operations.clone()))?;

        let encryption_failures = Counter::new(
            "rustfs_encryption_failures_total",
            "Total number of encryption failures",
        )?;
        registry.register(Box::new(encryption_failures.clone()))?;

        let decryption_failures = Counter::new(
            "rustfs_decryption_failures_total",
            "Total number of decryption failures",
        )?;
        registry.register(Box::new(decryption_failures.clone()))?;

        let kms_operations = CounterVec::new(
            prometheus::Opts::new("rustfs_kms_operations_total", "Total number of KMS operations"),
            &["operation"],
        )?;
        registry.register(Box::new(kms_operations.clone()))?;

        let encryption_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "rustfs_encryption_duration_seconds",
                "Duration of encryption operations in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )?;
        registry.register(Box::new(encryption_duration.clone()))?;

        let decryption_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "rustfs_decryption_duration_seconds",
                "Duration of decryption operations in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )?;
        registry.register(Box::new(decryption_duration.clone()))?;

        let kms_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "rustfs_kms_duration_seconds",
                "Duration of KMS operations in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["operation"],
        )?;
        registry.register(Box::new(kms_duration.clone()))?;

        let data_encrypted_bytes = Counter::new(
            "rustfs_data_encrypted_bytes_total",
            "Total bytes encrypted",
        )?;
        registry.register(Box::new(data_encrypted_bytes.clone()))?;

        let data_decrypted_bytes = Counter::new(
            "rustfs_data_decrypted_bytes_total",
            "Total bytes decrypted",
        )?;
        registry.register(Box::new(data_decrypted_bytes.clone()))?;

        Ok(Self {
            encryption_operations,
            decryption_operations,
            encryption_failures,
            decryption_failures,
            kms_operations,
            encryption_duration,
            decryption_duration,
            kms_duration,
            data_encrypted_bytes,
            data_decrypted_bytes,
        })
    }

    pub fn record_encryption(&self, duration: std::time::Duration, bytes: u64) {
        self.encryption_operations.inc();
        self.encryption_duration.observe(duration.as_secs_f64());
        self.data_encrypted_bytes.inc_by(bytes as f64);
    }

    pub fn record_decryption(&self, duration: std::time::Duration, bytes: u64) {
        self.decryption_operations.inc();
        self.decryption_duration.observe(duration.as_secs_f64());
        self.data_decrypted_bytes.inc_by(bytes as f64);
    }

    pub fn record_kms_operation(&self, operation: &str, duration: std::time::Duration) {
        self.kms_operations.with_label_values(&[operation]).inc();
        self.kms_duration
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());
    }

    pub fn record_encryption_failure(&self) {
        self.encryption_failures.inc();
    }

    pub fn record_decryption_failure(&self) {
        self.decryption_failures.inc();
    }
}

pub struct EncryptionTimer {
    start: Instant,
    metrics: Arc<EncryptionMetrics>,
    operation: String,
    bytes: u64,
}

impl EncryptionTimer {
    pub fn new(metrics: Arc<EncryptionMetrics>, operation: &str, bytes: u64) -> Self {
        Self {
            start: Instant::now(),
            metrics,
            operation: operation.to_string(),
            bytes,
        }
    }

    pub fn finish(self) {
        let duration = self.start.elapsed();
        match self.operation.as_str() {
            "encrypt" => self.metrics.record_encryption(duration, self.bytes),
            "decrypt" => self.metrics.record_decryption(duration, self.bytes),
            _ => (), // Handle other operations if needed
        }
    }
}

impl Drop for EncryptionTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        match self.operation.as_str() {
            "encrypt" => self.metrics.record_encryption(duration, self.bytes),
            "decrypt" => self.metrics.record_decryption(duration, self.bytes),
            _ => (),
        }
    }
}