// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

//! Adaptive buffer sizing optimization for different workload types.
//!
//! This module provides intelligent buffer size selection based on file size and workload profile
//! to achieve optimal balance between performance, memory usage, and security.

use rustfs_config::{KI_B, MI_B};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// Global buffer configuration that can be set at application startup
static GLOBAL_BUFFER_CONFIG: OnceLock<RustFSBufferConfig> = OnceLock::new();

/// Global flag indicating whether buffer profiles are enabled
static BUFFER_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable buffer profiling globally
///
/// This controls whether the opt-in buffer profiling feature is active.
///
/// # Arguments
/// * `enabled` - Whether to enable buffer profiling
pub fn set_buffer_profile_enabled(enabled: bool) {
    BUFFER_PROFILE_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Check if buffer profiling is enabled globally
pub fn is_buffer_profile_enabled() -> bool {
    BUFFER_PROFILE_ENABLED.load(Ordering::Relaxed)
}

/// Initialize the global buffer configuration
///
/// This should be called once at application startup with the desired profile.
/// If not called, the default GeneralPurpose profile will be used.
///
/// # Arguments
/// * `config` - The buffer configuration to use globally
///
/// # Examples
/// ```ignore
/// use rustfs::config::workload_profiles::{RustFSBufferConfig, WorkloadProfile};
///
/// // Initialize with AiTraining profile
/// init_global_buffer_config(RustFSBufferConfig::new(WorkloadProfile::AiTraining));
/// ```
pub fn init_global_buffer_config(config: RustFSBufferConfig) {
    let _ = GLOBAL_BUFFER_CONFIG.set(config);
}

/// Get the global buffer configuration
///
/// Returns the configured profile, or GeneralPurpose if not initialized.
pub fn get_global_buffer_config() -> &'static RustFSBufferConfig {
    GLOBAL_BUFFER_CONFIG.get_or_init(RustFSBufferConfig::default)
}

/// Workload profile types that define buffer sizing strategies
#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadProfile {
    /// General purpose - default configuration with balanced performance and memory
    GeneralPurpose,
    /// AI/ML training: optimized for large sequential reads with maximum throughput
    AiTraining,
    /// Data analytics: mixed read-write patterns with moderate buffer sizes
    DataAnalytics,
    /// Web workloads: small file intensive with minimal memory overhead
    WebWorkload,
    /// Industrial IoT: real-time streaming with low latency priority
    IndustrialIoT,
    /// Secure storage: security first, memory constrained for compliance
    SecureStorage,
    /// Custom configuration for specialized requirements
    Custom(BufferConfig),
}

/// Buffer size configuration for adaptive buffering
#[derive(Debug, Clone, PartialEq)]
pub struct BufferConfig {
    /// Minimum buffer size in bytes (for very small files or memory-constrained environments)
    pub min_size: usize,
    /// Maximum buffer size in bytes (cap for large files to prevent excessive memory usage)
    pub max_size: usize,
    /// Default size for unknown file size scenarios (streaming/chunked uploads)
    pub default_unknown: usize,
    /// File size thresholds and corresponding buffer sizes: (file_size_threshold, buffer_size)
    /// Thresholds should be in ascending order
    pub thresholds: Vec<(i64, usize)>,
}

/// Complete buffer configuration for RustFS
#[derive(Debug, Clone)]
pub struct RustFSBufferConfig {
    /// Selected workload profile
    pub workload: WorkloadProfile,
    /// Computed buffer configuration (either from profile or custom)
    pub base_config: BufferConfig,
}

impl WorkloadProfile {
    /// Parse a workload profile from a string name
    ///
    /// # Arguments
    /// * `name` - The name of the profile (case-insensitive)
    ///
    /// # Returns
    /// The corresponding WorkloadProfile, or GeneralPurpose if name is not recognized
    ///
    /// # Examples
    /// ```
    /// use rustfs::config::workload_profiles::WorkloadProfile;
    ///
    /// let profile = WorkloadProfile::from_name("AiTraining");
    /// let profile2 = WorkloadProfile::from_name("aitraining"); // case-insensitive
    /// let profile3 = WorkloadProfile::from_name("unknown"); // defaults to GeneralPurpose
    /// ```
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "generalpurpose" | "general" => WorkloadProfile::GeneralPurpose,
            "aitraining" | "ai" => WorkloadProfile::AiTraining,
            "dataanalytics" | "analytics" => WorkloadProfile::DataAnalytics,
            "webworkload" | "web" => WorkloadProfile::WebWorkload,
            "industrialiot" | "iot" => WorkloadProfile::IndustrialIoT,
            "securestorage" | "secure" => WorkloadProfile::SecureStorage,
            _ => {
                // Default to GeneralPurpose for unknown profiles
                WorkloadProfile::GeneralPurpose
            }
        }
    }

    /// Get the buffer configuration for this workload profile
    pub fn config(&self) -> BufferConfig {
        match self {
            WorkloadProfile::GeneralPurpose => Self::general_purpose_config(),
            WorkloadProfile::AiTraining => Self::ai_training_config(),
            WorkloadProfile::DataAnalytics => Self::data_analytics_config(),
            WorkloadProfile::WebWorkload => Self::web_workload_config(),
            WorkloadProfile::IndustrialIoT => Self::industrial_iot_config(),
            WorkloadProfile::SecureStorage => Self::secure_storage_config(),
            WorkloadProfile::Custom(config) => config.clone(),
        }
    }

    /// General purpose configuration: balanced performance and memory usage
    /// - Small files (< 1MB): 64KB buffer
    /// - Medium files (1MB-100MB): 256KB buffer
    /// - Large files (>= 100MB): 1MB buffer
    fn general_purpose_config() -> BufferConfig {
        BufferConfig {
            min_size: 64 * KI_B,
            max_size: MI_B,
            default_unknown: MI_B,
            thresholds: vec![
                (MI_B as i64, 64 * KI_B),        // < 1MB: 64KB
                (100 * MI_B as i64, 256 * KI_B), // 1MB-100MB: 256KB
                (i64::MAX, MI_B),                // >= 100MB: 1MB
            ],
        }
    }

    /// AI/ML training configuration: optimized for large sequential reads
    /// - Small files (< 10MB): 512KB buffer
    /// - Medium files (10MB-500MB): 2MB buffer
    /// - Large files (>= 500MB): 4MB buffer for maximum throughput
    fn ai_training_config() -> BufferConfig {
        BufferConfig {
            min_size: 512 * KI_B,
            max_size: 4 * MI_B,
            default_unknown: 2 * MI_B,
            thresholds: vec![
                (10 * MI_B as i64, 512 * KI_B), // < 10MB: 512KB
                (500 * MI_B as i64, 2 * MI_B),  // 10MB-500MB: 2MB
                (i64::MAX, 4 * MI_B),           // >= 500MB: 4MB
            ],
        }
    }

    /// Data analytics configuration: mixed read-write patterns
    /// - Small files (< 5MB): 128KB buffer
    /// - Medium files (5MB-200MB): 512KB buffer
    /// - Large files (>= 200MB): 2MB buffer
    fn data_analytics_config() -> BufferConfig {
        BufferConfig {
            min_size: 128 * KI_B,
            max_size: 2 * MI_B,
            default_unknown: 512 * KI_B,
            thresholds: vec![
                (5 * MI_B as i64, 128 * KI_B),   // < 5MB: 128KB
                (200 * MI_B as i64, 512 * KI_B), // 5MB-200MB: 512KB
                (i64::MAX, 2 * MI_B),            // >= 200MB: 2MB
            ],
        }
    }

    /// Web workload configuration: small file intensive
    /// - Small files (< 512KB): 32KB buffer to minimize memory
    /// - Medium files (512KB-10MB): 128KB buffer
    /// - Large files (>= 10MB): 256KB buffer (rare for web assets)
    fn web_workload_config() -> BufferConfig {
        BufferConfig {
            min_size: 32 * KI_B,
            max_size: 256 * KI_B,
            default_unknown: 128 * KI_B,
            thresholds: vec![
                (512 * KI_B as i64, 32 * KI_B), // < 512KB: 32KB
                (10 * MI_B as i64, 128 * KI_B), // 512KB-10MB: 128KB
                (i64::MAX, 256 * KI_B),         // >= 10MB: 256KB
            ],
        }
    }

    /// Industrial IoT configuration: real-time streaming with low latency
    /// - Small files (< 1MB): 64KB buffer for quick processing
    /// - Medium files (1MB-50MB): 256KB buffer
    /// - Large files (>= 50MB): 512KB buffer (cap for memory constraints)
    fn industrial_iot_config() -> BufferConfig {
        BufferConfig {
            min_size: 64 * KI_B,
            max_size: 512 * KI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![
                (MI_B as i64, 64 * KI_B),       // < 1MB: 64KB
                (50 * MI_B as i64, 256 * KI_B), // 1MB-50MB: 256KB
                (i64::MAX, 512 * KI_B),         // >= 50MB: 512KB
            ],
        }
    }

    /// Secure storage configuration: security first, memory constrained
    /// - Small files (< 1MB): 32KB buffer (minimal memory footprint)
    /// - Medium files (1MB-50MB): 128KB buffer
    /// - Large files (>= 50MB): 256KB buffer (strict memory limit for compliance)
    fn secure_storage_config() -> BufferConfig {
        BufferConfig {
            min_size: 32 * KI_B,
            max_size: 256 * KI_B,
            default_unknown: 128 * KI_B,
            thresholds: vec![
                (MI_B as i64, 32 * KI_B),       // < 1MB: 32KB
                (50 * MI_B as i64, 128 * KI_B), // 1MB-50MB: 128KB
                (i64::MAX, 256 * KI_B),         // >= 50MB: 256KB
            ],
        }
    }

    /// Detect special OS environment and return appropriate workload profile
    /// Supports Chinese secure operating systems (Kylin, NeoKylin, Unity OS, etc.)
    pub fn detect_os_environment() -> Option<WorkloadProfile> {
        #[cfg(target_os = "linux")]
        {
            // Read /etc/os-release to detect Chinese secure OS distributions
            if let Ok(content) = std::fs::read_to_string("/etc/os-release") {
                let content_lower = content.to_lowercase();
                // Check for Chinese secure OS distributions
                if content_lower.contains("kylin")
                    || content_lower.contains("neokylin")
                    || content_lower.contains("uos")
                    || content_lower.contains("unity")
                    || content_lower.contains("openkylin")
                {
                    // Use SecureStorage profile for Chinese secure OS environments
                    return Some(WorkloadProfile::SecureStorage);
                }
            }
        }
        None
    }
}

impl BufferConfig {
    /// Calculate the optimal buffer size for a given file size
    ///
    /// # Arguments
    /// * `file_size` - The size of the file in bytes, or -1 if unknown
    ///
    /// # Returns
    /// Optimal buffer size in bytes based on the configuration
    pub fn calculate_buffer_size(&self, file_size: i64) -> usize {
        // Handle unknown or negative file sizes
        if file_size < 0 {
            return self.default_unknown.clamp(self.min_size, self.max_size);
        }

        // Find the appropriate buffer size from thresholds
        for (threshold, buffer_size) in &self.thresholds {
            if file_size < *threshold {
                return (*buffer_size).clamp(self.min_size, self.max_size);
            }
        }

        // Fallback to max_size if no threshold matched (shouldn't happen with i64::MAX threshold)
        self.max_size
    }

    /// Validate the buffer configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_size == 0 {
            return Err("min_size must be greater than 0".to_string());
        }
        if self.max_size < self.min_size {
            return Err("max_size must be >= min_size".to_string());
        }
        if self.default_unknown < self.min_size || self.default_unknown > self.max_size {
            return Err("default_unknown must be between min_size and max_size".to_string());
        }
        if self.thresholds.is_empty() {
            return Err("thresholds cannot be empty".to_string());
        }

        // Validate thresholds are in ascending order
        let mut prev_threshold = -1i64;
        for (threshold, buffer_size) in &self.thresholds {
            if *threshold <= prev_threshold {
                return Err("thresholds must be in ascending order".to_string());
            }
            if *buffer_size < self.min_size || *buffer_size > self.max_size {
                return Err(format!(
                    "buffer_size {} must be between min_size {} and max_size {}",
                    buffer_size, self.min_size, self.max_size
                ));
            }
            prev_threshold = *threshold;
        }

        Ok(())
    }
}

impl RustFSBufferConfig {
    /// Create a new buffer configuration with the given workload profile
    pub fn new(workload: WorkloadProfile) -> Self {
        let base_config = workload.config();
        Self { workload, base_config }
    }

    /// Create a configuration with auto-detected OS environment
    /// Falls back to GeneralPurpose if no special environment detected
    pub fn with_auto_detect() -> Self {
        let workload = WorkloadProfile::detect_os_environment().unwrap_or(WorkloadProfile::GeneralPurpose);
        Self::new(workload)
    }

    /// Get the buffer size for a given file size
    pub fn get_buffer_size(&self, file_size: i64) -> usize {
        self.base_config.calculate_buffer_size(file_size)
    }
}

impl Default for RustFSBufferConfig {
    fn default() -> Self {
        Self::new(WorkloadProfile::GeneralPurpose)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_general_purpose_config() {
        let config = WorkloadProfile::GeneralPurpose.config();

        // Test small files (< 1MB) - should use 64KB
        assert_eq!(config.calculate_buffer_size(0), 64 * KI_B);
        assert_eq!(config.calculate_buffer_size(512 * KI_B as i64), 64 * KI_B);
        assert_eq!(config.calculate_buffer_size((MI_B - 1) as i64), 64 * KI_B);

        // Test medium files (1MB - 100MB) - should use 256KB
        assert_eq!(config.calculate_buffer_size(MI_B as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size((50 * MI_B) as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size((100 * MI_B - 1) as i64), 256 * KI_B);

        // Test large files (>= 100MB) - should use 1MB
        assert_eq!(config.calculate_buffer_size((100 * MI_B) as i64), MI_B);
        assert_eq!(config.calculate_buffer_size((500 * MI_B) as i64), MI_B);
        assert_eq!(config.calculate_buffer_size((10 * 1024 * MI_B) as i64), MI_B);

        // Test unknown size
        assert_eq!(config.calculate_buffer_size(-1), MI_B);
    }

    #[test]
    fn test_ai_training_config() {
        let config = WorkloadProfile::AiTraining.config();

        // Test small files
        assert_eq!(config.calculate_buffer_size((5 * MI_B) as i64), 512 * KI_B);
        assert_eq!(config.calculate_buffer_size((10 * MI_B - 1) as i64), 512 * KI_B);

        // Test medium files
        assert_eq!(config.calculate_buffer_size((10 * MI_B) as i64), 2 * MI_B);
        assert_eq!(config.calculate_buffer_size((100 * MI_B) as i64), 2 * MI_B);
        assert_eq!(config.calculate_buffer_size((500 * MI_B - 1) as i64), 2 * MI_B);

        // Test large files
        assert_eq!(config.calculate_buffer_size((500 * MI_B) as i64), 4 * MI_B);
        assert_eq!(config.calculate_buffer_size((1024 * MI_B) as i64), 4 * MI_B);

        // Test unknown size
        assert_eq!(config.calculate_buffer_size(-1), 2 * MI_B);
    }

    #[test]
    fn test_web_workload_config() {
        let config = WorkloadProfile::WebWorkload.config();

        // Test small files
        assert_eq!(config.calculate_buffer_size((100 * KI_B) as i64), 32 * KI_B);
        assert_eq!(config.calculate_buffer_size((512 * KI_B - 1) as i64), 32 * KI_B);

        // Test medium files
        assert_eq!(config.calculate_buffer_size((512 * KI_B) as i64), 128 * KI_B);
        assert_eq!(config.calculate_buffer_size((5 * MI_B) as i64), 128 * KI_B);
        assert_eq!(config.calculate_buffer_size((10 * MI_B - 1) as i64), 128 * KI_B);

        // Test large files
        assert_eq!(config.calculate_buffer_size((10 * MI_B) as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size((50 * MI_B) as i64), 256 * KI_B);

        // Test unknown size
        assert_eq!(config.calculate_buffer_size(-1), 128 * KI_B);
    }

    #[test]
    fn test_secure_storage_config() {
        let config = WorkloadProfile::SecureStorage.config();

        // Test small files
        assert_eq!(config.calculate_buffer_size((500 * KI_B) as i64), 32 * KI_B);
        assert_eq!(config.calculate_buffer_size((MI_B - 1) as i64), 32 * KI_B);

        // Test medium files
        assert_eq!(config.calculate_buffer_size(MI_B as i64), 128 * KI_B);
        assert_eq!(config.calculate_buffer_size((25 * MI_B) as i64), 128 * KI_B);
        assert_eq!(config.calculate_buffer_size((50 * MI_B - 1) as i64), 128 * KI_B);

        // Test large files
        assert_eq!(config.calculate_buffer_size((50 * MI_B) as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size((100 * MI_B) as i64), 256 * KI_B);

        // Test unknown size
        assert_eq!(config.calculate_buffer_size(-1), 128 * KI_B);
    }

    #[test]
    fn test_industrial_iot_config() {
        let config = WorkloadProfile::IndustrialIoT.config();

        // Test configuration
        assert_eq!(config.calculate_buffer_size((500 * KI_B) as i64), 64 * KI_B);
        assert_eq!(config.calculate_buffer_size((25 * MI_B) as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size((100 * MI_B) as i64), 512 * KI_B);
        assert_eq!(config.calculate_buffer_size(-1), 256 * KI_B);
    }

    #[test]
    fn test_data_analytics_config() {
        let config = WorkloadProfile::DataAnalytics.config();

        // Test configuration
        assert_eq!(config.calculate_buffer_size((2 * MI_B) as i64), 128 * KI_B);
        assert_eq!(config.calculate_buffer_size((100 * MI_B) as i64), 512 * KI_B);
        assert_eq!(config.calculate_buffer_size((500 * MI_B) as i64), 2 * MI_B);
        assert_eq!(config.calculate_buffer_size(-1), 512 * KI_B);
    }

    #[test]
    fn test_custom_config() {
        let custom_config = BufferConfig {
            min_size: 16 * KI_B,
            max_size: 512 * KI_B,
            default_unknown: 128 * KI_B,
            thresholds: vec![(MI_B as i64, 64 * KI_B), (i64::MAX, 256 * KI_B)],
        };

        let profile = WorkloadProfile::Custom(custom_config.clone());
        let config = profile.config();

        assert_eq!(config.calculate_buffer_size(512 * KI_B as i64), 64 * KI_B);
        assert_eq!(config.calculate_buffer_size(2 * MI_B as i64), 256 * KI_B);
        assert_eq!(config.calculate_buffer_size(-1), 128 * KI_B);
    }

    #[test]
    fn test_buffer_config_validation() {
        // Valid configuration
        let valid_config = BufferConfig {
            min_size: 32 * KI_B,
            max_size: MI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![(MI_B as i64, 128 * KI_B), (i64::MAX, 512 * KI_B)],
        };
        assert!(valid_config.validate().is_ok());

        // Invalid: min_size is 0
        let invalid_config = BufferConfig {
            min_size: 0,
            max_size: MI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![(MI_B as i64, 128 * KI_B)],
        };
        assert!(invalid_config.validate().is_err());

        // Invalid: max_size < min_size
        let invalid_config = BufferConfig {
            min_size: MI_B,
            max_size: 32 * KI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![(MI_B as i64, 128 * KI_B)],
        };
        assert!(invalid_config.validate().is_err());

        // Invalid: default_unknown out of range
        let invalid_config = BufferConfig {
            min_size: 32 * KI_B,
            max_size: 256 * KI_B,
            default_unknown: MI_B,
            thresholds: vec![(MI_B as i64, 128 * KI_B)],
        };
        assert!(invalid_config.validate().is_err());

        // Invalid: empty thresholds
        let invalid_config = BufferConfig {
            min_size: 32 * KI_B,
            max_size: MI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![],
        };
        assert!(invalid_config.validate().is_err());

        // Invalid: thresholds not in ascending order
        let invalid_config = BufferConfig {
            min_size: 32 * KI_B,
            max_size: MI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![(100 * MI_B as i64, 512 * KI_B), (MI_B as i64, 128 * KI_B)],
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_rustfs_buffer_config() {
        let config = RustFSBufferConfig::new(WorkloadProfile::GeneralPurpose);
        assert_eq!(config.get_buffer_size(500 * KI_B as i64), 64 * KI_B);
        assert_eq!(config.get_buffer_size(50 * MI_B as i64), 256 * KI_B);
        assert_eq!(config.get_buffer_size(200 * MI_B as i64), MI_B);

        let default_config = RustFSBufferConfig::default();
        assert_eq!(default_config.get_buffer_size(500 * KI_B as i64), 64 * KI_B);
    }

    #[test]
    fn test_workload_profile_equality() {
        assert_eq!(WorkloadProfile::GeneralPurpose, WorkloadProfile::GeneralPurpose);
        assert_ne!(WorkloadProfile::GeneralPurpose, WorkloadProfile::AiTraining);

        let custom1 = BufferConfig {
            min_size: 32 * KI_B,
            max_size: MI_B,
            default_unknown: 256 * KI_B,
            thresholds: vec![(MI_B as i64, 128 * KI_B)],
        };
        let custom2 = custom1.clone();

        assert_eq!(WorkloadProfile::Custom(custom1.clone()), WorkloadProfile::Custom(custom2));
    }

    #[test]
    fn test_workload_profile_from_name() {
        // Test exact matches (case-insensitive)
        assert_eq!(WorkloadProfile::from_name("GeneralPurpose"), WorkloadProfile::GeneralPurpose);
        assert_eq!(WorkloadProfile::from_name("generalpurpose"), WorkloadProfile::GeneralPurpose);
        assert_eq!(WorkloadProfile::from_name("GENERALPURPOSE"), WorkloadProfile::GeneralPurpose);
        assert_eq!(WorkloadProfile::from_name("general"), WorkloadProfile::GeneralPurpose);

        assert_eq!(WorkloadProfile::from_name("AiTraining"), WorkloadProfile::AiTraining);
        assert_eq!(WorkloadProfile::from_name("aitraining"), WorkloadProfile::AiTraining);
        assert_eq!(WorkloadProfile::from_name("ai"), WorkloadProfile::AiTraining);

        assert_eq!(WorkloadProfile::from_name("DataAnalytics"), WorkloadProfile::DataAnalytics);
        assert_eq!(WorkloadProfile::from_name("dataanalytics"), WorkloadProfile::DataAnalytics);
        assert_eq!(WorkloadProfile::from_name("analytics"), WorkloadProfile::DataAnalytics);

        assert_eq!(WorkloadProfile::from_name("WebWorkload"), WorkloadProfile::WebWorkload);
        assert_eq!(WorkloadProfile::from_name("webworkload"), WorkloadProfile::WebWorkload);
        assert_eq!(WorkloadProfile::from_name("web"), WorkloadProfile::WebWorkload);

        assert_eq!(WorkloadProfile::from_name("IndustrialIoT"), WorkloadProfile::IndustrialIoT);
        assert_eq!(WorkloadProfile::from_name("industrialiot"), WorkloadProfile::IndustrialIoT);
        assert_eq!(WorkloadProfile::from_name("iot"), WorkloadProfile::IndustrialIoT);

        assert_eq!(WorkloadProfile::from_name("SecureStorage"), WorkloadProfile::SecureStorage);
        assert_eq!(WorkloadProfile::from_name("securestorage"), WorkloadProfile::SecureStorage);
        assert_eq!(WorkloadProfile::from_name("secure"), WorkloadProfile::SecureStorage);

        // Test unknown name defaults to GeneralPurpose
        assert_eq!(WorkloadProfile::from_name("unknown"), WorkloadProfile::GeneralPurpose);
        assert_eq!(WorkloadProfile::from_name("invalid"), WorkloadProfile::GeneralPurpose);
        assert_eq!(WorkloadProfile::from_name(""), WorkloadProfile::GeneralPurpose);
    }

    #[test]
    fn test_global_buffer_config() {
        use super::{is_buffer_profile_enabled, set_buffer_profile_enabled};

        // Test enable/disable
        set_buffer_profile_enabled(true);
        assert!(is_buffer_profile_enabled());

        set_buffer_profile_enabled(false);
        assert!(!is_buffer_profile_enabled());

        // Reset for other tests
        set_buffer_profile_enabled(false);
    }
}
