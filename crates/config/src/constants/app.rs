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

use const_str::concat;

/// Application name
/// Default value: RustFS
/// Environment variable: RUSTFS_APP_NAME
pub const APP_NAME: &str = "RustFS";
/// Application version
/// Default value: 1.0.0
/// Environment variable: RUSTFS_VERSION
pub const VERSION: &str = "1.0.0";

/// Default configuration logger level
/// Default value: error
/// Environment variable: RUSTFS_OBS_LOGGER_LEVEL
pub const DEFAULT_LOG_LEVEL: &str = "error";

/// Default configuration use stdout
/// Default value: false
pub const USE_STDOUT: bool = false;

/// Default configuration sample ratio
/// Default value: 1.0
pub const SAMPLE_RATIO: f64 = 1.0;
/// Default configuration meter interval
/// Default value: 30
pub const METER_INTERVAL: u64 = 30;

/// Default configuration service version
/// Default value: 1.0.0
/// Environment variable: RUSTFS_OBS_SERVICE_VERSION
/// Uses the same value as VERSION constant
pub const SERVICE_VERSION: &str = "1.0.0";

/// Default configuration environment
/// Default value: production
pub const ENVIRONMENT: &str = "production";

/// Default console enable
/// This is the default value for the console server.
/// It is used to enable or disable the console server.
/// Default value: true
/// Environment variable: RUSTFS_CONSOLE_ENABLE
/// Command line argument: --console-enable
/// Example: RUSTFS_CONSOLE_ENABLE=true
/// Example: --console-enable true
pub const DEFAULT_CONSOLE_ENABLE: bool = true;

/// Default OBS configuration endpoint
/// Environment variable: DEFAULT_OBS_ENDPOINT
/// Command line argument: --obs-endpoint
/// Example: DEFAULT_OBS_ENDPOINT="http://localost:4317"
/// Example: --obs-endpoint http://localost:4317
pub const DEFAULT_OBS_ENDPOINT: &str = "";

/// Default TLS key for rustfs
/// This is the default key for TLS.
pub const RUSTFS_TLS_KEY: &str = "rustfs_key.pem";

/// Default TLS cert for rustfs
/// This is the default cert for TLS.
pub const RUSTFS_TLS_CERT: &str = "rustfs_cert.pem";

/// Default public certificate filename for rustfs
/// This is the default public certificate filename for rustfs.
/// It is used to store the public certificate of the application.
/// Default value: public.crt
pub const RUSTFS_PUBLIC_CERT: &str = "public.crt";

/// Default CA certificate filename for rustfs
/// This is the default CA certificate filename for rustfs.
/// It is used to store the CA certificate of the application.
/// Default value: ca.crt
pub const RUSTFS_CA_CERT: &str = "ca.crt";

/// Default HTTP prefix for rustfs
/// This is the default HTTP prefix for rustfs.
/// It is used to identify HTTP URLs.
/// Default value: http://
pub const RUSTFS_HTTP_PREFIX: &str = "http://";

/// Default HTTPS prefix for rustfs
/// This is the default HTTPS prefix for rustfs.
/// It is used to identify HTTPS URLs.
/// Default value: https://
pub const RUSTFS_HTTPS_PREFIX: &str = "https://";

/// Default port for rustfs
/// This is the default port for rustfs.
/// This is used to bind the server to a specific port.
pub const DEFAULT_PORT: u16 = 9000;

/// Default address for rustfs
/// This is the default address for rustfs.
pub const DEFAULT_ADDRESS: &str = concat!(":", DEFAULT_PORT);

/// Default port for rustfs console
/// This is the default port for rustfs console.
pub const DEFAULT_CONSOLE_PORT: u16 = 9001;

/// Default address for rustfs console
/// This is the default address for rustfs console.
pub const DEFAULT_CONSOLE_ADDRESS: &str = concat!(":", DEFAULT_CONSOLE_PORT);

/// Default log filename for rustfs
/// This is the default log filename for rustfs.
/// It is used to store the logs of the application.
/// Default value: rustfs.log
/// Environment variable: RUSTFS_OBSERVABILITY_LOG_FILENAME
pub const DEFAULT_LOG_FILENAME: &str = "rustfs";

/// Default OBS log filename for rustfs
/// This is the default log filename for OBS.
/// It is used to store the logs of the application.
/// Default value: rustfs.log
pub const DEFAULT_OBS_LOG_FILENAME: &str = concat!(DEFAULT_LOG_FILENAME, "");

/// Default log directory for rustfs
/// This is the default log directory for rustfs.
/// It is used to store the logs of the application.
/// Default value: logs
/// Environment variable: RUSTFS_LOG_DIRECTORY
pub const DEFAULT_LOG_DIR: &str = "logs";

/// Default log rotation size mb for rustfs
/// This is the default log rotation size for rustfs.
/// It is used to rotate the logs of the application.
/// Default value: 100 MB
/// Environment variable: RUSTFS_OBS_LOG_ROTATION_SIZE_MB
pub const DEFAULT_LOG_ROTATION_SIZE_MB: u64 = 100;

/// Default log rotation time for rustfs
/// This is the default log rotation time for rustfs.
/// It is used to rotate the logs of the application.
/// Default value: hour, eg: day,hour,minute,second
/// Environment variable: RUSTFS_OBS_LOG_ROTATION_TIME
pub const DEFAULT_LOG_ROTATION_TIME: &str = "hour";

/// Default log keep files for rustfs
/// This is the default log keep files for rustfs.
/// It is used to keep the logs of the application.
/// Default value: 30
/// Environment variable: RUSTFS_OBS_LOG_KEEP_FILES
pub const DEFAULT_LOG_KEEP_FILES: usize = 30;

/// Default log local logging enabled for rustfs
/// This is the default log local logging enabled for rustfs.
/// It is used to enable or disable local logging of the application.
/// Default value: false
/// Environment variable: RUSTFS_OBS_LOGL_STDOUT_ENABLED
pub const DEFAULT_OBS_LOG_STDOUT_ENABLED: bool = false;

/// Constant representing 1 Kibibyte (1024 bytes)
/// Default value: 1024
pub const KI_B: usize = 1024;
/// Constant representing 1 Mebibyte (1024 * 1024 bytes)
/// Default value: 1048576
pub const MI_B: usize = 1024 * 1024;

/// Environment variable for gRPC authentication token
/// Used to set the authentication token for gRPC communication
/// Example: RUSTFS_GRPC_AUTH_TOKEN=your_token_here
/// Default value: No default value. RUSTFS_SECRET_KEY value is recommended.
pub const ENV_GRPC_AUTH_TOKEN: &str = "RUSTFS_GRPC_AUTH_TOKEN";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_basic_constants() {
        // Test application basic constants
        assert_eq!(APP_NAME, "RustFS");
        assert!(!APP_NAME.contains(' '), "App name should not contain spaces");

        assert_eq!(VERSION, "1.0.0");

        assert_eq!(SERVICE_VERSION, "1.0.0");
        assert_eq!(VERSION, SERVICE_VERSION, "Version and service version should be consistent");
    }

    #[test]
    fn test_logging_constants() {
        // Test logging related constants
        assert_eq!(DEFAULT_LOG_LEVEL, "error");
        assert!(
            ["trace", "debug", "info", "warn", "error"].contains(&DEFAULT_LOG_LEVEL),
            "Log level should be a valid tracing level"
        );

        assert_eq!(SAMPLE_RATIO, 1.0);

        assert_eq!(METER_INTERVAL, 30);
    }

    #[test]
    fn test_environment_constants() {
        // Test environment related constants
        assert_eq!(ENVIRONMENT, "production");
        assert!(
            ["development", "staging", "production", "test"].contains(&ENVIRONMENT),
            "Environment should be a standard environment name"
        );
    }

    #[test]
    fn test_file_path_constants() {
        assert_eq!(RUSTFS_TLS_KEY, "rustfs_key.pem");
        assert!(RUSTFS_TLS_KEY.ends_with(".pem"), "TLS key should be PEM format");

        assert_eq!(RUSTFS_TLS_CERT, "rustfs_cert.pem");
        assert!(RUSTFS_TLS_CERT.ends_with(".pem"), "TLS cert should be PEM format");
    }

    #[test]
    fn test_port_constants() {
        // Test port related constants
        assert_eq!(DEFAULT_PORT, 9000);

        assert_eq!(DEFAULT_CONSOLE_PORT, 9001);

        assert_ne!(DEFAULT_PORT, DEFAULT_CONSOLE_PORT, "Main port and console port should be different");
    }

    #[test]
    fn test_address_constants() {
        // Test address related constants
        assert_eq!(DEFAULT_ADDRESS, ":9000");
        assert!(DEFAULT_ADDRESS.starts_with(':'), "Address should start with colon");
        assert!(
            DEFAULT_ADDRESS.contains(&DEFAULT_PORT.to_string()),
            "Address should contain the default port"
        );

        assert_eq!(DEFAULT_CONSOLE_ADDRESS, ":9001");
        assert!(DEFAULT_CONSOLE_ADDRESS.starts_with(':'), "Console address should start with colon");
        assert!(
            DEFAULT_CONSOLE_ADDRESS.contains(&DEFAULT_CONSOLE_PORT.to_string()),
            "Console address should contain the console port"
        );

        assert_ne!(
            DEFAULT_ADDRESS, DEFAULT_CONSOLE_ADDRESS,
            "Main address and console address should be different"
        );
    }

    #[test]
    fn test_const_str_concat_functionality() {
        // Test const_str::concat macro functionality
        let expected_address = format!(":{DEFAULT_PORT}");
        assert_eq!(DEFAULT_ADDRESS, expected_address);

        let expected_console_address = format!(":{DEFAULT_CONSOLE_PORT}");
        assert_eq!(DEFAULT_CONSOLE_ADDRESS, expected_console_address);
    }

    #[test]
    fn test_string_constants_validity() {
        // Test validity of string constants
        let string_constants = [
            APP_NAME,
            VERSION,
            DEFAULT_LOG_LEVEL,
            SERVICE_VERSION,
            ENVIRONMENT,
            RUSTFS_TLS_KEY,
            RUSTFS_TLS_CERT,
            DEFAULT_ADDRESS,
            DEFAULT_CONSOLE_ADDRESS,
        ];

        for constant in &string_constants {
            assert!(!constant.is_empty(), "String constant should not be empty: {constant}");
            assert!(!constant.starts_with(' '), "String constant should not start with space: {constant}");
            assert!(!constant.ends_with(' '), "String constant should not end with space: {constant}");
        }
    }

    #[test]
    fn test_numeric_constants_validity() {
        // Test validity of numeric constants
        assert!(SAMPLE_RATIO.is_finite(), "Sample ratio should be finite");
        assert!(!SAMPLE_RATIO.is_nan(), "Sample ratio should not be NaN");

        // All these are const values, so range checks are redundant
        // assert!(METER_INTERVAL < u64::MAX, "Meter interval should be reasonable");
        // assert!(MAX_CONNECTIONS < usize::MAX, "Max connections should be reasonable");
        // assert!(DEFAULT_TIMEOUT_MS < u64::MAX, "Timeout should be reasonable");

        // These are const non-zero values, so zero checks are redundant
        assert_ne!(DEFAULT_PORT, 0, "Default port should not be zero");
        assert_ne!(DEFAULT_CONSOLE_PORT, 0, "Console port should not be zero");
    }

    #[test]
    fn test_configuration_consistency() {
        // Test configuration consistency

        // Version consistency
        assert_eq!(VERSION, SERVICE_VERSION, "Application version should match service version");

        // Port conflict check
        let ports = [DEFAULT_PORT, DEFAULT_CONSOLE_PORT];
        let mut unique_ports = std::collections::HashSet::new();
        for port in &ports {
            assert!(unique_ports.insert(port), "Port {port} is duplicated");
        }

        // Address format consistency
        assert_eq!(DEFAULT_ADDRESS, format!(":{DEFAULT_PORT}"));
        assert_eq!(DEFAULT_CONSOLE_ADDRESS, format!(":{DEFAULT_CONSOLE_PORT}"));
    }
}
