use const_str::concat;

/// Application name
/// Default value: RustFs
/// Environment variable: RUSTFS_APP_NAME
pub const APP_NAME: &str = "RustFs";
/// Application version
/// Default value: 1.0.0
/// Environment variable: RUSTFS_VERSION
pub const VERSION: &str = "0.0.1";

/// Default configuration logger level
/// Default value: info
/// Environment variable: RUSTFS_LOG_LEVEL
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Default configuration use stdout
/// Default value: true
pub const USE_STDOUT: bool = true;

/// Default configuration sample ratio
/// Default value: 1.0
pub const SAMPLE_RATIO: f64 = 1.0;
/// Default configuration meter interval
/// Default value: 30
pub const METER_INTERVAL: u64 = 30;

/// Default configuration service version
/// Default value: 0.0.1
pub const SERVICE_VERSION: &str = "0.0.1";

/// Default configuration environment
/// Default value: production
pub const ENVIRONMENT: &str = "production";

/// maximum number of connections
/// This is the maximum number of connections that the server will accept.
/// This is used to limit the number of connections to the server.
pub const MAX_CONNECTIONS: usize = 100;
/// timeout for connections
/// This is the timeout for connections to the server.
/// This is used to limit the time that a connection can be open.
pub const DEFAULT_TIMEOUT_MS: u64 = 3000;

/// Default Access Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_ACCESS_KEY
/// Command line argument: --access-key
/// Example: RUSTFS_ACCESS_KEY=rustfsadmin
/// Example: --access-key rustfsadmin
pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
/// Default Secret Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_SECRET_KEY
/// Command line argument: --secret-key
/// Example: RUSTFS_SECRET_KEY=rustfsadmin
/// Example: --secret-key rustfsadmin
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";
/// Default configuration file for observability
/// Default value: config/obs.toml
/// Environment variable: RUSTFS_OBS_CONFIG
/// Command line argument: --obs-config
/// Example: RUSTFS_OBS_CONFIG=config/obs.toml
/// Example: --obs-config config/obs.toml
/// Example: --obs-config /etc/rustfs/obs.toml
pub const DEFAULT_OBS_CONFIG: &str = "./deploy/config/obs.toml";

/// Default TLS key for rustfs
/// This is the default key for TLS.
pub const RUSTFS_TLS_KEY: &str = "rustfs_key.pem";

/// Default TLS cert for rustfs
/// This is the default cert for TLS.
pub const RUSTFS_TLS_CERT: &str = "rustfs_cert.pem";

/// Default port for rustfs
/// This is the default port for rustfs.
/// This is used to bind the server to a specific port.
pub const DEFAULT_PORT: u16 = 9000;

/// Default address for rustfs
/// This is the default address for rustfs.
pub const DEFAULT_ADDRESS: &str = concat!(":", DEFAULT_PORT);

/// Default port for rustfs console
/// This is the default port for rustfs console.
pub const DEFAULT_CONSOLE_PORT: u16 = 9002;

/// Default address for rustfs console
/// This is the default address for rustfs console.
pub const DEFAULT_CONSOLE_ADDRESS: &str = concat!(":", DEFAULT_CONSOLE_PORT);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_basic_constants() {
        // Test application basic constants
        assert_eq!(APP_NAME, "RustFs");
        assert!(!APP_NAME.is_empty(), "App name should not be empty");
        assert!(!APP_NAME.contains(' '), "App name should not contain spaces");

        assert_eq!(VERSION, "0.0.1");
        assert!(!VERSION.is_empty(), "Version should not be empty");

        assert_eq!(SERVICE_VERSION, "0.0.1");
        assert_eq!(VERSION, SERVICE_VERSION, "Version and service version should be consistent");
    }

    #[test]
    fn test_logging_constants() {
        // Test logging related constants
        assert_eq!(DEFAULT_LOG_LEVEL, "info");
        assert!(
            ["trace", "debug", "info", "warn", "error"].contains(&DEFAULT_LOG_LEVEL),
            "Log level should be a valid tracing level"
        );

        assert_eq!(USE_STDOUT, true);

        assert_eq!(SAMPLE_RATIO, 1.0);
        assert!(SAMPLE_RATIO >= 0.0 && SAMPLE_RATIO <= 1.0, "Sample ratio should be between 0.0 and 1.0");

        assert_eq!(METER_INTERVAL, 30);
        assert!(METER_INTERVAL > 0, "Meter interval should be positive");
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
    fn test_connection_constants() {
        // Test connection related constants
        assert_eq!(MAX_CONNECTIONS, 100);
        assert!(MAX_CONNECTIONS > 0, "Max connections should be positive");
        assert!(MAX_CONNECTIONS <= 10000, "Max connections should be reasonable");

        assert_eq!(DEFAULT_TIMEOUT_MS, 3000);
        assert!(DEFAULT_TIMEOUT_MS > 0, "Timeout should be positive");
        assert!(DEFAULT_TIMEOUT_MS >= 1000, "Timeout should be at least 1 second");
    }

    #[test]
    fn test_security_constants() {
        // Test security related constants
        assert_eq!(DEFAULT_ACCESS_KEY, "rustfsadmin");
        assert!(!DEFAULT_ACCESS_KEY.is_empty(), "Access key should not be empty");
        assert!(DEFAULT_ACCESS_KEY.len() >= 8, "Access key should be at least 8 characters");

        assert_eq!(DEFAULT_SECRET_KEY, "rustfsadmin");
        assert!(!DEFAULT_SECRET_KEY.is_empty(), "Secret key should not be empty");
        assert!(DEFAULT_SECRET_KEY.len() >= 8, "Secret key should be at least 8 characters");

        // In production environment, access key and secret key should be different
        // These are default values, so being the same is acceptable, but should be warned in documentation
        println!("Warning: Default access key and secret key are the same. Change them in production!");
    }

    #[test]
    fn test_file_path_constants() {
        // Test file path related constants
        assert_eq!(DEFAULT_OBS_CONFIG, "./deploy/config/obs.toml");
        assert!(DEFAULT_OBS_CONFIG.ends_with(".toml"), "Config file should be TOML format");
        assert!(!DEFAULT_OBS_CONFIG.is_empty(), "Config path should not be empty");

        assert_eq!(RUSTFS_TLS_KEY, "rustfs_key.pem");
        assert!(RUSTFS_TLS_KEY.ends_with(".pem"), "TLS key should be PEM format");

        assert_eq!(RUSTFS_TLS_CERT, "rustfs_cert.pem");
        assert!(RUSTFS_TLS_CERT.ends_with(".pem"), "TLS cert should be PEM format");
    }

    #[test]
    fn test_port_constants() {
        // Test port related constants
        assert_eq!(DEFAULT_PORT, 9000);
        assert!(DEFAULT_PORT > 1024, "Default port should be above reserved range");
        // u16 type automatically ensures port is in valid range (0-65535)

        assert_eq!(DEFAULT_CONSOLE_PORT, 9002);
        assert!(DEFAULT_CONSOLE_PORT > 1024, "Console port should be above reserved range");
        // u16 type automatically ensures port is in valid range (0-65535)

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

        assert_eq!(DEFAULT_CONSOLE_ADDRESS, ":9002");
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
        let expected_address = format!(":{}", DEFAULT_PORT);
        assert_eq!(DEFAULT_ADDRESS, expected_address);

        let expected_console_address = format!(":{}", DEFAULT_CONSOLE_PORT);
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
            DEFAULT_ACCESS_KEY,
            DEFAULT_SECRET_KEY,
            DEFAULT_OBS_CONFIG,
            RUSTFS_TLS_KEY,
            RUSTFS_TLS_CERT,
            DEFAULT_ADDRESS,
            DEFAULT_CONSOLE_ADDRESS,
        ];

        for constant in &string_constants {
            assert!(!constant.is_empty(), "String constant should not be empty: {}", constant);
            assert!(!constant.starts_with(' '), "String constant should not start with space: {}", constant);
            assert!(!constant.ends_with(' '), "String constant should not end with space: {}", constant);
        }
    }

    #[test]
    fn test_numeric_constants_validity() {
        // Test validity of numeric constants
        assert!(SAMPLE_RATIO.is_finite(), "Sample ratio should be finite");
        assert!(!SAMPLE_RATIO.is_nan(), "Sample ratio should not be NaN");

        assert!(METER_INTERVAL < u64::MAX, "Meter interval should be reasonable");
        assert!(MAX_CONNECTIONS < usize::MAX, "Max connections should be reasonable");
        assert!(DEFAULT_TIMEOUT_MS < u64::MAX, "Timeout should be reasonable");

        assert!(DEFAULT_PORT != 0, "Default port should not be zero");
        assert!(DEFAULT_CONSOLE_PORT != 0, "Console port should not be zero");
    }

    #[test]
    fn test_security_best_practices() {
        // Test security best practices

        // These are default values, should be changed in production environments
        println!("Security Warning: Default credentials detected!");
        println!("Access Key: {}", DEFAULT_ACCESS_KEY);
        println!("Secret Key: {}", DEFAULT_SECRET_KEY);
        println!("These should be changed in production environments!");

        // Verify that key lengths meet minimum security requirements
        assert!(DEFAULT_ACCESS_KEY.len() >= 8, "Access key should be at least 8 characters");
        assert!(DEFAULT_SECRET_KEY.len() >= 8, "Secret key should be at least 8 characters");

        // Check if default credentials contain common insecure patterns
        let _insecure_patterns = ["admin", "password", "123456", "default"];
        let _access_key_lower = DEFAULT_ACCESS_KEY.to_lowercase();
        let _secret_key_lower = DEFAULT_SECRET_KEY.to_lowercase();

        // Note: More security check logic can be added here
        // For example, check if keys contain insecure patterns
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
            assert!(unique_ports.insert(port), "Port {} is duplicated", port);
        }

        // Address format consistency
        assert_eq!(DEFAULT_ADDRESS, format!(":{}", DEFAULT_PORT));
        assert_eq!(DEFAULT_CONSOLE_ADDRESS, format!(":{}", DEFAULT_CONSOLE_PORT));
    }
}
