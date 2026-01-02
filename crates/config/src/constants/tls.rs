//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/// TLS related environment variable names and default values
/// Environment variable to enable TLS key logging
/// When set to "1", RustFS will log TLS keys to the specified file for debugging purposes.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TLS_KEYLOG=1
pub const ENV_TLS_KEYLOG: &str = "RUSTFS_TLS_KEYLOG";

/// Default value for TLS key logging
/// By default, RustFS does not log TLS keys.
/// To change this behavior, set the environment variable RUSTFS_TLS_KEYLOG=1
pub const DEFAULT_TLS_KEYLOG: bool = false;

/// Environment variable to trust system CA certificates
/// When set to "1", RustFS will trust system CA certificates in addition to any
/// custom CA certificates provided in the configuration.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TRUST_SYSTEM_CA=1
pub const ENV_TRUST_SYSTEM_CA: &str = "RUSTFS_TRUST_SYSTEM_CA";

/// Default value for trusting system CA certificates
/// By default, RustFS does not trust system CA certificates.
/// To change this behavior, set the environment variable RUSTFS_TRUST_SYSTEM_CA=1
pub const DEFAULT_TRUST_SYSTEM_CA: bool = false;

/// Environment variable to trust leaf certificates as CA
/// When set to "1", RustFS will treat leaf certificates as CA certificates for trust validation.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TRUST_LEAF_CERT_AS_CA=1
pub const ENV_TRUST_LEAF_CERT_AS_CA: &str = "RUSTFS_TRUST_LEAF_CERT_AS_CA";

/// Default value for trusting leaf certificates as CA
/// By default, RustFS does not trust leaf certificates as CA.
/// To change this behavior, set the environment variable RUSTFS_TRUST_LEAF_CERT_AS_CA=1
pub const DEFAULT_TRUST_LEAF_CERT_AS_CA: bool = false;

/// Default filename for client CA certificate
/// client_ca.crt (CA bundle for verifying client certificates in server mTLS)
pub const RUSTFS_CLIENT_CA_CERT_FILENAME: &str = "client_ca.crt";

/// Environment variable for client certificate file path
/// RUSTFS_MTLS_CLIENT_CERT
/// Specifies the file path to the client certificate used for mTLS authentication.
/// If not set, RustFS will look for the default filename "client_cert.pem" in the current directory.
/// To set, use the environment variable RUSTFS_MTLS_CLIENT_CERT=/path/to/client_cert.pem
pub const ENV_MTLS_CLIENT_CERT: &str = "RUSTFS_MTLS_CLIENT_CERT";

/// Default filename for client certificate
/// client_cert.pem
pub const RUSTFS_CLIENT_CERT_FILENAME: &str = "client_cert.pem";

/// Environment variable for client private key file path
/// RUSTFS_MTLS_CLIENT_KEY
/// Specifies the file path to the client private key used for mTLS authentication.
/// If not set, RustFS will look for the default filename "client_key.pem" in the current directory.
/// To set, use the environment variable RUSTFS_MTLS_CLIENT_KEY=/path/to/client_key.pem
pub const ENV_MTLS_CLIENT_KEY: &str = "RUSTFS_MTLS_CLIENT_KEY";

/// Default filename for client private key
/// client_key.pem
pub const RUSTFS_CLIENT_KEY_FILENAME: &str = "client_key.pem";

/// RUSTFS_SERVER_MTLS_ENABLE
/// Environment variable to enable server mTLS
/// When set to "1", RustFS server will require client certificates for authentication.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_SERVER_MTLS_ENABLE=1
pub const ENV_SERVER_MTLS_ENABLE: &str = "RUSTFS_SERVER_MTLS_ENABLE";

/// Default value for enabling server mTLS
/// By default, RustFS server mTLS is disabled.
/// To change this behavior, set the environment variable RUSTFS_SERVER_MTLS_ENABLE=1
pub const DEFAULT_SERVER_MTLS_ENABLE: bool = false;
