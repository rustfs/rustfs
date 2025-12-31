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

/// Predefined CPU profiling path for RustFS server.
/// This path is used to access CPU profiling data.
pub(crate) const PROFILE_CPU_PATH: &str = "/profile/cpu";

/// This path is used to access memory profiling data.
pub(crate) const PROFILE_MEMORY_PATH: &str = "/profile/memory";

/// Favicon path to handle browser requests for the favicon.
/// This path serves the favicon.ico file.
pub(crate) const FAVICON_PATH: &str = "/favicon.ico";

/// Predefined health check path for RustFS server.
/// This path is used to check the health status of the server.
pub(crate) const HEALTH_PREFIX: &str = "/health";

/// Predefined administrative prefix for RustFS server routes.
/// This prefix is used for endpoints that handle administrative tasks
/// such as configuration, monitoring, and management.
pub(crate) const ADMIN_PREFIX: &str = "/rustfs/admin";

/// Environment variable name for overriding the default
/// administrative prefix path.
pub(crate) const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

/// Predefined console prefix for RustFS server routes.
/// This prefix is used for endpoints that handle console-related tasks
/// such as user interface and management.
pub(crate) const CONSOLE_PREFIX: &str = "/rustfs/console";

/// Predefined RPC prefix for RustFS server routes.
/// This prefix is used for endpoints that handle remote procedure calls (RPC).
pub(crate) const RPC_PREFIX: &str = "/rustfs/rpc";

/// Predefined gRPC service prefix for RustFS server.
/// This prefix is used for gRPC service endpoints.
/// For example, the full gRPC method path would be "/node_service.NodeService/MethodName".
pub(crate) const TONIC_PREFIX: &str = "/node_service.NodeService";

/// LOGO art for RustFS server.
pub(crate) const LOGO: &str = r#"

░█▀▄░█░█░█▀▀░▀█▀░█▀▀░█▀▀
░█▀▄░█░█░▀▀█░░█░░█▀▀░▀▀█
░▀░▀░▀▀▀░▀▀▀░░▀░░▀░░░▀▀▀

"#;
