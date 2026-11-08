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

#![deny(unsafe_code)]

pub mod common;
pub mod constants;

#[cfg(feature = "ftps")]
pub mod ftps;

#[cfg(feature = "swift")]
pub mod swift;

#[cfg(feature = "webdav")]
pub mod webdav;

#[cfg(feature = "sftp")]
pub mod sftp;

pub use common::session::Protocol;
pub use common::{AuthorizationError, ProtocolPrincipal, S3Action, SessionContext, authorize_operation};

#[cfg(feature = "ftps")]
pub use ftps::{config::FtpsConfig, server::FtpsServer};

#[cfg(feature = "swift")]
pub use swift::handler::SwiftService;

#[cfg(feature = "webdav")]
pub use webdav::{config::WebDavConfig, server::WebDavServer};

#[cfg(feature = "sftp")]
pub use sftp::{SftpConfig, SftpInitError, SftpServer};

#[cfg(all(test, feature = "fips", feature = "ftps", feature = "webdav"))]
mod fips_tests {
    use crate::common::dummy_storage::DummyBackend;
    use crate::{FtpsConfig, FtpsServer, WebDavConfig, WebDavServer};

    #[test]
    fn protocols_reject_incompatible_host_provider() {
        const CHILD: &str = "RUSTFS_TEST_PROTOCOL_FIPS_PROVIDER";
        if std::env::var_os(CHILD).is_some() {
            let mut provider = rustfs_tls_runtime::default_crypto_provider();
            provider.kx_groups = vec![rustls::crypto::aws_lc_rs::kx_group::X25519];
            provider.install_default().expect("install incompatible host provider");
            let cert_dir = tempfile::tempdir().expect("certificate directory");
            let cert_dir = cert_dir.path().to_string_lossy().into_owned();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let ftps_config = FtpsConfig {
                    cert_dir: Some(cert_dir.clone()),
                    ..Default::default()
                };
                let ftps = FtpsServer::new(ftps_config, DummyBackend::default())
                    .await
                    .expect("FTPS config");
                let (_sender, receiver) = tokio::sync::broadcast::channel(1);
                let error = ftps
                    .start(receiver)
                    .await
                    .expect_err("FTPS must reject the incompatible provider");
                assert!(error.to_string().contains("process-wide rustls crypto provider"));

                let webdav_config = WebDavConfig {
                    bind_addr: ([127, 0, 0, 1], 0).into(),
                    cert_dir: Some(cert_dir),
                    ..Default::default()
                };
                let webdav = WebDavServer::new(webdav_config, DummyBackend::default())
                    .await
                    .expect("WebDAV config");
                let (_sender, receiver) = tokio::sync::broadcast::channel(1);
                let error = webdav
                    .start(receiver)
                    .await
                    .expect_err("WebDAV must reject the incompatible provider");
                assert!(error.to_string().contains("process-wide rustls crypto provider"));
            });
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("protocols_reject_incompatible_host_provider")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("NO_PROXY", "localhost,127.0.0.1,::1")
            .env("no_proxy", "localhost,127.0.0.1,::1")
            .output()
            .expect("start isolated protocol TLS test");
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
