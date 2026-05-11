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

//! SSH server entry point for the SFTP subsystem.
//!
//! Owns the russh server, accepts incoming SSH connections, performs
//! password authentication against IAM, and dispatches the SFTP subsystem
//! request to a per-session driver instance.
//!
//! Cipher, KEX, MAC, and host key algorithm lists are compile-time constants.
//! There are no env var overrides for the crypto allowlist.

use super::config::{SftpConfig, SftpInitError};
use super::constants::limits::{
    DEFAULT_BACKEND_OP_TIMEOUT_SECS, DEFAULT_HANDLES_PER_SESSION, HANDSHAKE_DEADLINE_SECS, KEEPALIVE_INTERVAL_SECS,
    KEEPALIVE_MAX, READ_CACHE_TOTAL_MEM_DEFAULT, READ_CACHE_WINDOW_DEFAULT, SSH_CHANNEL_BUFFER_SIZE, SSH_EVENT_BUFFER_SIZE,
    SSH_MAXIMUM_PACKET_SIZE,
};
use super::constants::protocol::SFTP_SUBSYSTEM_NAME;
use super::lifecycle::{SessionDiag, SessionRegistry, new_session_registry};
use super::wedge_watchdog;
use crate::common::client::s3::StorageBackend;
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
use russh::keys::{self, PrivateKey, PublicKeyBase64};
use russh::server::{Auth, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Pty, Sig};
use rustfs_config::{
    DEFAULT_SFTP_HOST_KEY_RELOAD_ENABLE, DEFAULT_SFTP_HOST_KEY_RELOAD_INTERVAL, ENV_SFTP_HOST_KEY_RELOAD_ENABLE,
    ENV_SFTP_HOST_KEY_RELOAD_INTERVAL,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::{Duration, MissedTickBehavior, timeout};
use tokio_util::sync::CancellationToken;

use crate::sftp::constants::limits::SHUTDOWN_DRAIN_TIMEOUT_SECS;

// Cipher, KEX, MAC, and host-key algorithm lists. All four are compile-time
// constants with no environment-variable override, so operators cannot
// accidentally downgrade to weak ciphers.

/// AEAD ciphers only. When an AEAD cipher is negotiated the MAC is implicit.
const SFTP_CIPHERS: &[russh::cipher::Name] = &[
    russh::cipher::CHACHA20_POLY1305,
    russh::cipher::AES_256_GCM,
    russh::cipher::AES_128_GCM,
];

/// Key exchange algorithms in preference order.
/// Post-quantum hybrid first, then modern elliptic curve, then FIPS DH and
/// ECDH-NIST, then the two mandatory extension markers.
const SFTP_KEX: &[russh::kex::Name] = &[
    russh::kex::MLKEM768X25519_SHA256,
    russh::kex::CURVE25519,
    russh::kex::CURVE25519_PRE_RFC_8731,
    russh::kex::DH_G16_SHA512,
    russh::kex::ECDH_SHA2_NISTP256,
    russh::kex::ECDH_SHA2_NISTP384,
    russh::kex::EXTENSION_SUPPORT_AS_SERVER,
    russh::kex::EXTENSION_OPENSSH_STRICT_KEX_AS_SERVER,
];

/// ETM-only MACs as defence-in-depth. The cipher list above is
/// AEAD-only so these MACs are unused under the current configuration.
/// They guard against a future cipher list change that adds a
/// non-AEAD cipher.
const SFTP_MACS: &[russh::mac::Name] = &[russh::mac::HMAC_SHA512_ETM, russh::mac::HMAC_SHA256_ETM];

/// Host key signature algorithms. ssh-rsa (SHA-1) is explicitly absent.
const SFTP_HOST_KEY_ALGORITHMS: &[keys::Algorithm] = &[
    keys::Algorithm::Ed25519,
    keys::Algorithm::Ecdsa {
        curve: keys::EcdsaCurve::NistP256,
    },
    keys::Algorithm::Ecdsa {
        curve: keys::EcdsaCurve::NistP384,
    },
    keys::Algorithm::Rsa {
        hash: Some(keys::HashAlg::Sha512),
    },
    keys::Algorithm::Rsa {
        hash: Some(keys::HashAlg::Sha256),
    },
];

/// Compression is disabled. SSH requires the "none" method, and all clients
/// support it. zlib compression adds CPU cost, has been a historical source
/// of vulnerabilities, and provides minimal benefit for SFTP workloads
/// where payloads are typically already compressed (images, archives, etc).
const SFTP_COMPRESSION: &[russh::compression::Name] = &[russh::compression::NONE];

fn build_preferred() -> russh::Preferred {
    russh::Preferred {
        kex: Cow::Borrowed(SFTP_KEX),
        key: Cow::Borrowed(SFTP_HOST_KEY_ALGORITHMS),
        cipher: Cow::Borrowed(SFTP_CIPHERS),
        mac: Cow::Borrowed(SFTP_MACS),
        compression: Cow::Borrowed(SFTP_COMPRESSION),
    }
}

fn build_ssh_config(host_keys: Vec<PrivateKey>, idle_timeout_secs: u64, banner: &str) -> Arc<russh::server::Config> {
    Arc::new(russh::server::Config {
        server_id: russh::SshId::Standard(Cow::from(banner.to_owned())),
        methods: MethodSet::from(&[MethodKind::Password][..]),
        // No artificial delay on auth failure. Matches the S3 and FTPS
        // baseline where auth failures return immediately.
        auth_rejection_time: std::time::Duration::from_secs(0),
        auth_rejection_time_initial: None,
        keys: host_keys,
        preferred: build_preferred(),
        inactivity_timeout: Some(std::time::Duration::from_secs(idle_timeout_secs)),
        keepalive_interval: Some(std::time::Duration::from_secs(KEEPALIVE_INTERVAL_SECS)),
        keepalive_max: KEEPALIVE_MAX,
        nodelay: true,
        // Rationale for the three values below lives on the constants.
        maximum_packet_size: SSH_MAXIMUM_PACKET_SIZE,
        channel_buffer_size: SSH_CHANNEL_BUFFER_SIZE,
        event_buffer_size: SSH_EVENT_BUFFER_SIZE,
        ..Default::default()
    })
}

#[derive(Debug)]
struct SshConfigHolder {
    current: RwLock<Arc<russh::server::Config>>,
    fingerprint: RwLock<u64>,
}

impl SshConfigHolder {
    fn new(config: Arc<russh::server::Config>) -> Self {
        let fingerprint = fingerprint_host_keys(&config.keys);
        Self {
            current: RwLock::new(config),
            fingerprint: RwLock::new(fingerprint),
        }
    }

    fn get(&self) -> Arc<russh::server::Config> {
        match self.current.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    async fn reload_from_config(&self, config: &SftpConfig) -> Result<Option<usize>, SftpInitError> {
        let host_keys = SftpConfig::load_host_keys(&config.host_key_dir).await?;
        let host_key_count = host_keys.len();
        let fingerprint = fingerprint_host_keys(&host_keys);
        let ssh_config = build_ssh_config(host_keys, config.idle_timeout_secs, &config.banner);

        let mut fingerprint_guard = match self.fingerprint.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *fingerprint_guard == fingerprint {
            return Ok(None);
        }

        match self.current.write() {
            Ok(mut guard) => *guard = ssh_config,
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = ssh_config;
            }
        }
        *fingerprint_guard = fingerprint;

        Ok(Some(host_key_count))
    }
}

fn fingerprint_host_keys(host_keys: &[PrivateKey]) -> u64 {
    let mut hasher = DefaultHasher::new();
    let mut public_keys: Vec<(u8, String)> = host_keys
        .iter()
        .map(|key| (host_key_algorithm_rank(key.algorithm()), key.public_key_base64()))
        .collect();
    public_keys.sort_unstable();

    for (algorithm_rank, public_key_base64) in public_keys {
        hasher.write_u8(algorithm_rank);
        hasher.write_usize(public_key_base64.len());
        hasher.write(public_key_base64.as_bytes());
    }

    hasher.finish()
}

fn host_key_algorithm_rank(algorithm: keys::Algorithm) -> u8 {
    match algorithm {
        keys::Algorithm::Ed25519 => 0,
        keys::Algorithm::Ecdsa { .. } => 1,
        keys::Algorithm::Rsa { .. } => 2,
        _ => 3,
    }
}

fn spawn_host_key_reload_loop(config: SftpConfig, holder: Arc<SshConfigHolder>, shutdown_token: CancellationToken) {
    let enabled = rustfs_utils::get_env_bool(ENV_SFTP_HOST_KEY_RELOAD_ENABLE, DEFAULT_SFTP_HOST_KEY_RELOAD_ENABLE);
    if !enabled {
        tracing::debug!(
            "SFTP host key hot reload is disabled (set {}=1 to enable)",
            ENV_SFTP_HOST_KEY_RELOAD_ENABLE
        );
        return;
    }

    let interval_secs =
        rustfs_utils::get_env_u64(ENV_SFTP_HOST_KEY_RELOAD_INTERVAL, DEFAULT_SFTP_HOST_KEY_RELOAD_INTERVAL).max(5);

    tracing::info!(
        host_key_dir = %config.host_key_dir.display(),
        interval_secs,
        "SFTP host key hot reload enabled"
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!(host_key_dir = %config.host_key_dir.display(), "SFTP host key hot reload task stopped");
                    break;
                }
                _ = interval.tick() => {}
            }

            match holder.reload_from_config(&config).await {
                Ok(Some(host_key_count)) => {
                    tracing::info!(
                        host_key_dir = %config.host_key_dir.display(),
                        host_key_count,
                        "SFTP host keys reloaded successfully"
                    );
                }
                Ok(None) => {
                    tracing::debug!(
                        host_key_dir = %config.host_key_dir.display(),
                        "SFTP host key material unchanged; skipping reload"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        host_key_dir = %config.host_key_dir.display(),
                        err = %err,
                        "SFTP host key reload failed; keeping previous keys"
                    );
                }
            }
        }
    });
}

/// SSH server hosting the SFTP subsystem.
pub struct SftpServer<S: StorageBackend> {
    config: SftpConfig,
    ssh_config: Arc<SshConfigHolder>,
    storage: S,
    /// Weak refs to live per-session activity records. Walked by the
    /// per-session wedge watchdog and by external observers that
    /// enumerate live sessions.
    session_registry: Arc<SessionRegistry>,
    /// Process-wide accumulator of live read cache memory in bytes,
    /// shared across every per-session SftpDriver. The Arc is cloned
    /// into each driver and from there into every per-handle
    /// ReadCache. Calls to the populate method on any cache, and the
    /// Drop impl on any cache, update this one global total. The
    /// total is enforced against config.read_cache_total_mem_bytes
    /// by the read_inner pre-populate check.
    read_cache_in_use: Arc<AtomicU64>,
}

impl<S: StorageBackend + Debug> Debug for SftpServer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpServer").field("config", &self.config).finish()
    }
}

impl<S> SftpServer<S>
where
    S: StorageBackend + Clone + Send + Sync + 'static + Debug,
{
    /// Build a new server from validated configuration and loaded host keys.
    pub fn new(config: SftpConfig, storage: S, host_keys: Vec<PrivateKey>) -> Result<Self, SftpInitError> {
        let ssh_config = Arc::new(SshConfigHolder::new(build_ssh_config(
            host_keys,
            config.idle_timeout_secs,
            &config.banner,
        )));
        Ok(Self {
            config,
            ssh_config,
            storage,
            session_registry: Arc::new(new_session_registry()),
            read_cache_in_use: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Borrow the configuration the server was built with.
    pub fn config(&self) -> &SftpConfig {
        &self.config
    }

    /// Start accepting SSH connections until a shutdown signal is received.
    ///
    /// Each accepted TCP stream is driven on a task tracked in a JoinSet.
    /// On shutdown the accept loop exits, then waits up to
    /// SHUTDOWN_DRAIN_TIMEOUT_SECS for the live session tasks to finish.
    /// When a session task ends, the Drop impl on SftpDriver runs and
    /// issues AbortMultipartUpload for every live upload_id where the
    /// cached abort_authorized is set to true. Sessions still running
    /// after SHUTDOWN_DRAIN_TIMEOUT_SECS are cancelled when the JoinSet
    /// is dropped. Stale upload_ids are reclaimed by the bucket
    /// AbortIncompleteMultipartUpload lifecycle rule.
    ///
    /// Hot-path design: completed sessions are drained at the top of
    /// every loop iteration via JoinSet::try_join_next, which never
    /// blocks. The select! below uses biased selection so accept is
    /// polled first on every iteration. This combination prevents any
    /// interaction between the session drain and the accept-of-next-
    /// connection. A second select! arm for join_next under tokio's
    /// unbiased random pick could delay accept for closely spaced
    /// connections where one session finishes as another arrives.
    /// Draining at loop top is synchronous and cannot preempt accept.
    pub async fn start(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), SftpInitError> {
        let listener = TcpListener::bind(self.config.bind_addr)
            .await
            .map_err(|e| SftpInitError::Server(format!("failed to bind {}: {}", self.config.bind_addr, e)))?;
        tracing::info!(bind_addr = %self.config.bind_addr, "SFTP server listening");

        let mut sessions: JoinSet<()> = JoinSet::new();
        // Parent cancellation token for the lifetime of this listener.
        // Each per-session cancel_token is a child via child_token(),
        // and the wedge watchdog selects on the same child. On
        // shutdown_rx fire below this token is cancelled before the
        // accept loop breaks, which cascades to every live session
        // and every watchdog: the watchdog tasks shut their dup'd
        // sockets so russh's inner tasks unblock at the next read,
        // and the session tasks drop the RunningSession futures and
        // return. drain_sessions then catches up much faster than
        // the SHUTDOWN_DRAIN_TIMEOUT_SECS ceiling because no session
        // has to wait for the watchdog's natural tick to fire.
        let server_shutdown_token = CancellationToken::new();
        spawn_host_key_reload_loop(self.config.clone(), Arc::clone(&self.ssh_config), server_shutdown_token.child_token());

        loop {
            self.drain_finished_tasks(&mut sessions);

            tokio::select! {
                // Accept has explicit priority. The biased pick plus the
                // synchronous drain above keep connection handoff deterministic.
                biased;
                accept_result = listener.accept() => {
                    self.handle_accept(accept_result, &mut sessions, &server_shutdown_token);
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!(
                        live_sessions = sessions.len(),
                        "SFTP server received shutdown signal",
                    );
                    // Cascade cancellation to every live session and
                    // its watchdog before the drain loop runs. Wedged
                    // sessions need their watchdog to call shutdown on
                    // the dup'd socket so russh's inner task can end
                    // by EOF; otherwise drain_sessions would block on
                    // those sessions for the full
                    // SHUTDOWN_DRAIN_TIMEOUT_SECS.
                    server_shutdown_token.cancel();
                    break;
                }
            }
        }

        drain_sessions(sessions).await;
        Ok(())
    }

    /// Drain finished session tasks from the JoinSet. Synchronous
    /// (try_join_next never blocks) and idempotent. Logs one event
    /// per drained task: debug for clean ends and cancellations,
    /// error for panics.
    fn drain_finished_tasks(&self, sessions: &mut JoinSet<()>) {
        while let Some(res) = sessions.try_join_next() {
            let live = sessions.len();
            match res {
                Ok(()) => tracing::debug!(live_sessions = live, "SFTP session task finished"),
                Err(e) if e.is_panic() => {
                    tracing::error!(err = %e, live_sessions = live, "SFTP session task panicked")
                }
                Err(e) => tracing::debug!(err = %e, live_sessions = live, "SFTP session task cancelled"),
            }
        }
    }

    /// Process one accept-loop result. On Ok, build the per-session
    /// state (SessionDiag, watchdog dup socket, child cancel token,
    /// SshSessionHandler) and spawn run_session into the JoinSet. On
    /// Err, log and return without spawning.
    fn handle_accept(
        &self,
        accept_result: std::io::Result<(TcpStream, SocketAddr)>,
        sessions: &mut JoinSet<()>,
        server_shutdown_token: &CancellationToken,
    ) {
        let (stream, peer_addr) = match accept_result {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(err = %e, "failed to accept connection");
                return;
            }
        };

        let ssh_config = self.ssh_config.get();
        // Capture local_addr for the wedge watchdog's TCP-state probe.
        // Failure here only happens if the kernel can no longer name
        // the accepted socket. Fall back to an unspecified address
        // that will not match any /proc/net/tcp row, so the probe
        // returns None and the watchdog uses its fallback silence
        // threshold rather than refusing to spawn.
        let local_addr = stream.local_addr().unwrap_or_else(|_| SocketAddr::from(([0u8; 4], 0)));
        let session_diag = Arc::new(SessionDiag::new(local_addr, peer_addr));
        {
            // The registry holds a Vec<Weak<SessionDiag>>. A poisoned
            // lock is recovered with PoisonError::into_inner: the Vec
            // is still consistent across panics, and the accept loop
            // must keep running.
            let mut reg = self.session_registry.lock().unwrap_or_else(|poisoned| {
                tracing::warn!("session registry mutex poisoned, recovering");
                poisoned.into_inner()
            });
            reg.push(Arc::downgrade(&session_diag));
        }
        let handler_session_diag = Arc::clone(&session_diag);
        let watchdog_session_diag = Arc::clone(&session_diag);
        // Duplicate the socket via the safe AsFd path before
        // run_stream consumes the TcpStream, so the watchdog can
        // shut the socket down on wedge detection without racing
        // russh for the original fd. The wedge probe itself reads
        // /proc/net/tcp[6] in lifecycle::probe_tcp_state and does
        // not touch this dup.
        let watchdog_socket = wedge_watchdog::dup_socket(&stream);
        if watchdog_socket.is_none() {
            tracing::warn!(
                peer = %peer_addr,
                session_id = session_diag.session_id,
                "wedge watchdog: dup_socket failed, session has no wedge protection (rare; usually fd exhaustion)",
            );
        }
        // Per-session cancellation token. Cascades from the
        // listener-wide server_shutdown_token so a graceful server
        // shutdown ends every live session promptly without waiting
        // on the watchdog's natural tick cadence.
        let session_shutdown_token = server_shutdown_token.child_token();
        let handler = SshSessionHandler {
            storage: Arc::new(self.storage.clone()),
            peer_addr,
            session_context: None,
            channels: HashMap::new(),
            read_only: self.config.read_only,
            part_size: self.config.part_size,
            handles_per_session: self.config.handles_per_session.unwrap_or(DEFAULT_HANDLES_PER_SESSION),
            backend_op_timeout_secs: self.config.backend_op_timeout_secs.unwrap_or(DEFAULT_BACKEND_OP_TIMEOUT_SECS),
            read_cache_window: self.config.read_cache_window_bytes.unwrap_or(READ_CACHE_WINDOW_DEFAULT),
            read_cache_total_mem_limit: self.config.read_cache_total_mem_bytes.unwrap_or(READ_CACHE_TOTAL_MEM_DEFAULT),
            read_cache_in_use: Arc::clone(&self.read_cache_in_use),
            session_diag: handler_session_diag,
        };

        tracing::debug!(
            peer = %peer_addr,
            // sessions.len() reads pre-spawn, so add one to include
            // the session about to be inserted.
            live_sessions = sessions.len() + 1,
            session_id = session_diag.session_id,
            "SFTP accept: spawning session task",
        );
        sessions.spawn(run_session(
            ssh_config,
            stream,
            handler,
            watchdog_socket,
            watchdog_session_diag,
            session_shutdown_token,
            peer_addr,
        ));
    }
}

#[cfg(all(test, unix))]
mod hot_reload_tests {
    use super::*;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::Path;
    use tempfile::TempDir;

    const PEM_BOUNDARY_DASHES: &str = "-----";
    const PEM_OPENSSH_LABEL: &str = "OPENSSH PRIVATE KEY";

    fn build_pem_block(body: &str) -> String {
        format!("{d}BEGIN {l}{d}\n{body}\n{d}END {l}{d}\n", d = PEM_BOUNDARY_DASHES, l = PEM_OPENSSH_LABEL,)
    }

    fn test_ed25519_pem() -> String {
        build_pem_block(
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n\
             QyNTUxOQAAACCkeMEUpnJEbOMBXiQfjZcHZMEbHW3DlNRL+Jbi1cIqMgAAAKDviRiQ74kY\n\
             kAAAAAtzc2gtZWQyNTUxOQAAACCkeMEUpnJEbOMBXiQfjZcHZMEbHW3DlNRL+Jbi1cIqMg\n\
             AAAEBb5q0DpuL1Rbx4CHUEaRQRSVn1xS2SF+A+qES7OkhrOKR4wRSmckRs4wFeJB+Nlwdk\n\
             wRsdbcOU1Ev4luLVwioyAAAAGHNpbW9uc0B1YnVudHUtbGludXgtMjQwNAECAwQF",
        )
    }

    fn test_ecdsa_pem() -> String {
        build_pem_block(
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS\n\
             1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQSBp+cYoqTsQzIF+eQS23gIOBFkIqhi\n\
             M8u54NeDrEyxKSewEHP+5i6/+1HURUWDnW+YfS6nbfGb8GxBkJ2ghVvZAAAAqPpS97P6Uv\n\
             ezAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBIGn5xiipOxDMgX5\n\
             5BLbeAg4EWQiqGIzy7ng14OsTLEpJ7AQc/7mLr/7UdRFRYOdb5h9Lqdt8ZvwbEGQnaCFW9\n\
             kAAAAgBdQn3JuP2lSrY3082L+jmYvESyPu9bSmzUe8yMuILzIAAAALdGVzdC12ZWN0b3IB\n\
             AgMEBQ==",
        )
    }

    fn write_file_with_mode(path: &Path, content: &str, mode: u32) {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true).mode(mode);
        let mut file = opts.open(path).expect("open file");
        std::io::Write::write_all(&mut file, content.as_bytes()).expect("write file");
    }

    fn test_config(host_key_dir: &Path) -> SftpConfig {
        SftpConfig {
            bind_addr: "0.0.0.0:2222".parse().unwrap(),
            host_key_dir: host_key_dir.to_path_buf(),
            idle_timeout_secs: 600,
            part_size: 16 * 1024 * 1024,
            handles_per_session: None,
            backend_op_timeout_secs: None,
            read_cache_window_bytes: None,
            read_cache_total_mem_bytes: None,
            read_only: false,
            banner: "SSH-2.0-RustFS".to_string(),
        }
    }

    #[tokio::test]
    async fn ssh_config_holder_reload_replaces_host_keys_for_new_sessions() {
        let dir = TempDir::new().expect("tempdir");
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);

        let config = test_config(dir.path());
        let initial_keys = SftpConfig::load_host_keys(dir.path()).await.expect("initial key load");
        let holder = SshConfigHolder::new(build_ssh_config(initial_keys, config.idle_timeout_secs, &config.banner));
        assert!(matches!(holder.get().keys[0].algorithm(), russh::keys::Algorithm::Ed25519));

        std::fs::remove_file(dir.path().join("ssh_host_ed25519_key")).expect("remove old key");
        write_file_with_mode(&dir.path().join("ssh_host_ecdsa_key"), &test_ecdsa_pem(), 0o600);

        let reloaded = holder.reload_from_config(&config).await.expect("reload host keys");
        assert_eq!(reloaded, Some(1));
        assert!(matches!(holder.get().keys[0].algorithm(), russh::keys::Algorithm::Ecdsa { .. }));
    }

    #[tokio::test]
    async fn ssh_config_holder_reload_skips_when_host_keys_are_unchanged() {
        let dir = TempDir::new().expect("tempdir");
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);

        let config = test_config(dir.path());
        let initial_keys = SftpConfig::load_host_keys(dir.path()).await.expect("initial key load");
        let holder = SshConfigHolder::new(build_ssh_config(initial_keys, config.idle_timeout_secs, &config.banner));

        let reloaded = holder.reload_from_config(&config).await.expect("reload host keys");
        assert_eq!(reloaded, None);
    }

    #[tokio::test]
    async fn fingerprint_host_keys_is_order_independent() {
        let dir = TempDir::new().expect("tempdir");
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);
        write_file_with_mode(&dir.path().join("ssh_host_ecdsa_key"), &test_ecdsa_pem(), 0o600);

        let keys = SftpConfig::load_host_keys(dir.path()).await.expect("load keys");
        let forward = fingerprint_host_keys(&keys);
        let reversed_keys: Vec<_> = keys.into_iter().rev().collect();
        let reversed = fingerprint_host_keys(&reversed_keys);

        assert_eq!(forward, reversed);
    }
}

/// Drive one accepted SSH session through handshake, optional
/// watchdog spawn, the post-handshake session loop, and cleanup.
/// Free function (not a method) so the spawn closure on the JoinSet
/// does not have to satisfy a 'static bound on a borrow of &self.
#[allow(clippy::too_many_arguments)]
async fn run_session<S>(
    ssh_config: Arc<russh::server::Config>,
    stream: tokio::net::TcpStream,
    handler: SshSessionHandler<S>,
    watchdog_socket: Option<socket2::Socket>,
    watchdog_session_diag: Arc<SessionDiag>,
    cancel_token: CancellationToken,
    peer_addr: SocketAddr,
) where
    S: StorageBackend + Send + Sync + 'static,
{
    tracing::debug!(peer = %peer_addr, "SFTP session task entered");
    // run_stream covers SSH KEX and password auth. Cap with a
    // wallclock deadline so a peer that completes TCP but stalls
    // before KEXINIT (or that drives KEX or auth so slowly that no
    // SSH-layer timer fires) cannot pin a spawn-task slot forever.
    // The post-handshake session loop has its own inactivity and
    // keepalive timers.
    let handshake_deadline = Duration::from_secs(HANDSHAKE_DEADLINE_SECS);
    let session = match timeout(handshake_deadline, russh::server::run_stream(ssh_config, stream, handler)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::debug!(peer = %peer_addr, err = %e, "SSH session setup failed");
            return;
        }
        Err(_elapsed) => {
            tracing::warn!(
                peer = %peer_addr,
                deadline_secs = HANDSHAKE_DEADLINE_SECS,
                "SSH handshake exceeded deadline; dropping connection",
            );
            return;
        }
    };
    tracing::debug!(peer = %peer_addr, "SFTP session run_stream returned; awaiting session loop");
    // Spawn the per-session wedge watchdog. The watchdog observes
    // the SFTP-handler activity stamp and a non-blocking peek on
    // the duplicated socket. On wedge detection it shuts the
    // socket down (so russh's inner task unwedges via EOF
    // propagation) and cancels the shared CancellationToken (so
    // this task drops RunningSession and ends).
    if let Some(socket) = watchdog_socket {
        wedge_watchdog::spawn_for_session(watchdog_session_diag, socket, cancel_token.clone());
    }
    // Await the RunningSession until the client disconnects, until
    // russh's inactivity_timeout and keepalive layers (set in
    // build_ssh_config) close a wedged peer, or until the watchdog
    // (or the listener-wide shutdown cascade) cancels.
    let session_result = tokio::select! {
        res = session => Some(res),
        _ = cancel_token.cancelled() => None,
    };
    // Either branch ends the watchdog: the cancel arm because
    // cancel already fired, the await arm by signalling cancel here
    // so the watchdog task exits its loop.
    cancel_token.cancel();
    let session_result = match session_result {
        Some(r) => r,
        None => {
            tracing::warn!(peer = %peer_addr, "SFTP session aborted by wedge watchdog");
            return;
        }
    };
    match session_result {
        Ok(()) => {
            tracing::debug!(peer = %peer_addr, "SFTP session ended cleanly");
        }
        Err(e) => {
            tracing::debug!(peer = %peer_addr, err = %e, "SSH session ended with error");
        }
    }
}

/// Wait up to SHUTDOWN_DRAIN_TIMEOUT_SECS for every live session task to
/// finish. Sessions that do not return within the window are cancelled
/// when drain_sessions returns and the JoinSet is dropped.
///
/// Scope: this drain covers the per-connection session tasks only. The
/// AbortMultipartUpload tasks that SftpDriver::Drop spawns via
/// tokio::spawn are fire-and-forget and are NOT tracked here, because
/// Drop is synchronous and cannot hand back a JoinHandle. Abort tasks
/// that do not complete before the runtime shuts down fall to the
/// bucket AbortIncompleteMultipartUpload lifecycle rule. Tracking them
/// would require Drop to own a shared JoinSet, which contradicts the
/// per-session ownership model.
async fn drain_sessions(mut sessions: JoinSet<()>) {
    if sessions.is_empty() {
        return;
    }
    let drain = async {
        while let Some(res) = sessions.join_next().await {
            if let Err(e) = res
                && e.is_panic()
            {
                tracing::error!(err = %e, "SFTP session task panicked during drain");
            }
        }
    };
    match timeout(Duration::from_secs(SHUTDOWN_DRAIN_TIMEOUT_SECS), drain).await {
        Ok(()) => tracing::info!("SFTP session drain complete"),
        Err(_) => tracing::warn!(
            timeout_secs = SHUTDOWN_DRAIN_TIMEOUT_SECS,
            live = sessions.len(),
            "SFTP session drain timed out, cancelling remaining sessions",
        ),
    }
}

/// Per-connection SSH handler. Implements russh::server::Handler.
///
/// Handles authentication against IAM and dispatches the SFTP subsystem.
/// All non-SFTP channel types are rejected.
struct SshSessionHandler<S: StorageBackend> {
    /// S3 storage backend shared across all sessions.
    storage: Arc<S>,

    /// Client IP from the TCP connection. Used for logging and for
    /// building SessionContext.
    peer_addr: SocketAddr,

    /// Session context built after successful auth_password.
    /// None before authentication.
    session_context: Option<SessionContext>,

    /// Open channels indexed by ChannelId. A HashMap rather than
    /// Option because SSH permits multiple concurrent channels per
    /// connection (RFC 4254 section 5.1).
    channels: HashMap<ChannelId, Channel<Msg>>,

    /// Whether write operations are rejected.
    read_only: bool,

    /// S3 multipart part size in bytes, forwarded to every per-session
    /// SftpDriver at subsystem_request time.
    part_size: u64,

    /// Maximum number of simultaneously-open SFTP handles per session,
    /// forwarded to every per-session SftpDriver at subsystem_request
    /// time.
    handles_per_session: usize,

    /// Per-call deadline applied to every StorageBackend invocation,
    /// forwarded to every per-session SftpDriver at subsystem_request
    /// time. Resolved from RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS or
    /// DEFAULT_BACKEND_OP_TIMEOUT_SECS at server-build time.
    backend_op_timeout_secs: u64,

    /// Per-handle read cache window size in bytes, forwarded to every
    /// per-session SftpDriver at subsystem_request time. Resolved
    /// from RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES or
    /// READ_CACHE_WINDOW_DEFAULT at server-build time.
    read_cache_window: u64,

    /// Process-wide cumulative read cache memory ceiling in bytes,
    /// forwarded to every per-session SftpDriver at subsystem_request
    /// time. Resolved from RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES or
    /// READ_CACHE_TOTAL_MEM_DEFAULT at server-build time.
    read_cache_total_mem_limit: u64,

    /// Process-wide accumulator of live read cache memory in bytes,
    /// shared across every per-session SftpDriver. The Arc is cloned
    /// from SftpServer.read_cache_in_use so all drivers contribute to
    /// one global total.
    read_cache_in_use: Arc<AtomicU64>,

    /// Per-session activity record. Stamped from auth_password and
    /// subsystem_request at session level, then handed to the per-session
    /// SftpDriver where the SFTP-handler stamps live.
    session_diag: Arc<SessionDiag>,
}

impl<S: StorageBackend + Send + Sync + 'static> russh::server::Handler for SshSessionHandler<S> {
    type Error = russh::Error;

    #[tracing::instrument(level = "warn", skip(self), fields(user = %user, peer = %self.peer_addr))]
    fn auth_none(&mut self, user: &str) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        async { Ok(Auth::reject()) }
    }

    // NOTE: the russh 0.60 default for auth_publickey_offered is
    // Auth::Accept, so this override is mandatory to prevent
    // signature verification running for an auth method that is
    // not offered.
    #[tracing::instrument(level = "debug", skip(self, _key), fields(user = %user, peer = %self.peer_addr))]
    fn auth_publickey_offered(
        &mut self,
        user: &str,
        _key: &keys::PublicKey,
    ) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        async { Ok(Auth::reject()) }
    }

    #[tracing::instrument(level = "warn", skip(self, _key), fields(user = %user, peer = %self.peer_addr))]
    fn auth_publickey(
        &mut self,
        user: &str,
        _key: &keys::PublicKey,
    ) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        async { Ok(Auth::reject()) }
    }

    #[tracing::instrument(level = "info", skip(self, password), fields(user = %user, peer = %self.peer_addr))]
    fn auth_password(
        &mut self,
        user: &str,
        password: &str,
    ) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        let user = user.to_owned();
        let password = password.to_owned();
        let peer_addr = self.peer_addr;
        let session_diag = Arc::clone(&self.session_diag);
        session_diag.stamp();

        async move {
            let iam_sys = match rustfs_iam::get() {
                Ok(sys) => sys,
                Err(e) => {
                    tracing::error!(err = %e, "IAM system unavailable");
                    return Ok(Auth::reject());
                }
            };

            let (identity_opt, is_valid) = match iam_sys.check_key(&user).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        user = %user,
                        err = %e,
                        "IAM check_key error"
                    );
                    return Ok(Auth::reject());
                }
            };

            let identity = match identity_opt {
                Some(id) => id,
                None => {
                    tracing::warn!(
                        user = %user,
                        peer = %peer_addr,
                        "SFTP auth rejected: unknown access key"
                    );
                    return Ok(Auth::reject());
                }
            };

            // Reject disabled or expired accounts. FTPS checks this at
            // ftps/server.rs:286. SFTP must do the same.
            if !is_valid {
                tracing::warn!(
                    user = %user,
                    peer = %peer_addr,
                    "SFTP auth rejected: account disabled or expired"
                );
                return Ok(Auth::reject());
            }

            // Constant-time secret comparison to prevent timing side-channel
            // attacks. Same primitive used by rustfs/src/auth.rs.
            use subtle::ConstantTimeEq;
            let secret_matches: bool = identity.credentials.secret_key.as_bytes().ct_eq(password.as_bytes()).into();

            if !secret_matches {
                tracing::warn!(
                    user = %user,
                    peer = %peer_addr,
                    "SFTP auth rejected: invalid secret key"
                );
                return Ok(Auth::reject());
            }

            let principal = ProtocolPrincipal::new(Arc::new(identity));
            self.session_context = Some(SessionContext::new(principal, Protocol::Sftp, peer_addr.ip()));

            tracing::info!(
                user = %user,
                peer = %peer_addr,
                "SFTP auth accepted"
            );
            Ok(Auth::Accept)
        }
    }

    #[tracing::instrument(level = "debug", skip(self, channel, _session), fields(peer = %self.peer_addr))]
    fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        let id = channel.id();
        self.channels.insert(id, channel);
        async { Ok(true) }
    }

    #[tracing::instrument(level = "debug", skip(self, _session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.channels.remove(&channel);
        async { Ok(()) }
    }

    fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.session_diag.stamp();
        // Inputs the future needs once we decide to actually run the SFTP
        // driver. None means "rejected synchronously. Just return Ok".
        struct RunInputs<S: StorageBackend> {
            stream: russh::ChannelStream<Msg>,
            storage: Arc<S>,
            session_context: SessionContext,
            read_only: bool,
            part_size: u64,
            handles_per_session: usize,
            backend_op_timeout_secs: u64,
            read_cache_window: u64,
            read_cache_total_mem_limit: u64,
            read_cache_in_use: Arc<AtomicU64>,
            session_diag: Arc<SessionDiag>,
        }

        let inputs: Result<Option<RunInputs<S>>, Self::Error> = (|| {
            if name != SFTP_SUBSYSTEM_NAME {
                tracing::warn!(
                    subsystem = %name,
                    peer = %self.peer_addr,
                    "rejecting unsupported subsystem"
                );
                session.channel_failure(channel_id)?;
                return Ok(None);
            }

            let channel = match self.channels.remove(&channel_id) {
                Some(ch) => ch,
                None => {
                    tracing::error!(
                        channel = ?channel_id,
                        "subsystem_request: no channel found"
                    );
                    session.channel_failure(channel_id)?;
                    return Ok(None);
                }
            };

            let session_context = match self.session_context.clone() {
                Some(ctx) => ctx,
                None => {
                    tracing::error!("subsystem_request before authentication");
                    session.channel_failure(channel_id)?;
                    return Ok(None);
                }
            };

            session.channel_success(channel_id)?;

            Ok(Some(RunInputs {
                stream: channel.into_stream(),
                storage: Arc::clone(&self.storage),
                session_context,
                read_only: self.read_only,
                part_size: self.part_size,
                handles_per_session: self.handles_per_session,
                backend_op_timeout_secs: self.backend_op_timeout_secs,
                read_cache_window: self.read_cache_window,
                read_cache_total_mem_limit: self.read_cache_total_mem_limit,
                read_cache_in_use: Arc::clone(&self.read_cache_in_use),
                session_diag: Arc::clone(&self.session_diag),
            }))
        })();

        async move {
            if let Some(inputs) = inputs? {
                // russh_sftp::server::run is async and spawns its own
                // task internally. Awaiting the returned future ensures
                // the spawn completes before subsystem_request returns.
                let driver = super::driver::SftpDriver::new(
                    inputs.storage,
                    inputs.session_context,
                    inputs.read_only,
                    inputs.part_size,
                    inputs.handles_per_session,
                    inputs.backend_op_timeout_secs,
                    inputs.read_cache_window,
                    inputs.read_cache_total_mem_limit,
                    inputs.read_cache_in_use,
                    inputs.session_diag,
                );
                russh_sftp::server::run(inputs.stream, driver).await;
            }
            Ok(())
        }
    }

    // Every channel-type method russh exposes is overridden explicitly so
    // a russh default flip from "reject" to "accept" cannot silently turn
    // this into a general SSH host. SFTP subsystem only.

    #[tracing::instrument(level = "warn", skip(self, _modes, session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn pty_request(
        &mut self,
        channel: ChannelId,
        _term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let result = session.channel_failure(channel);
        async move {
            result?;
            Ok(())
        }
    }

    #[tracing::instrument(level = "warn", skip(self, session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let result = session.channel_failure(channel);
        async move {
            result?;
            Ok(())
        }
    }

    #[tracing::instrument(level = "warn", skip(self, _data, session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn exec_request(
        &mut self,
        channel: ChannelId,
        _data: &[u8],
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let result = session.channel_failure(channel);
        async move {
            result?;
            Ok(())
        }
    }

    // Signal requests carry want_reply = false on the wire (RFC 4254
    // section 6.9), so there is no channel_failure to send. The
    // override exists to log probe attempts and to keep the SFTP
    // server SFTP-only by code rather than by relying on the russh
    // default.
    #[tracing::instrument(level = "warn", skip(self, _session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn signal(
        &mut self,
        channel: ChannelId,
        signal: Sig,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        tracing::warn!(channel = ?channel, signal = ?signal, "rejecting SFTP signal request");
        async { Ok(()) }
    }

    #[tracing::instrument(level = "warn", skip(self, session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn env_request(
        &mut self,
        channel: ChannelId,
        _variable_name: &str,
        _variable_value: &str,
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let result = session.channel_failure(channel);
        async move {
            result?;
            Ok(())
        }
    }

    #[tracing::instrument(level = "warn", skip(self, session), fields(peer = %self.peer_addr, channel = ?channel))]
    fn x11_request(
        &mut self,
        channel: ChannelId,
        _single_connection: bool,
        _x11_auth_protocol: &str,
        _x11_auth_cookie: &str,
        _x11_screen_number: u32,
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let result = session.channel_failure(channel);
        async move {
            result?;
            Ok(())
        }
    }

    #[tracing::instrument(level = "warn", skip(self, _session), fields(peer = %self.peer_addr, address = %address))]
    fn tcpip_forward(
        &mut self,
        address: &str,
        _port: &mut u32,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    #[tracing::instrument(level = "warn", skip(self, _session), fields(peer = %self.peer_addr, address = %address))]
    fn cancel_tcpip_forward(
        &mut self,
        address: &str,
        _port: u32,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    #[tracing::instrument(level = "warn", skip(self, _session), fields(peer = %self.peer_addr, channel = ?_channel))]
    fn agent_request(
        &mut self,
        _channel: ChannelId,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    // Channel-open rejections. russh 0.60 defaults all of these to
    // Ok(false), but we override them explicitly with a warn log so
    // (a) probe attempts are visible in operator logs and
    // (b) a future russh default flip cannot silently allow these
    //     channel types.

    #[tracing::instrument(level = "warn", skip(self, _channel, _session), fields(peer = %self.peer_addr, host = %host_to_connect, port = port_to_connect))]
    fn channel_open_direct_tcpip(
        &mut self,
        _channel: Channel<Msg>,
        host_to_connect: &str,
        port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    #[tracing::instrument(level = "warn", skip(self, _channel, _session), fields(peer = %self.peer_addr, host = %host_to_connect, port = port_to_connect))]
    fn channel_open_forwarded_tcpip(
        &mut self,
        _channel: Channel<Msg>,
        host_to_connect: &str,
        port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    #[tracing::instrument(level = "warn", skip(self, _channel, _session), fields(peer = %self.peer_addr))]
    fn channel_open_x11(
        &mut self,
        _channel: Channel<Msg>,
        _originator_address: &str,
        _originator_port: u32,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }

    #[tracing::instrument(level = "warn", skip(self, _channel, _session), fields(peer = %self.peer_addr, socket = %socket_path))]
    fn channel_open_direct_streamlocal(
        &mut self,
        _channel: Channel<Msg>,
        socket_path: &str,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(false) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_sha1_rsa_in_host_key_algorithms() {
        let preferred = build_preferred();
        for algorithm in preferred.key.iter() {
            if let keys::Algorithm::Rsa { hash } = algorithm {
                assert!(hash.is_some(), "ssh-rsa SHA-1 must not appear in host key list");
            }
        }
    }

    #[test]
    fn strict_kex_server_marker_present() {
        let preferred = build_preferred();
        assert!(
            preferred.kex.contains(&russh::kex::EXTENSION_OPENSSH_STRICT_KEX_AS_SERVER),
            "Terrapin strict-KEX server marker must be in KEX list"
        );
    }

    #[test]
    fn ext_info_server_marker_present() {
        let preferred = build_preferred();
        assert!(
            preferred.kex.contains(&russh::kex::EXTENSION_SUPPORT_AS_SERVER),
            "ext-info-s must be in KEX list for server-sig-algs extension"
        );
    }

    #[test]
    fn all_ciphers_are_aead() {
        // If a non-AEAD cipher is added the MAC list must be reviewed.
        let cipher_names: Vec<&str> = SFTP_CIPHERS.iter().map(|c| c.as_ref()).collect();
        for name in &cipher_names {
            assert!(
                name.contains("poly1305") || name.contains("gcm"),
                "cipher {} is not AEAD; review MAC list if adding non-AEAD ciphers",
                name
            );
        }
    }

    #[test]
    fn cipher_preference_order() {
        // ChaCha20-Poly1305 must be first: constant-time on all hardware.
        assert_eq!(SFTP_CIPHERS[0], russh::cipher::CHACHA20_POLY1305);
        // AES-256-GCM before AES-128-GCM: prefer larger key size.
        assert_eq!(SFTP_CIPHERS[1], russh::cipher::AES_256_GCM);
        assert_eq!(SFTP_CIPHERS[2], russh::cipher::AES_128_GCM);
    }

    #[test]
    fn kex_preference_order() {
        // Post-quantum hybrid must be first for forward secrecy.
        assert_eq!(SFTP_KEX[0], russh::kex::MLKEM768X25519_SHA256);
        // curve25519 (RFC 8731) must come before pre-RFC variant.
        assert_eq!(SFTP_KEX[1], russh::kex::CURVE25519);
        assert_eq!(SFTP_KEX[2], russh::kex::CURVE25519_PRE_RFC_8731);
    }

    #[test]
    fn host_key_algorithm_preference_order() {
        // Ed25519 must be first: strongest, fastest, no nonce pitfalls.
        assert_eq!(SFTP_HOST_KEY_ALGORITHMS[0], keys::Algorithm::Ed25519);
        // RSA must come after ECDSA (ECDSA is smaller and faster).
        let first_rsa = SFTP_HOST_KEY_ALGORITHMS
            .iter()
            .position(|a| matches!(a, keys::Algorithm::Rsa { .. }))
            .expect("RSA must be in host key list");
        let first_ecdsa = SFTP_HOST_KEY_ALGORITHMS
            .iter()
            .position(|a| matches!(a, keys::Algorithm::Ecdsa { .. }))
            .expect("ECDSA must be in host key list");
        assert!(first_ecdsa < first_rsa, "ECDSA must appear before RSA in preference order");
    }

    #[test]
    fn ssh_config_zombie_connection_protection() {
        let config = build_ssh_config(Vec::new(), 600, "SSH-2.0-RustFS");

        // Idle timeout kills connections with no activity.
        assert_eq!(config.inactivity_timeout, Some(std::time::Duration::from_secs(600)),);

        // Keepalive probes detect dead TCP connections where the client
        // disappeared without sending FIN.
        assert_eq!(config.keepalive_interval, Some(std::time::Duration::from_secs(KEEPALIVE_INTERVAL_SECS)),);
        assert_eq!(config.keepalive_max, KEEPALIVE_MAX);
    }

    #[test]
    fn ssh_config_has_zero_auth_rejection_delay() {
        let config = build_ssh_config(Vec::new(), 600, "SSH-2.0-RustFS");
        assert_eq!(
            config.auth_rejection_time,
            std::time::Duration::from_secs(0),
            "auth rejection time must be zero to match S3/FTPS baseline"
        );
    }

    #[test]
    fn ssh_config_advertises_only_password() {
        let config = build_ssh_config(Vec::new(), 600, "SSH-2.0-RustFS");
        assert!(config.methods.contains(&MethodKind::Password));
        assert!(!config.methods.contains(&MethodKind::PublicKey));
    }
}
