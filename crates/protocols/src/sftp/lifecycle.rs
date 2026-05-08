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

//! Per-session lifecycle bookkeeping plus the kernel TCP-state probe.
//!
//! Holds the per-session activity stamp and the weak-ref registry the
//! accept loop walks. Both are load-bearing infrastructure for the
//! per-session wedge watchdog (wedge_watchdog.rs): the watchdog uses
//! the activity stamp to decide whether a session is silent, and the
//! TCP-state probe to disambiguate slow operations from CLOSE_WAIT.
//!
//! Activity stamps are written from every SFTP handler entry/exit and
//! from auth_password / subsystem_request. They are read by the
//! watchdog tick loop.
//!
//! The TCP-state probe parses /proc/net/tcp and /proc/net/tcp6, looks
//! up the row matching the (local, peer) tuple, and returns the kernel
//! TCP state. Only Linux exposes the procfs files. On other targets
//! the probe returns None and the watchdog falls back to its absolute
//! silence threshold. Live ports are hex'd in the kernel's
//! per-architecture byte order (little-endian within each 4-byte chunk).

use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Per-session activity record. Constructed once per accepted SSH
/// connection in the accept loop, cloned via Arc into the SshSessionHandler
/// and the SftpDriver, registered weakly into the SessionRegistry so an
/// outside observer can enumerate live sessions without holding their
/// lifetime.
#[allow(dead_code)]
pub struct SessionDiag {
    pub session_id: u64,
    pub local: SocketAddr,
    pub peer: SocketAddr,
    pub accepted_at: Instant,
    pub last_activity_ms: AtomicU64,
}

impl SessionDiag {
    pub(super) fn new(local: SocketAddr, peer: SocketAddr) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        Self {
            session_id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            local,
            peer,
            accepted_at: Instant::now(),
            last_activity_ms: AtomicU64::new(now_ms),
        }
    }

    /// Update last_activity_ms to now. One Relaxed atomic store after
    /// one SystemTime read.
    pub(super) fn stamp(&self) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.last_activity_ms.store(now_ms, Ordering::Relaxed);
    }
}

/// Mutex-guarded vector of weak references to live SessionDiags. The
/// accept loop pushes a new Weak on every connection; consumers walk
/// the vector and upgrade each Weak to read the stamp, retaining only
/// those whose strong count is still positive.
pub(super) type SessionRegistry = Mutex<Vec<Weak<SessionDiag>>>;

pub(super) fn new_session_registry() -> SessionRegistry {
    Mutex::new(Vec::new())
}

/// Kernel TCP state for one connection, as reported by /proc/net/tcp[6].
/// Values follow the Linux TCP state numbering used in the procfs files.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum TcpState {
    /// 0x01. Connection is open and exchanging data.
    Established,
    /// 0x08. Peer FIN'd, the local application has not yet closed
    /// the socket. This is the wedge signature.
    CloseWait,
    /// Any other state (FIN_WAIT_1, FIN_WAIT_2, LAST_ACK, TIME_WAIT,
    /// CLOSING, etc.) carrying the raw hex byte for diagnostics. The
    /// watchdog treats these as not-yet-wedge: the connection is in a
    /// transient close handshake or steady non-wedge state.
    Other(u8),
}

/// Look up the kernel TCP state for the connection between (local, peer).
/// Reads /proc/net/tcp and /proc/net/tcp6, matches by hex'd address-port
/// tuple, and returns the parsed state.
///
/// Returns None when:
/// - /proc/net/tcp[6] cannot be read (non-Linux target, missing /proc).
/// - No row matches the requested (local, peer) tuple. Either the
///   connection has been finalised by the kernel and removed from the
///   table, or one or both addresses do not have a renderable form
///   for the relevant procfs file.
pub(super) fn probe_tcp_state(local: SocketAddr, peer: SocketAddr) -> Option<TcpState> {
    if let Ok(content) = std::fs::read_to_string("/proc/net/tcp")
        && let Some(state) = lookup_tcp_state(&content, local, peer, false)
    {
        return Some(state);
    }
    if let Ok(content) = std::fs::read_to_string("/proc/net/tcp6")
        && let Some(state) = lookup_tcp_state(&content, local, peer, true)
    {
        return Some(state);
    }
    None
}

/// Search procfs content for a row matching (local, peer). The
/// ipv6_file flag selects the address-rendering convention. tcp6
/// uses 32-character hex strings and tcp uses 8-character, both with
/// little-endian byte order within each 4-byte chunk.
fn lookup_tcp_state(content: &str, local: SocketAddr, peer: SocketAddr, ipv6_file: bool) -> Option<TcpState> {
    let local_hex = render_proc_net_tcp_addr(local, ipv6_file)?;
    let peer_hex = render_proc_net_tcp_addr(peer, ipv6_file)?;
    for line in content.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let _sl = fields.next()?;
        let f_local = fields.next()?;
        let f_peer = fields.next()?;
        let f_state = fields.next()?;
        if f_local == local_hex && f_peer == peer_hex {
            let raw = u8::from_str_radix(f_state, 16).ok()?;
            return Some(match raw {
                0x01 => TcpState::Established,
                0x08 => TcpState::CloseWait,
                other => TcpState::Other(other),
            });
        }
    }
    None
}

/// Render an IpAddr and port pair for the /proc/net/tcp[6] format. Returns
/// None when the SocketAddr cannot be expressed in the chosen file's
/// convention (e.g., a non-IPv4-mapped IPv6 address asked for tcp).
///
/// Format details:
/// - tcp: 8-character upper-case hex of the IPv4 octets in
///   little-endian order, then ':', then 4-character upper-case hex
///   of the port.
/// - tcp6: 32-character upper-case hex of the IPv6 octets in 4
///   chunks of 4 bytes, little-endian within each chunk, then ':',
///   then the same 4-character port suffix as tcp.
///
/// IPv4 SocketAddrs presented to tcp6 are mapped via ::ffff:a.b.c.d
/// before rendering. IPv4-mapped IPv6 SocketAddrs presented to tcp
/// are unwrapped before rendering. Mismatches return None.
fn render_proc_net_tcp_addr(addr: SocketAddr, ipv6_file: bool) -> Option<String> {
    let port = addr.port();
    if !ipv6_file {
        let v4 = match addr.ip() {
            IpAddr::V4(v4) => v4,
            IpAddr::V6(v6) => v6.to_ipv4_mapped()?,
        };
        let octets = v4.octets();
        Some(format!(
            "{:02X}{:02X}{:02X}{:02X}:{:04X}",
            octets[3], octets[2], octets[1], octets[0], port
        ))
    } else {
        let bytes: [u8; 16] = match addr.ip() {
            IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
            IpAddr::V6(v6) => v6.octets(),
        };
        let mut s = String::with_capacity(33);
        for chunk_idx in 0..4 {
            let start = chunk_idx * 4;
            for i in 0..4 {
                write!(&mut s, "{:02X}", bytes[start + 3 - i]).ok()?;
            }
        }
        write!(&mut s, ":{:04X}", port).ok()?;
        Some(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    #[test]
    fn render_ipv4_loopback_for_tcp_file() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        assert_eq!(render_proc_net_tcp_addr(addr, false).as_deref(), Some("0100007F:08AE"));
    }

    #[test]
    fn render_ipv4_loopback_mapped_for_tcp6_file() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        assert_eq!(
            render_proc_net_tcp_addr(addr, true).as_deref(),
            Some("0000000000000000FFFF00000100007F:08AE")
        );
    }

    #[test]
    fn render_native_ipv6_for_tcp6_file() {
        let addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 2222, 0, 0));
        // ::1 is fifteen zero bytes followed by 0x01. Chunks (LE within
        // each 4-byte word): 00000000 00000000 00000000 01000000.
        assert_eq!(
            render_proc_net_tcp_addr(addr, true).as_deref(),
            Some("00000000000000000000000001000000:08AE")
        );
    }

    #[test]
    fn render_native_ipv6_for_tcp_file_returns_none() {
        let addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 2222, 0, 0));
        // ::1 is not IPv4-mapped, so it cannot be rendered for tcp.
        assert!(render_proc_net_tcp_addr(addr, false).is_none());
    }

    #[test]
    fn lookup_finds_close_wait_in_tcp_file() {
        let content = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
                       0: 0100007F:08AE 0100007F:DEAD 08 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0\n";
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0xDEAD));
        assert_eq!(lookup_tcp_state(content, local, peer, false), Some(TcpState::CloseWait));
    }

    #[test]
    fn lookup_finds_established_in_tcp6_file() {
        let content = "  sl  local_address                         remote_address                        st\n\
                       0: 0000000000000000FFFF00000100007F:08AE 0000000000000000FFFF00000100007F:DEAD 01 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0\n";
        // SocketAddr is IPv4 form but the row is IPv4-mapped IPv6 in tcp6.
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0xDEAD));
        assert_eq!(lookup_tcp_state(content, local, peer, true), Some(TcpState::Established));
    }

    #[test]
    fn lookup_returns_none_when_no_match() {
        let content = "  sl  local_address rem_address   st\n\
                       0: 0100007F:08AE 0100007F:CAFE 01 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0\n";
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0xDEAD));
        assert_eq!(lookup_tcp_state(content, local, peer, false), None);
    }

    #[test]
    fn lookup_returns_other_for_unfamiliar_state() {
        let content = "  sl  local_address rem_address   st\n\
                       0: 0100007F:08AE 0100007F:DEAD 05 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0\n";
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2222));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0xDEAD));
        // 0x05 = FIN_WAIT_2, an Other state from the watchdog's view.
        assert_eq!(lookup_tcp_state(content, local, peer, false), Some(TcpState::Other(0x05)));
    }
}
