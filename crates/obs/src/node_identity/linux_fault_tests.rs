// Copyright 2026 RustFS Team
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

use super::{current_local_node_identity, local_node_identity_test_guard};

fn install_netlink_fault(action: u32) {
    // This test-only filter applies to the calling worker and its descendants,
    // never to another test thread. Offsets follow the x86_64 seccomp_data ABI.
    let mut instructions = [
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 4,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 1,
            jf: 0,
            k: 0xc000_003e,
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_KILL_PROCESS,
        },
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 3,
            k: u32::try_from(libc::SYS_socket).expect("x86_64 socket syscall number fits u32"),
        },
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 16,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: u32::try_from(libc::AF_NETLINK).expect("netlink address family fits u32"),
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: action,
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
    ];
    let program = libc::sock_fprog {
        len: u16::try_from(instructions.len()).expect("fault filter instruction count fits u16"),
        filter: instructions.as_mut_ptr(),
    };

    // SAFETY: prctl has the documented argument layout; program and its fixed
    // instruction array remain alive until the kernel has copied the filter.
    let no_new_privileges = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, libc::c_ulong::from(1_u32), 0_u64, 0_u64, 0_u64) };
    assert_eq!(no_new_privileges, 0, "enable worker-local seccomp: {}", std::io::Error::last_os_error());
    // SAFETY: the kernel reads the live sock_fprog and copies its bounded array
    // during this call; SECCOMP_MODE_FILTER does not retain either pointer.
    let installed = unsafe {
        libc::prctl(
            libc::PR_SET_SECCOMP,
            libc::c_ulong::from(libc::SECCOMP_MODE_FILTER),
            std::ptr::from_ref(&program),
            0_u64,
            0_u64,
        )
    };
    assert_eq!(installed, 0, "install worker-local netlink fault: {}", std::io::Error::last_os_error());
}

async fn with_faulted_worker<T: Send + 'static>(initial: &str, action: u32, work: impl FnOnce() -> T + Send + 'static) -> T {
    let _guard = local_node_identity_test_guard().await;
    let previous = rustfs_common::get_global_local_node_name().await;
    rustfs_common::set_global_local_node_name(initial).await;
    let worker = std::thread::spawn(move || {
        install_netlink_fault(action);
        work()
    });
    let result = worker.join();
    rustfs_common::set_global_local_node_name(&previous).await;
    result.expect("faulted identity worker should complete")
}

#[tokio::test]
async fn initialized_identity_does_not_enter_netlink_even_when_it_would_trap() {
    let identity = with_faulted_worker("node-a:9000", libc::SECCOMP_RET_TRAP, current_local_node_identity).await;

    assert_eq!(identity, "node-a:9000");
}

#[tokio::test]
async fn missing_identity_keeps_loopback_fallback_when_netlink_is_denied() {
    let action = libc::SECCOMP_RET_ERRNO | u32::try_from(libc::EPERM).expect("EPERM fits seccomp errno data");
    let (address_missing, identity) = with_faulted_worker("", action, || {
        // A real dependency call confirms that both address families encounter
        // the injected kernel failure before checking the production entry.
        (rustfs_utils::get_local_ip().is_none(), current_local_node_identity())
    })
    .await;

    assert!(address_missing, "netlink denial must make real address discovery unavailable");
    assert_eq!(identity, "127.0.0.1");
}

#[tokio::test]
async fn missing_identity_recovers_after_publication_with_netlink_denied() {
    let action = libc::SECCOMP_RET_ERRNO | u32::try_from(libc::EPERM).expect("EPERM fits seccomp errno data");
    let runtime = tokio::runtime::Handle::current();
    let (unavailable, fallback, published) = with_faulted_worker("", action, move || {
        let unavailable = rustfs_common::try_get_global_local_node_name().is_none();
        let fallback = current_local_node_identity();
        runtime.block_on(rustfs_common::set_global_local_node_name("node-b:9000"));
        (unavailable, fallback, current_local_node_identity())
    })
    .await;

    assert!(unavailable, "the real nonblocking getter must observe the missing name");
    assert_eq!(fallback, "127.0.0.1");
    assert_eq!(published, "node-b:9000");
}
