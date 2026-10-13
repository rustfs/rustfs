pub mod backend;
#[cfg(feature = "backend-jemalloc")]
pub mod jemalloc;
#[cfg(all(
    feature = "backend-pprof-rs",
    any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        all(
            target_os = "macos",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    )
))]
pub mod pprof;
#[cfg(all(
    feature = "backend-pprof-rs",
    any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        all(
            target_os = "macos",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    )
))]
mod pprofrs;

#[cfg(all(
    feature = "backend-pprof-rs",
    any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        all(
            target_os = "macos",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    )
))]
pub use pprof::*;

#[cfg(all(
    feature = "backend-pprof-rs",
    not(any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        all(
            target_os = "macos",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    ))
))]
compile_error!("feature `backend-pprof-rs` is only supported on Linux/macOS x86_64/aarch64");

pub mod ruleset;
pub mod tests;
pub mod types;

pub use backend::*;
#[cfg(feature = "backend-jemalloc")]
pub use jemalloc::*;
pub use ruleset::*;
pub use types::*;
