//! Static ECStore layout boundaries.
//!
//! This module owns read-only layout descriptors used to keep static set
//! topology separate from runtime `Sets`/`SetDisks` orchestration before any
//! file moves happen.

pub(crate) mod disks_layout;
pub(crate) mod endpoint;
pub(crate) mod endpoints;
pub(crate) mod format;
pub(crate) mod set_heal;
pub(crate) mod set_layout;
