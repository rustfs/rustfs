//! Static ECStore layout boundaries.
//!
//! This module owns read-only layout descriptors used to keep static set
//! topology separate from runtime `Sets`/`SetDisks` orchestration before any
//! file moves happen.

pub(crate) mod set_layout;
