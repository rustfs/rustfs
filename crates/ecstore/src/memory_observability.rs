
/// Check mimalloc arena configuration and log diagnostics
pub fn log_mimalloc_diagnostics() {
    #[cfg(feature = "mimalloc")]
    {
        use rustfs_mimalloc::MiMalloc;
        
        // Check arena_max_object_size
        let arena_max_obj_size = MiMalloc::option_get_size(
            rustfs_mimalloc_sys::mi_option_t::mi_option_arena_max_object_size
        );
        tracing::info!(
            arena_max_object_size_bytes = arena_max_obj_size,
            "mimalloc arena_max_object_size"
        );
        
        // Check if pagemap is enabled
        let pagemap_commit = MiMalloc::option_is_enabled(
            rustfs_mimalloc_sys::mi_option_t::mi_option_pagemap_commit
        );
        tracing::info!(
            pagemap_commit = pagemap_commit,
            "mimalloc pagemap_commit"
        );
        
        // Log version
        let version = MiMalloc::version();
        tracing::info!(
            mimalloc_version = version,
            "mimalloc version"
        );
    }
}
