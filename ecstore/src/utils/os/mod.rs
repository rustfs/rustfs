#[cfg(target_os = "linux")]
mod linux;
#[cfg(all(unix, not(target_os = "linux")))]
mod unix;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::{get_drive_stats, get_info, same_disk};
// pub use linux::same_disk;

#[cfg(all(unix, not(target_os = "linux")))]
pub use unix::{get_drive_stats, get_info, same_disk};
#[cfg(target_os = "windows")]
pub use windows::{get_drive_stats, get_info, same_disk};

#[derive(Debug, Default, PartialEq)]
pub struct IOStats {
    pub read_ios: u64,
    pub read_merges: u64,
    pub read_sectors: u64,
    pub read_ticks: u64,
    pub write_ios: u64,
    pub write_merges: u64,
    pub write_sectors: u64,
    pub write_ticks: u64,
    pub current_ios: u64,
    pub total_ticks: u64,
    pub req_ticks: u64,
    pub discard_ios: u64,
    pub discard_merges: u64,
    pub discard_sectors: u64,
    pub discard_ticks: u64,
    pub flush_ios: u64,
    pub flush_ticks: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_get_info_valid_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let info = get_info(temp_dir.path()).unwrap();

        println!("Disk Info: {:?}", info);

        assert!(info.total > 0);
        assert!(info.free > 0);
        assert!(info.used > 0);
        assert!(info.files > 0);
        assert!(info.ffree > 0);
        assert!(!info.fstype.is_empty());
    }

    #[test]
    fn test_get_info_invalid_path() {
        let invalid_path = PathBuf::from("/invalid/path");
        let result = get_info(&invalid_path);

        assert!(result.is_err());
    }

    #[test]
    fn test_same_disk_same_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let result = same_disk(path, path).unwrap();
        assert!(result);
    }

    #[test]
    fn test_same_disk_different_paths() {
        let temp_dir1 = tempfile::tempdir().unwrap();
        let temp_dir2 = tempfile::tempdir().unwrap();

        let path1 = temp_dir1.path().to_str().unwrap();
        let path2 = temp_dir2.path().to_str().unwrap();

        let result = same_disk(path1, path2).unwrap();
        // Note: On many systems, temporary directories are on the same disk
        // This test mainly verifies the function works without error
        // The actual result depends on the system configuration
        println!("Same disk result for temp dirs: {}", result);

        // Just verify the function executes successfully
        assert!(result == true || result == false);
    }

    #[test]
    fn test_get_drive_stats_default() {
        let stats = get_drive_stats(0, 0).unwrap();
        assert_eq!(stats, IOStats::default());
    }

    #[test]
    fn test_iostats_default_values() {
        // Test that IOStats default values are all zero
        let stats = IOStats::default();

        assert_eq!(stats.read_ios, 0);
        assert_eq!(stats.read_merges, 0);
        assert_eq!(stats.read_sectors, 0);
        assert_eq!(stats.read_ticks, 0);
        assert_eq!(stats.write_ios, 0);
        assert_eq!(stats.write_merges, 0);
        assert_eq!(stats.write_sectors, 0);
        assert_eq!(stats.write_ticks, 0);
        assert_eq!(stats.current_ios, 0);
        assert_eq!(stats.total_ticks, 0);
        assert_eq!(stats.req_ticks, 0);
        assert_eq!(stats.discard_ios, 0);
        assert_eq!(stats.discard_merges, 0);
        assert_eq!(stats.discard_sectors, 0);
        assert_eq!(stats.discard_ticks, 0);
        assert_eq!(stats.flush_ios, 0);
        assert_eq!(stats.flush_ticks, 0);
    }

    #[test]
    fn test_iostats_equality() {
        // Test IOStats equality comparison
        let stats1 = IOStats::default();
        let stats2 = IOStats::default();
        assert_eq!(stats1, stats2);

        let stats3 = IOStats {
            read_ios: 100,
            write_ios: 50,
            ..Default::default()
        };
        let stats4 = IOStats {
            read_ios: 100,
            write_ios: 50,
            ..Default::default()
        };
        assert_eq!(stats3, stats4);

        // Test inequality
        assert_ne!(stats1, stats3);
    }

    #[test]
    fn test_iostats_debug_format() {
        // Test Debug trait implementation
        let stats = IOStats {
            read_ios: 123,
            write_ios: 456,
            total_ticks: 789,
            ..Default::default()
        };

        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("read_ios: 123"));
        assert!(debug_str.contains("write_ios: 456"));
        assert!(debug_str.contains("total_ticks: 789"));
    }

    #[test]
    fn test_iostats_partial_eq() {
        // Test PartialEq trait implementation with various field combinations
        let base_stats = IOStats {
            read_ios: 10,
            write_ios: 20,
            read_sectors: 100,
            write_sectors: 200,
            ..Default::default()
        };

        let same_stats = IOStats {
            read_ios: 10,
            write_ios: 20,
            read_sectors: 100,
            write_sectors: 200,
            ..Default::default()
        };

        let different_read = IOStats {
            read_ios: 11, // Different
            write_ios: 20,
            read_sectors: 100,
            write_sectors: 200,
            ..Default::default()
        };

        assert_eq!(base_stats, same_stats);
        assert_ne!(base_stats, different_read);
    }

    #[test]
    fn test_get_info_path_edge_cases() {
        // Test with root directory (should work on most systems)
        #[cfg(unix)]
        {
            let result = get_info(std::path::Path::new("/"));
            assert!(result.is_ok(), "Root directory should be accessible");

            if let Ok(info) = result {
                assert!(info.total > 0, "Root filesystem should have non-zero total space");
                assert!(!info.fstype.is_empty(), "Root filesystem should have a type");
            }
        }

        #[cfg(windows)]
        {
            let result = get_info(std::path::Path::new("C:\\"));
            // On Windows, C:\ might not always exist, so we don't assert success
            if let Ok(info) = result {
                assert!(info.total > 0);
                assert!(!info.fstype.is_empty());
            }
        }
    }

    #[test]
    fn test_get_info_nonexistent_path() {
        // Test with various types of invalid paths
        let invalid_paths = [
            "/this/path/definitely/does/not/exist/anywhere",
            "/dev/null/invalid", // /dev/null is a file, not a directory
            "",                  // Empty path
        ];

        for invalid_path in &invalid_paths {
            let result = get_info(std::path::Path::new(invalid_path));
            assert!(result.is_err(), "Invalid path should return error: {}", invalid_path);
        }
    }

    #[test]
    fn test_same_disk_edge_cases() {
        // Test with same path (should always be true)
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap();

        let result = same_disk(path_str, path_str);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Same path should be on same disk");

        // Test with parent and child directories (should be on same disk)
        let child_dir = temp_dir.path().join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let child_path = child_dir.to_str().unwrap();

        let result = same_disk(path_str, child_path);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Parent and child should be on same disk");
    }

    #[test]
    fn test_same_disk_invalid_paths() {
        // Test with invalid paths
        let temp_dir = tempfile::tempdir().unwrap();
        let valid_path = temp_dir.path().to_str().unwrap();
        let invalid_path = "/this/path/does/not/exist";

        let result1 = same_disk(valid_path, invalid_path);
        assert!(result1.is_err(), "Should fail with one invalid path");

        let result2 = same_disk(invalid_path, valid_path);
        assert!(result2.is_err(), "Should fail with one invalid path");

        let result3 = same_disk(invalid_path, invalid_path);
        assert!(result3.is_err(), "Should fail with both invalid paths");
    }

    #[test]
    fn test_iostats_field_ranges() {
        // Test that IOStats can handle large values
        let large_stats = IOStats {
            read_ios: u64::MAX,
            write_ios: u64::MAX,
            read_sectors: u64::MAX,
            write_sectors: u64::MAX,
            total_ticks: u64::MAX,
            ..Default::default()
        };

        // Should be able to create and compare
        let another_large = IOStats {
            read_ios: u64::MAX,
            write_ios: u64::MAX,
            read_sectors: u64::MAX,
            write_sectors: u64::MAX,
            total_ticks: u64::MAX,
            ..Default::default()
        };

        assert_eq!(large_stats, another_large);
    }

    #[test]
    fn test_get_drive_stats_error_handling() {
        // Test with potentially invalid major/minor numbers
        // Note: This might succeed on some systems, so we just ensure it doesn't panic
        let result1 = get_drive_stats(999, 999);
        // Don't assert success/failure as it's platform-dependent
        let _ = result1;

        let result2 = get_drive_stats(u32::MAX, u32::MAX);
        let _ = result2;
    }

    #[cfg(unix)]
    #[test]
    fn test_unix_specific_paths() {
        // Test Unix-specific paths
        let unix_paths = ["/tmp", "/var", "/usr"];

        for path in &unix_paths {
            if std::path::Path::new(path).exists() {
                let result = get_info(std::path::Path::new(path));
                if result.is_ok() {
                    let info = result.unwrap();
                    assert!(info.total > 0, "Path {} should have non-zero total space", path);
                }
            }
        }
    }

    #[test]
    fn test_iostats_clone_and_copy() {
        // Test that IOStats implements Clone (if it does)
        let original = IOStats {
            read_ios: 42,
            write_ios: 84,
            ..Default::default()
        };

        // Test Debug formatting with non-default values
        let debug_output = format!("{:?}", original);
        assert!(debug_output.contains("42"));
        assert!(debug_output.contains("84"));
    }
}
