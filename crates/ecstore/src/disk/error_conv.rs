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

use crate::disk::error::DiskError;

pub fn to_file_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => DiskError::FileNotFound.into(),
        std::io::ErrorKind::PermissionDenied => DiskError::FileAccessDenied.into(),
        std::io::ErrorKind::IsADirectory => DiskError::IsNotRegular.into(),
        std::io::ErrorKind::NotADirectory => DiskError::FileAccessDenied.into(),
        std::io::ErrorKind::DirectoryNotEmpty => DiskError::FileAccessDenied.into(),
        std::io::ErrorKind::UnexpectedEof => DiskError::FaultyDisk.into(),
        std::io::ErrorKind::TooManyLinks => DiskError::TooManyOpenFiles.into(),
        std::io::ErrorKind::InvalidInput => DiskError::FileNotFound.into(),
        std::io::ErrorKind::InvalidData => DiskError::FileCorrupt.into(),
        std::io::ErrorKind::StorageFull => DiskError::DiskFull.into(),
        _ => io_err,
    }
}

pub fn to_volume_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => DiskError::VolumeNotFound.into(),
        std::io::ErrorKind::PermissionDenied => DiskError::DiskAccessDenied.into(),
        std::io::ErrorKind::DirectoryNotEmpty => DiskError::VolumeNotEmpty.into(),
        std::io::ErrorKind::NotADirectory => DiskError::IsNotRegular.into(),
        std::io::ErrorKind::Other => match io_err.downcast::<DiskError>() {
            Ok(err) => match err {
                DiskError::FileNotFound => DiskError::VolumeNotFound.into(),
                DiskError::FileAccessDenied => DiskError::DiskAccessDenied.into(),
                err => err.into(),
            },
            Err(err) => to_file_error(err),
        },
        _ => to_file_error(io_err),
    }
}

pub fn to_disk_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => DiskError::DiskNotFound.into(),
        std::io::ErrorKind::PermissionDenied => DiskError::DiskAccessDenied.into(),
        std::io::ErrorKind::Other => match io_err.downcast::<DiskError>() {
            Ok(err) => match err {
                DiskError::FileNotFound => DiskError::DiskNotFound.into(),
                DiskError::VolumeNotFound => DiskError::DiskNotFound.into(),
                DiskError::FileAccessDenied => DiskError::DiskAccessDenied.into(),
                DiskError::VolumeAccessDenied => DiskError::DiskAccessDenied.into(),
                err => err.into(),
            },
            Err(err) => to_volume_error(err),
        },
        _ => to_volume_error(io_err),
    }
}

// only errors from FileSystem operations
pub fn to_access_error(io_err: std::io::Error, per_err: DiskError) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::PermissionDenied => per_err.into(),
        std::io::ErrorKind::NotADirectory => per_err.into(),
        std::io::ErrorKind::NotFound => DiskError::VolumeNotFound.into(),
        std::io::ErrorKind::UnexpectedEof => DiskError::FaultyDisk.into(),
        std::io::ErrorKind::Other => match io_err.downcast::<DiskError>() {
            Ok(err) => match err {
                DiskError::DiskAccessDenied => per_err.into(),
                DiskError::FileAccessDenied => per_err.into(),
                DiskError::FileNotFound => DiskError::VolumeNotFound.into(),
                err => err.into(),
            },
            Err(err) => to_volume_error(err),
        },
        _ => to_volume_error(io_err),
    }
}

pub fn to_unformatted_disk_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => DiskError::UnformattedDisk.into(),
        std::io::ErrorKind::PermissionDenied => DiskError::DiskAccessDenied.into(),
        std::io::ErrorKind::UnexpectedEof => DiskError::UnformattedDisk.into(),
        std::io::ErrorKind::InvalidData => DiskError::UnformattedDisk.into(),
        std::io::ErrorKind::Other => match io_err.downcast::<DiskError>() {
            Ok(err) => match err {
                DiskError::FileNotFound => DiskError::UnformattedDisk.into(),
                DiskError::DiskNotFound => DiskError::UnformattedDisk.into(),
                DiskError::VolumeNotFound => DiskError::UnformattedDisk.into(),
                DiskError::FileAccessDenied => DiskError::DiskAccessDenied.into(),
                DiskError::DiskAccessDenied => DiskError::DiskAccessDenied.into(),
                _ => DiskError::UnformattedDisk.into(),
            },
            Err(_err) => DiskError::UnformattedDisk.into(),
        },
        _ => DiskError::UnformattedDisk.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    // Helper function to create IO errors with specific kinds
    fn create_io_error(kind: ErrorKind) -> IoError {
        IoError::new(kind, "test error")
    }

    // Helper function to create IO errors with DiskError as the source
    fn create_io_error_with_disk_error(disk_error: DiskError) -> IoError {
        IoError::other(disk_error)
    }

    // Helper function to check if an IoError contains a specific DiskError
    fn contains_disk_error(io_error: IoError, expected: DiskError) -> bool {
        if let Ok(disk_error) = io_error.downcast::<DiskError>() {
            std::mem::discriminant(&disk_error) == std::mem::discriminant(&expected)
        } else {
            false
        }
    }

    #[test]
    fn test_to_file_error_basic_conversions() {
        // Test NotFound -> FileNotFound
        let result = to_file_error(create_io_error(ErrorKind::NotFound));
        assert!(contains_disk_error(result, DiskError::FileNotFound));

        // Test PermissionDenied -> FileAccessDenied
        let result = to_file_error(create_io_error(ErrorKind::PermissionDenied));
        assert!(contains_disk_error(result, DiskError::FileAccessDenied));

        // Test IsADirectory -> IsNotRegular
        let result = to_file_error(create_io_error(ErrorKind::IsADirectory));
        assert!(contains_disk_error(result, DiskError::IsNotRegular));

        // Test NotADirectory -> FileAccessDenied
        let result = to_file_error(create_io_error(ErrorKind::NotADirectory));
        assert!(contains_disk_error(result, DiskError::FileAccessDenied));

        // Test DirectoryNotEmpty -> FileAccessDenied
        let result = to_file_error(create_io_error(ErrorKind::DirectoryNotEmpty));
        assert!(contains_disk_error(result, DiskError::FileAccessDenied));

        // Test UnexpectedEof -> FaultyDisk
        let result = to_file_error(create_io_error(ErrorKind::UnexpectedEof));
        assert!(contains_disk_error(result, DiskError::FaultyDisk));

        // Test TooManyLinks -> TooManyOpenFiles
        #[cfg(unix)]
        {
            let result = to_file_error(create_io_error(ErrorKind::TooManyLinks));
            assert!(contains_disk_error(result, DiskError::TooManyOpenFiles));
        }

        // Test InvalidInput -> FileNotFound
        let result = to_file_error(create_io_error(ErrorKind::InvalidInput));
        assert!(contains_disk_error(result, DiskError::FileNotFound));

        // Test InvalidData -> FileCorrupt
        let result = to_file_error(create_io_error(ErrorKind::InvalidData));
        assert!(contains_disk_error(result, DiskError::FileCorrupt));

        // Test StorageFull -> DiskFull
        #[cfg(unix)]
        {
            let result = to_file_error(create_io_error(ErrorKind::StorageFull));
            assert!(contains_disk_error(result, DiskError::DiskFull));
        }
    }

    #[test]
    fn test_to_file_error_passthrough_unknown() {
        // Test that unknown error kinds are passed through unchanged
        let original = create_io_error(ErrorKind::Interrupted);
        let result = to_file_error(original);
        assert_eq!(result.kind(), ErrorKind::Interrupted);
    }

    #[test]
    fn test_to_volume_error_basic_conversions() {
        // Test NotFound -> VolumeNotFound
        let result = to_volume_error(create_io_error(ErrorKind::NotFound));
        assert!(contains_disk_error(result, DiskError::VolumeNotFound));

        // Test PermissionDenied -> DiskAccessDenied
        let result = to_volume_error(create_io_error(ErrorKind::PermissionDenied));
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test DirectoryNotEmpty -> VolumeNotEmpty
        let result = to_volume_error(create_io_error(ErrorKind::DirectoryNotEmpty));
        assert!(contains_disk_error(result, DiskError::VolumeNotEmpty));

        // Test NotADirectory -> IsNotRegular
        let result = to_volume_error(create_io_error(ErrorKind::NotADirectory));
        assert!(contains_disk_error(result, DiskError::IsNotRegular));
    }

    #[test]
    fn test_to_volume_error_other_with_disk_error() {
        // Test Other error kind with FileNotFound DiskError -> VolumeNotFound
        let io_error = create_io_error_with_disk_error(DiskError::FileNotFound);
        let result = to_volume_error(io_error);
        assert!(contains_disk_error(result, DiskError::VolumeNotFound));

        // Test Other error kind with FileAccessDenied DiskError -> DiskAccessDenied
        let io_error = create_io_error_with_disk_error(DiskError::FileAccessDenied);
        let result = to_volume_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test Other error kind with other DiskError -> passthrough
        let io_error = create_io_error_with_disk_error(DiskError::DiskFull);
        let result = to_volume_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskFull));
    }

    #[test]
    fn test_to_volume_error_fallback_to_file_error() {
        // Test fallback to to_file_error for unknown error kinds
        let result = to_volume_error(create_io_error(ErrorKind::Interrupted));
        assert_eq!(result.kind(), ErrorKind::Interrupted);
    }

    #[test]
    fn test_to_disk_error_basic_conversions() {
        // Test NotFound -> DiskNotFound
        let result = to_disk_error(create_io_error(ErrorKind::NotFound));
        assert!(contains_disk_error(result, DiskError::DiskNotFound));

        // Test PermissionDenied -> DiskAccessDenied
        let result = to_disk_error(create_io_error(ErrorKind::PermissionDenied));
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));
    }

    #[test]
    fn test_to_disk_error_other_with_disk_error() {
        // Test Other error kind with FileNotFound DiskError -> DiskNotFound
        let io_error = create_io_error_with_disk_error(DiskError::FileNotFound);
        let result = to_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskNotFound));

        // Test Other error kind with VolumeNotFound DiskError -> DiskNotFound
        let io_error = create_io_error_with_disk_error(DiskError::VolumeNotFound);
        let result = to_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskNotFound));

        // Test Other error kind with FileAccessDenied DiskError -> DiskAccessDenied
        let io_error = create_io_error_with_disk_error(DiskError::FileAccessDenied);
        let result = to_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test Other error kind with VolumeAccessDenied DiskError -> DiskAccessDenied
        let io_error = create_io_error_with_disk_error(DiskError::VolumeAccessDenied);
        let result = to_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test Other error kind with other DiskError -> passthrough
        let io_error = create_io_error_with_disk_error(DiskError::DiskFull);
        let result = to_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskFull));
    }

    #[test]
    fn test_to_disk_error_fallback_to_volume_error() {
        // Test fallback to to_volume_error for unknown error kinds
        let result = to_disk_error(create_io_error(ErrorKind::Interrupted));
        assert_eq!(result.kind(), ErrorKind::Interrupted);
    }

    #[test]
    fn test_to_access_error_basic_conversions() {
        let permission_error = DiskError::FileAccessDenied;

        // Test PermissionDenied -> specified permission error
        let result = to_access_error(create_io_error(ErrorKind::PermissionDenied), permission_error);
        assert!(contains_disk_error(result, DiskError::FileAccessDenied));

        // Test NotADirectory -> specified permission error
        let result = to_access_error(create_io_error(ErrorKind::NotADirectory), DiskError::FileAccessDenied);
        assert!(contains_disk_error(result, DiskError::FileAccessDenied));

        // Test NotFound -> VolumeNotFound
        let result = to_access_error(create_io_error(ErrorKind::NotFound), DiskError::FileAccessDenied);
        assert!(contains_disk_error(result, DiskError::VolumeNotFound));

        // Test UnexpectedEof -> FaultyDisk
        let result = to_access_error(create_io_error(ErrorKind::UnexpectedEof), DiskError::FileAccessDenied);
        assert!(contains_disk_error(result, DiskError::FaultyDisk));
    }

    #[test]
    fn test_to_access_error_other_with_disk_error() {
        let permission_error = DiskError::VolumeAccessDenied;

        // Test Other error kind with DiskAccessDenied -> specified permission error
        let io_error = create_io_error_with_disk_error(DiskError::DiskAccessDenied);
        let result = to_access_error(io_error, permission_error);
        assert!(contains_disk_error(result, DiskError::VolumeAccessDenied));

        // Test Other error kind with FileAccessDenied -> specified permission error
        let io_error = create_io_error_with_disk_error(DiskError::FileAccessDenied);
        let result = to_access_error(io_error, DiskError::VolumeAccessDenied);
        assert!(contains_disk_error(result, DiskError::VolumeAccessDenied));

        // Test Other error kind with FileNotFound -> VolumeNotFound
        let io_error = create_io_error_with_disk_error(DiskError::FileNotFound);
        let result = to_access_error(io_error, DiskError::VolumeAccessDenied);
        assert!(contains_disk_error(result, DiskError::VolumeNotFound));

        // Test Other error kind with other DiskError -> passthrough
        let io_error = create_io_error_with_disk_error(DiskError::DiskFull);
        let result = to_access_error(io_error, DiskError::VolumeAccessDenied);
        assert!(contains_disk_error(result, DiskError::DiskFull));
    }

    #[test]
    fn test_to_access_error_fallback_to_volume_error() {
        let permission_error = DiskError::FileAccessDenied;

        // Test fallback to to_volume_error for unknown error kinds
        let result = to_access_error(create_io_error(ErrorKind::Interrupted), permission_error);
        assert_eq!(result.kind(), ErrorKind::Interrupted);
    }

    #[test]
    fn test_to_unformatted_disk_error_basic_conversions() {
        // Test NotFound -> UnformattedDisk
        let result = to_unformatted_disk_error(create_io_error(ErrorKind::NotFound));
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));

        // Test PermissionDenied -> DiskAccessDenied
        let result = to_unformatted_disk_error(create_io_error(ErrorKind::PermissionDenied));
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));
    }

    #[test]
    fn test_to_unformatted_disk_error_other_with_disk_error() {
        // Test Other error kind with FileNotFound -> UnformattedDisk
        let io_error = create_io_error_with_disk_error(DiskError::FileNotFound);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));

        // Test Other error kind with DiskNotFound -> UnformattedDisk
        let io_error = create_io_error_with_disk_error(DiskError::DiskNotFound);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));

        // Test Other error kind with VolumeNotFound -> UnformattedDisk
        let io_error = create_io_error_with_disk_error(DiskError::VolumeNotFound);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));

        // Test Other error kind with FileAccessDenied -> DiskAccessDenied
        let io_error = create_io_error_with_disk_error(DiskError::FileAccessDenied);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test Other error kind with DiskAccessDenied -> DiskAccessDenied
        let io_error = create_io_error_with_disk_error(DiskError::DiskAccessDenied);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::DiskAccessDenied));

        // Test Other error kind with other DiskError -> UnformattedDisk
        let io_error = create_io_error_with_disk_error(DiskError::DiskFull);
        let result = to_unformatted_disk_error(io_error);
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));
    }

    #[test]
    fn test_to_unformatted_disk_error_recursive_behavior() {
        // Test with non-Other error kind that should be handled without infinite recursion
        let result = to_unformatted_disk_error(create_io_error(ErrorKind::Interrupted));
        // This should not cause infinite recursion and should produce UnformattedDisk
        assert!(contains_disk_error(result, DiskError::UnformattedDisk));
    }

    #[test]
    fn test_error_chain_conversions() {
        // Test complex error conversion chains
        let original_error = create_io_error(ErrorKind::NotFound);

        // Chain: NotFound -> FileNotFound (via to_file_error) -> VolumeNotFound (via to_volume_error)
        let file_error = to_file_error(original_error);
        let volume_error = to_volume_error(file_error);
        assert!(contains_disk_error(volume_error, DiskError::VolumeNotFound));
    }

    #[test]
    fn test_cross_platform_error_kinds() {
        // Test error kinds that may not be available on all platforms
        #[cfg(unix)]
        {
            let result = to_file_error(create_io_error(ErrorKind::TooManyLinks));
            assert!(contains_disk_error(result, DiskError::TooManyOpenFiles));
        }

        #[cfg(unix)]
        {
            let result = to_file_error(create_io_error(ErrorKind::StorageFull));
            assert!(contains_disk_error(result, DiskError::DiskFull));
        }
    }

    #[test]
    fn test_error_conversion_with_different_kinds() {
        // Test multiple error kinds to ensure comprehensive coverage
        let test_cases = vec![
            (ErrorKind::NotFound, DiskError::FileNotFound),
            (ErrorKind::PermissionDenied, DiskError::FileAccessDenied),
            (ErrorKind::IsADirectory, DiskError::IsNotRegular),
            (ErrorKind::InvalidData, DiskError::FileCorrupt),
        ];

        for (kind, expected_disk_error) in test_cases {
            let result = to_file_error(create_io_error(kind));
            assert!(
                contains_disk_error(result, expected_disk_error.clone()),
                "Failed for ErrorKind::{kind:?} -> DiskError::{expected_disk_error:?}"
            );
        }
    }

    #[test]
    fn test_volume_error_conversion_chain() {
        // Test volume error conversion with different input types
        let test_cases = vec![
            (ErrorKind::NotFound, DiskError::VolumeNotFound),
            (ErrorKind::PermissionDenied, DiskError::DiskAccessDenied),
            (ErrorKind::DirectoryNotEmpty, DiskError::VolumeNotEmpty),
        ];

        for (kind, expected_disk_error) in test_cases {
            let result = to_volume_error(create_io_error(kind));
            assert!(
                contains_disk_error(result, expected_disk_error.clone()),
                "Failed for ErrorKind::{kind:?} -> DiskError::{expected_disk_error:?}"
            );
        }
    }
}
