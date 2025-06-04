use crate::error::Result;
use crate::filemeta::*;
use std::collections::HashMap;
use time::OffsetDateTime;
use uuid::Uuid;

/// 创建一个真实的 xl.meta 文件数据用于测试
pub fn create_real_xlmeta() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // 创建一个真实的对象版本
    let version_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef")?;
    let data_dir = Uuid::parse_str("fedcba98-7654-3210-fedc-ba9876543210")?;

    let mut metadata = HashMap::new();
    metadata.insert("Content-Type".to_string(), "text/plain".to_string());
    metadata.insert("X-Amz-Meta-Author".to_string(), "test-user".to_string());
    metadata.insert("X-Amz-Meta-Created".to_string(), "2024-01-15T10:30:00Z".to_string());

    let object_version = MetaObject {
        version_id: Some(version_id),
        data_dir: Some(data_dir),
        erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
        erasure_m: 4,
        erasure_n: 2,
        erasure_block_size: 1024 * 1024, // 1MB
        erasure_index: 1,
        erasure_dist: vec![0, 1, 2, 3, 4, 5],
        bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
        part_numbers: vec![1],
        part_etags: vec!["d41d8cd98f00b204e9800998ecf8427e".to_string()],
        part_sizes: vec![1024],
        part_actual_sizes: vec![1024],
        part_indices: Vec::new(),
        size: 1024,
        mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200)?), // 2024-01-15 10:30:00 UTC
        meta_sys: HashMap::new(),
        meta_user: metadata,
    };

    let file_version = FileMetaVersion {
        version_type: VersionType::Object,
        object: Some(object_version),
        delete_marker: None,
        write_version: 1,
    };

    let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
    fm.versions.push(shallow_version);

    // 添加一个删除标记版本
    let delete_version_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555")?;
    let delete_marker = MetaDeleteMarker {
        version_id: Some(delete_version_id),
        mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312260)?), // 1分钟后
        meta_sys: None,
    };

    let delete_file_version = FileMetaVersion {
        version_type: VersionType::Delete,
        object: None,
        delete_marker: Some(delete_marker),
        write_version: 2,
    };

    let delete_shallow_version = FileMetaShallowVersion::try_from(delete_file_version)?;
    fm.versions.push(delete_shallow_version);

    // 添加一个 Legacy 版本用于测试
    let legacy_version_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")?;
    let legacy_version = FileMetaVersion {
        version_type: VersionType::Legacy,
        object: None,
        delete_marker: None,
        write_version: 3,
    };

    let mut legacy_shallow = FileMetaShallowVersion::try_from(legacy_version)?;
    legacy_shallow.header.version_id = Some(legacy_version_id);
    legacy_shallow.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(1705312140)?); // 更早的时间
    fm.versions.push(legacy_shallow);

    // 按修改时间排序（最新的在前）
    fm.versions.sort_by(|a, b| b.header.mod_time.cmp(&a.header.mod_time));

    fm.marshal_msg()
}

/// 创建一个包含多个版本的复杂 xl.meta 文件
pub fn create_complex_xlmeta() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // 创建10个版本的对象
    for i in 0..10 {
        let version_id = Uuid::new_v4();
        let data_dir = if i % 3 == 0 { Some(Uuid::new_v4()) } else { None };

        let mut metadata = HashMap::new();
        metadata.insert("Content-Type".to_string(), "application/octet-stream".to_string());
        metadata.insert("X-Amz-Meta-Version".to_string(), i.to_string());
        metadata.insert("X-Amz-Meta-Test".to_string(), format!("test-value-{}", i));

        let object_version = MetaObject {
            version_id: Some(version_id),
            data_dir,
            erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
            erasure_m: 4,
            erasure_n: 2,
            erasure_block_size: 1024 * 1024,
            erasure_index: (i % 6) as usize,
            erasure_dist: vec![0, 1, 2, 3, 4, 5],
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: vec![1],
            part_etags: vec![format!("etag-{:08x}", i)],
            part_sizes: vec![1024 * (i + 1) as usize],
            part_actual_sizes: vec![1024 * (i + 1) as usize],
            part_indices: Vec::new(),
            size: 1024 * (i + 1) as usize,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200 + i * 60)?),
            meta_sys: HashMap::new(),
            meta_user: metadata,
        };

        let file_version = FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(object_version),
            delete_marker: None,
            write_version: (i + 1) as u64,
        };

        let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
        fm.versions.push(shallow_version);

        // 每隔3个版本添加一个删除标记
        if i % 3 == 2 {
            let delete_version_id = Uuid::new_v4();
            let delete_marker = MetaDeleteMarker {
                version_id: Some(delete_version_id),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200 + i * 60 + 30)?),
                meta_sys: None,
            };

            let delete_file_version = FileMetaVersion {
                version_type: VersionType::Delete,
                object: None,
                delete_marker: Some(delete_marker),
                write_version: (i + 100) as u64,
            };

            let delete_shallow_version = FileMetaShallowVersion::try_from(delete_file_version)?;
            fm.versions.push(delete_shallow_version);
        }
    }

    // 按修改时间排序（最新的在前）
    fm.versions.sort_by(|a, b| b.header.mod_time.cmp(&a.header.mod_time));

    fm.marshal_msg()
}

/// 创建一个损坏的 xl.meta 文件用于错误处理测试
pub fn create_corrupted_xlmeta() -> Vec<u8> {
    let mut data = vec![
        // 正确的文件头
        b'X', b'L', b'2', b' ', // 版本号
        1, 0, 3, 0, // 版本号
        0xc6, 0x00, 0x00, 0x00, 0x10, // 正确的 bin32 长度标记，但数据长度不匹配
    ];

    // 添加不足的数据（少于声明的长度）
    data.extend_from_slice(&[0x42; 8]); // 只有8字节，但声明了16字节

    data
}

/// 创建一个空的 xl.meta 文件
pub fn create_empty_xlmeta() -> Result<Vec<u8>> {
    let fm = FileMeta::new();
    fm.marshal_msg()
}

/// 验证解析结果的辅助函数
pub fn verify_parsed_metadata(fm: &FileMeta, expected_versions: usize) -> Result<()> {
    assert_eq!(fm.versions.len(), expected_versions, "版本数量不匹配");
    assert_eq!(fm.meta_ver, crate::filemeta::XL_META_VERSION, "元数据版本不匹配");

    // 验证版本是否按修改时间排序
    for i in 1..fm.versions.len() {
        let prev_time = fm.versions[i - 1].header.mod_time;
        let curr_time = fm.versions[i].header.mod_time;

        if let (Some(prev), Some(curr)) = (prev_time, curr_time) {
            assert!(prev >= curr, "版本未按修改时间正确排序");
        }
    }

    Ok(())
}

/// 创建一个包含内联数据的 xl.meta 文件
pub fn create_xlmeta_with_inline_data() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // 添加内联数据
    let inline_data = b"This is inline data for testing purposes";
    let version_id = Uuid::new_v4();
    fm.data.replace(&version_id.to_string(), inline_data.to_vec())?;

    let object_version = MetaObject {
        version_id: Some(version_id),
        data_dir: None,
        erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
        erasure_m: 1,
        erasure_n: 1,
        erasure_block_size: 64 * 1024,
        erasure_index: 0,
        erasure_dist: vec![0, 1],
        bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
        part_numbers: vec![1],
        part_etags: Vec::new(),
        part_sizes: vec![inline_data.len()],
        part_actual_sizes: Vec::new(),
        part_indices: Vec::new(),
        size: inline_data.len(),
        mod_time: Some(OffsetDateTime::now_utc()),
        meta_sys: HashMap::new(),
        meta_user: HashMap::new(),
    };

    let file_version = FileMetaVersion {
        version_type: VersionType::Object,
        object: Some(object_version),
        delete_marker: None,
        write_version: 1,
    };

    let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
    fm.versions.push(shallow_version);

    fm.marshal_msg()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_real_xlmeta() {
        let data = create_real_xlmeta().expect("创建测试数据失败");
        assert!(!data.is_empty(), "生成的数据不应为空");

        // 验证文件头
        assert_eq!(&data[0..4], b"XL2 ", "文件头不正确");

        // 尝试解析
        let fm = FileMeta::load(&data).expect("解析失败");
        verify_parsed_metadata(&fm, 3).expect("验证失败");
    }

    #[test]
    fn test_create_complex_xlmeta() {
        let data = create_complex_xlmeta().expect("创建复杂测试数据失败");
        assert!(!data.is_empty(), "生成的数据不应为空");

        let fm = FileMeta::load(&data).expect("解析失败");
        assert!(fm.versions.len() >= 10, "应该有至少10个版本");
    }

    #[test]
    fn test_create_xlmeta_with_inline_data() {
        let data = create_xlmeta_with_inline_data().expect("创建内联数据测试失败");
        assert!(!data.is_empty(), "生成的数据不应为空");

        let fm = FileMeta::load(&data).expect("解析失败");
        assert_eq!(fm.versions.len(), 1, "应该有1个版本");
        assert!(!fm.data.as_slice().is_empty(), "应该包含内联数据");
    }

    #[test]
    fn test_corrupted_xlmeta_handling() {
        let data = create_corrupted_xlmeta();
        let result = FileMeta::load(&data);
        assert!(result.is_err(), "损坏的数据应该解析失败");
    }

    #[test]
    fn test_empty_xlmeta() {
        let data = create_empty_xlmeta().expect("创建空测试数据失败");
        let fm = FileMeta::load(&data).expect("解析空数据失败");
        assert_eq!(fm.versions.len(), 0, "空文件应该没有版本");
    }
}
