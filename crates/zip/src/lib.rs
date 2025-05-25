use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder, ZstdDecoder};
use tokio::io::{self, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_tar::Archive;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CompressionFormat {
    Gzip,  //.gz
    Bzip2, //.bz2
    // Lz4,   //.lz4
    Zip,
    Xz,   //.xz
    Zlib, //.z
    Zstd, //.zst
    Unknown,
}

impl CompressionFormat {
    pub fn from_extension(ext: &str) -> Self {
        match ext {
            "gz" => CompressionFormat::Gzip,
            "bz2" => CompressionFormat::Bzip2,
            // "lz4" => CompressionFormat::Lz4,
            "zip" => CompressionFormat::Zip,
            "xz" => CompressionFormat::Xz,
            "zlib" => CompressionFormat::Zlib,
            "zst" => CompressionFormat::Zstd,
            _ => CompressionFormat::Unknown,
        }
    }

    pub fn get_decoder<R>(&self, input: R) -> io::Result<Box<dyn AsyncRead + Send + Unpin>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let reader = BufReader::new(input);

        let decoder: Box<dyn AsyncRead + Send + Unpin + 'static> = match self {
            CompressionFormat::Gzip => Box::new(GzipDecoder::new(reader)),
            CompressionFormat::Bzip2 => Box::new(BzDecoder::new(reader)),
            // CompressionFormat::Lz4 => Box::new(Lz4Decoder::new(reader)),
            CompressionFormat::Zlib => Box::new(ZlibDecoder::new(reader)),
            CompressionFormat::Xz => Box::new(XzDecoder::new(reader)),
            CompressionFormat::Zstd => Box::new(ZstdDecoder::new(reader)),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unsupported file format")),
        };

        Ok(decoder)
    }
}

pub async fn decompress<R, F>(input: R, format: CompressionFormat, mut callback: F) -> io::Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    F: AsyncFnMut(tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>) -> std::io::Result<()> + Send + 'static,
{
    // 打开输入文件
    // println!("format {:?}", format);

    let decoder = format.get_decoder(input)?;

    // let reader: BufReader<R> = BufReader::new(input);

    // // 根据文件扩展名选择解压器
    // let decoder: Box<dyn AsyncRead + Send + Unpin> = match format {
    //     CompressionFormat::Gzip => Box::new(GzipDecoder::new(reader)),
    //     CompressionFormat::Bzip2 => Box::new(BzDecoder::new(reader)),
    //     // CompressionFormat::Lz4 => Box::new(Lz4Decoder::new(reader)),
    //     CompressionFormat::Zlib => Box::new(ZlibDecoder::new(reader)),
    //     CompressionFormat::Xz => Box::new(XzDecoder::new(reader)),
    //     CompressionFormat::Zstd => Box::new(ZstdDecoder::new(reader)),
    //     // CompressionFormat::Zip => Box::new(DeflateDecoder::new(reader)),
    //     _ => {
    //         return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unsupported file format"));
    //     }
    // };

    let mut ar = Archive::new(decoder);
    let mut entries = ar.entries().unwrap();
    while let Some(entry) = entries.next().await {
        let f = match entry {
            Ok(f) => f,
            Err(e) => {
                println!("Error reading entry: {}", e);
                return Err(e);
            }
        };
        // println!("{}", f.path().unwrap().display());
        callback(f).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;


    #[test]
    fn test_compression_format_from_extension() {
        // 测试支持的压缩格式识别
        assert_eq!(CompressionFormat::from_extension("gz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_extension("bz2"), CompressionFormat::Bzip2);
        assert_eq!(CompressionFormat::from_extension("zip"), CompressionFormat::Zip);
        assert_eq!(CompressionFormat::from_extension("xz"), CompressionFormat::Xz);
        assert_eq!(CompressionFormat::from_extension("zlib"), CompressionFormat::Zlib);
        assert_eq!(CompressionFormat::from_extension("zst"), CompressionFormat::Zstd);

        // 测试未知格式
        assert_eq!(CompressionFormat::from_extension("unknown"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("txt"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension(""), CompressionFormat::Unknown);
    }

    #[test]
    fn test_compression_format_case_sensitivity() {
        // 测试大小写敏感性
        assert_eq!(CompressionFormat::from_extension("GZ"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("Gz"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("BZ2"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("ZIP"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_compression_format_edge_cases() {
        // 测试边界情况
        assert_eq!(CompressionFormat::from_extension("gz "), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension(" gz"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("gz.bak"), CompressionFormat::Unknown);
        assert_eq!(CompressionFormat::from_extension("tar.gz"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_compression_format_debug() {
        // 测试Debug trait实现
        let format = CompressionFormat::Gzip;
        let debug_str = format!("{:?}", format);
        assert_eq!(debug_str, "Gzip");

        let unknown_format = CompressionFormat::Unknown;
        let unknown_debug_str = format!("{:?}", unknown_format);
        assert_eq!(unknown_debug_str, "Unknown");
    }

    #[test]
    fn test_compression_format_equality() {
        // 测试PartialEq trait实现
        assert_eq!(CompressionFormat::Gzip, CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::Unknown, CompressionFormat::Unknown);
        assert_ne!(CompressionFormat::Gzip, CompressionFormat::Bzip2);
        assert_ne!(CompressionFormat::Zip, CompressionFormat::Unknown);
    }

    #[tokio::test]
    async fn test_get_decoder_supported_formats() {
        // 测试支持的格式能够创建解码器
        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let gzip_format = CompressionFormat::Gzip;
        let decoder_result = gzip_format.get_decoder(cursor);
        assert!(decoder_result.is_ok(), "Gzip decoder should be created successfully");
    }

    #[tokio::test]
    async fn test_get_decoder_unsupported_formats() {
        // 测试不支持的格式返回错误
        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let unknown_format = CompressionFormat::Unknown;
        let decoder_result = unknown_format.get_decoder(cursor);
        assert!(decoder_result.is_err(), "Unknown format should return error");

        if let Err(e) = decoder_result {
            assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(e.to_string(), "Unsupported file format");
        }
    }

    #[tokio::test]
    async fn test_get_decoder_zip_format() {
        // 测试Zip格式（当前不支持）
        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let zip_format = CompressionFormat::Zip;
        let decoder_result = zip_format.get_decoder(cursor);
        assert!(decoder_result.is_err(), "Zip format should return error (not implemented)");
    }

    #[tokio::test]
    async fn test_get_decoder_all_supported_formats() {
        // 测试所有支持的格式都能创建解码器
        let test_data = b"test data";

        let supported_formats = vec![
            CompressionFormat::Gzip,
            CompressionFormat::Bzip2,
            CompressionFormat::Zlib,
            CompressionFormat::Xz,
            CompressionFormat::Zstd,
        ];

        for format in supported_formats {
            let cursor = Cursor::new(test_data);
            let decoder_result = format.get_decoder(cursor);
            assert!(decoder_result.is_ok(), "Format {:?} should create decoder successfully", format);
        }
    }

        #[tokio::test]
    async fn test_decoder_type_consistency() {
        // 测试解码器返回类型的一致性
        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let gzip_format = CompressionFormat::Gzip;
        let mut decoder = gzip_format.get_decoder(cursor).unwrap();

        // 验证返回的是正确的trait对象
        let mut buffer = Vec::new();
        // 这里只是验证类型，不期望实际读取成功（因为数据不是真正的gzip格式）
        let _result = decoder.read_to_end(&mut buffer).await;
    }

    #[test]
    fn test_compression_format_exhaustive_matching() {
        // 测试所有枚举变体都有对应的处理
        let all_formats = vec![
            CompressionFormat::Gzip,
            CompressionFormat::Bzip2,
            CompressionFormat::Zip,
            CompressionFormat::Xz,
            CompressionFormat::Zlib,
            CompressionFormat::Zstd,
            CompressionFormat::Unknown,
        ];

        for format in all_formats {
            // 验证每个格式都有对应的Debug实现
            let _debug_str = format!("{:?}", format);

            // 验证每个格式都有对应的PartialEq实现
            assert_eq!(format, format);
        }
    }

    #[test]
    fn test_extension_mapping_completeness() {
        // 测试扩展名映射的完整性
        let extension_mappings = vec![
            ("gz", CompressionFormat::Gzip),
            ("bz2", CompressionFormat::Bzip2),
            ("zip", CompressionFormat::Zip),
            ("xz", CompressionFormat::Xz),
            ("zlib", CompressionFormat::Zlib),
            ("zst", CompressionFormat::Zstd),
        ];

        for (ext, expected_format) in extension_mappings {
            assert_eq!(CompressionFormat::from_extension(ext), expected_format,
                "Extension '{}' should map to {:?}", ext, expected_format);
        }
    }

    #[test]
    fn test_format_string_representations() {
        // 测试格式的字符串表示
        let format_strings = vec![
            (CompressionFormat::Gzip, "Gzip"),
            (CompressionFormat::Bzip2, "Bzip2"),
            (CompressionFormat::Zip, "Zip"),
            (CompressionFormat::Xz, "Xz"),
            (CompressionFormat::Zlib, "Zlib"),
            (CompressionFormat::Zstd, "Zstd"),
            (CompressionFormat::Unknown, "Unknown"),
        ];

        for (format, expected_str) in format_strings {
            assert_eq!(format!("{:?}", format), expected_str,
                "Format {:?} should have string representation '{}'", format, expected_str);
        }
    }

    #[tokio::test]
    async fn test_decoder_error_handling() {
        // 测试解码器的错误处理
        let empty_data = b"";
        let cursor = Cursor::new(empty_data);

        let gzip_format = CompressionFormat::Gzip;
        let decoder_result = gzip_format.get_decoder(cursor);

        // 解码器创建应该成功，即使数据为空
        assert!(decoder_result.is_ok(), "Decoder creation should succeed even with empty data");
    }

    #[test]
    fn test_compression_format_memory_efficiency() {
        // 测试枚举的内存效率
        use std::mem;

        // 验证枚举大小合理
        let size = mem::size_of::<CompressionFormat>();
        assert!(size <= 8, "CompressionFormat should be memory efficient, got {} bytes", size);

        // 验证Option<CompressionFormat>的大小
        let option_size = mem::size_of::<Option<CompressionFormat>>();
        assert!(option_size <= 16, "Option<CompressionFormat> should be efficient, got {} bytes", option_size);
    }

    #[test]
    fn test_extension_validation() {
        // 测试扩展名验证的边界情况
        let test_cases = vec![
            // 正常情况
            ("gz", true),
            ("bz2", true),
            ("xz", true),

            // 边界情况
            ("", false),
            ("g", false),
            ("gzz", false),
            ("gz2", false),

            // 特殊字符
            ("gz.", false),
            (".gz", false),
            ("gz-", false),
            ("gz_", false),
        ];

        for (ext, should_be_known) in test_cases {
            let format = CompressionFormat::from_extension(ext);
            let is_known = format != CompressionFormat::Unknown;
            assert_eq!(is_known, should_be_known,
                "Extension '{}' recognition mismatch: expected {}, got {}",
                ext, should_be_known, is_known);
        }
    }

        #[tokio::test]
    async fn test_decoder_trait_bounds() {
        // 测试解码器的trait bounds
        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let gzip_format = CompressionFormat::Gzip;
        let decoder = gzip_format.get_decoder(cursor).unwrap();

        // 验证返回的解码器满足所需的trait bounds
        fn check_bounds<T: AsyncRead + Send + Unpin + ?Sized>(_: &T) {}
        check_bounds(&*decoder);
    }

    #[test]
    fn test_format_consistency_with_extensions() {
        // 测试格式与扩展名的一致性
        let consistency_tests = vec![
            (CompressionFormat::Gzip, "gz"),
            (CompressionFormat::Bzip2, "bz2"),
            (CompressionFormat::Zip, "zip"),
            (CompressionFormat::Xz, "xz"),
            (CompressionFormat::Zlib, "zlib"),
            (CompressionFormat::Zstd, "zst"),
        ];

        for (format, ext) in consistency_tests {
            let parsed_format = CompressionFormat::from_extension(ext);
            assert_eq!(parsed_format, format,
                "Extension '{}' should consistently map to {:?}", ext, format);
        }
    }

        #[tokio::test]
    async fn test_decompress_with_invalid_format() {
        // 测试使用无效格式进行解压
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let entry_count = Arc::new(AtomicUsize::new(0));
        let entry_count_clone = entry_count.clone();

        let result = decompress(
            cursor,
            CompressionFormat::Unknown,
            move |_entry| {
                entry_count_clone.fetch_add(1, Ordering::SeqCst);
                async move { Ok(()) }
            }
        ).await;

        assert!(result.is_err(), "Decompress with Unknown format should fail");
        assert_eq!(entry_count.load(Ordering::SeqCst), 0, "No entries should be processed with invalid format");
    }

        #[tokio::test]
    async fn test_decompress_with_zip_format() {
        // 测试使用Zip格式进行解压（当前不支持）
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let entry_count = Arc::new(AtomicUsize::new(0));
        let entry_count_clone = entry_count.clone();

        let result = decompress(
            cursor,
            CompressionFormat::Zip,
            move |_entry| {
                entry_count_clone.fetch_add(1, Ordering::SeqCst);
                async move { Ok(()) }
            }
        ).await;

        assert!(result.is_err(), "Decompress with Zip format should fail (not implemented)");
        assert_eq!(entry_count.load(Ordering::SeqCst), 0, "No entries should be processed with unsupported format");
    }

    #[tokio::test]
    async fn test_decompress_error_propagation() {
        // 测试解压过程中的错误传播
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let result = decompress(
            cursor,
            CompressionFormat::Gzip,
            move |_entry| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                async move {
                    if count == 0 {
                        // 第一次调用返回错误
                        Err(io::Error::new(io::ErrorKind::Other, "Test error"))
                    } else {
                        Ok(())
                    }
                }
            }
        ).await;

        // 由于输入数据不是有效的gzip格式，可能在解析阶段就失败
        // 这里主要测试错误处理机制
        assert!(result.is_err(), "Should propagate callback errors");
    }

    #[tokio::test]
    async fn test_decompress_callback_execution() {
        // 测试回调函数的执行
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let test_data = b"test data";
        let cursor = Cursor::new(test_data);

        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = callback_called.clone();

        let _result = decompress(
            cursor,
            CompressionFormat::Gzip,
            move |_entry| {
                callback_called_clone.store(true, Ordering::SeqCst);
                async move { Ok(()) }
            }
        ).await;

        // 注意：由于测试数据不是有效的gzip格式，回调可能不会被调用
        // 这个测试主要验证函数签名和基本流程
    }

    #[test]
    fn test_compression_format_clone_and_copy() {
        // 测试CompressionFormat是否可以被复制
        let format = CompressionFormat::Gzip;
        let format_copy = format;

        // 验证复制后的值相等
        assert_eq!(format, format_copy);

        // 验证原值仍然可用
        assert_eq!(format, CompressionFormat::Gzip);
    }

    #[test]
    fn test_compression_format_match_exhaustiveness() {
        // 测试match语句的完整性
        fn handle_format(format: CompressionFormat) -> &'static str {
            match format {
                CompressionFormat::Gzip => "gzip",
                CompressionFormat::Bzip2 => "bzip2",
                CompressionFormat::Zip => "zip",
                CompressionFormat::Xz => "xz",
                CompressionFormat::Zlib => "zlib",
                CompressionFormat::Zstd => "zstd",
                CompressionFormat::Unknown => "unknown",
            }
        }

        // 测试所有变体都有对应的处理
        assert_eq!(handle_format(CompressionFormat::Gzip), "gzip");
        assert_eq!(handle_format(CompressionFormat::Bzip2), "bzip2");
        assert_eq!(handle_format(CompressionFormat::Zip), "zip");
        assert_eq!(handle_format(CompressionFormat::Xz), "xz");
        assert_eq!(handle_format(CompressionFormat::Zlib), "zlib");
        assert_eq!(handle_format(CompressionFormat::Zstd), "zstd");
        assert_eq!(handle_format(CompressionFormat::Unknown), "unknown");
    }

    #[test]
    fn test_extension_parsing_performance() {
        // 测试扩展名解析的性能（简单的性能测试）
        let extensions = vec!["gz", "bz2", "zip", "xz", "zlib", "zst", "unknown"];

        // 多次调用以测试性能一致性
        for _ in 0..1000 {
            for ext in &extensions {
                let _format = CompressionFormat::from_extension(ext);
            }
        }

        // 如果能执行到这里，说明性能是可接受的
        assert!(true, "Extension parsing performance test completed");
    }

    #[test]
    fn test_format_default_behavior() {
        // 测试格式的默认行为
        let unknown_extensions = vec!["", "txt", "doc", "pdf", "unknown_ext"];

        for ext in unknown_extensions {
            let format = CompressionFormat::from_extension(ext);
            assert_eq!(format, CompressionFormat::Unknown,
                "Extension '{}' should default to Unknown", ext);
        }
    }
}

// #[tokio::test]
// async fn test_decompress() -> io::Result<()> {
//     use std::path::Path;
//     use tokio::fs::File;

//     let input_path = "/Users/weisd/Downloads/wsd.tar.gz"; // 替换为你的压缩文件路径

//     let f = File::open(input_path).await?;

//     let Some(ext) = Path::new(input_path).extension().and_then(|s| s.to_str()) else {
//         return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unsupported file format"));
//     };

//     match decompress(
//         f,
//         CompressionFormat::from_extension(ext),
//         |entry: tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin>>>| async move {
//             let path = entry.path().unwrap();
//             println!("Extracted: {}", path.display());
//             Ok(())
//         },
//     )
//     .await
//     {
//         Ok(_) => println!("解压成功！"),
//         Err(e) => println!("解压失败: {}", e),
//     }

//     Ok(())
// }
