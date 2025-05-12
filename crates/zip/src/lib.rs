use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder, ZstdDecoder};
use tokio::io::{self, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_tar::Archive;

#[derive(Debug, PartialEq)]
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
