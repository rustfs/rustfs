use pin_project_lite::pin_project;
use rustfs_utils::{HashAlgorithm, read_full, write_all};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

pin_project! {
    /// BitrotReader reads (hash+data) blocks from an async reader and verifies hash integrity.
    pub struct BitrotReader<R> {
        #[pin]
        inner: R,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        buf: Vec<u8>,
        hash_buf: Vec<u8>,
        hash_read: usize,
        data_buf: Vec<u8>,
        data_read: usize,
        hash_checked: bool,
    }
}

impl<R> BitrotReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create a new BitrotReader.
    pub fn new(inner: R, shard_size: usize, algo: HashAlgorithm) -> Self {
        let hash_size = algo.size();
        Self {
            inner,
            hash_algo: algo,
            shard_size,
            buf: Vec::new(),
            hash_buf: vec![0u8; hash_size],
            hash_read: 0,
            data_buf: Vec::new(),
            data_read: 0,
            hash_checked: false,
        }
    }

    /// Read a single (hash+data) block, verify hash, and return the number of bytes read into `out`.
    /// Returns an error if hash verification fails or data exceeds shard_size.
    pub async fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.len() > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {} exceeds shard size {}", out.len(), self.shard_size),
            ));
        }

        let hash_size = self.hash_algo.size();
        // Read hash
        let mut hash_buf = vec![0u8; hash_size];
        if hash_size > 0 {
            self.inner.read_exact(&mut hash_buf).await?;
        }

        let data_len = read_full(&mut self.inner, out).await?;

        // // Read data
        // let mut data_len = 0;
        // while data_len < out.len() {
        //     let n = self.inner.read(&mut out[data_len..]).await?;
        //     if n == 0 {
        //         break;
        //     }
        //     data_len += n;
        //     // Only read up to one shard_size block
        //     if data_len >= self.shard_size {
        //         break;
        //     }
        // }

        if hash_size > 0 {
            let actual_hash = self.hash_algo.hash_encode(&out[..data_len]);
            if actual_hash != hash_buf {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bitrot hash mismatch"));
            }
        }
        Ok(data_len)
    }
}

pin_project! {
    /// BitrotWriter writes (hash+data) blocks to an async writer.
    pub struct BitrotWriter<W> {
        #[pin]
        inner: W,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        buf: Vec<u8>,
        finished: bool,
    }
}

impl<W> BitrotWriter<W>
where
    W: AsyncWrite + Unpin + Send + Sync,
{
    /// Create a new BitrotWriter.
    pub fn new(inner: W, shard_size: usize, algo: HashAlgorithm) -> Self {
        let hash_algo = algo;
        Self {
            inner,
            hash_algo,
            shard_size,
            buf: Vec::new(),
            finished: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Write a (hash+data) block. Returns the number of data bytes written.
    /// Returns an error if called after a short write or if data exceeds shard_size.
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.finished {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bitrot writer already finished"));
        }

        if buf.len() > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {} exceeds shard size {}", buf.len(), self.shard_size),
            ));
        }

        if buf.len() < self.shard_size {
            self.finished = true;
        }

        let hash_algo = &self.hash_algo;

        if hash_algo.size() > 0 {
            let hash = hash_algo.hash_encode(buf);
            self.buf.extend_from_slice(&hash);
        }

        self.buf.extend_from_slice(buf);

        // Write hash+data in one call
        let mut n = write_all(&mut self.inner, &self.buf).await?;

        if n < hash_algo.size() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "short write: not enough bytes written",
            ));
        }

        n -= hash_algo.size();

        self.buf.clear();

        Ok(n)
    }
}

pub fn bitrot_shard_file_size(size: usize, shard_size: usize, algo: HashAlgorithm) -> usize {
    if algo != HashAlgorithm::HighwayHash256S {
        return size;
    }
    size.div_ceil(shard_size) * algo.size() + size
}

pub async fn bitrot_verify<R: AsyncRead + Unpin + Send>(
    mut r: R,
    want_size: usize,
    part_size: usize,
    algo: HashAlgorithm,
    _want: Vec<u8>,
    mut shard_size: usize,
) -> std::io::Result<()> {
    let mut hash_buf = vec![0; algo.size()];
    let mut left = want_size;

    if left != bitrot_shard_file_size(part_size, shard_size, algo.clone()) {
        return Err(std::io::Error::other("bitrot shard file size mismatch"));
    }

    while left > 0 {
        let n = r.read_exact(&mut hash_buf).await?;
        left -= n;

        if left < shard_size {
            shard_size = left;
        }

        let mut buf = vec![0; shard_size];
        let read = r.read_exact(&mut buf).await?;

        let actual_hash = algo.hash_encode(&buf);
        if actual_hash != hash_buf[0..n] {
            return Err(std::io::Error::other("bitrot hash mismatch"));
        }

        left -= read;
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::{BitrotReader, BitrotWriter};
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_bitrot_read_write_ok() {
        let data = b"hello world! this is a test shard.";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        // 读
        let reader = bitrot_writer.into_inner();
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);

            out.extend_from_slice(&buf[..m]);
            n += m;
        }

        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }

    #[tokio::test]
    async fn test_bitrot_read_hash_mismatch() {
        let data = b"test data for bitrot";
        let data_size = data.len();
        let shard_size = 8;
        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);
        for chunk in data.chunks(shard_size) {
            let _ = bitrot_writer.write(chunk).await.unwrap();
        }
        let mut written = bitrot_writer.into_inner().into_inner();
        // change the last byte to make hash mismatch
        let pos = written.len() - 1;
        written[pos] ^= 0xFF;
        let reader = Cursor::new(written);
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256);

        let count = data_size.div_ceil(shard_size);

        let mut idx = 0;
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let res = bitrot_reader.read(&mut buf).await;

            if idx == count - 1 {
                // 最后一个块，应该返回错误
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
                break;
            }

            let m = res.unwrap();

            assert_eq!(&buf[..m], &data[n..n + m]);

            n += m;
            idx += 1;
        }
    }

    #[tokio::test]
    async fn test_bitrot_read_write_none_hash() {
        let data = b"bitrot none hash test data!";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::None);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        let reader = bitrot_writer.into_inner();
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::None);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);
            out.extend_from_slice(&buf[..m]);
            n += m;
        }
        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }
}
