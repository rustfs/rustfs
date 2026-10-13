//! Bounded snapshot transport bytes; receive ownership follows the buffer into install.
use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

pub struct NativeSnapshotData {
    bytes: Vec<u8>,
    position: u64,
    limit: u64,
    pub(crate) receive_guard: Option<crate::raft_state_machine::ReceiveGuard>,
}

fn boundary() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "native snapshot byte limit")
}

impl NativeSnapshotData {
    pub fn new(max_bytes: u64) -> io::Result<Self> {
        if max_bytes == 0 || usize::try_from(max_bytes).is_err() {
            return Err(boundary());
        }
        Ok(Self {
            bytes: Vec::new(),
            position: 0,
            limit: max_bytes,
            receive_guard: None,
        })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn attach_receive_guard(&mut self, guard: crate::raft_state_machine::ReceiveGuard) {
        self.receive_guard = Some(guard);
    }
}

impl Read for NativeSnapshotData {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let position = usize::try_from(self.position).map_err(|_| boundary())?;
        let available = self.bytes.get(position..).unwrap_or_default();
        let count = output.len().min(available.len());
        output[..count].copy_from_slice(&available[..count]);
        self.position = self
            .position
            .checked_add(u64::try_from(count).map_err(|_| boundary())?)
            .ok_or_else(boundary)?;
        Ok(count)
    }
}

impl Write for NativeSnapshotData {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        let count = u64::try_from(input.len()).map_err(|_| boundary())?;
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.limit)
            .ok_or_else(boundary)?;
        let position = usize::try_from(self.position).map_err(|_| boundary())?;
        let length = usize::try_from(end).map_err(|_| boundary())?;
        if length > self.bytes.len() {
            self.bytes
                .try_reserve_exact(length - self.bytes.len())
                .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "native snapshot allocation"))?;
            self.bytes.resize(length, 0);
        }
        self.bytes[position..length].copy_from_slice(input);
        self.position = end;
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for NativeSnapshotData {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => u64::try_from(self.bytes.len())
                .map_err(|_| boundary())?
                .checked_add_signed(offset),
        }
        .filter(|position| *position <= self.limit)
        .ok_or_else(boundary)?;
        self.position = position;
        Ok(position)
    }
}

impl AsyncRead for NativeSnapshotData {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let position = match usize::try_from(this.position) {
            Ok(position) => position,
            Err(_) => return Poll::Ready(Err(boundary())),
        };
        let available = this.bytes.get(position..).unwrap_or_default();
        let count = output.remaining().min(available.len());
        let end = this
            .position
            .checked_add(u64::try_from(count).map_err(|_| boundary())?)
            .ok_or_else(boundary)?;
        output.put_slice(&available[..count]);
        this.position = end;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for NativeSnapshotData {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, input: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Write::write(self.get_mut(), input))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for NativeSnapshotData {
    fn start_seek(self: Pin<&mut Self>, from: SeekFrom) -> io::Result<()> {
        Seek::seek(self.get_mut(), from).map(|_| ())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_snapshot_data_rejects_growth_before_mutation() {
        let mut data = NativeSnapshotData::new(4).expect("bounded snapshot");
        Write::write_all(&mut data, b"ab").expect("initial bytes");
        assert!(Write::write_all(&mut data, b"cde").is_err());
        assert_eq!(data.as_slice(), b"ab", "rejected write leaves bytes intact");
        assert_eq!(Seek::stream_position(&mut data).expect("position"), 2);
        Seek::seek(&mut data, SeekFrom::Start(4)).expect("seek exactly to limit");
        Write::write(&mut data, b"").expect("empty write");
        assert_eq!(data.as_slice(), b"ab", "empty write never allocates a gap");
        assert!(Write::write(&mut data, b"x").is_err());
        assert!(Seek::seek(&mut data, SeekFrom::Current(1)).is_err());
        assert!(Seek::seek(&mut data, SeekFrom::Current(i64::MIN)).is_err());
        assert_eq!(Seek::stream_position(&mut data).expect("unchanged position"), 4);
        Seek::seek(&mut data, SeekFrom::Start(3)).expect("bounded sparse position");
        Write::write_all(&mut data, b"z").expect("write ending exactly at limit");
        assert_eq!(data.as_slice(), b"ab\0z");
    }

    #[test]
    fn native_snapshot_data_checks_length_overflow() {
        assert!(NativeSnapshotData::new(0).is_err());
        if usize::BITS == 64 {
            let mut data = NativeSnapshotData::new(u64::MAX).expect("representable byte limit");
            Seek::seek(&mut data, SeekFrom::Start(u64::MAX)).expect("seek does not allocate");
            assert!(Write::write(&mut data, b"x").is_err());
            assert!(Seek::seek(&mut data, SeekFrom::Current(1)).is_err());
            assert!(data.as_slice().is_empty(), "overflow never allocates");
        }
    }

    #[tokio::test]
    async fn native_snapshot_data_async_io_uses_the_same_bounds() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
        let mut data = NativeSnapshotData::new(4).expect("bounded async snapshot");
        AsyncWriteExt::write_all(&mut data, b"abcd").await.expect("write exact limit");
        assert!(AsyncWriteExt::write_all(&mut data, b"e").await.is_err());
        assert!(AsyncSeekExt::seek(&mut data, SeekFrom::Start(5)).await.is_err());
        AsyncSeekExt::seek(&mut data, SeekFrom::End(-2))
            .await
            .expect("rewind within snapshot");
        let mut output = Vec::new();
        AsyncReadExt::read_to_end(&mut data, &mut output)
            .await
            .expect("read snapshot suffix");
        assert_eq!(output, b"cd");
        AsyncWriteExt::shutdown(&mut data).await.expect("transport shutdown");
        assert_eq!(data.as_slice(), b"abcd");
    }
}
