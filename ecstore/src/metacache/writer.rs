use crate::disk::MetaCacheEntry;
use crate::error::Error;
use crate::error::Result;
use rmp::decode::RmpRead;
use rmp::encode::RmpWrite;
use rmp::Marker;
use std::io::Read;
use std::io::Write;
use std::str::from_utf8;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
// use std::sync::Arc;
// use tokio::sync::mpsc;
// use tokio::sync::mpsc::Sender;
// use tokio::task;

const METACACHE_STREAM_VERSION: u8 = 2;

#[derive(Debug)]
pub struct MetacacheWriter<W> {
    wr: W,
    created: bool,
    // err: Option<Error>,
    buf: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> MetacacheWriter<W> {
    pub fn new(wr: W) -> Self {
        Self {
            wr,
            created: false,
            // err: None,
            buf: Vec::new(),
        }
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.wr.write_all(&self.buf).await?;
        self.buf.clear();

        Ok(())
    }

    pub async fn init(&mut self) -> Result<()> {
        if !self.created {
            rmp::encode::write_u8(&mut self.buf, METACACHE_STREAM_VERSION).map_err(|e| Error::msg(format!("{:?}", e)))?;
            self.flush().await?;
            self.created = true;
        }
        Ok(())
    }

    pub async fn write(&mut self, objs: &[MetaCacheEntry]) -> Result<()> {
        if objs.is_empty() {
            return Ok(());
        }

        self.init().await?;

        for obj in objs.iter() {
            if obj.name.is_empty() {
                return Err(Error::msg("metacacheWriter: no name"));
            }

            self.write_obj(obj).await?;
        }

        Ok(())
    }

    pub async fn write_obj(&mut self, obj: &MetaCacheEntry) -> Result<()> {
        println!("write_obj {:?}", &obj);

        self.init().await?;

        rmp::encode::write_bool(&mut self.buf, true).map_err(|e| Error::msg(format!("{:?}", e)))?;

        rmp::encode::write_str(&mut self.buf, &obj.name).map_err(|e| Error::msg(format!("{:?}", e)))?;

        rmp::encode::write_bin(&mut self.buf, &obj.metadata).map_err(|e| Error::msg(format!("{:?}", e)))?;

        self.flush().await?;

        Ok(())
    }

    // pub async fn stream(&mut self) -> Result<Sender<MetaCacheEntry>> {
    //     let (sender, mut receiver) = mpsc::channel::<MetaCacheEntry>(100);

    //     let wr = Arc::new(self);

    //     task::spawn(async move {
    //         while let Some(obj) = receiver.recv().await {
    //             // if obj.name.is_empty() || self.err.is_some() {
    //             //     continue;
    //             // }

    //             let _ = wr.write_obj(&obj);

    //             // if let Err(err) = rmp::encode::write_bool(&mut self.wr, true) {
    //             //     self.err = Some(Error::new(err));
    //             //     continue;
    //             // }

    //             // if let Err(err) = rmp::encode::write_str(&mut self.wr, &obj.name) {
    //             //     self.err = Some(Error::new(err));
    //             //     continue;
    //             // }

    //             // if let Err(err) = rmp::encode::write_bin(&mut self.wr, &obj.metadata) {
    //             //     self.err = Some(Error::new(err));
    //             //     continue;
    //             // }
    //         }
    //     });

    //     Ok(sender)
    // }

    pub async fn close(&mut self) -> Result<()> {
        rmp::encode::write_bool(&mut self.buf, false).map_err(|e| Error::msg(format!("{:?}", e)))?;
        self.flush().await?;
        Ok(())
    }
}

pub struct MetacacheReader<R> {
    rd: R,
    init: bool,
    err: Option<Error>,
    buf: Vec<u8>,
    offset: usize,
}

impl<R: AsyncRead + Unpin> MetacacheReader<R> {
    pub fn new(rd: R) -> Self {
        Self {
            rd,
            init: false,
            err: None,
            buf: Vec::new(),
            offset: 0,
        }
    }

    pub async fn read_more(&mut self, read_size: usize) -> Result<&[u8]> {
        let ext_size = read_size + self.offset;

        let extra = ext_size - self.offset;
        if self.buf.capacity() >= ext_size {
            // Extend the buffer if we have enough space.
            self.buf.resize(ext_size, 0);
        } else {
            self.buf.extend(vec![0u8; extra]);
        }

        let pref = self.offset;

        self.rd.read_exact(&mut self.buf[pref..ext_size]).await?;

        self.offset += read_size;

        let data = &self.buf[pref..ext_size];

        Ok(data)
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.offset = 0;
    }

    async fn check_init(&mut self) -> Result<()> {
        if !self.init {
            let ver = match rmp::decode::read_u8(&mut self.read_more(2).await?) {
                Ok(res) => res,
                Err(err) => {
                    self.err = Some(Error::msg(format!("{:?}", err)));
                    0
                }
            };
            match ver {
                1 | 2 => (),
                _ => {
                    self.err = Some(Error::msg("invalid version"));
                }
            }

            self.init = true;
        }
        Ok(())
    }

    async fn read_str_len(&mut self) -> Result<u32> {
        let mark = match rmp::decode::read_marker(&mut self.read_more(1).await?) {
            Ok(res) => res,
            Err(err) => {
                let serr = format!("{:?}", err);
                self.err = Some(Error::msg(&serr));
                return Err(Error::msg(&serr));
            }
        };

        match mark {
            Marker::FixStr(size) => Ok(u32::from(size)),
            Marker::Str8 => Ok(u32::from(self.read_u8().await?)),
            Marker::Str16 => Ok(u32::from(self.read_u16().await?)),
            Marker::Str32 => Ok(self.read_u32().await?),
            _ => Err(Error::msg("str marker err")),
        }
    }

    async fn read_bin_len(&mut self) -> Result<u32> {
        let mark = match rmp::decode::read_marker(&mut self.read_more(1).await?) {
            Ok(res) => res,
            Err(err) => {
                let serr = format!("{:?}", err);
                self.err = Some(Error::msg(&serr));
                return Err(Error::msg(&serr));
            }
        };

        match mark {
            Marker::Bin8 => Ok(u32::from(self.read_u8().await?)),
            Marker::Bin16 => Ok(u32::from(self.read_u16().await?)),
            Marker::Bin32 => Ok(self.read_u32().await?),
            _ => Err(Error::msg("bin marker err")),
        }
    }

    async fn read_u8(&mut self) -> Result<u8> {
        let buf = self.read_more(1).await?;

        Ok(u8::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    async fn read_u16(&mut self) -> Result<u16> {
        let buf = self.read_more(2).await?;

        Ok(u16::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    async fn read_u32(&mut self) -> Result<u32> {
        let buf = self.read_more(4).await?;

        Ok(u32::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    pub async fn peek(&mut self) -> Result<Option<MetaCacheEntry>> {
        self.check_init().await?;

        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        match rmp::decode::read_bool(&mut self.read_more(1).await?) {
            Ok(res) => {
                if !res {
                    return Ok(None);
                }
            }
            Err(err) => {
                let serr = format!("{:?}", err);
                self.err = Some(Error::msg(&serr));
                return Err(Error::msg(&serr));
            }
        };

        let l = self.read_str_len().await?;

        let buf = self.read_more(l as usize).await?;
        let name_buf = buf.to_vec();
        let name = match from_utf8(&name_buf) {
            Ok(decoded) => decoded.to_owned(),
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                return Err(Error::msg(err.to_string()));
            }
        };

        let l = self.read_bin_len().await?;

        let buf = self.read_more(l as usize).await?;

        let metadata = buf.to_vec();

        self.reset();

        Ok(Some(MetaCacheEntry {
            name,
            metadata,
            cached: None,
            reusable: false,
        }))
    }

    pub async fn read_all(&mut self) -> Result<Vec<MetaCacheEntry>> {
        let mut ret = Vec::new();

        loop {
            if let Some(entry) = self.peek().await? {
                ret.push(entry);
                continue;
            }

            break;
        }

        Ok(ret)
    }
}

#[tokio::test]
async fn test_writer() {
    use crate::io::VecAsyncReader;
    use crate::io::VecAsyncWriter;

    let mut f = VecAsyncWriter::new(Vec::new());

    let mut w = MetacacheWriter::new(&mut f);

    let mut objs = Vec::new();
    for i in 0..10 {
        let info = MetaCacheEntry {
            name: format!("item{}", i),
            metadata: vec![0u8, 10],
            cached: None,
            reusable: false,
        };
        println!("old {:?}", &info);
        objs.push(info);
    }

    w.write(&objs).await.unwrap();

    w.close().await.unwrap();

    let data = f.get_buffer().to_vec();

    let nf = VecAsyncReader::new(data);

    let mut r = MetacacheReader::new(nf);
    let nobjs = r.read_all().await.unwrap();

    for info in nobjs.iter() {
        println!("new {:?}", &info);
    }

    assert_eq!(objs, nobjs)
}
