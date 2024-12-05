use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::str::from_utf8;

use crate::disk::MetaCacheEntry;
use crate::error::Error;
use crate::error::Result;

const METACACHE_STREAM_VERSION: u8 = 2;

pub struct MetacacheWriter<W> {
    wr: W,
    buf: Vec<u8>,
    created: bool,
}

impl<W: Write + Unpin> MetacacheWriter<W> {
    pub fn new(wr: W, block_size: usize) -> Self {
        Self {
            wr,
            buf: Vec::with_capacity(block_size),
            created: false,
        }
    }

    async fn write(&mut self, objs: &[MetaCacheEntry]) -> Result<()> {
        if objs.is_empty() {
            return Ok(());
        }

        if !self.created {
            rmp::encode::write_u8(&mut self.wr, METACACHE_STREAM_VERSION)?;
            self.created = false;
        }

        for obj in objs.iter() {
            if obj.name.is_empty() {
                return Err(Error::msg("metacacheWriter: no name"));
            }

            rmp::encode::write_bool(&mut self.wr, true)?;

            rmp::encode::write_str(&mut self.wr, &obj.name)?;

            rmp::encode::write_bin(&mut self.wr, &obj.metadata)?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        rmp::encode::write_bool(&mut self.wr, false)?;

        self.wr.flush()?;
        Ok(())
    }
}

pub struct MetacacheReader<R> {
    rd: R,
    init: bool,
    err: Option<Error>,
    buf: Vec<u8>,
}

impl<R: Read> MetacacheReader<R> {
    pub fn new(rd: R) -> Self {
        Self {
            rd,
            init: false,
            err: None,
            buf: Vec::new(),
        }
    }

    pub fn check_init(&mut self) {
        if !self.init {
            let ver = match rmp::decode::read_u8(&mut self.rd) {
                Ok(res) => res,
                Err(err) => {
                    self.err = Some(Error::msg(err.to_string()));
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
    }

    pub fn peek(&mut self) -> Result<MetaCacheEntry> {
        self.check_init();

        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        match rmp::decode::read_bool(&mut self.rd) {
            Ok(res) => {
                if !res {
                    self.err = Some(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
                    return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
                }
            }
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                return Err(Error::new(err));
            }
        };

        let l = match rmp::decode::read_str_len(&mut self.rd) {
            Ok(res) => res,
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                return Err(Error::new(err));
            }
        };

        self.buf.resize(l as usize, 0);
        let name = match self.rd.read_exact(&mut self.buf) {
            Ok(()) => {
                let name_buf = self.buf.to_vec();
                match from_utf8(&name_buf) {
                    Ok(decoded) => Ok(decoded.to_owned()),
                    Err(err) => {
                        self.err = Some(Error::msg(err.to_string()));
                        Err(Error::msg(err.to_string()))
                    }
                }
            }
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                Err(Error::msg(err.to_string()))
            }
        }?;

        let l = match rmp::decode::read_bin_len(&mut self.rd) {
            Ok(res) => res,
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                return Err(Error::new(err));
            }
        };
        self.buf.resize(l as usize, 0);
        match self.rd.read_exact(&mut self.buf) {
            Ok(res) => res,
            Err(err) => {
                self.err = Some(Error::msg(err.to_string()));
                return Err(Error::new(err));
            }
        };

        let metadata = self.buf.clone();

        Ok(MetaCacheEntry {
            name,
            metadata,
            cached: None,
            reusable: false,
        })
    }
}

#[tokio::test]
async fn test_writer() {
    use std::fs::File;
    use std::fs::OpenOptions;

    let file_path = "./test_writer.txt";
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .unwrap();

    // let wr = Writer::File(f);

    let mut w = MetacacheWriter::new(f, 1024);

    let mut objs = Vec::new();
    for i in 0..10 {
        objs.push(MetaCacheEntry {
            name: format!("item{}", i),
            metadata: vec![0u8, 10],
            cached: None,
            reusable: false,
        });
    }

    w.write(&objs).await.unwrap();
    w.close().await.unwrap();

    let nf = File::open(file_path).unwrap();

    let meta = nf.metadata().unwrap();

    println!("{}", meta.len());
}
