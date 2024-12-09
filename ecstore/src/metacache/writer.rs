use crate::disk::MetaCacheEntry;
use crate::error::Error;
use crate::error::Result;
use std::io::Read;
use std::io::Write;
use std::str::from_utf8;
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
}

impl<W: Write> MetacacheWriter<W> {
    pub fn new(wr: W) -> Self {
        Self {
            wr,
            created: false,
            // err: None,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        if !self.created {
            rmp::encode::write_u8(&mut self.wr, METACACHE_STREAM_VERSION)?;
            self.created = true;
        }
        Ok(())
    }

    pub fn write(&mut self, objs: &[MetaCacheEntry]) -> Result<()> {
        if objs.is_empty() {
            return Ok(());
        }

        self.init()?;

        for obj in objs.iter() {
            if obj.name.is_empty() {
                return Err(Error::msg("metacacheWriter: no name"));
            }

            self.write_obj(obj)?;
        }

        Ok(())
    }

    pub fn write_obj(&mut self, obj: &MetaCacheEntry) -> Result<()> {
        println!("write_obj {:?}", &obj);

        self.init()?;
        rmp::encode::write_bool(&mut self.wr, true)?;

        rmp::encode::write_str(&mut self.wr, &obj.name)?;

        rmp::encode::write_bin(&mut self.wr, &obj.metadata)?;
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

    pub fn close(&mut self) -> Result<()> {
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

    fn check_init(&mut self) {
        if !self.init {
            // let mut buf = match self.read_buf(1).await {
            //     Ok(res) => res,
            //     Err(err) => {
            //         self.err = Some(Error::msg(err.to_string()));
            //         return;
            //     }
            // };
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

    pub fn peek(&mut self) -> Result<Option<MetaCacheEntry>> {
        self.check_init();

        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        match rmp::decode::read_bool(&mut self.rd) {
            Ok(res) => {
                if !res {
                    return Ok(None);
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

        Ok(Some(MetaCacheEntry {
            name,
            metadata,
            cached: None,
            reusable: false,
        }))
    }

    pub fn read_all(&mut self) -> Result<Vec<MetaCacheEntry>> {
        let mut ret = Vec::new();

        loop {
            if let Some(entry) = self.peek()? {
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
    use crate::io::AsyncToSync;
    use crate::io::VecAsyncReader;
    use crate::io::VecAsyncWriter;

    let mut f = VecAsyncWriter::new(Vec::new());

    let mut w = MetacacheWriter::new(AsyncToSync::new_writer(&mut f));

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

    w.write(&objs).unwrap();

    w.close().unwrap();

    let nf = VecAsyncReader::new(f.get_buffer().to_vec());

    let mut r = MetacacheReader::new(AsyncToSync::new_reader(nf));
    let nobjs = r.read_all().unwrap();

    for info in nobjs.iter() {
        println!("new {:?}", &info);
    }

    assert_eq!(objs, nobjs)
}
