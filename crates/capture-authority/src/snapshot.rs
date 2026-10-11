//! Streaming checkpoint framing. Decoding targets isolated staging only: a failure
//! can leave partial rows. Success verifies framing and digest, not business
//! semantics; callers must validate semantics before promotion to live storage.
use crate::{CaptureBinding, CaptureMembership, LogId};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub cluster_id: [u8; 16],
    pub applied: Option<LogId>,
    pub membership: CaptureMembership,
    pub binding: CaptureBinding,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Section {
    Heads,
    Versions,
    Events,
    Operations,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub total_bytes: u64,
    pub membership_bytes: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub records: u64,
}
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("invalid snapshot framing: {0}")]
    Format(&'static str),
    #[error("snapshot limit exceeded: {0}")]
    Limit(&'static str),
    #[error("snapshot digest mismatch")]
    Digest,
    #[error("snapshot IO failed")]
    Io(#[from] std::io::Error),
}
/// Row values remain opaque. Operation values will contain the canonical decision
/// LogId followed by the existing canonical operation bytes, validated by callers.
pub struct Row<'a> {
    pub section: Section,
    pub key: &'a [u8],
    pub value: &'a [u8],
}
/// Implementations must write exclusively to isolated staging, never live tables.
pub trait StagingSink {
    fn row(&mut self, section: Section, key: &[u8], value: &[u8]) -> std::io::Result<()>;
}
const MAGIC: &[u8; 6] = b"CSNP\x01\x01";
const SECTIONS: [Section; 4] = [Section::Heads, Section::Versions, Section::Events, Section::Operations];

fn limit(ok: bool, name: &'static str) -> Result<(), SnapshotError> {
    if ok { Ok(()) } else { Err(SnapshotError::Limit(name)) }
}
fn format(ok: bool, name: &'static str) -> Result<(), SnapshotError> {
    if ok { Ok(()) } else { Err(SnapshotError::Format(name)) }
}
fn header_valid(h: &Header) -> Result<(), SnapshotError> {
    format(h.cluster_id != [0; 16], "zero cluster ID")?;
    match (h.applied, *h.membership.log_id()) {
        (None, _) => format(
            h.membership.log_id().is_none() && h.membership.configs().is_empty() && h.membership.nodes().is_empty(),
            "membership without applied cut",
        ),
        (Some(applied), Some(member)) => {
            format(member.index < applied.index || member == applied, "membership beyond applied cut")
        }
        _ => Ok(()),
    }
}
fn membership_size(h: &Header, limits: Limits) -> Result<(), SnapshotError> {
    // Preflight the existing codec's wire sizes before its output allocation.
    let mut size = 6u64 + if h.membership.log_id().is_some() { 24 } else { 0 } + 8;
    for group in h.membership.configs() {
        let voters = u64::try_from(group.len()).map_err(|_| SnapshotError::Limit("membership"))?;
        size = size
            .checked_add(4)
            .and_then(|n| voters.checked_mul(8).and_then(|v| n.checked_add(v)))
            .ok_or(SnapshotError::Limit("membership"))?;
        limit(size <= limits.membership_bytes, "membership")?;
    }
    for node in h.membership.nodes().values() {
        // CNOD v1: five-byte prefix, four-byte URI length, raw URI bytes.
        let length = u64::try_from(node.rpc_uri().len())
            .map_err(|_| SnapshotError::Limit("membership"))?
            .checked_add(9)
            .ok_or(SnapshotError::Limit("membership"))?;
        size = size
            .checked_add(12)
            .and_then(|n| n.checked_add(length))
            .ok_or(SnapshotError::Limit("membership"))?;
        limit(size <= limits.membership_bytes, "membership")?;
    }
    limit(size <= limits.membership_bytes, "membership")?;
    let header_bytes = 6u64 + 16 + 1 + if h.applied.is_some() { 24 } else { 0 } + 8 + 40 + 36 + 32;
    limit(size.checked_add(header_bytes).is_some_and(|n| n <= limits.total_bytes), "total bytes")
}
struct Output<W> {
    io: W,
    hash: Sha256,
    used: u64,
    max: u64,
}
impl<W: Write> Output<W> {
    fn bytes(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let len = u64::try_from(bytes.len()).map_err(|_| SnapshotError::Limit("total bytes"))?;
        let next = self.used.checked_add(len).ok_or(SnapshotError::Limit("total bytes"))?;
        limit(next.checked_add(32).is_some_and(|n| n <= self.max), "total bytes")?;
        self.io.write_all(bytes)?;
        self.hash.update(bytes);
        self.used = next;
        Ok(())
    }
    fn number(&mut self, n: u64) -> Result<(), SnapshotError> {
        self.bytes(&n.to_be_bytes())
    }
}
struct Input<R> {
    io: R,
    hash: Sha256,
    used: u64,
    max: u64,
}
impl<R: Read> Input<R> {
    fn reserve(&self, len: u64) -> Result<(), SnapshotError> {
        limit(
            self.used
                .checked_add(len)
                .and_then(|n| n.checked_add(32))
                .is_some_and(|n| n <= self.max),
            "total bytes",
        )
    }
    fn bytes(&mut self, bytes: &mut [u8]) -> Result<(), SnapshotError> {
        let len = u64::try_from(bytes.len()).map_err(|_| SnapshotError::Limit("total bytes"))?;
        self.reserve(len)?;
        self.io.read_exact(bytes)?;
        self.hash.update(&*bytes);
        self.used += len;
        Ok(())
    }
    fn byte(&mut self) -> Result<u8, SnapshotError> {
        let mut b = [0];
        self.bytes(&mut b)?;
        Ok(b[0])
    }
    fn number(&mut self) -> Result<u64, SnapshotError> {
        let mut b = [0; 8];
        self.bytes(&mut b)?;
        Ok(u64::from_be_bytes(b))
    }
    fn vector(&mut self, len: u64) -> Result<Vec<u8>, SnapshotError> {
        self.reserve(len)?;
        let len = usize::try_from(len).map_err(|_| SnapshotError::Limit("allocation"))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| SnapshotError::Limit("allocation"))?;
        bytes.resize(len, 0);
        self.bytes(&mut bytes)?;
        Ok(bytes)
    }
}
fn row_lengths(k: u64, v: u64, limits: Limits) -> Result<u64, SnapshotError> {
    limit(k <= limits.key_bytes, "key bytes")?;
    limit(v <= limits.value_bytes, "value bytes")?;
    k.checked_add(v).ok_or(SnapshotError::Limit("record bytes"))
}
/// Incremental encoder for isolated snapshot output. Completion verifies framing,
/// not persistence or installation. A row error permanently fences the writer.
pub struct Encoder<W: Write> {
    out: Output<W>,
    counts: [u64; 4],
    section: usize,
    written: u64,
    previous: Option<Vec<u8>>,
    limits: Limits,
    failed: bool,
}
impl<W: Write> Encoder<W> {
    pub fn new(writer: W, header: &Header, counts: [u64; 4], limits: Limits) -> Result<Self, SnapshotError> {
        header_valid(header)?;
        let count = counts
            .iter()
            .try_fold(0u64, |n, c| n.checked_add(*c).ok_or(SnapshotError::Limit("records")))?;
        limit(count <= limits.records, "records")?;
        format(header.applied.is_some() || count == 0, "rows without applied cut")?;
        membership_size(header, limits)?;
        let membership = header.membership.encode().map_err(|_| SnapshotError::Format("membership"))?;
        let membership_len = u64::try_from(membership.len()).map_err(|_| SnapshotError::Limit("membership"))?;
        limit(membership_len <= limits.membership_bytes, "membership")?;
        let mut out = Output {
            io: writer,
            hash: Sha256::new(),
            used: 0,
            max: limits.total_bytes,
        };
        out.bytes(MAGIC)?;
        out.bytes(&header.cluster_id)?;
        match header.applied {
            None => out.bytes(&[0])?,
            Some(id) => {
                out.bytes(&[1])?;
                for n in [id.term, id.leader_node, id.index] {
                    out.number(n)?;
                }
            }
        }
        out.number(membership_len)?;
        out.bytes(&membership)?;
        out.number(header.binding.revision)?;
        out.bytes(&header.binding.binding_id)?;
        out.bytes(&header.binding.target_id)?;
        out.bytes(&[1])?;
        out.number(counts[0])?;
        let mut encoder = Self {
            out,
            counts,
            section: 0,
            written: 0,
            previous: None,
            limits,
            failed: false,
        };
        encoder.advance_sections()?;
        Ok(encoder)
    }

    fn advance_sections(&mut self) -> Result<(), SnapshotError> {
        while self.section < SECTIONS.len() && self.written == self.counts[self.section] {
            self.section += 1;
            self.written = 0;
            self.previous = None;
            if self.section < SECTIONS.len() {
                self.out
                    .bytes(&[u8::try_from(self.section + 1).map_err(|_| SnapshotError::Format("section"))?])?;
                self.out.number(self.counts[self.section])?;
            }
        }
        Ok(())
    }

    pub fn row(&mut self, row: Row<'_>) -> Result<(), SnapshotError> {
        format(!self.failed, "encoder failed")?;
        let result = self.write_row(row);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn write_row(&mut self, row: Row<'_>) -> Result<(), SnapshotError> {
        format(self.section < SECTIONS.len(), "record count")?;
        format(row.section == SECTIONS[self.section], "section order")?;
        let k = u64::try_from(row.key.len()).map_err(|_| SnapshotError::Limit("key bytes"))?;
        let v = u64::try_from(row.value.len()).map_err(|_| SnapshotError::Limit("value bytes"))?;
        let length = row_lengths(k, v, self.limits)?
            .checked_add(16)
            .ok_or(SnapshotError::Limit("record bytes"))?;
        // Reserve the still-unwritten section framing as well as the footer.
        let framing = u64::try_from(SECTIONS.len() - self.section - 1)
            .map_err(|_| SnapshotError::Limit("total bytes"))?
            .checked_mul(9)
            .ok_or(SnapshotError::Limit("total bytes"))?;
        limit(
            self.out
                .used
                .checked_add(length)
                .and_then(|n| n.checked_add(framing))
                .and_then(|n| n.checked_add(32))
                .is_some_and(|n| n <= self.out.max),
            "total bytes",
        )?;
        format(self.previous.as_ref().is_none_or(|p| p.as_slice() < row.key), "key order")?;
        self.out.number(k)?;
        self.out.number(v)?;
        self.out.bytes(row.key)?;
        self.out.bytes(row.value)?;
        self.previous = Some(row.key.to_vec());
        self.written += 1;
        self.advance_sections()
    }

    pub fn finish(self) -> Result<W, SnapshotError> {
        format(!self.failed, "encoder failed")?;
        format(self.section == SECTIONS.len(), "record count")?;
        let mut out = self.out;
        out.io.write_all(&out.hash.finalize())?;
        Ok(out.io)
    }
}

pub fn encode<'a>(
    writer: impl Write,
    header: &Header,
    counts: [u64; 4],
    rows: impl IntoIterator<Item = Row<'a>>,
    limits: Limits,
) -> Result<(), SnapshotError> {
    let mut encoder = Encoder::new(writer, header, counts, limits)?;
    for row in rows {
        encoder.row(row)?;
    }
    encoder.finish()?;
    Ok(())
}
pub fn decode(reader: impl Read, sink: &mut impl StagingSink, limits: Limits) -> Result<Header, SnapshotError> {
    let mut input = Input {
        io: reader,
        hash: Sha256::new(),
        used: 0,
        max: limits.total_bytes,
    };
    let mut magic = [0; 6];
    input.bytes(&mut magic)?;
    format(&magic == MAGIC, "magic/version/algorithm")?;
    let mut cluster_id = [0; 16];
    input.bytes(&mut cluster_id)?;
    let applied = match input.byte()? {
        0 => None,
        1 => Some(LogId {
            term: input.number()?,
            leader_node: input.number()?,
            index: input.number()?,
        }),
        _ => return Err(SnapshotError::Format("applied tag")),
    };
    let m = input.number()?;
    limit(m <= limits.membership_bytes, "membership")?;
    let membership = CaptureMembership::decode(&input.vector(m)?).map_err(|_| SnapshotError::Format("membership"))?;
    let revision = input.number()?;
    let mut binding_id = [0; 16];
    input.bytes(&mut binding_id)?;
    let mut target_id = [0; 16];
    input.bytes(&mut target_id)?;
    let header = Header {
        cluster_id,
        applied,
        membership,
        binding: CaptureBinding {
            revision,
            binding_id,
            target_id,
        },
    };
    header_valid(&header)?;
    let mut records = 0u64;
    for (index, section) in SECTIONS.into_iter().enumerate() {
        let tag = input.byte()?;
        format(usize::from(tag) == index + 1, "section order")?;
        let count = input.number()?;
        records = records.checked_add(count).ok_or(SnapshotError::Limit("records"))?;
        limit(records <= limits.records, "records")?;
        format(header.applied.is_some() || count == 0, "rows without applied cut")?;
        input.reserve(count.checked_mul(16).ok_or(SnapshotError::Limit("record bytes"))?)?;
        let mut previous: Option<Vec<u8>> = None;
        for _ in 0..count {
            let k = input.number()?;
            let v = input.number()?;
            let len = row_lengths(k, v, limits)?;
            input.reserve(len)?;
            let key = input.vector(k)?;
            let value = input.vector(v)?;
            format(previous.as_ref().is_none_or(|p| p.as_slice() < key.as_slice()), "key order")?;
            sink.row(section, &key, &value)?;
            previous = Some(key);
        }
    }
    let mut footer = [0; 32];
    input.io.read_exact(&mut footer)?;
    if input.hash.finalize().as_slice() != footer {
        return Err(SnapshotError::Digest);
    }
    let mut extra = [0];
    loop {
        match input.io.read(&mut extra) {
            Ok(0) => break,
            Ok(_) => return Err(SnapshotError::Format("trailing bytes")),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(SnapshotError::Io(e)),
        }
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        collections::BTreeMap,
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    fn limits() -> Limits {
        Limits {
            total_bytes: 4096,
            membership_bytes: 512,
            key_bytes: 32,
            value_bytes: 64,
            records: 16,
        }
    }
    fn header() -> Header {
        Header {
            cluster_id: [1; 16],
            applied: Some(LogId {
                term: 2,
                leader_node: 7,
                index: 9,
            }),
            membership: CaptureMembership::new(None, vec![], BTreeMap::new()).expect("empty membership"),
            binding: CaptureBinding {
                revision: 3,
                binding_id: [4; 16],
                target_id: [5; 16],
            },
        }
    }
    const SECTIONS: [Section; 4] = [Section::Heads, Section::Versions, Section::Events, Section::Operations];
    fn rows() -> Vec<Row<'static>> {
        vec![
            Row {
                section: Section::Heads,
                key: b"a",
                value: b"head",
            },
            Row {
                section: Section::Heads,
                key: b"b",
                value: b"next",
            },
            Row {
                section: Section::Versions,
                key: b"v",
                value: b"version",
            },
            Row {
                section: Section::Operations,
                key: b"o",
                value: b"opaque-operation",
            },
        ]
    }
    #[derive(Default)]
    struct Sink(Vec<(Section, Vec<u8>, Vec<u8>)>);
    impl StagingSink for Sink {
        fn row(&mut self, s: Section, k: &[u8], v: &[u8]) -> io::Result<()> {
            self.0.push((s, k.to_vec(), v.to_vec()));
            Ok(())
        }
    }
    // No Seek implementation; every read is deliberately short.
    struct Chunked<'a>(&'a [u8]);
    impl Read for Chunked<'_> {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            let n = b.len().min(3).min(self.0.len());
            b[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }
    fn encoded() -> Vec<u8> {
        let mut b = vec![];
        let result = encode(&mut b, &header(), [2, 1, 0, 1], rows(), limits());
        assert!(result.is_ok(), "U22_CODEC_ROUNDTRIP: {result:?}");
        b
    }
    #[test]
    fn u23_redb_guard_rows() -> Result<(), Box<dyn std::error::Error>> {
        use redb::{ReadableDatabase, ReadableTable, TableDefinition};
        const TABLE: TableDefinition<&str, &str> = TableDefinition::new("u23_rows");
        let dir = tempfile::tempdir()?;
        let db = redb::Database::create(dir.path().join("rows.redb"))?;
        let fixture = [("a", "head"), ("b", "next")];
        let write = db.begin_write()?;
        {
            let mut table = write.open_table(TABLE)?;
            for (key, value) in fixture {
                table.insert(key, value)?;
            }
        }
        write.commit()?;
        let read = db.begin_read()?;
        let table = read.open_table(TABLE)?;
        let mut bytes = Vec::new();
        let result = Encoder::new(&mut bytes, &header(), [2, 0, 0, 0], limits());
        assert!(result.is_ok(), "U23_STREAM_GUARD: constructor failed");
        let mut encoder = result?;
        for row in table.iter()? {
            let (key, value) = row?;
            encoder.row(Row {
                section: Section::Heads,
                key: key.value().as_bytes(),
                value: value.value().as_bytes(),
            })?;
        }
        encoder.finish()?;
        let mut expected = Vec::new();
        encode(
            &mut expected,
            &header(),
            [2, 0, 0, 0],
            fixture.into_iter().map(|(key, value)| Row {
                section: Section::Heads,
                key: key.as_bytes(),
                value: value.as_bytes(),
            }),
            limits(),
        )?;
        assert_eq!(bytes, expected, "U23_STREAM_GUARD: wire changed");
        let mut sink = Sink::default();
        assert_eq!(decode(bytes.as_slice(), &mut sink, limits())?, header());
        assert_eq!(sink.0.len(), fixture.len());
        for ((section, key, value), (expected_key, expected_value)) in sink.0.iter().zip(fixture) {
            assert_eq!(*section, Section::Heads);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value, expected_value.as_bytes());
        }
        Ok(())
    }

    #[test]
    fn u23_failure_fence() {
        use std::{cell::Cell, rc::Rc};
        struct Recovering {
            fail: Rc<Cell<bool>>,
            writes: Rc<Cell<usize>>,
        }
        impl Write for Recovering {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.writes.set(self.writes.get() + 1);
                if self.fail.replace(false) {
                    Err(io::Error::other("once"))
                } else {
                    Ok(bytes.len())
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let fail = Rc::new(Cell::new(false));
        let writes = Rc::new(Cell::new(0));
        let mut encoder = Encoder::new(
            Recovering {
                fail: fail.clone(),
                writes: writes.clone(),
            },
            &header(),
            [1, 0, 0, 0],
            limits(),
        )
        .expect("U23_FAILURE_FENCE: construct");
        fail.set(true);
        assert!(matches!(
            encoder.row(Row {
                section: Section::Heads,
                key: b"a",
                value: b"v"
            }),
            Err(SnapshotError::Io(_))
        ));
        let after_failure = writes.get();
        assert!(
            encoder
                .row(Row {
                    section: Section::Heads,
                    key: b"a",
                    value: b"v"
                })
                .is_err(),
            "U23_FAILURE_FENCE: recovered after IO error"
        );
        assert!(encoder.finish().is_err(), "U23_FAILURE_FENCE: finish after IO error");
        assert_eq!(writes.get(), after_failure, "U23_FAILURE_FENCE: wrote after error");
        for invalid in [
            Row {
                section: Section::Versions,
                key: b"a",
                value: b"v",
            },
            Row {
                section: Section::Heads,
                key: &[0; 33],
                value: b"v",
            },
        ] {
            let mut output = Vec::new();
            let mut encoder = Encoder::new(&mut output, &header(), [1, 0, 0, 0], limits()).expect("construct validation encoder");
            assert!(encoder.row(invalid).is_err());
            assert!(
                encoder
                    .row(Row {
                        section: Section::Heads,
                        key: b"a",
                        value: b"v"
                    })
                    .is_err(),
                "U23_FAILURE_FENCE: validation retry"
            );
            assert!(encoder.finish().is_err(), "U23_FAILURE_FENCE: validation finish");
        }
        for key in [b"b".as_slice(), b"a".as_slice()] {
            let mut output = Vec::new();
            let mut encoder = Encoder::new(&mut output, &header(), [2, 0, 0, 0], limits()).expect("construct ordered encoder");
            encoder
                .row(Row {
                    section: Section::Heads,
                    key: b"b",
                    value: b"v",
                })
                .expect("first row");
            assert!(
                encoder
                    .row(Row {
                        section: Section::Heads,
                        key,
                        value: b"v"
                    })
                    .is_err()
            );
            assert!(encoder.finish().is_err(), "U23_FAILURE_FENCE: key order finish");
        }
    }

    #[test]
    fn u23_finish_counts() {
        for counts in [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 0, 1]] {
            let mut output = Vec::new();
            let encoder = Encoder::new(&mut output, &header(), counts, limits()).expect("construct incomplete encoder");
            assert!(encoder.finish().is_err(), "U23_FINISH_COUNT: incomplete accepted");
            assert!(
                decode(output.as_slice(), &mut Sink::default(), limits()).is_err(),
                "U23_FINISH_COUNT: incomplete footer"
            );
        }
        for counts in [[0, 0, 0, 0], [0, 0, 0, 1]] {
            let mut output = Vec::new();
            let mut encoder = Encoder::new(&mut output, &header(), counts, limits()).expect("construct empty-section encoder");
            if counts[3] == 1 {
                encoder
                    .row(Row {
                        section: Section::Operations,
                        key: b"o",
                        value: b"v",
                    })
                    .expect("skip empty sections");
            }
            encoder.finish().expect("U23_FINISH_COUNT: exact count");
            decode(output.as_slice(), &mut Sink::default(), limits()).expect("valid finished snapshot");
        }
        let mut output = Vec::new();
        let mut encoder = Encoder::new(&mut output, &header(), [1, 0, 0, 0], limits()).expect("construct extra-row encoder");
        encoder
            .row(Row {
                section: Section::Heads,
                key: b"a",
                value: b"v",
            })
            .expect("declared row");
        assert!(
            encoder
                .row(Row {
                    section: Section::Heads,
                    key: b"b",
                    value: b"v"
                })
                .is_err(),
            "U23_FINISH_COUNT: extra row"
        );
        assert!(encoder.finish().is_err(), "U23_FINISH_COUNT: extra row finish");
    }

    #[test]
    fn u23_row_limits() {
        use std::{cell::Cell, rc::Rc};
        struct Counting(Rc<Cell<usize>>);
        impl Write for Counting {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.set(self.0.get() + bytes.len());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let empty = {
            let mut bytes = Vec::new();
            encode(&mut bytes, &header(), [0; 4], [], limits()).expect("empty framing length");
            u64::try_from(bytes.len()).expect("length")
        };
        for (bounded, key, value, label) in [
            (limits(), &[0; 33][..], b"v".as_slice(), "key bytes"),
            (limits(), b"a".as_slice(), &[0; 65][..], "value bytes"),
            (
                Limits {
                    total_bytes: empty + 17,
                    ..limits()
                },
                b"a".as_slice(),
                b"v".as_slice(),
                "total bytes",
            ),
        ] {
            let written = Rc::new(Cell::new(0));
            let mut encoder =
                Encoder::new(Counting(written.clone()), &header(), [1, 0, 0, 0], bounded).expect("construct bounded encoder");
            let before = written.get();
            assert!(
                matches!(encoder.row(Row { section: Section::Heads, key, value }), Err(SnapshotError::Limit(name)) if name == label),
                "U23_ROW_LIMIT: {label}"
            );
            assert_eq!(written.get(), before, "U23_ROW_LIMIT: wrote rejected row");
            assert!(encoder.finish().is_err(), "U23_ROW_LIMIT: failed finish");
            assert_eq!(written.get(), before, "U23_ROW_LIMIT: wrote footer");
        }
    }

    // Independent wire fixture: magic, version, hash algorithm, cluster, optional
    // applied ID, length-prefixed complete membership, binding, four sections.
    fn raw(h: &Header, entries: &[(u8, Vec<(Vec<u8>, Vec<u8>)>)]) -> Vec<u8> {
        let mut b = b"CSNP\x01\x01".to_vec();
        b.extend(h.cluster_id);
        match h.applied {
            None => b.push(0),
            Some(id) => {
                b.push(1);
                for n in [id.term, id.leader_node, id.index] {
                    b.extend(n.to_be_bytes());
                }
            }
        }
        let m = h.membership.encode().expect("membership fixture");
        b.extend(u64::try_from(m.len()).expect("fixture length").to_be_bytes());
        b.extend(m);
        b.extend(h.binding.revision.to_be_bytes());
        b.extend(h.binding.binding_id);
        b.extend(h.binding.target_id);
        for (tag, rows) in entries {
            b.push(*tag);
            b.extend(u64::try_from(rows.len()).expect("fixture count").to_be_bytes());
            for (k, v) in rows {
                b.extend(u64::try_from(k.len()).expect("key length").to_be_bytes());
                b.extend(u64::try_from(v.len()).expect("value length").to_be_bytes());
                b.extend(k);
                b.extend(v);
            }
        }
        resign(&mut b);
        b
    }
    fn empty_entries() -> Vec<(u8, Vec<(Vec<u8>, Vec<u8>)>)> {
        (1..=4).map(|t| (t, vec![])).collect()
    }
    fn resign_payload(b: &mut Vec<u8>) {
        b.truncate(b.len() - 32);
        resign(b);
    }
    fn resign(b: &mut Vec<u8>) {
        let d = Sha256::digest(&*b);
        b.extend_from_slice(&d);
    }
    fn first_section_offset(h: &Header) -> usize {
        6 + 16 + 1 + if h.applied.is_some() { 24 } else { 0 } + 8 + h.membership.encode().expect("membership").len() + 40
    }
    fn rejected(b: &[u8]) {
        assert!(decode(Chunked(b), &mut Sink::default(), limits()).is_err(), "invalid framing accepted");
    }
    #[test]
    fn u22_roundtrip_stream() {
        let b = encoded();
        let mut sink = Sink::default();
        let h = decode(Chunked(&b), &mut sink, limits()).expect("U22_CODEC_ROUNDTRIP decode");
        assert_eq!(h, header(), "U22_CODEC_ROUNDTRIP header");
        let expected: Vec<_> = rows()
            .into_iter()
            .map(|r| (r.section, r.key.to_vec(), r.value.to_vec()))
            .collect();
        assert_eq!(sink.0, expected, "U22_CODEC_ROUNDTRIP rows");
        let entries = vec![
            (1, vec![(b"a".to_vec(), b"head".to_vec()), (b"b".to_vec(), b"next".to_vec())]),
            (2, vec![(b"v".to_vec(), b"version".to_vec())]),
            (3, vec![]),
            (4, vec![(b"o".to_vec(), b"opaque-operation".to_vec())]),
        ];
        assert_eq!(b, raw(&header(), &entries), "deterministic wire");
    }
    #[test]
    fn u22_tampered_digest() {
        let mut b = raw(&header(), &empty_entries());
        b[6] ^= 1;
        let result = decode(Chunked(&b), &mut Sink::default(), limits());
        assert!(matches!(result, Err(SnapshotError::Digest)), "U22_DIGEST: {result:?}");
        let mut b = raw(&header(), &empty_entries());
        let end = b.len() - 1;
        b[end] ^= 1;
        assert!(
            matches!(decode(Chunked(&b), &mut Sink::default(), limits()), Err(SnapshotError::Digest)),
            "U22_DIGEST footer"
        );
    }
    #[test]
    fn u22_strict_framing() {
        let good = raw(&header(), &empty_entries());
        for offset in [0, 4, 5, 22] {
            let mut b = good.clone();
            b[offset] = 255;
            resign_payload(&mut b);
            rejected(&b);
        }
        for end in [0, 5, 22, good.len() - 1] {
            rejected(&good[..end]);
        }
        let mut b = good.clone();
        b.push(0);
        rejected(&b);
        let mut b = good.clone();
        let o = first_section_offset(&header());
        b[o + 1..o + 9].copy_from_slice(&1u64.to_be_bytes());
        resign_payload(&mut b);
        rejected(&b);
        let mut e = empty_entries();
        e.pop();
        rejected(&raw(&header(), &e));
        let mut e = empty_entries();
        e[2].0 = 9;
        rejected(&raw(&header(), &e));
    }
    #[test]
    fn u22_bounded_lengths() {
        let mut e = empty_entries();
        e[0].1.push((vec![1; 33], vec![2]));
        let b = raw(&header(), &e);
        let mut sink = Sink::default();
        let result = decode(Chunked(&b), &mut sink, limits());
        assert!(matches!(result, Err(SnapshotError::Limit(_))), "U22_LIMIT: {result:?}");
        assert!(sink.0.is_empty(), "U22_LIMIT before staging");
        let o = first_section_offset(&header());
        for (offset, length) in [(o + 9, u64::MAX), (o + 17, u64::MAX), (o + 1, u64::MAX), (47, u64::MAX)] {
            let mut b = b.clone();
            b[offset..offset + 8].copy_from_slice(&length.to_be_bytes());
            resign_payload(&mut b);
            let result = decode(Chunked(&b), &mut Sink::default(), limits());
            assert!(matches!(result, Err(SnapshotError::Limit(_))), "U22_LIMIT overflow {offset}: {result:?}");
        }
        let good = raw(&header(), &empty_entries());
        let mut l = limits();
        l.total_bytes = 16;
        assert!(matches!(decode(Chunked(&good), &mut Sink::default(), l), Err(SnapshotError::Limit(_))));
        let mut out = vec![];
        let mut l = limits();
        l.key_bytes = 0;
        assert!(matches!(
            encode(&mut out, &header(), [2, 1, 0, 1], rows(), l),
            Err(SnapshotError::Limit(_))
        ));
    }
    #[test]
    fn u22_ordered_sections() {
        for keys in [[b"b", b"a"], [b"a", b"a"]] {
            let mut e = empty_entries();
            e[0].1 = keys.into_iter().map(|k| (k.to_vec(), vec![1])).collect();
            let b = raw(&header(), &e);
            let result = decode(Chunked(&b), &mut Sink::default(), limits());
            assert!(matches!(result, Err(SnapshotError::Format(_))), "U22_ORDER: {result:?}");
        }
        let mut e = empty_entries();
        e.swap(0, 1);
        rejected(&raw(&header(), &e));
        let mut out = vec![];
        assert!(encode(&mut out, &header(), [1, 0, 0, 0], [], limits()).is_err(), "count mismatch");
        let rs = [
            Row {
                section: SECTIONS[1],
                key: b"a",
                value: b"b",
            },
            Row {
                section: SECTIONS[0],
                key: b"a",
                value: b"b",
            },
        ];
        assert!(encode(&mut out, &header(), [1, 1, 0, 0], rs, limits()).is_err(), "section regression");
    }
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("read fault"))
        }
    }
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("write fault"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl StagingSink for Broken {
        fn row(&mut self, _: Section, _: &[u8], _: &[u8]) -> io::Result<()> {
            Err(io::Error::other("sink fault"))
        }
    }
    #[test]
    fn u22_io_failures() {
        let r = decode(Broken, &mut Sink::default(), limits());
        assert!(matches!(r, Err(SnapshotError::Io(_))));
        let e = encode(Broken, &header(), [0; 4], [], limits());
        assert!(matches!(e, Err(SnapshotError::Io(_))));
        let mut e = empty_entries();
        e[0].1.push((vec![1], vec![2]));
        let r = decode(Chunked(&raw(&header(), &e)), &mut Broken, limits());
        assert!(matches!(r, Err(SnapshotError::Io(_))));
        use std::error::Error;
        let err = SnapshotError::Io(io::Error::other("cause"));
        assert!(err.source().is_some());
    }
    #[test]
    fn u22_header_pair() {
        let mut h = header();
        h.cluster_id = [0; 16];
        rejected(&raw(&h, &empty_entries()));
        let mut h = header();
        h.applied = None;
        let b = raw(&h, &empty_entries());
        assert_eq!(decode(Chunked(&b), &mut Sink::default(), limits()).expect("empty checkpoint"), h);
        let mut e = empty_entries();
        e[0].1.push((vec![1], vec![2]));
        rejected(&raw(&h, &e));
        for id in [
            LogId {
                term: 2,
                leader_node: 7,
                index: 10,
            },
            LogId {
                term: 3,
                leader_node: 7,
                index: 9,
            },
        ] {
            let mut h = header();
            h.membership = CaptureMembership::new(Some(id), vec![], BTreeMap::new()).expect("membership ID");
            rejected(&raw(&h, &empty_entries()));
            h.applied = None;
            rejected(&raw(&h, &empty_entries()));
        }
        let mut joint = header();
        let nodes = (1..=4)
            .map(|id| {
                (
                    id,
                    crate::CaptureNode::new(format!("http://127.0.0.1:{}", 9000 + id)).expect("joint node"),
                )
            })
            .collect();
        joint.membership =
            CaptureMembership::new(joint.applied, vec![[1, 2].into_iter().collect(), [2, 3].into_iter().collect()], nodes)
                .expect("joint membership and learner");
        let mut wire = vec![];
        encode(&mut wire, &joint, [0; 4], [], limits()).expect("encode complete joint membership");
        assert_eq!(
            decode(Chunked(&wire), &mut Sink::default(), limits()).expect("decode complete joint membership"),
            joint
        );
        let mut h = header();
        h.membership = CaptureMembership::new(h.applied, vec![], BTreeMap::new()).expect("equal cut");
        assert_eq!(
            decode(Chunked(&raw(&h, &empty_entries())), &mut Sink::default(), limits()).expect("matching ID"),
            h
        );
    }
    #[test]
    fn u22_incremental_sink() {
        struct ProgressRead<'a> {
            bytes: &'a [u8],
            read: Arc<AtomicUsize>,
        }
        impl Read for ProgressRead<'_> {
            fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
                let n = b.len().min(1).min(self.bytes.len());
                b[..n].copy_from_slice(&self.bytes[..n]);
                self.bytes = &self.bytes[n..];
                self.read.fetch_add(n, Ordering::SeqCst);
                Ok(n)
            }
        }
        struct ProgressSink {
            read: Arc<AtomicUsize>,
            end: usize,
            rows: usize,
        }
        impl StagingSink for ProgressSink {
            fn row(&mut self, _: Section, _: &[u8], _: &[u8]) -> io::Result<()> {
                assert!(self.read.load(Ordering::SeqCst) <= self.end, "sink must precede footer");
                self.rows += 1;
                Ok(())
            }
        }
        let b = encoded();
        let read = Arc::new(AtomicUsize::new(0));
        let mut sink = ProgressSink {
            read: read.clone(),
            end: b.len() - 32,
            rows: 0,
        };
        decode(ProgressRead { bytes: &b, read }, &mut sink, limits()).expect("incremental decode");
        assert_eq!(sink.rows, 4);
        let mut b = b;
        let last = b.len() - 1;
        b[last] ^= 1;
        let mut sink = Sink::default();
        assert!(matches!(decode(Chunked(&b), &mut sink, limits()), Err(SnapshotError::Digest)));
        assert_eq!(sink.0.len(), 4, "failure leaves isolated partial staging");
    }
}
