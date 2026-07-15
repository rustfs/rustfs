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

//! Input expansion: files, directories, archives (.zip/.tar/.tar.gz/.zst/.gz),
//! and arbitrary readers -> line-parsed [`LogEvent`]s via a callback.
//!
//! Nothing is ever extracted to disk: archive entries are streamed (or, for
//! nested zips that need `Seek`, buffered in memory under a cap), and entry
//! paths are only used as provenance strings — which makes the walker immune
//! to zip-slip by construction. A single unreadable input is recorded in
//! [`IngestReport::skipped`] and never fails the whole run; only a missing
//! top-level path returns `Err`.

mod detect;
mod report;

pub use report::{IngestReport, SkipReason};

use crate::model::LogEvent;
use crate::parse::LineParser;
use detect::{Format, detect, looks_binary};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

const PEEK_LEN: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Max archive nesting depth (gzip->tar counts 2). Default 3.
    pub max_depth: u32,
    /// Max entries processed per archive. Default 10_000.
    pub max_entries_per_archive: u64,
    /// Global cap on decompressed bytes fed to parsers. Default 8 GiB.
    pub max_total_bytes: u64,
    /// Nested archives (which need Seek) are buffered in memory up to this. Default 512 MiB.
    pub max_in_memory_archive: u64,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_entries_per_archive: 10_000,
            max_total_bytes: 8 * 1024 * 1024 * 1024,
            max_in_memory_archive: 512 * 1024 * 1024,
        }
    }
}

struct Ctx<'a> {
    opts: &'a IngestOptions,
    report: IngestReport,
    on_event: &'a mut dyn FnMut(LogEvent),
    byte_budget_exhausted: bool,
}

impl Ctx<'_> {
    fn skip(&mut self, provenance: impl Into<String>, reason: SkipReason) {
        self.report.skipped.push((provenance.into(), reason));
    }
}

/// Ingests a file or directory. Directories are walked recursively without
/// following symlinks; the first-level subdirectory name becomes the node
/// label (see rustfs/backlog#1284 for the full labeling rules).
pub fn ingest_path(path: &Path, opts: &IngestOptions, on_event: &mut dyn FnMut(LogEvent)) -> std::io::Result<IngestReport> {
    let meta = std::fs::metadata(path)?;
    let mut ctx = Ctx {
        opts,
        report: IngestReport::default(),
        on_event,
        byte_budget_exhausted: false,
    };

    if meta.is_dir() {
        for entry in walkdir::WalkDir::new(path).follow_links(false).sort_by_file_name() {
            let entry = match entry {
                Ok(e) => e,
                Err(err) => {
                    let at = err
                        .path()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| path.display().to_string());
                    ctx.skip(at, SkipReason::Unreadable);
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let provenance = entry.path().display().to_string();
            if ctx.byte_budget_exhausted {
                ctx.skip(provenance, SkipReason::ByteCap);
                continue;
            }
            let node = entry
                .path()
                .strip_prefix(path)
                .ok()
                .and_then(|rel| rel.parent()?.components().next())
                .and_then(|c| match c {
                    std::path::Component::Normal(os) => os.to_str().map(Arc::from),
                    _ => None,
                });
            match File::open(entry.path()) {
                Ok(file) => walk_file(file, provenance, node, &mut ctx),
                Err(_) => ctx.skip(provenance, SkipReason::Unreadable),
            }
        }
    } else {
        let file = File::open(path)?;
        walk_file(file, path.display().to_string(), None, &mut ctx);
    }
    Ok(ctx.report)
}

/// Ingests an arbitrary reader (stdin, in-memory buffers). `name` seeds the
/// provenance string and the extension-based format fallback.
pub fn ingest_reader(
    reader: &mut dyn Read,
    name: &str,
    opts: &IngestOptions,
    on_event: &mut dyn FnMut(LogEvent),
) -> std::io::Result<IngestReport> {
    let mut ctx = Ctx {
        opts,
        report: IngestReport::default(),
        on_event,
        byte_budget_exhausted: false,
    };
    walk_stream(reader, name.to_string(), name.to_string(), None, 0, &mut ctx);
    Ok(ctx.report)
}

/// Top-level files get a seekable fast path so large customer zips are read
/// directly from disk instead of being buffered in memory.
fn walk_file(mut file: File, provenance: String, node: Option<Arc<str>>, ctx: &mut Ctx) {
    let mut head = Vec::with_capacity(PEEK_LEN);
    if (&mut file).take(PEEK_LEN as u64).read_to_end(&mut head).is_err() || file.seek(SeekFrom::Start(0)).is_err() {
        ctx.skip(provenance, SkipReason::Unreadable);
        return;
    }
    let name = leaf_name(&provenance).to_string();
    if detect(&head, &name) == Format::Zip {
        match zip::ZipArchive::new(file) {
            Ok(mut archive) => walk_zip_entries(&mut archive, &provenance, &node, 0, ctx),
            Err(_) => ctx.skip(provenance, SkipReason::Unreadable),
        }
        return;
    }
    walk_stream(&mut file, provenance, name, node, 0, ctx);
}

/// Core recursive walker over a non-seekable stream. `effective_name` is the
/// name used for extension fallback; each decompression layer strips its own
/// extension from it (`x.tar.gz` -> `x.tar`) so re-detection terminates.
fn walk_stream(
    reader: &mut dyn Read,
    provenance: String,
    effective_name: String,
    node: Option<Arc<str>>,
    depth: u32,
    ctx: &mut Ctx,
) {
    if ctx.byte_budget_exhausted {
        ctx.skip(provenance, SkipReason::ByteCap);
        return;
    }
    if depth > ctx.opts.max_depth {
        ctx.skip(provenance, SkipReason::TooDeep);
        return;
    }

    let mut head = Vec::with_capacity(PEEK_LEN);
    if reader.take(PEEK_LEN as u64).read_to_end(&mut head).is_err() {
        ctx.skip(provenance, SkipReason::Unreadable);
        return;
    }
    let mut full = Cursor::new(head.clone()).chain(reader);

    match detect(&head, &effective_name) {
        Format::Plain => {
            if looks_binary(&head) {
                ctx.skip(provenance, SkipReason::Binary);
                return;
            }
            parse_lines(&mut full, provenance, node, ctx);
        }
        Format::Gzip => {
            let inner_name = strip_ext(&effective_name, &[".gz", ".tgz"]);
            let mut decoder = flate2::read::MultiGzDecoder::new(full);
            walk_stream(&mut decoder, provenance, inner_name, node, depth + 1, ctx);
        }
        Format::Zstd => {
            let inner_name = strip_ext(&effective_name, &[".zst"]);
            match zstd::stream::read::Decoder::new(full) {
                Ok(mut decoder) => walk_stream(&mut decoder, provenance, inner_name, node, depth + 1, ctx),
                Err(_) => ctx.skip(provenance, SkipReason::Unreadable),
            }
        }
        Format::Tar => {
            let mut archive = tar::Archive::new(full);
            let entries = match archive.entries() {
                Ok(entries) => entries,
                Err(_) => {
                    ctx.skip(provenance, SkipReason::Unreadable);
                    return;
                }
            };
            let mut count = 0u64;
            for entry in entries {
                let mut entry = match entry {
                    Ok(e) => e,
                    Err(_) => {
                        // A corrupt tar stream cannot be resynchronized.
                        ctx.skip(provenance.clone(), SkipReason::Unreadable);
                        break;
                    }
                };
                if !entry.header().entry_type().is_file() {
                    continue;
                }
                count += 1;
                if count > ctx.opts.max_entries_per_archive {
                    ctx.skip(provenance.clone(), SkipReason::EntryCap);
                    break;
                }
                let entry_path = entry
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| "<invalid-path>".to_string());
                let child_provenance = format!("{provenance}!{entry_path}");
                let child_node = node_from_entry_path(&entry_path).or_else(|| node.clone());
                let child_name = leaf_name(&entry_path).to_string();
                walk_stream(&mut entry, child_provenance, child_name, child_node, depth + 1, ctx);
            }
        }
        Format::Zip => {
            // Nested zips need Seek: buffer in memory under the cap.
            let cap = ctx.opts.max_in_memory_archive;
            let mut buf = Vec::new();
            match full.take(cap + 1).read_to_end(&mut buf) {
                Ok(n) if n as u64 > cap => {
                    ctx.skip(provenance, SkipReason::ArchiveTooLargeForMemory);
                    return;
                }
                Ok(_) => {}
                Err(_) => {
                    ctx.skip(provenance, SkipReason::Unreadable);
                    return;
                }
            }
            match zip::ZipArchive::new(Cursor::new(buf)) {
                Ok(mut archive) => walk_zip_entries(&mut archive, &provenance, &node, depth, ctx),
                Err(_) => ctx.skip(provenance, SkipReason::Unreadable),
            }
        }
    }
}

fn walk_zip_entries<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    provenance: &str,
    node: &Option<Arc<str>>,
    depth: u32,
    ctx: &mut Ctx,
) {
    let total = archive.len();
    for index in 0..total {
        if index as u64 >= ctx.opts.max_entries_per_archive {
            ctx.skip(provenance.to_string(), SkipReason::EntryCap);
            break;
        }
        let mut entry = match archive.by_index(index) {
            Ok(e) => e,
            Err(_) => {
                ctx.skip(format!("{provenance}!<entry {index}>"), SkipReason::Unreadable);
                continue;
            }
        };
        if entry.is_dir() {
            continue;
        }
        let entry_path = entry.name().to_string();
        let child_provenance = format!("{provenance}!{entry_path}");
        let child_node = node_from_entry_path(&entry_path).or_else(|| node.clone());
        let child_name = leaf_name(&entry_path).to_string();
        walk_stream(&mut entry, child_provenance, child_name, child_node, depth + 1, ctx);
    }
}

fn parse_lines(reader: &mut dyn Read, provenance: String, node: Option<Arc<str>>, ctx: &mut Ctx) {
    let file: Arc<str> = Arc::from(provenance.as_str());
    let mut parser = LineParser::new(Arc::clone(&file), node);
    let mut buffered = BufReader::with_capacity(64 * 1024, reader);
    let mut raw = Vec::new();
    let mut line_no = 0u64;
    loop {
        raw.clear();
        let read = match std::io::BufRead::read_until(&mut buffered, b'\n', &mut raw) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => {
                ctx.skip(file.to_string(), SkipReason::Unreadable);
                break;
            }
        };
        if ctx.report.bytes_fed + read as u64 > ctx.opts.max_total_bytes {
            ctx.byte_budget_exhausted = true;
            ctx.skip(file.to_string(), SkipReason::ByteCap);
            break;
        }
        ctx.report.bytes_fed += read as u64;
        line_no += 1;
        let text = String::from_utf8_lossy(&raw);
        let line = text.trim_end_matches(['\n', '\r']);
        for event in parser.feed(line, line_no) {
            (ctx.on_event)(event);
        }
    }
    if let Some(event) = parser.finish() {
        (ctx.on_event)(event);
    }
    ctx.report.files_parsed += 1;
    ctx.report.stats.merge(parser.stats());
}

/// First path segment of an archive entry path, if any ("node1/rustfs.log"
/// -> "node1"). Leading "./" segments are ignored.
fn node_from_entry_path(path: &str) -> Option<Arc<str>> {
    let path = path.trim_start_matches("./");
    let (first, rest) = path.split_once('/')?;
    if first.is_empty() || rest.is_empty() {
        return None;
    }
    Some(Arc::from(first))
}

fn leaf_name(path: &str) -> &str {
    path.rsplit(['/', '!']).next().unwrap_or(path)
}

fn strip_ext(name: &str, exts: &[&str]) -> String {
    let lower = name.to_ascii_lowercase();
    for ext in exts {
        if lower.ends_with(ext) {
            let stripped = &name[..name.len() - ext.len()];
            // `.tgz` is gzip-wrapped tar: keep re-detection working.
            if ext.eq_ignore_ascii_case(".tgz") {
                return format!("{stripped}.tar");
            }
            return stripped.to_string();
        }
    }
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EventKind;
    use std::io::Write;

    const JSON_LINE: &str = r#"{"timestamp":"2026-07-15T14:23:01.123456+08:00","level":"ERROR","target":"rustfs::scanner::io","message":"Disk health check marked disk faulty","reason":"faulty_disk"}"#;

    fn collect(path: &Path, opts: &IngestOptions) -> (Vec<LogEvent>, IngestReport) {
        let mut events = Vec::new();
        let report = ingest_path(path, opts, &mut |ev| events.push(ev)).expect("ingest");
        (events, report)
    }

    fn write_file(dir: &Path, rel: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(&path, content).expect("write");
        path
    }

    fn zip_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, data) in entries {
            writer
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .expect("start_file");
            writer.write_all(data).expect("write entry");
        }
        writer.finish().expect("finish").into_inner()
    }

    fn tar_gz_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(gz);
        for (name, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, *name, *data).expect("append");
        }
        builder.into_inner().expect("tar finish").finish().expect("gz finish")
    }

    fn zstd_bytes(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(Cursor::new(data), 3).expect("zstd")
    }

    #[test]
    fn directory_with_subdir_assigns_node_labels() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_file(dir.path(), "a.log", format!("{JSON_LINE}\n{JSON_LINE}\n").as_bytes());
        write_file(dir.path(), "node2/b.log", format!("{JSON_LINE}\n").as_bytes());

        let (events, report) = collect(dir.path(), &IngestOptions::default());
        assert_eq!(events.len(), 3);
        assert_eq!(report.files_parsed, 2);
        assert_eq!(events.iter().filter(|e| e.node.is_none()).count(), 2);
        assert_eq!(events.iter().filter(|e| e.node.as_deref() == Some("node2")).count(), 1);
    }

    #[test]
    fn zip_with_per_node_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let zip = zip_bytes(&[
            ("node1/rustfs.log", format!("{JSON_LINE}\n").as_bytes()),
            ("node2/rustfs.log", format!("{JSON_LINE}\n").as_bytes()),
        ]);
        let path = write_file(dir.path(), "customer.zip", &zip);

        let (events, report) = collect(&path, &IngestOptions::default());
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].node.as_deref(), Some("node1"));
        assert_eq!(events[1].node.as_deref(), Some("node2"));
        assert!(
            events[0].source.file.contains("customer.zip!node1/rustfs.log"),
            "got {}",
            events[0].source.file
        );
        assert!(report.skipped.is_empty());
    }

    #[test]
    fn zstd_and_multi_member_gzip_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let zst = write_file(dir.path(), "rustfs.log.zst", &zstd_bytes(format!("{JSON_LINE}\n").as_bytes()));
        let (events, _) = collect(&zst, &IngestOptions::default());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Json);

        // Two concatenated gzip members must both be decoded (MultiGzDecoder).
        let mut gz = Vec::new();
        for _ in 0..2 {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(format!("{JSON_LINE}\n").as_bytes()).expect("write");
            gz.extend(enc.finish().expect("finish"));
        }
        let gz_path = write_file(dir.path(), "rustfs.log.gz", &gz);
        let (events, _) = collect(&gz_path, &IngestOptions::default());
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn tar_gz_containing_zst_is_three_layers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inner_zst = zstd_bytes(format!("{JSON_LINE}\n").as_bytes());
        let targz = tar_gz_bytes(&[("var/log/archive.rustfs.log.zst", &inner_zst)]);
        let path = write_file(dir.path(), "logs.tar.gz", &targz);

        let (events, report) = collect(&path, &IngestOptions::default());
        assert_eq!(events.len(), 1, "skipped: {:?}", report.skipped);
        assert_eq!(events[0].node.as_deref(), Some("var"));
        assert!(events[0].source.file.ends_with("logs.tar.gz!var/log/archive.rustfs.log.zst"));

        // gzip(1) -> tar entry(2) -> zstd(3) -> plain needs depth 4 > max 2.
        let tight = IngestOptions {
            max_depth: 2,
            ..Default::default()
        };
        let (events, report) = collect(&path, &tight);
        assert!(events.is_empty());
        assert_eq!(report.skipped.last().expect("skip").1, SkipReason::TooDeep);
    }

    #[test]
    fn nested_zip_respects_memory_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inner = zip_bytes(&[("inner/rustfs.log", format!("{JSON_LINE}\n").as_bytes())]);
        let outer = zip_bytes(&[("bundle/inner.zip", &inner)]);
        let path = write_file(dir.path(), "outer.zip", &outer);

        let (events, _) = collect(&path, &IngestOptions::default());
        assert_eq!(events.len(), 1);
        // Inner zip entry path wins over the outer "bundle" label.
        assert_eq!(events[0].node.as_deref(), Some("inner"));

        let tiny = IngestOptions {
            max_in_memory_archive: 1,
            ..Default::default()
        };
        let (events, report) = collect(&path, &tiny);
        assert!(events.is_empty());
        assert_eq!(report.skipped.last().expect("skip").1, SkipReason::ArchiveTooLargeForMemory);
    }

    #[test]
    fn binary_files_are_skipped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(dir.path(), "core.bin", &vec![0u8; 1024]);
        let (events, report) = collect(&path, &IngestOptions::default());
        assert!(events.is_empty());
        assert_eq!(report.skipped, vec![(path.display().to_string(), SkipReason::Binary)]);
        assert_eq!(report.files_parsed, 0);
    }

    #[test]
    fn byte_cap_stops_ingest_but_keeps_prior_events() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_file(dir.path(), "a.log", format!("{JSON_LINE}\n{JSON_LINE}\n{JSON_LINE}\n").as_bytes());
        write_file(dir.path(), "b.log", format!("{JSON_LINE}\n").as_bytes());

        let opts = IngestOptions {
            max_total_bytes: (JSON_LINE.len() + 1) as u64,
            ..Default::default()
        };
        let (events, report) = collect(dir.path(), &opts);
        assert_eq!(events.len(), 1);
        assert!(report.bytes_fed <= opts.max_total_bytes);
        let reasons: Vec<_> = report.skipped.iter().map(|(_, r)| *r).collect();
        assert!(reasons.contains(&SkipReason::ByteCap));
        // The second file is skipped up-front once the budget is gone.
        assert_eq!(reasons.iter().filter(|r| **r == SkipReason::ByteCap).count(), 2);
    }

    #[test]
    fn hostile_entry_paths_never_touch_the_filesystem() {
        let dir = tempfile::tempdir().expect("tempdir");
        // tar::Builder refuses `..` paths, so write the raw GNU header name
        // directly — real hostile archives do exactly this.
        let data = format!("{JSON_LINE}\n");
        let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(gz);
        let mut header = tar::Header::new_gnu();
        {
            let gnu = header.as_gnu_mut().expect("gnu header");
            let name = b"../escape.log";
            gnu.name[..name.len()].copy_from_slice(name);
        }
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, data.as_bytes()).expect("append");
        let targz = builder.into_inner().expect("tar finish").finish().expect("gz finish");
        let path = write_file(dir.path(), "hostile.tar.gz", &targz);

        let scratch = tempfile::tempdir().expect("tempdir");
        let before: Vec<_> = std::fs::read_dir(scratch.path()).expect("read_dir").collect();
        let (events, _) = collect(&path, &IngestOptions::default());
        assert_eq!(events.len(), 1);
        assert!(events[0].source.file.ends_with("!../escape.log"));
        let after: Vec<_> = std::fs::read_dir(scratch.path()).expect("read_dir").collect();
        assert_eq!(before.len(), after.len());
        assert!(!dir.path().parent().expect("parent").join("escape.log").exists());
    }

    #[test]
    fn missing_path_errors_unreadable_file_skips() {
        let missing = ingest_path(Path::new("/nonexistent/definitely-not-here"), &IngestOptions::default(), &mut |_| {});
        assert!(missing.is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().expect("tempdir");
            write_file(dir.path(), "ok.log", format!("{JSON_LINE}\n").as_bytes());
            let locked = write_file(dir.path(), "locked.log", b"secret\n");
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");

            let (events, report) = collect(dir.path(), &IngestOptions::default());
            assert_eq!(events.len(), 1);
            assert_eq!(report.skipped, vec![(locked.display().to_string(), SkipReason::Unreadable)]);

            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).expect("chmod back");
        }
    }

    #[test]
    fn ingest_reader_handles_stdin_like_streams() {
        let data = format!("{JSON_LINE}\nplain trailer\n");
        let mut cursor = Cursor::new(data.into_bytes());
        let mut events = Vec::new();
        let report = ingest_reader(&mut cursor, "stdin", &IngestOptions::default(), &mut |ev| events.push(ev)).expect("ingest");
        assert_eq!(events.len(), 2);
        assert_eq!(&*events[0].source.file, "stdin");
        assert_eq!(events[1].kind, EventKind::Text);
        assert_eq!(report.stats.total_lines, 2);
        assert_eq!(report.stats.json_ok, 1);
        assert_eq!(report.stats.text_lines, 1);
    }
}
