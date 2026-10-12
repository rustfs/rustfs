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

//! Server-side image and video processing for RustFS.
//!
//! Derives resized / re-encoded variants of stored objects so a bucket can act
//! as an image or video host: one upload, every resolution and format the
//! clients ask for.
//!
//! Work is delegated to the external `ffmpeg` / `ffprobe` binaries rather than
//! linked as a library. Linking libav would make every RustFS build depend on
//! system multimedia headers; spawning keeps the crate dependency-free and
//! lets operators pin, swap or omit the toolchain through configuration.
//!
//! When no usable binary is configured every entry point reports
//! [`Error::ToolUnavailable`] instead of failing, so deployments that never
//! enable processing behave exactly as before.

pub mod image;
pub mod plan;
pub mod probe;
pub mod video;

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;

pub use plan::{ImageFormat, ImageVariant, MediaProfile, VariantKind, VideoVariant};
pub use probe::{MediaInfo, MediaKind, VideoStream};

/// Default ceilings applied when a caller does not configure the binary path.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);
/// Last few hundred lines of ffmpeg stderr are enough to explain a failure and
/// small enough to keep in a log line.
const MAX_CAPTURED_STDERR: usize = 8192;

#[derive(Debug, Error)]
pub enum Error {
    #[error("media processing is unavailable: {0}")]
    ToolUnavailable(String),

    #[error("`{tool}` failed with exit code {code}: {stderr}")]
    ToolFailed { tool: &'static str, code: i32, stderr: String },

    #[error("`{tool}` did not finish within {timeout:?}")]
    ToolTimeout { tool: &'static str, timeout: Duration },

    #[error("`{tool}` could not be started: {source}")]
    ToolSpawn {
        tool: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("unsupported media: {0}")]
    Unsupported(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse ffprobe output: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Configuration for the external binaries driving the pipeline.
#[derive(Clone, Debug)]
pub struct Engine {
    ffmpeg: PathBuf,
    ffprobe: PathBuf,
    timeout: Duration,
}

impl Engine {
    /// Builds an engine from explicit binary paths.
    pub fn new(ffmpeg: impl Into<PathBuf>, ffprobe: impl Into<PathBuf>) -> Self {
        Self {
            ffmpeg: ffmpeg.into(),
            ffprobe: ffprobe.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Resolves `ffmpeg` and `ffprobe` from `PATH`, which is what a container
    /// image or a package install gives us.
    pub fn from_path() -> Self {
        Self::new("ffmpeg", "ffprobe")
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Probes `path` and reports what it holds.
    pub async fn probe(&self, path: &Path) -> Result<MediaInfo> {
        probe::probe_with(&self.ffprobe, path, self.timeout).await
    }

    /// Produces one derived object.
    ///
    /// `request.info` is the probe result for the same input, when the caller
    /// already has one. Passing it lets the video pipeline take the
    /// stream-copy fast path instead of re-encoding a file that already matches.
    pub async fn render(&self, input: &Path, output: &Path, request: &RenderRequest<'_>) -> Result<()> {
        match request.kind {
            VariantKind::Image(spec) => image::render_with_engine(self, input, output, spec).await,
            VariantKind::Video(spec) => video::render_with_engine(self, input, output, spec, request.info).await,
        }
    }

    /// Expands `profile` against `info` into the concrete jobs worth running.
    ///
    /// Variants are skipped when they would not shrink the source, so a 720p
    /// upload never produces a "1080p" copy that is really 720p upscaled.
    pub fn plan(&self, info: &MediaInfo, profile: &MediaProfile, key: &str) -> Vec<PlannedVariant> {
        plan::expand(info, profile, key)
    }

    /// True when the configured ffmpeg can encode with `name`.
    ///
    /// Distribution builds ship wildly different codec sets — a Debian image
    /// with `ffmpeg` often lacks `libwebp` — so callers check this before
    /// promising an operator a format their toolchain cannot produce.
    pub async fn supports_encoder(&self, name: &str) -> bool {
        match self.encoders().await {
            // An empty listing means the parse failed, not that the build has no
            // encoders; vetoing on that would silently disable the feature.
            Ok(encoders) if !encoders.is_empty() => encoders.iter().any(|encoder| encoder == name),
            _ => true,
        }
    }

    /// True when the configured ffmpeg actually runs.
    ///
    /// [`Engine::supports_encoder`] deliberately answers "yes" when the encoder
    /// listing cannot be read, so a transient start-up quirk cannot disable a
    /// working feature. That is the wrong answer for the feature *gate*: a host
    /// with no ffmpeg at all must be reported as unable to process media rather
    /// than as capable of everything.
    pub async fn is_available(&self) -> bool {
        self.run_ffmpeg(&["-version".to_owned()]).await.is_ok()
    }

    /// Every encoder the configured ffmpeg advertises.
    ///
    /// `ffmpeg -encoders` writes the listing to stdout on current builds, but
    /// older ones put it on stderr; both are read so a version difference
    /// cannot silently look like an encoder-free build.
    pub async fn encoders(&self) -> Result<Vec<String>> {
        let output = self.run_ffmpeg(&["-hide_banner".to_owned(), "-encoders".to_owned()]).await?;
        let from_stdout = parse_encoders(&String::from_utf8_lossy(&output.stdout));
        if !from_stdout.is_empty() {
            return Ok(from_stdout);
        }
        Ok(parse_encoders(&output.stderr))
    }

    async fn run(&self, tool: &'static str, program: &Path, args: &[String]) -> Result<ToolOutput> {
        // The child must never inherit a TTY: ffmpeg would block forever waiting on
        // stdin even though -nostdin is also passed.
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let child = command.spawn().map_err(|source| Error::ToolSpawn { tool, source })?;

        let wait = child.wait_with_output();
        let output = match tokio::time::timeout(self.timeout, wait).await {
            Ok(Ok(output)) => output,
            Ok(Err(source)) => return Err(Error::ToolSpawn { tool, source }),
            Err(_) => {
                // kill_on_drop stops the child when the future is dropped here.
                return Err(Error::ToolTimeout {
                    tool,
                    timeout: self.timeout,
                });
            }
        };

        let stderr = tail(String::from_utf8_lossy(&output.stderr).into_owned());
        if !output.status.success() {
            return Err(Error::ToolFailed {
                tool,
                code: output.status.code().unwrap_or(-1),
                stderr,
            });
        }

        Ok(ToolOutput {
            stdout: output.stdout,
            stderr,
        })
    }

    pub(crate) async fn run_ffprobe(&self, args: &[String]) -> Result<ToolOutput> {
        self.run("ffprobe", &self.ffprobe, args).await
    }

    pub(crate) async fn run_ffmpeg(&self, args: &[String]) -> Result<ToolOutput> {
        self.run("ffmpeg", &self.ffmpeg, args).await
    }
}

/// Captured output of a completed tool run.
#[derive(Debug)]
pub(crate) struct ToolOutput {
    pub stdout: Vec<u8>,
    /// Retained for diagnostics; ffmpeg reports progress and warnings here.
    pub stderr: String,
}

impl Default for Engine {
    fn default() -> Self {
        Self::from_path()
    }
}

/// One unit of work produced by [`Engine::plan`].
#[derive(Debug, Clone)]
pub struct PlannedVariant {
    /// Object key the derived bytes should be stored under.
    pub key: String,
    pub kind: VariantKind,
}

#[derive(Debug, Clone, Copy)]
pub struct RenderRequest<'a> {
    pub kind: &'a VariantKind,
    /// What the probe said about the input, when the caller has it.
    pub info: Option<&'a MediaInfo>,
}

fn tail(text: String) -> String {
    if text.len() <= MAX_CAPTURED_STDERR {
        return text.trim().to_owned();
    }
    // Keep the end, but never split a UTF-8 code point.
    let mut start = text.len() - MAX_CAPTURED_STDERR;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &text[start..]).trim().to_owned()
}

/// Extracts encoder names from `ffmpeg -encoders` output.
///
/// The listing is a legend followed by a dashed rule and then one entry per
/// line: ` V....D libx264   H.264 ...`. Audio (`A`) and video (`V`) encoders are
/// both kept, because a video rendition needs both halves and a profile is only
/// usable if its audio encoder exists too. Subtitle, data and attachment
/// encoders are dropped — nothing here can target those.
fn parse_encoders(listing: &str) -> Vec<String> {
    listing
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("---"))
        .skip(1)
        .filter_map(|line| {
            let flags = line.get(..6)?;
            if !(flags.contains('V') || flags.contains('A')) {
                return None;
            }
            let name = line.split_whitespace().nth(1)?;
            Some(name.to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_tail_keeps_the_end_and_stays_on_char_boundaries() {
        let long = tail("x".repeat(MAX_CAPTURED_STDERR * 2));
        assert!(long.starts_with('…'));
        assert!(long.len() <= MAX_CAPTURED_STDERR + '…'.len_utf8());

        let multibyte = tail("错误".repeat(MAX_CAPTURED_STDERR));
        assert!(std::str::from_utf8(multibyte.as_bytes()).is_ok());
        assert!(multibyte.ends_with('误'));
    }

    #[test]
    fn short_stderr_is_untouched() {
        assert_eq!(tail("  boom  \n".to_owned()), "boom");
    }

    #[test]
    fn the_encoder_listing_keeps_video_and_audio_and_drops_the_rest() {
        let listing = "\
Encoders:
 V..... = Video
 A..... = Audio
 ------
 V....D libx264           H.264 ...
 V....D libx265           H.265 ...
 A....D aac               AAC ...
 A....D libopus           Opus ...
 V..... png               PNG (Portable Network Graphics) image
 S..... subrip            SubRip subtitles
";
        // Audio encoders must survive: a video rendition needs an audio codec
        // too, and dropping these makes every video rendition look unsupported.
        assert_eq!(parse_encoders(listing), vec!["libx264", "libx265", "aac", "libopus", "png"]);
    }

    #[test]
    fn an_unreadable_encoder_listing_yields_nothing() {
        assert!(parse_encoders("").is_empty());
        assert!(parse_encoders("some unrelated output").is_empty());
    }

    #[tokio::test]
    async fn an_unreadable_listing_never_vetoes_an_encoder() {
        // ffmpeg is absent here, so the listing cannot be read. Reporting "no
        // encoders" would turn a start-up quirk into a disabled feature.
        let engine = Engine::new("rustfs-no-such-ffmpeg", "rustfs-no-such-ffmpeg");
        assert!(engine.supports_encoder("libx264").await);
    }
}
