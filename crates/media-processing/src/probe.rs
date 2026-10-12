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

//! Reads what an object actually contains, so the planner can decide whether a
//! variant is worth producing before any bytes are rewritten.

use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::{Engine, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Video,
    Image,
    Audio,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct VideoStream {
    pub codec: String,
    pub width: u32,
    pub height: u32,
    pub pix_fmt: String,
    /// Average frame rate in frames per second.
    pub fps: Option<f64>,
    /// Audio codec of the first audio stream, e.g. `aac` or `opus`.
    pub audio_codec: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MediaInfo {
    pub kind: MediaKind,
    /// Container name as reported by ffprobe, e.g. `matroska,webm`.
    pub format: String,
    pub duration: Option<Duration>,
    pub video: Option<VideoStream>,
    pub has_audio: bool,
}

impl MediaInfo {
    #[must_use]
    pub fn width(&self) -> u32 {
        self.video.as_ref().map_or(0, |stream| stream.width)
    }

    #[must_use]
    pub fn height(&self) -> u32 {
        self.video.as_ref().map_or(0, |stream| stream.height)
    }

    #[must_use]
    pub fn fps(&self) -> Option<f64> {
        self.video.as_ref().and_then(|stream| stream.fps)
    }

    /// Pixel layout of the video stream, or `None` when there is none.
    #[must_use]
    pub fn pixel_format(&self) -> Option<&str> {
        self.video.as_ref().map(|stream| stream.pix_fmt.as_str())
    }

    /// Codec of the first audio stream, if any.
    #[must_use]
    pub fn audio_codec(&self) -> Option<&str> {
        self.video.as_ref().and_then(|stream| stream.audio_codec.as_deref())
    }

    /// The muxer names this container answers to.
    ///
    /// ffprobe reports a comma-separated list of every demuxer that recognises
    /// the file — `mov,mp4,m4a,3gp,3g2,mj2` for an mp4 — and only one of those
    /// is the real container. Comparing a rendition's muxer against that whole
    /// list is how a "same container" check avoids reporting a spurious
    /// conversion.
    #[must_use]
    pub fn container_extensions(&self) -> Vec<&str> {
        self.format
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .collect()
    }

    /// True when a browser can play this file as it stands.
    ///
    /// Codec support varies by platform — HEVC in particular depends on the
    /// hardware — so the bar is the intersection every major browser decodes:
    /// 8-bit 4:2:0 H.264. Anything else needs a rendition.
    #[must_use]
    pub fn is_browser_playable(&self) -> bool {
        let Some(video) = self.video.as_ref() else {
            return false;
        };
        let codec_ok = matches!(video.codec.as_str(), "h264" | "avc1");
        let depth_ok = matches!(video.pix_fmt.as_str(), "yuv420p" | "yuvj420p" | "" | "unknown");
        codec_ok && depth_ok
    }
}

#[derive(Deserialize)]
struct ProbeOutput {
    #[serde(default)]
    format: ProbeFormat,
    #[serde(default)]
    streams: Vec<ProbeStream>,
}

#[derive(Default, Deserialize)]
struct ProbeFormat {
    #[serde(default)]
    format_name: String,
    #[serde(default)]
    duration: Option<String>,
}

#[derive(Deserialize)]
struct ProbeStream {
    #[serde(default)]
    codec_type: String,
    #[serde(default)]
    codec_name: String,
    #[serde(default)]
    width: u32,
    #[serde(default)]
    height: u32,
    #[serde(default)]
    pix_fmt: String,
    #[serde(default)]
    avg_frame_rate: String,
    #[serde(default, deserialize_with = "de_frame_count")]
    nb_frames: Option<u64>,
}

/// Accepts `nb_frames` as either a JSON number or a JSON string.
///
/// ffprobe emits it quoted on most builds, but the type is not part of its
/// contract. Deserializing strictly rejects the quoted form, and a rejected
/// probe payload means the object is silently treated as undecodable — every
/// rendition for that upload disappears with nothing in the log to explain it.
// `Result` is the crate's own alias in this module, so the serde error type
// has to be spelled out for the signature to mean anything.
fn de_frame_count<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // `unwrap_or(Null)` rather than `?`: the crate's own `Error` shadows
    // `std::result::Result` in this module, so the `?` would not convert.
    let raw = serde_json::Value::deserialize(deserializer).unwrap_or(serde_json::Value::Null);
    Ok(match raw {
        serde_json::Value::Number(frames) => frames.as_u64(),
        serde_json::Value::String(frames) => frames.trim().parse().ok(),
        _ => None,
    })
}

/// Parses ffprobe's `num/den` rational frame rate.
///
/// ffprobe also emits sentinels — `0/0` for a still, `N/A` when the container
/// carries no rate at all — and both have to collapse to "unknown" rather than
/// to a frame rate a rendition would then try to match against.
fn parse_frame_rate(value: &str) -> Option<f64> {
    let (numerator, denominator) = value.split_once('/')?;
    let numerator: f64 = numerator.trim().parse().ok()?;
    let denominator: f64 = denominator.trim().parse().ok()?;
    if !numerator.is_finite() || !denominator.is_finite() || denominator == 0.0 {
        return None;
    }
    let fps = numerator / denominator;
    (fps.is_finite() && fps > 0.0).then_some(fps)
}

impl ProbeOutput {
    fn into_info(self) -> MediaInfo {
        let audio_codec = self
            .streams
            .iter()
            .find(|stream| stream.codec_type == "audio")
            .map(|stream| stream.codec_name.clone());

        let video = self
            .streams
            .iter()
            .find(|stream| stream.codec_type == "video")
            .map(|stream| VideoStream {
                codec: stream.codec_name.clone(),
                width: stream.width,
                height: stream.height,
                pix_fmt: stream.pix_fmt.clone(),
                fps: parse_frame_rate(&stream.avg_frame_rate),
                audio_codec: audio_codec.clone(),
            });

        let has_audio = audio_codec.is_some();

        // ffprobe reports a still image as a one-frame video stream, so the
        // container is the only thing that separates the two pipelines.
        // Two things mark a still. Some containers only ever hold one — a
        // `png_pipe` is a picture by definition. AVIF and HEIC do not: they
        // live in the ISO-BMFF family, so ffprobe reports them as
        // `mov,mp4,m4a,3gp,3g2,mj2`, indistinguishable from a video file by
        // name. A single-frame stream is what actually separates them, and it
        // is present on both.
        let is_image = is_still_container(&self.format.format_name)
            || self
                .streams
                .iter()
                .any(|stream| stream.codec_type == "video" && stream.nb_frames == Some(1));

        let kind = match (&video, has_audio, is_image) {
            (Some(_), _, true) => MediaKind::Image,
            (Some(_), _, false) => MediaKind::Video,
            (None, true, _) => MediaKind::Audio,
            (None, false, _) => MediaKind::Unknown,
        };

        MediaInfo {
            kind,
            format: self.format.format_name,
            duration: self
                .format
                .duration
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                .map(Duration::from_secs_f64),
            video,
            has_audio,
        }
    }
}

fn is_still_container(format_name: &str) -> bool {
    // `image2` is what ffmpeg reports for a JPEG or BMP it just wrote, so it
    // has to be treated as a still even though the name suggests a sequence.
    const STILL_CONTAINERS: &[&str] = &[
        "png_pipe",
        "png",
        "jpeg_pipe",
        "jpeg",
        "mjpeg",
        "image2",
        "image2pipe",
        "webp_pipe",
        "webp",
        "bmp_pipe",
        "bmp",
        "tiff_pipe",
        "tiff",
        "gif",
        "jpegls_pipe",
        "jpegls",
        "avif_pipe",
        "avif",
        "heif",
        "apng",
    ];
    format_name.split(',').any(|name| STILL_CONTAINERS.contains(&name))
}

/// Runs ffprobe against `path` and classifies the result.
pub async fn probe(ffprobe: &Path, path: &Path, timeout: Duration) -> Result<MediaInfo> {
    // ffprobe is the only binary this call needs, so it is wired in as both
    // tools; the unused slot is never invoked.
    let engine = Engine::new(ffprobe, ffprobe).with_timeout(timeout);
    probe_with_engine(&engine, path).await
}

pub(crate) async fn probe_with(ffprobe: &Path, path: &Path, timeout: Duration) -> Result<MediaInfo> {
    let engine = Engine::new(ffprobe, ffprobe).with_timeout(timeout);
    probe_with_engine(&engine, path).await
}

async fn probe_with_engine(engine: &Engine, path: &Path) -> Result<MediaInfo> {
    let args = vec![
        "-v".to_owned(),
        "error".to_owned(),
        "-print_format".to_owned(),
        "json".to_owned(),
        "-show_format".to_owned(),
        "-show_streams".to_owned(),
        "-i".to_owned(),
        path.display().to_string(),
    ];

    let output = engine.run_ffprobe(&args).await.map_err(map_probe_error)?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str::<ProbeOutput>(&stdout)
        .map(ProbeOutput::into_info)
        .map_err(|error| {
            tracing::debug!(%error, stderr = %output.stderr, "ffprobe output rejected");
            Error::Parse(error.to_string())
        })
}

/// A missing ffprobe means the feature was never configured, not that the
/// object is broken.
fn map_probe_error(error: Error) -> Error {
    match error {
        Error::ToolSpawn { .. } => Error::ToolUnavailable(error.to_string()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn still_containers_are_recognised_in_comma_lists() {
        assert!(is_still_container("png_pipe"));
        assert!(is_still_container("jpeg_pipe"));
        // ffmpeg reports a freshly written jpeg as image2.
        assert!(is_still_container("image2"));
        assert!(!is_still_container("matroska,webm"));
        assert!(!is_still_container("mov,mp4,m4a,3gp,3g2,mj2"));
    }

    #[test]
    fn a_single_frame_iso_bmff_stream_is_an_image() {
        // ffprobe reports AVIF as an mp4-family container, so the frame count is
        // the only thing that marks it as a picture.
        let parsed: ProbeOutput = serde_json::from_str(
            r#"{
                "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2"},
                "streams": [
                    {"codec_type": "video", "codec_name": "av1", "width": 64, "height": 48, "pix_fmt": "yuv420p", "nb_frames": "1"}
                ]
            }"#,
        )
        .expect("valid probe payload");
        assert_eq!(parsed.into_info().kind, MediaKind::Image);
    }

    #[test]
    fn a_multi_frame_iso_bmff_stream_is_video() {
        let parsed: ProbeOutput = serde_json::from_str(
            r#"{
                "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": "1.0"},
                "streams": [
                    {"codec_type": "video", "codec_name": "h264", "width": 64, "height": 48, "pix_fmt": "yuv420p", "nb_frames": "25"}
                ]
            }"#,
        )
        .expect("valid probe payload");
        assert_eq!(parsed.into_info().kind, MediaKind::Video);
    }

    #[test]
    fn a_single_frame_stream_in_a_movie_container_is_video() {
        let parsed: ProbeOutput = serde_json::from_str(
            r#"{
                "format": {"format_name": "mov,mp4,m4a", "duration": "2.0"},
                "streams": [
                    {"codec_type": "video", "codec_name": "h264", "width": 320, "height": 240, "pix_fmt": "yuv420p"}
                ]
            }"#,
        )
        .expect("valid probe payload");

        let info = parsed.into_info();
        assert_eq!(info.kind, MediaKind::Video);
        assert_eq!(info.width(), 320);
        assert!(!info.has_audio);
        assert_eq!(info.duration, Some(Duration::from_secs_f64(2.0)));
    }

    #[test]
    fn a_png_still_is_an_image_with_no_audio() {
        let parsed: ProbeOutput = serde_json::from_str(
            r#"{
                "format": {"format_name": "png_pipe"},
                "streams": [{"codec_type": "video", "codec_name": "png", "width": 8, "height": 8, "pix_fmt": "rgb24"}]
            }"#,
        )
        .expect("valid probe payload");

        let info = parsed.into_info();
        assert_eq!(info.kind, MediaKind::Image);
        assert_eq!(info.duration, None);
    }

    #[test]
    fn audio_only_and_empty_inputs_are_classified_without_panicking() {
        let audio: ProbeOutput = serde_json::from_str(
            r#"{"format": {"format_name": "mp3"}, "streams": [{"codec_type": "audio", "codec_name": "mp3"}]}"#,
        )
        .expect("valid probe payload");
        assert_eq!(audio.into_info().kind, MediaKind::Audio);

        let empty: ProbeOutput = serde_json::from_str(r#"{"format": {}, "streams": []}"#).expect("valid probe payload");
        let info = empty.into_info();
        assert_eq!(info.kind, MediaKind::Unknown);
        assert_eq!(info.width(), 0);
    }

    #[test]
    fn a_zero_duration_is_treated_as_unknown() {
        let parsed: ProbeOutput =
            serde_json::from_str(r#"{"format": {"format_name": "mp4", "duration": "0.000000"}, "streams": []}"#)
                .expect("valid probe payload");
        assert_eq!(parsed.into_info().duration, None);
    }
}
