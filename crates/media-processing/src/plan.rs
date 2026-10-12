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

//! Turns a [`MediaProfile`] plus a probed object into the concrete list of
//! variants worth rendering, and names where each one is stored.

use serde::{Deserialize, Serialize};

use crate::PlannedVariant;
use crate::probe::{MediaInfo, MediaKind};

/// Default object prefix for derived media. Variants are ordinary objects in
/// the same bucket so existing policies, lifecycle rules and presigned URLs
/// apply to them unchanged.
pub const DEFAULT_VARIANT_PREFIX: &str = "variants";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ImageFormat {
    /// Keeps the source format; only the size changes.
    #[default]
    Keep,
    Jpeg,
    Png,
    Webp,
    Avif,
    Tiff,
    Bmp,
    Gif,
}

impl ImageFormat {
    /// Extension to store under, given the source object's extension.
    #[must_use]
    pub fn extension(self, source: &str) -> String {
        match self {
            Self::Keep => source.trim_start_matches('.').to_ascii_lowercase(),
            Self::Jpeg => "jpg".to_owned(),
            Self::Png => "png".to_owned(),
            Self::Webp => "webp".to_owned(),
            Self::Avif => "avif".to_owned(),
            Self::Tiff => "tiff".to_owned(),
            Self::Bmp => "bmp".to_owned(),
            Self::Gif => "gif".to_owned(),
        }
    }

    /// The encoder ffmpeg needs for this format.
    ///
    /// `None` only for `Keep`, whose encoder is decided by the source's own
    /// format — see [`ImageFormat::source_encoder`].
    #[must_use]
    pub fn default_encoder(self) -> Option<&'static str> {
        match self {
            Self::Keep => None,
            Self::Jpeg => Some("mjpeg"),
            Self::Png => Some("png"),
            Self::Webp => Some("libwebp"),
            Self::Avif => Some("libaom-av1"),
            Self::Tiff => Some("tiff"),
            Self::Bmp => Some("bmp"),
            Self::Gif => Some("gif"),
        }
    }

    /// Encoder for a `Keep` rendition of a source with this extension.
    ///
    /// This cannot be left to ffmpeg: the generic still muxer defaults to
    /// `mjpeg` whatever the source was, so an unset encoder writes JPEG bytes
    /// into a file the planner named `.png`. Clients dispatch on the extension
    /// and the container's own magic, and a mismatch either fails to decode or
    /// renders as a broken image. Every still rendition therefore names its
    /// encoder explicitly.
    #[must_use]
    pub fn source_encoder(extension: &str) -> &'static str {
        match extension.trim_start_matches('.').to_ascii_lowercase().as_str() {
            "png" => "png",
            "bmp" => "bmp",
            "tif" | "tiff" => "tiff",
            "gif" => "gif",
            "webp" => "libwebp",
            "avif" | "avifs" | "heic" | "heif" => "libaom-av1",
            // jpg, jpeg, and anything the muxer can be told to write.
            _ => "mjpeg",
        }
    }

    /// Muxer ffmpeg needs for this format.
    ///
    /// Derived objects are written to scratch files whose name carries no
    /// useful extension, and ffmpeg cannot infer a muxer from a bare name. The
    /// muxer therefore has to be named explicitly, otherwise every render fails
    /// at output initialisation.
    #[must_use]
    pub fn muxer(self, source: &str) -> &'static str {
        match self {
            // `webp` and `avif` have dedicated muxers; everything else a still
            // can wear is handled by the generic single-image muxer.
            Self::Webp => "webp",
            Self::Avif => "avif",
            Self::Keep if source.trim_start_matches('.').eq_ignore_ascii_case("webp") => "webp",
            Self::Keep if source.trim_start_matches('.').eq_ignore_ascii_case("avif") => "avif",
            _ => "image2",
        }
    }

    /// True when re-encoding into this format can plausibly shrink a source
    /// that is already the same format and size.
    ///
    /// PNG and BMP are already lossless-and-uncompressed or trivially small, so
    /// a same-size PNG-to-PNG rendition is pure loss. Formats that compress hard
    /// (webp, avif, jpeg) can win even against a same-size source.
    #[must_use]
    pub fn recompresses(self) -> bool {
        matches!(self, Self::Jpeg | Self::Webp | Self::Avif)
    }
}

/// How a rendition's bounds relate to the source's aspect ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FitMode {
    /// Fit entirely inside the bounds, keeping the aspect ratio. The result may
    /// be smaller than the bounds on one axis, and is never enlarged.
    #[default]
    Contain,
    /// Fill the bounds and crop whatever overflows. The result is exactly the
    /// requested size on both axes.
    Cover,
    /// Stretch to exactly the bounds, ignoring the aspect ratio.
    Fill,
}

impl FitMode {
    #[must_use]
    pub fn crops(self) -> bool {
        matches!(self, Self::Cover)
    }
}

/// Video codecs a rendition can target.
///
/// Each entry knows its own encoder, its natural pixel format and whether
/// browsers decode it everywhere, so callers never have to spell out an ffmpeg
/// invocation by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum VideoCodec {
    /// 8-bit H.264 in an mp4/webm container: the format every browser plays.
    #[default]
    H264,
    /// H.265 / HEVC. Smaller at equal quality, but hardware-dependent playback.
    H265,
    /// VP9 in a webm container.
    Vp9,
    /// AV1. Best compression, slowest to encode, newest to decode.
    Av1,
    /// MPEG-4 Part 2, for legacy players.
    Mpeg4,
    /// Copy the source streams without re-encoding.
    Copy,
}

impl VideoCodec {
    #[must_use]
    pub fn encoder(self) -> Option<&'static str> {
        match self {
            Self::H264 => Some("libx264"),
            Self::H265 => Some("libx265"),
            Self::Vp9 => Some("libvpx-vp9"),
            Self::Av1 => Some("libsvtav1"),
            Self::Mpeg4 => Some("mpeg4"),
            Self::Copy => None,
        }
    }

    /// Pixel format the encoder produces when the variant does not ask for one.
    ///
    /// Anything outside this set silently breaks playback for part of the
    /// audience, so the default is the one format every decoder of that codec
    /// accepts.
    #[must_use]
    pub fn default_pixel_format(self) -> Option<&'static str> {
        match self {
            Self::H264 | Self::H265 | Self::Mpeg4 => Some("yuv420p"),
            Self::Vp9 => Some("yuv420p"),
            Self::Av1 => Some("yuv420p10le"),
            Self::Copy => None,
        }
    }

    /// True when every mainstream browser decodes the result without help.
    #[must_use]
    pub fn is_universal(self) -> bool {
        matches!(self, Self::H264)
    }

    /// True when the codec carries `-crf`.
    #[must_use]
    pub fn supports_crf(self) -> bool {
        !matches!(self, Self::Copy)
    }

    /// ffmpeg arguments for the encoder's speed setting.
    ///
    /// `-preset` is x264/x265 naming and takes words like `veryfast`. libvpx
    /// spells the same knob `-cpu-used` and takes an integer 0-8, and libsvtav1
    /// takes an integer 0-13 under the same flag name. Handing `veryfast` to
    /// either of them aborts the render, so a preset that is not a number is
    /// dropped rather than translated — guessing which end of the scale the
    /// word meant would silently change the output's quality.
    #[must_use]
    pub fn speed_args(self, preset: &str) -> Vec<String> {
        match self {
            Self::H264 | Self::H265 => vec!["-preset".to_owned(), preset.to_owned()],
            Self::Vp9 | Self::Av1 => match preset.trim().parse::<u32>() {
                Ok(level) => vec!["-cpu-used".to_owned(), level.to_string()],
                Err(_) => Vec::new(),
            },
            Self::Mpeg4 | Self::Copy => Vec::new(),
        }
    }
}

/// Audio codecs a rendition can target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AudioCodec {
    /// AAC in mp4: the universal choice.
    #[default]
    Aac,
    /// Opus in webm: better than AAC at the same bitrate.
    Opus,
    /// MP3, for players that predate anything else.
    Mp3,
    /// Vorbis in webm / ogg.
    Vorbis,
    /// Drop the audio track entirely.
    None,
}

impl AudioCodec {
    #[must_use]
    pub fn encoder(self) -> Option<&'static str> {
        match self {
            Self::Aac => Some("aac"),
            Self::Opus => Some("libopus"),
            Self::Mp3 => Some("libmp3lame"),
            Self::Vorbis => Some("libvorbis"),
            Self::None => None,
        }
    }

    /// MP3 is CBR/VBR through `-q:a`, not `-b:a`; every other target takes a
    /// bitrate.
    #[must_use]
    pub fn accepts_bitrate(self) -> bool {
        !matches!(self, Self::None | Self::Mp3)
    }
}

/// Normalises an operator-supplied pixel format name.
///
/// ffmpeg's names are all lowercase; accepting `YUV420P` and passing it through
/// would fail at render time with a message nobody can act on.
#[must_use]
pub fn normalize_pixel_format(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

/// Whether `value` names a pixel format ffmpeg knows about.
///
/// Used to reject a mistyped profile at configuration time instead of failing
/// once per rendition per upload.
#[must_use]
pub fn is_known_pixel_format(value: &str) -> bool {
    KNOWN_PIXEL_FORMATS.contains(&value)
}

/// Pixel formats worth offering for a rendition.
///
/// Deliberately short: this is the set that behaves predictably across the
/// decoders clients actually use, not a copy of ffmpeg's `pix_fmts` list.
pub const KNOWN_PIXEL_FORMATS: &[&str] = &[
    // Planar and semi-planar YUV, the formats video decoders accept.
    "yuv420p",
    "yuv420p10le",
    "yuv422p",
    "yuv422p10le",
    "yuv444p",
    "yuv444p10le",
    "nv12",
    "nv16",
    "nv21",
    "yuv410p",
    "yuv411p",
    "yuv440p",
    // Planar RGB — lossless and large, useful for editing masters.
    "gbrp",
    "gbrp10le",
    // Packed formats, the natural choice for a still image.
    "rgb24",
    "rgba",
    "bgr24",
    "bgra",
    "gray",
    "gray16le",
    "yuyv422",
    "uyvy422",
    "pal8",
];

/// The flag an encoder takes for its quality setting.
///
/// Every codec spells this differently, and getting it wrong is worse than
/// omitting it: `-q:v 80` on libwebp is not a percentage at all, and
/// `-quality 4` on mjpeg is read as a quantiser far worse than the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityFlag {
    /// `-q:v N`, the mjpeg quantiser scale (2 best, 31 worst).
    Quantizer,
    /// `-quality N`, a 0-100 percentage (libwebp).
    Percent,
    /// `-crf N` paired with `-b:v 0` for constant quality.
    Crf,
    /// `-compression_level N`, a 0-9 effort scale (png).
    Compression,
    /// The target has no quality knob at all.
    Unsupported,
}

/// Resolves the quality flag for an image rendition.
///
/// `Unsupported` means the target has no quality knob at all — bmp, tiff and gif
/// are not encoders that accept one, and passing an unknown option to ffmpeg
/// aborts the whole render rather than being ignored. The format decides first;
/// an operator's `encoder` override only refines the answer for codecs that share
/// a name with a known one (`webp`, `libaom-av1`, …).
#[must_use]
pub fn image_quality_flag(format: ImageFormat, encoder: Option<&str>, source: &str) -> QualityFlag {
    let encoder = encoder.unwrap_or_default().to_ascii_lowercase();
    if encoder.contains("webp") {
        return QualityFlag::Percent;
    }
    if encoder.contains("aom") || encoder.contains("svtav1") || encoder.contains("svt") {
        return QualityFlag::Crf;
    }
    match format {
        ImageFormat::Webp => QualityFlag::Percent,
        ImageFormat::Avif => QualityFlag::Crf,
        ImageFormat::Jpeg => QualityFlag::Quantizer,
        ImageFormat::Png => QualityFlag::Compression,
        // `Keep` re-encodes into whatever the source already was, so the source
        // extension decides which knob that container has.
        ImageFormat::Keep if source.trim_start_matches('.').eq_ignore_ascii_case("png") => QualityFlag::Compression,
        ImageFormat::Keep | ImageFormat::Tiff | ImageFormat::Bmp | ImageFormat::Gif => QualityFlag::Unsupported,
    }
}

/// One image rendition. `max_width` / `max_height` are upper bounds: a source
/// smaller than the bound is never scaled up.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageVariant {
    pub name: String,
    #[serde(alias = "maxWidth")]
    pub max_width: Option<u32>,
    #[serde(alias = "maxHeight")]
    pub max_height: Option<u32>,
    #[serde(default)]
    pub format: ImageFormat,
    /// How the bounds relate to the source aspect ratio.
    #[serde(default)]
    pub fit: FitMode,
    /// Overrides the encoder implied by `format`. Distribution builds ship very
    /// different codec sets, so operators pin this when a format is unavailable.
    #[serde(default)]
    pub encoder: Option<String>,
    /// Quality passed to the encoder, in whatever scale the target codec uses.
    #[serde(default)]
    #[serde(alias = "quality")]
    pub quality: Option<String>,
    /// Output pixel format, e.g. `gray` for a monochrome thumbnail. Left unset
    /// the source's own layout is kept.
    #[serde(default, alias = "pixelFormat")]
    pub pixel_format: Option<String>,
    /// Value passed to `-vf scale`, defaulting to bicubic when unset.
    #[serde(default, alias = "scaleAlgorithm")]
    pub scale_algorithm: Option<String>,
}

impl Default for ImageVariant {
    fn default() -> Self {
        Self {
            name: "original".to_owned(),
            max_width: None,
            max_height: None,
            format: ImageFormat::Keep,
            fit: FitMode::default(),
            encoder: None,
            quality: None,
            pixel_format: None,
            scale_algorithm: None,
        }
    }
}

impl ImageVariant {
    /// Encoder to invoke for a source with this extension.
    #[must_use]
    pub fn encoder(&self, source_extension: &str) -> Option<&str> {
        self.encoder
            .as_deref()
            .or(self.format.default_encoder())
            .or(Some(ImageFormat::source_encoder(source_extension)))
    }

    /// Encoder that must exist for this rendition whatever the source turns out
    /// to be, or `None` when nothing about it can be checked up front.
    ///
    /// `Keep` renditions follow the source's format, so all that can be said in
    /// advance is that the fallback codec — the one the generic muxer defaults
    /// to, and the one every build has — is available.
    #[must_use]
    pub fn required_encoder(&self) -> Option<&str> {
        self.encoder.as_deref().or(self.format.default_encoder()).or(Some("mjpeg"))
    }

    /// ffmpeg quality arguments for this rendition.
    #[must_use]
    pub fn quality_args(&self, source_extension: &str) -> Vec<String> {
        let Some(quality) = self.quality.as_deref() else {
            return Vec::new();
        };
        match image_quality_flag(self.format, self.encoder(source_extension), source_extension) {
            QualityFlag::Percent => vec!["-quality".to_owned(), quality.to_owned()],
            QualityFlag::Crf => vec!["-crf".to_owned(), quality.to_owned(), "-b:v".to_owned(), "0".to_owned()],
            QualityFlag::Compression => vec!["-compression_level".to_owned(), quality.to_owned()],
            QualityFlag::Quantizer => vec!["-q:v".to_owned(), quality.to_owned()],
            // A target with no quality knob must not be handed one: ffmpeg
            // aborts on an unknown option rather than quietly ignoring it.
            QualityFlag::Unsupported => Vec::new(),
        }
    }

    /// Output pixel format, normalised, or `None` to keep the source's.
    #[must_use]
    pub fn output_pixel_format(&self) -> Option<String> {
        self.pixel_format.as_deref().and_then(normalize_pixel_format)
    }

    /// Muxer ffmpeg needs to write this rendition.
    #[must_use]
    pub fn muxer(&self, source_extension: &str) -> &'static str {
        self.format.muxer(source_extension)
    }

    /// Builds the `-vf` filter chain for this rendition.
    ///
    /// Each bound is clamped through `min()` against the source dimension:
    /// `force_original_aspect_ratio` prevents distortion but happily *enlarges*,
    /// so a 480p source handed a 720p filter would come back upscaled unless
    /// the bound is clamped here.
    pub(crate) fn filters(&self) -> String {
        let algorithm = self.scale_algorithm.as_deref().unwrap_or("bicubic");
        let mut filters: Vec<String> = Vec::new();

        let bounds: Option<String> = match (self.max_width, self.max_height) {
            (None, None) => None,
            (Some(width), None) => Some(format!("scale=w=min(iw\\,{width}):h=-2:flags={algorithm}")),
            (None, Some(height)) => Some(format!("scale=w=-2:h=min(ih\\,{height}):flags={algorithm}")),
            // Both axes pinned: the aspect ratio is either preserved (leaving one
            // axis short) or cropped away entirely.
            (Some(width), Some(height)) => Some(match self.fit {
                FitMode::Contain => {
                    format!("scale=w={width}:h={height}:flags={algorithm}:force_original_aspect_ratio=decrease")
                }
                FitMode::Cover => {
                    format!("scale=w={width}:h={height}:flags={algorithm}:force_original_aspect_ratio=increase")
                }
                FitMode::Fill => format!("scale=w={width}:h={height}:flags={algorithm}"),
            }),
        };

        if let Some(bounds) = bounds {
            filters.push(bounds);
            // Cover scales past the bounds so both axes are covered; the excess
            // is cropped back off, from the centre by default.
            if self.fit.crops() {
                if let (Some(width), Some(height)) = (self.max_width, self.max_height) {
                    filters.push(format!("crop={width}:{height}"));
                }
            }
        }

        if let Some(pixel_format) = self.output_pixel_format() {
            filters.push(format!("format={pixel_format}"));
        }

        filters.join(",")
    }

    /// True when this rendition is worth rendering.
    ///
    /// A smaller size is the obvious reason. The other is a format change: a
    /// 6 MB PNG at the same dimensions still deserves a WebP copy, which is the
    /// whole point of running an image host.
    #[must_use]
    pub fn shrinks(&self, info: &MediaInfo, source_extension: &str) -> bool {
        let smaller = match (self.max_width, self.max_height) {
            (None, None) => false,
            // Fit only decides how a bounded rendition fills its box; an
            // unbounded one has nothing to fill.
            (Some(width), None) => info.width() > width,
            (None, Some(height)) => info.height() > height,
            (Some(width), Some(height)) => info.width() > width || info.height() > height,
        };
        let format_changed =
            !matches!(self.format, ImageFormat::Keep) && self.format.extension(source_extension) != source_extension;
        smaller || format_changed
    }
}

/// One video rendition, encoded for browsers rather than copied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoVariant {
    pub name: String,
    #[serde(alias = "maxHeight")]
    pub max_height: Option<u32>,
    /// Upper bound on width; the long axis of a vertical clip.
    #[serde(default, alias = "maxWidth")]
    pub max_width: Option<u32>,
    /// How the bounds relate to the source aspect ratio.
    #[serde(default)]
    pub fit: FitMode,
    /// Video codec to target. `copy` remuxes without re-encoding.
    #[serde(default, alias = "videoCodec")]
    pub video_codec: VideoCodec,
    /// Audio codec to target; `none` drops the audio track.
    #[serde(default, alias = "audioCodec")]
    pub audio_codec: AudioCodec,
    /// Output pixel format. Left unset the codec's own default is used.
    #[serde(default, alias = "pixelFormat")]
    pub pixel_format: Option<String>,
    /// Container to mux into; defaults to the source's, falling back to mp4.
    #[serde(default)]
    #[serde(alias = "container")]
    pub container: Option<String>,
    #[serde(default = "default_crf")]
    pub crf: u32,
    #[serde(default = "default_preset")]
    #[serde(alias = "preset")]
    pub preset: String,
    #[serde(default = "default_audio_bitrate")]
    #[serde(alias = "audioBitrate")]
    pub audio_bitrate: String,
    /// Upper bound on frame rate; 60fps sources are halved for most clients
    /// without anyone noticing.
    #[serde(default, alias = "maxFps", alias = "fps")]
    pub max_fps: Option<f64>,
}

fn default_crf() -> u32 {
    23
}

fn default_preset() -> String {
    "veryfast".to_owned()
}

fn default_audio_bitrate() -> String {
    "128k".to_owned()
}

impl Default for VideoVariant {
    fn default() -> Self {
        Self {
            name: "original".to_owned(),
            max_height: None,
            max_width: None,
            fit: FitMode::default(),
            video_codec: VideoCodec::default(),
            audio_codec: AudioCodec::default(),
            pixel_format: None,
            container: None,
            crf: default_crf(),
            preset: default_preset(),
            audio_bitrate: default_audio_bitrate(),
            max_fps: None,
        }
    }
}

impl VideoVariant {
    /// Output pixel format, normalised, falling back to the codec's own.
    #[must_use]
    pub fn output_pixel_format(&self) -> Option<String> {
        self.pixel_format
            .as_deref()
            .and_then(normalize_pixel_format)
            .or_else(|| self.video_codec.default_pixel_format().map(str::to_owned))
    }

    /// Muxer ffmpeg needs to write this rendition.
    ///
    /// Defaults to mp4 rather than to the source's container. A bucket acting
    /// as a video host exists to hand out files clients can play, and inheriting
    /// the source's container is how a 10-bit HEVC `.mkv` ends up with a
    /// `.mkv` rendition that is exactly as unplayable as its source. An
    /// unrecognised requested container falls back to mp4 for the same reason.
    #[must_use]
    pub fn muxer(&self) -> &'static str {
        self.container.as_deref().and_then(video_muxer).unwrap_or("mp4")
    }

    /// Extension the stored object is named under.
    #[must_use]
    pub fn extension(&self) -> &'static str {
        muxer_extension(self.muxer())
    }

    /// True when this rendition is worth rendering.
    ///
    /// A smaller frame is the obvious reason. The others are a source no client
    /// can play, or a rendition that deliberately differs from the source in
    /// codec, container, pixel layout, frame rate or audio — a 1080p HEVC file
    /// needs a 1080p H.264 copy, and skipping it because the height already
    /// matches would leave it unplayable.
    #[must_use]
    pub fn shrinks(&self, info: &MediaInfo) -> bool {
        let smaller = self.max_height.is_some_and(|height| info.height() > height)
            || self.max_width.is_some_and(|width| info.width() > width);
        smaller || self.differs_from_source(info)
    }

    /// True when the rendition asks for something other than what the source
    /// already carries, so producing it is a real conversion.
    fn differs_from_source(&self, info: &MediaInfo) -> bool {
        let source_codec = info.video.as_ref().map(|stream| stream.codec.as_str());
        let codec_matches = match (self.video_codec, source_codec) {
            // Nothing to compare against, so assume this is a conversion.
            (_, None) => true,
            (VideoCodec::Copy, Some(_)) => true,
            (VideoCodec::H264, Some(codec)) => codec == "h264" || codec == "avc1",
            (VideoCodec::H265, Some(codec)) => codec == "hevc" || codec == "h265",
            (VideoCodec::Vp9, Some(codec)) => codec == "vp9",
            (VideoCodec::Av1, Some(codec)) => codec == "av1",
            (VideoCodec::Mpeg4, Some(codec)) => codec == "mpeg4",
        };

        let wanted = self.extension();
        let container_matches = info
            .container_extensions()
            .iter()
            .any(|name| name.eq_ignore_ascii_case(wanted));

        let pixel_matches = match (self.output_pixel_format(), info.pixel_format()) {
            // An unset pixel format means "the codec's default", which is what
            // the source already carries when it decodes cleanly.
            (None, _) => true,
            (Some(_), None) => false,
            (Some(requested), Some(actual)) => requested == actual,
        };

        let fps_matches = self.max_fps.is_none_or(|bound| info.fps().is_none_or(|fps| fps <= bound));

        // Audio has to be compared by codec, not merely by presence. A source
        // carrying Opus in an mp4 is a real conversion away from AAC, and one
        // that skips it leaves the file unplayable on any client that does not
        // decode Opus — which is most of them.
        let audio_changed = match (self.audio_codec, info.has_audio) {
            // Dropping audio is a change when there is audio to drop.
            (AudioCodec::None, true) => true,
            // A silent source satisfies both "keep the audio" and "drop it".
            (_, false) => false,
            // An audio track that does not match the target is a conversion.
            // An unrecognised source codec maps to `None`, which differs from
            // every target, so the rendition is produced rather than skipped.
            (wanted, true) => source_audio_codec(info) != Some(wanted),
        };

        !codec_matches || !container_matches || !pixel_matches || !fps_matches || audio_changed
    }
}

/// Extension an object muxed by `muxer` is named under.
///
/// The muxer name and the file extension are not the same vocabulary — the
/// `matroska` muxer writes `.mkv` — and the extension is what a derived object
/// key ends in, so the mapping has to be explicit rather than assumed.
#[must_use]
pub fn muxer_extension(muxer: &str) -> &'static str {
    match muxer {
        "mov" => "mov",
        "3gp" => "3gp",
        "webm" => "webm",
        "matroska" => "mkv",
        "avi" => "avi",
        "mpegts" => "ts",
        "flv" => "flv",
        "ogv" => "ogv",
        "asf" => "wmv",
        "vob" => "vob",
        _ => "mp4",
    }
}

/// The audio codec a source already carries, in the rendition's vocabulary.
///
/// ffprobe reports `mp3` where the encoder is `libmp3lame`, so the names are
/// mapped rather than compared; anything the map does not know becomes `None`
/// only for a silent source, and otherwise falls to a mismatch that forces the
/// rendition to be produced — the safe direction.
fn source_audio_codec(info: &MediaInfo) -> Option<AudioCodec> {
    match info.audio_codec()? {
        "aac" => Some(AudioCodec::Aac),
        "opus" => Some(AudioCodec::Opus),
        "mp3" => Some(AudioCodec::Mp3),
        "vorbis" => Some(AudioCodec::Vorbis),
        _ => None,
    }
}

/// Muxer ffmpeg needs for a container name or extension.
///
/// Derived objects are written to scratch files whose names carry no useful
/// extension, and ffmpeg refuses to guess a muxer from a bare name, so the
/// muxer always has to be named explicitly.
#[must_use]
pub fn video_muxer(container: &str) -> Option<&'static str> {
    let name = container.trim().trim_start_matches('.').to_ascii_lowercase();
    let muxer = match name.as_str() {
        "mp4" | "m4v" | "m4a" | "m4p" | "ismv" | "f4v" => "mp4",
        "mov" | "qt" => "mov",
        "3gp" | "3g2" => "3gp",
        "webm" => "webm",
        "mkv" | "mka" | "mk3d" => "matroska",
        "avi" => "avi",
        "ts" | "m2ts" | "mts" => "mpegts",
        "flv" => "flv",
        "ogv" => "ogv",
        "wmv" | "asf" => "asf",
        "vob" => "vob",
        _ => return None,
    };
    Some(muxer)
}

/// A glob restricted to what object keys actually contain: `*` inside a path
/// segment, `**` across segments, `?` for one character. Keeping the matcher
/// here means operators write plain patterns instead of regexp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyPattern {
    pub pattern: String,
    #[serde(default, alias = "negate")]
    pub negate: bool,
}

impl KeyPattern {
    fn matches(&self, key: &str) -> bool {
        glob_match(&self.pattern, key) != self.negate
    }
}

/// One processing rule: which keys it claims, and what it produces for them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaRule {
    pub name: String,

    /// Objects under any of these prefixes match. Empty means "any prefix".
    #[serde(default, alias = "includePrefixes")]
    pub include_prefixes: Vec<String>,
    /// Objects ending in any of these suffixes match (with or without a dot).
    /// Empty means "any suffix".
    #[serde(default, alias = "includeSuffixes")]
    pub include_suffixes: Vec<String>,
    /// Applied after the includes; anything matching is dropped.
    #[serde(default, alias = "excludePrefixes")]
    pub exclude_prefixes: Vec<String>,
    #[serde(default, alias = "excludeSuffixes")]
    pub exclude_suffixes: Vec<String>,
    /// Which pipelines this rule drives.
    #[serde(default)]
    pub images: Vec<ImageVariant>,
    #[serde(default)]
    pub videos: Vec<VideoVariant>,
    /// Delete the source once every variant has been stored.
    #[serde(default, alias = "deleteSource")]
    pub delete_source: bool,
}

impl Default for MediaRule {
    fn default() -> Self {
        Self {
            name: "default".to_owned(),
            include_prefixes: Vec::new(),
            include_suffixes: Vec::new(),
            exclude_prefixes: Vec::new(),
            exclude_suffixes: Vec::new(),
            images: Vec::new(),
            videos: Vec::new(),
            delete_source: false,
        }
    }
}

impl MediaRule {
    /// True when this rule claims `key`.
    #[must_use]
    pub fn accepts(&self, key: &str) -> bool {
        if self.exclude_prefixes.iter().any(|prefix| key.starts_with(prefix)) {
            return false;
        }
        if self.exclude_suffixes.iter().any(|suffix| has_suffix(key, suffix)) {
            return false;
        }
        // An empty include list means the rule does not filter on that axis.
        let prefix_ok = self.include_prefixes.is_empty() || self.include_prefixes.iter().any(|prefix| key.starts_with(prefix));
        let suffix_ok = self.include_suffixes.is_empty() || self.include_suffixes.iter().any(|suffix| has_suffix(key, suffix));
        prefix_ok && suffix_ok
    }

    #[must_use]
    pub fn handles(&self, kind: MediaKind) -> bool {
        match kind {
            MediaKind::Image => !self.images.is_empty(),
            MediaKind::Video => !self.videos.is_empty(),
            MediaKind::Audio | MediaKind::Unknown => false,
        }
    }

    /// The first rule that claims the key wins, so ordering in the stored
    /// profile is meaningful and overlapping rules stay predictable.
    pub(crate) fn first_match<'a>(rules: &'a [MediaRule], key: &str) -> Option<&'a MediaRule> {
        rules.iter().find(|rule| rule.accepts(key))
    }
}

fn has_suffix(key: &str, suffix: &str) -> bool {
    let suffix = suffix.trim_start_matches('.');
    let name = key.rsplit('/').next().unwrap_or(key);
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return suffix.is_empty();
    };
    extension.eq_ignore_ascii_case(suffix) || stem.eq_ignore_ascii_case(suffix)
}

/// What a `*` in the pattern is allowed to consume.
#[derive(Clone, Copy, PartialEq)]
enum StarKind {
    /// `*` — any run of characters inside one path segment.
    Within,
    /// `**` — any run of characters, separators included.
    Any,
    /// `**/` — zero or more whole path segments.
    Segments,
}

impl StarKind {
    fn at(pattern: &[char], index: usize) -> (Self, usize) {
        match (pattern.get(index + 1), pattern.get(index + 2)) {
            (Some('*'), Some('/')) => (Self::Segments, 3),
            (Some('*'), _) => (Self::Any, 2),
            _ => (Self::Within, 1),
        }
    }
}

/// Matches `pattern` against `text`.
///
/// `*` stays inside one path segment, `**` crosses separators, and `**/`
/// matches any number of whole segments including none at all. Backtracking
/// explores every split, so overlapping stars behave the way operators expect;
/// the memo keeps that bounded to linear work in pattern times key length.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let width = text.len() + 1;
    let mut memo = vec![None; (pattern.len() + 1) * width];
    glob_walk(&pattern, &text, 0, 0, &mut memo, width)
}

fn glob_walk(
    pattern: &[char],
    text: &[char],
    pattern_at: usize,
    text_at: usize,
    memo: &mut Vec<Option<bool>>,
    width: usize,
) -> bool {
    let key = pattern_at * width + text_at;
    if let Some(cached) = memo[key] {
        return cached;
    }

    let result = if pattern_at == pattern.len() {
        text_at == text.len()
    } else if pattern[pattern_at] == '*' {
        let (kind, width_of_star) = StarKind::at(pattern, pattern_at);
        let next = pattern_at + width_of_star;
        // The star may match nothing at all...
        glob_walk(pattern, text, next, text_at, memo, width)
            // ...or swallow progressively more input.
            || (0..=text.len().saturating_sub(text_at)).any(|offset| {
                let end = text_at + offset;
                if offset > 0 && !star_may_cross(kind, &text[text_at..end]) {
                    return false;
                }
                glob_walk(pattern, text, next, end, memo, width)
            })
    } else if text_at < text.len() && (pattern[pattern_at] == '?' || pattern[pattern_at] == text[text_at]) {
        glob_walk(pattern, text, pattern_at + 1, text_at + 1, memo, width)
    } else {
        false
    };

    memo[key] = Some(result);
    result
}

/// Whether everything a star consumed was legal for its kind.
fn star_may_cross(kind: StarKind, consumed: &[char]) -> bool {
    match kind {
        StarKind::Within => !consumed.contains(&'/'),
        StarKind::Any => true,
        // `**/` may only end on a segment boundary.
        StarKind::Segments => consumed.last() == Some(&'/'),
    }
}

/// What to derive for one bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaProfile {
    #[serde(default, alias = "enabled")]
    pub enabled: bool,
    #[serde(default = "default_variant_prefix", alias = "variantPrefix")]
    pub variant_prefix: String,
    /// Rules are evaluated in order; the first that claims the key wins.
    #[serde(default)]
    pub rules: Vec<MediaRule>,
    /// Glob patterns applied to every key before rule matching.
    #[serde(default)]
    pub include: Vec<KeyPattern>,
    #[serde(default)]
    pub exclude: Vec<KeyPattern>,
    /// Skip sources below this size; a 4 KB GIF is not worth a decode.
    #[serde(default = "default_min_source_bytes", alias = "minSourceBytes")]
    pub min_source_bytes: u64,
    /// Skip sources above this size to bound the cost of a single upload.
    #[serde(default = "default_max_source_bytes", alias = "maxSourceBytes")]
    pub max_source_bytes: u64,
    /// A rendition must be smaller than this fraction of its source to be
    /// stored. Set to 1.0 to keep every rendition, or 0.0 to disable the check.
    #[serde(default = "default_min_variant_ratio", alias = "minVariantRatio")]
    pub min_variant_ratio: f64,
}

fn default_min_variant_ratio() -> f64 {
    0.95
}

fn default_variant_prefix() -> String {
    DEFAULT_VARIANT_PREFIX.to_owned()
}

fn default_min_source_bytes() -> u64 {
    1024
}

fn default_max_source_bytes() -> u64 {
    8 * 1024 * 1024 * 1024
}

impl Default for MediaProfile {
    /// A sensible image/video host profile: three image sizes and three
    /// common video resolutions, claimed by one catch-all rule.
    ///
    /// Every WebP rendition has a JPEG twin. Distribution builds very often lack
    /// `libwebp` — Debian's `ffmpeg` does — and without the twin a build like
    /// that would silently drop every thumbnail and derive nothing at all,
    /// because the profile's only formats are the ones it cannot encode.
    fn default() -> Self {
        let sized = |name: &str, bound: u32, format: ImageFormat| ImageVariant {
            name: name.to_owned(),
            max_width: Some(bound),
            fit: FitMode::Contain,
            format,
            ..ImageVariant::default()
        };

        Self {
            enabled: true,
            variant_prefix: default_variant_prefix(),
            rules: vec![MediaRule {
                name: "media-host".to_owned(),
                images: vec![
                    sized("thumb", 320, ImageFormat::Webp),
                    sized("thumb-jpeg", 320, ImageFormat::Jpeg),
                    sized("medium", 1280, ImageFormat::Webp),
                    sized("medium-jpeg", 1280, ImageFormat::Jpeg),
                    ImageVariant {
                        name: "large".to_owned(),
                        max_width: Some(2560),
                        format: ImageFormat::Keep,
                        ..ImageVariant::default()
                    },
                ],
                videos: vec![
                    VideoVariant {
                        name: "480p".to_owned(),
                        max_height: Some(480),
                        ..VideoVariant::default()
                    },
                    VideoVariant {
                        name: "720p".to_owned(),
                        max_height: Some(720),
                        ..VideoVariant::default()
                    },
                    VideoVariant {
                        name: "1080p".to_owned(),
                        max_height: Some(1080),
                        ..VideoVariant::default()
                    },
                ],
                ..MediaRule::default()
            }],
            include: Vec::new(),
            exclude: Vec::new(),
            min_source_bytes: default_min_source_bytes(),
            max_source_bytes: default_max_source_bytes(),
            min_variant_ratio: default_min_variant_ratio(),
        }
    }
}

impl MediaProfile {
    /// True when the object is large enough to be worth decoding.
    #[must_use]
    pub fn accepts_size(&self, bytes: u64) -> bool {
        bytes >= self.min_source_bytes && bytes <= self.max_source_bytes
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// True when the key survives the profile's global filters.
    #[must_use]
    pub fn accepts_key(&self, key: &str) -> bool {
        self.include.iter().all(|pattern| pattern.matches(key)) && self.exclude.iter().all(|pattern| !pattern.matches(key))
    }

    /// The rule that claims `key`, if any.
    #[must_use]
    pub fn rule_for(&self, key: &str) -> Option<&MediaRule> {
        MediaRule::first_match(&self.rules, key)
    }

    /// Full admission check: enabled, size in range, key admitted, some rule
    /// claims the key.
    #[must_use]
    pub fn accepts(&self, key: &str, bytes: u64) -> bool {
        self.enabled && self.accepts_size(bytes) && self.accepts_key(key) && self.rule_for(key).is_some()
    }
}

#[derive(Debug, Clone)]
pub enum VariantKind {
    Image(ImageVariant),
    Video(VideoVariant),
}

/// Builds the job list for one object.
///
/// `key` is the source object key; the returned keys live under the profile's
/// prefix, mirroring the source path so two files with the same basename in
/// different folders cannot collide.
#[must_use]
pub fn expand(info: &MediaInfo, profile: &MediaProfile, key: &str) -> Vec<PlannedVariant> {
    let Some(rule) = profile.rule_for(key) else {
        return Vec::new();
    };
    let extension = extension_of(key);
    let prefix = profile.variant_prefix.trim_matches('/');

    match info.kind {
        MediaKind::Image => rule
            .images
            .iter()
            .filter(|variant| variant.shrinks(info, &extension))
            .map(|variant| PlannedVariant {
                key: variant_key(prefix, key, &variant.name, &variant.format.extension(&extension)),
                kind: VariantKind::Image(variant.clone()),
            })
            .collect(),
        MediaKind::Video => rule
            .videos
            .iter()
            .filter(|variant| variant.shrinks(info))
            .map(|variant| PlannedVariant {
                key: variant_key(prefix, key, &variant.name, variant.extension()),
                kind: VariantKind::Video(variant.clone()),
            })
            .collect(),
        MediaKind::Audio | MediaKind::Unknown => Vec::new(),
    }
}

/// True when the derived bytes are worth storing.
///
/// Re-encoding a file that is already tiny and well-compressed usually makes it
/// bigger, so a variant is only worth storing if it actually saves space.
///
/// `forced` covers the renditions where size is the wrong measure: a source no
/// client can decode still deserves a playable copy even when that copy is
/// larger, and so does a deliberate format or pixel-layout conversion, which
/// exists precisely to be a different thing from the source.
#[must_use]
pub fn is_useful_variant(source_bytes: u64, variant_bytes: u64, min_ratio: f64, forced: bool) -> bool {
    if forced {
        return true;
    }
    if source_bytes == 0 {
        return true;
    }
    let ratio = variant_bytes as f64 / source_bytes as f64;
    ratio < min_ratio.clamp(0.01, 1.0)
}

/// True when a rendition exists to change the source's *kind* rather than its
/// size, and so is worth storing even if it comes out larger.
#[must_use]
pub fn is_conversion(kind: &VariantKind, info: &MediaInfo, source_extension: &str) -> bool {
    match kind {
        // A source clients cannot play has to be converted even though the
        // playable copy is routinely larger than the original.
        VariantKind::Video(_) => {
            !info.is_browser_playable()
                || info
                    .video
                    .as_ref()
                    .is_none_or(|stream| !matches!(stream.codec.as_str(), "h264" | "avc1"))
        }
        VariantKind::Image(variant) => {
            variant.format != ImageFormat::Keep && variant.format.extension(source_extension) != source_extension
        }
    }
}

/// Resolves the key a caller should read for a named variant of `key`.
#[must_use]
pub fn variant_key(prefix: &str, key: &str, variant: &str, extension: &str) -> String {
    format!("{prefix}/{key}/{variant}.{extension}")
}

fn extension_of(key: &str) -> String {
    let basename = key.rsplit('/').next().unwrap_or(key);
    basename
        .rsplit_once('.')
        .map_or_else(String::new, |(_, extension)| extension.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::VideoStream;

    fn stream(width: u32, height: u32) -> VideoStream {
        VideoStream {
            codec: "h264".to_owned(),
            width,
            height,
            pix_fmt: "yuv420p".to_owned(),
            fps: Some(30.0),
            audio_codec: Some("aac".to_owned()),
        }
    }

    fn video(width: u32, height: u32) -> MediaInfo {
        MediaInfo {
            kind: MediaKind::Video,
            // mp4, so the default H.264/AAC rendition matches it exactly.
            format: "mov,mp4,m4a,3gp,3g2,mj2".to_owned(),
            duration: None,
            video: Some(stream(width, height)),
            has_audio: true,
        }
    }

    fn hevc(width: u32, height: u32) -> MediaInfo {
        MediaInfo {
            kind: MediaKind::Video,
            format: "matroska,webm".to_owned(),
            duration: None,
            video: Some(VideoStream {
                codec: "hevc".to_owned(),
                width,
                height,
                pix_fmt: "yuv420p".to_owned(),
                fps: Some(30.0),
                audio_codec: Some("aac".to_owned()),
            }),
            has_audio: true,
        }
    }

    fn image(width: u32, height: u32) -> MediaInfo {
        MediaInfo {
            kind: MediaKind::Image,
            format: "png_pipe".to_owned(),
            duration: None,
            video: Some(VideoStream {
                codec: "png".to_owned(),
                width,
                height,
                pix_fmt: "rgb24".to_owned(),
                fps: None,
                audio_codec: None,
            }),
            has_audio: false,
        }
    }

    #[test]
    fn variants_land_under_the_prefix_mirroring_the_source_key() {
        // Matroska is not a browser container, so even the 1080p rendition of a
        // 1080p source is worth rendering.
        let planned = expand(&hevc(1920, 1080), &MediaProfile::default(), "media/2024/clip.mkv");
        let keys: Vec<_> = planned.iter().map(|variant| variant.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "variants/media/2024/clip.mkv/480p.mp4",
                "variants/media/2024/clip.mkv/720p.mp4",
                "variants/media/2024/clip.mkv/1080p.mp4"
            ]
        );
    }

    #[test]
    fn renditions_larger_than_the_source_are_dropped() {
        // A 480p source must not gain a "720p" copy that is really an upscale —
        // but it still needs every rendition when the codec is unplayable.
        let playable = video(854, 480);
        assert!(playable.is_browser_playable());
        let planned = expand(&playable, &MediaProfile::default(), "clip.mp4");
        assert_eq!(planned.len(), 0);

        let planned = expand(&video(1920, 800), &MediaProfile::default(), "clip.mp4");
        let keys: Vec<_> = planned.iter().map(|variant| variant.key.as_str()).collect();
        assert_eq!(keys, vec!["variants/clip.mp4/480p.mp4", "variants/clip.mp4/720p.mp4"]);
    }

    #[test]
    fn an_unplayable_source_gets_renditions_even_at_its_own_resolution() {
        // 1080p HEVC: every rendition the profile asks for is worth encoding,
        // including 1080p, because the source is not browser-decodable at all.
        let source = hevc(1920, 1080);
        assert!(!source.is_browser_playable());

        let planned = expand(&source, &MediaProfile::default(), "clip.mkv");
        let keys: Vec<_> = planned.iter().map(|variant| variant.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "variants/clip.mkv/480p.mp4",
                "variants/clip.mkv/720p.mp4",
                "variants/clip.mkv/1080p.mp4"
            ]
        );
    }

    #[test]
    fn ten_bit_h264_is_treated_as_unplayable() {
        let ten_bit = MediaInfo {
            kind: MediaKind::Video,
            format: "mp4".to_owned(),
            duration: None,
            video: Some(VideoStream {
                codec: "h264".to_owned(),
                width: 1920,
                height: 1080,
                pix_fmt: "yuv420p10le".to_owned(),
                fps: Some(30.0),
                audio_codec: Some("aac".to_owned()),
            }),
            has_audio: true,
        };
        assert!(!ten_bit.is_browser_playable());
        // An 8-bit rendition of it is the conversion that matters, and a 10-bit
        // rendition of an already-10-bit source is just a copy.
        assert!(VideoVariant::default().shrinks(&ten_bit));
        let also_ten_bit = VideoVariant {
            name: "10bit".to_owned(),
            pixel_format: Some("yuv420p10le".to_owned()),
            ..VideoVariant::default()
        };
        assert!(!also_ten_bit.shrinks(&ten_bit));
    }

    #[test]
    fn a_same_size_format_change_is_still_worth_rendering() {
        // 400x300 is under the 320p thumb bound in one dimension only, so this
        // asserts the format-change path: a small PNG still deserves a WebP.
        let png = image(400, 300);
        let keep = ImageVariant {
            name: "large".to_owned(),
            max_width: Some(2560),
            format: ImageFormat::Keep,
            ..ImageVariant::default()
        };
        assert!(!keep.shrinks(&png, "png"), "same format and same size is a no-op");

        let webp = ImageVariant {
            name: "small".to_owned(),
            max_width: Some(2560),
            format: ImageFormat::Webp,
            ..ImageVariant::default()
        };
        assert!(webp.shrinks(&png, "png"), "a format change is worth the encode");
    }

    #[test]
    fn image_renditions_follow_the_source_extension_when_kept() {
        let planned = expand(&image(4000, 3000), &MediaProfile::default(), "photos/a.png");
        let keys: Vec<_> = planned.iter().map(|variant| variant.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "variants/photos/a.png/thumb.webp",
                "variants/photos/a.png/thumb-jpeg.jpg",
                "variants/photos/a.png/medium.webp",
                "variants/photos/a.png/medium-jpeg.jpg",
                "variants/photos/a.png/large.png"
            ],
            "every webp rendition has a jpeg twin so a build without libwebp still derives something"
        );
    }

    #[test]
    fn audio_and_unknown_objects_produce_nothing() {
        let audio = MediaInfo {
            kind: MediaKind::Audio,
            format: "mp3".to_owned(),
            duration: None,
            video: None,
            has_audio: true,
        };
        assert!(expand(&audio, &MediaProfile::default(), "song.mp3").is_empty());

        let unknown = MediaInfo {
            kind: MediaKind::Unknown,
            format: String::new(),
            duration: None,
            video: None,
            has_audio: false,
        };
        assert!(expand(&unknown, &MediaProfile::default(), "notes.txt").is_empty());
    }

    #[test]
    fn a_custom_prefix_is_trimmed_of_slashes() {
        let profile = MediaProfile {
            variant_prefix: "/derived/".to_owned(),
            ..MediaProfile::default()
        };
        let planned = expand(&video(3840, 2160), &profile, "clip.mp4");
        assert_eq!(planned[0].key, "derived/clip.mp4/480p.mp4");
    }

    #[test]
    fn scale_filters_clamp_every_bound_so_a_source_is_never_enlarged() {
        let bounded = ImageVariant {
            name: "thumb".to_owned(),
            max_width: Some(320),
            ..ImageVariant::default()
        };
        // The bound is clamped through min() against the source, so a 240px
        // source handed a 320px filter comes back 240px, not upscaled.
        assert_eq!(bounded.filters(), "scale=w=min(iw\\,320):h=-2:flags=bicubic");

        let width_only = ImageVariant {
            name: "medium".to_owned(),
            max_width: Some(1280),
            ..ImageVariant::default()
        };
        // -2 keeps the frame size even, which yuv420p chroma subsampling requires.
        assert_eq!(width_only.filters(), "scale=w=min(iw\\,1280):h=-2:flags=bicubic");

        assert_eq!(ImageVariant::default().filters(), "");
    }

    #[test]
    fn a_cover_rendition_crops_to_exactly_the_requested_box() {
        let cover = ImageVariant {
            name: "square".to_owned(),
            max_width: Some(400),
            max_height: Some(400),
            fit: FitMode::Cover,
            ..ImageVariant::default()
        };
        assert_eq!(
            cover.filters(),
            "scale=w=400:h=400:flags=bicubic:force_original_aspect_ratio=increase,crop=400:400"
        );
    }

    #[test]
    fn a_pixel_format_request_becomes_a_trailing_filter() {
        let gray = ImageVariant {
            name: "gray".to_owned(),
            max_width: Some(320),
            pixel_format: Some("gray".to_owned()),
            ..ImageVariant::default()
        };
        assert_eq!(gray.filters(), "scale=w=min(iw\\,320):h=-2:flags=bicubic,format=gray");
        assert_eq!(gray.output_pixel_format().as_deref(), Some("gray"));
    }

    #[test]
    fn pixel_formats_are_normalised_and_validated() {
        assert_eq!(normalize_pixel_format(" YUV420P ").as_deref(), Some("yuv420p"));
        assert_eq!(normalize_pixel_format("   "), None);
        assert!(is_known_pixel_format("yuv420p"));
        assert!(is_known_pixel_format("gray"));
        assert!(!is_known_pixel_format("yuv420"), "a truncated name is not a format");
    }

    #[test]
    fn video_muxers_cover_the_containers_a_browser_expects() {
        assert_eq!(video_muxer("mp4"), Some("mp4"));
        assert_eq!(video_muxer(".MKV"), Some("matroska"));
        assert_eq!(video_muxer("webm"), Some("webm"));
        assert_eq!(video_muxer("mov"), Some("mov"));
        assert_eq!(video_muxer("nonsense"), None);
    }

    #[test]
    fn an_unknown_container_falls_back_to_mp4_rather_than_the_source() {
        let variant = VideoVariant {
            name: "v".to_owned(),
            container: Some("nonsense".to_owned()),
            ..VideoVariant::default()
        };
        assert_eq!(variant.muxer(), "mp4");
        assert_eq!(variant.extension(), "mp4");

        // A named container that muxes under a different name than its
        // extension still gets the extension its readers expect.
        let mkv = VideoVariant {
            name: "v".to_owned(),
            container: Some("mkv".to_owned()),
            ..VideoVariant::default()
        };
        assert_eq!(mkv.muxer(), "matroska");
        assert_eq!(mkv.extension(), "mkv");
    }

    #[test]
    fn a_video_codec_drives_the_renditions_own_pixel_format() {
        let av1 = VideoVariant {
            name: "av1".to_owned(),
            video_codec: VideoCodec::Av1,
            ..VideoVariant::default()
        };
        assert_eq!(av1.output_pixel_format().as_deref(), Some("yuv420p10le"));

        let h264 = VideoVariant {
            name: "h264".to_owned(),
            ..VideoVariant::default()
        };
        assert_eq!(h264.output_pixel_format().as_deref(), Some("yuv420p"));

        // An explicit request always wins over the codec default.
        let override_pixel = VideoVariant {
            name: "custom".to_owned(),
            video_codec: VideoCodec::H265,
            pixel_format: Some("YUV444P10LE".to_owned()),
            ..VideoVariant::default()
        };
        assert_eq!(override_pixel.output_pixel_format().as_deref(), Some("yuv444p10le"));
    }

    #[test]
    fn a_frame_rate_bound_is_a_reason_to_render() {
        let mut source = video(1280, 720);
        source.video.as_mut().expect("video stream").fps = Some(60.0);

        let capped = VideoVariant {
            name: "30fps".to_owned(),
            max_fps: Some(30.0),
            ..VideoVariant::default()
        };
        assert!(capped.shrinks(&source), "a 60fps source must be re-timed");

        let already = VideoVariant {
            name: "60fps".to_owned(),
            max_fps: Some(60.0),
            ..VideoVariant::default()
        };
        assert!(!already.shrinks(&source), "a 60fps source at a 60fps bound is a no-op");
    }

    #[test]
    fn an_audio_codec_mismatch_is_a_reason_to_render() {
        // The camera case: H.264 video with Opus audio in an mp4. Nothing about
        // the video needs converting, but Opus-in-mp4 does not decode on every
        // client, so an AAC rendition is the only playable copy.
        let opus_source = MediaInfo {
            kind: MediaKind::Video,
            format: "mov,mp4,m4a,3gp,3g2,mj2".to_owned(),
            duration: None,
            video: Some(VideoStream {
                codec: "h264".to_owned(),
                width: 1920,
                height: 1080,
                pix_fmt: "yuv420p".to_owned(),
                fps: Some(30.0),
                audio_codec: Some("opus".to_owned()),
            }),
            has_audio: true,
        };

        let to_aac = VideoVariant {
            name: "360p".to_owned(),
            max_height: Some(360),
            ..VideoVariant::default()
        };
        assert!(to_aac.shrinks(&opus_source), "Opus audio must be converted to AAC");

        // The same variant against a source that already carries AAC is a copy.
        let aac_source = MediaInfo {
            video: Some(VideoStream {
                audio_codec: Some("aac".to_owned()),
                ..opus_source.video.clone().expect("video stream")
            }),
            ..opus_source.clone()
        };
        assert!(
            !VideoVariant {
                max_height: None,
                ..to_aac.clone()
            }
            .shrinks(&aac_source),
            "an identical H.264/AAC rendition needs no re-encode"
        );

        // A source whose audio codec is not one the map knows still produces a
        // rendition: an unknown codec is assumed to differ rather than match.
        let exotic = MediaInfo {
            video: Some(VideoStream {
                audio_codec: Some("pcm_s16le".to_owned()),
                ..opus_source.video.clone().expect("video stream")
            }),
            ..opus_source.clone()
        };
        assert!(
            VideoVariant {
                max_height: None,
                ..to_aac.clone()
            }
            .shrinks(&exotic),
            "an unrecognised audio codec must not be assumed to match the target"
        );
    }

    #[test]
    fn dropping_audio_from_a_source_that_has_it_is_a_reason_to_render() {
        let silent = VideoVariant {
            name: "silent".to_owned(),
            audio_codec: AudioCodec::None,
            ..VideoVariant::default()
        };
        assert!(silent.shrinks(&video(640, 480)));

        // A source that already has no audio track has nothing to strip.
        let mut silent_source = video(640, 480);
        silent_source.has_audio = false;
        silent_source.video.as_mut().expect("video stream").audio_codec = None;
        assert!(!silent.shrinks(&silent_source));
    }

    #[test]
    fn a_codec_change_is_a_reason_to_render_even_when_nothing_else_differs() {
        let source = video(1280, 720);
        let vp9 = VideoVariant {
            name: "vp9".to_owned(),
            video_codec: VideoCodec::Vp9,
            container: Some("webm".to_owned()),
            ..VideoVariant::default()
        };
        assert!(vp9.shrinks(&source), "an mp4 h264 source is not a webm vp9 one");
    }

    #[test]
    fn a_rendition_matching_its_source_exactly_is_not_rendered() {
        let source = video(1280, 720);
        let identity = VideoVariant {
            name: "identity".to_owned(),
            ..VideoVariant::default()
        };
        assert!(!identity.shrinks(&source), "same codec, container, pixels and audio is a copy");
    }

    #[test]
    fn size_bounds_skip_the_pointless_tiny_objects() {
        let profile = MediaProfile::default();
        assert!(!profile.accepts_size(512));
        assert!(profile.accepts_size(4096));
        assert!(!profile.accepts_size(u64::MAX));
    }

    #[test]
    fn extensionless_keys_still_produce_a_variant_key() {
        let profile = MediaProfile {
            rules: vec![MediaRule {
                images: vec![ImageVariant {
                    name: "thumb".to_owned(),
                    max_width: Some(100),
                    format: ImageFormat::Webp,
                    ..ImageVariant::default()
                }],
                ..MediaRule::default()
            }],
            ..MediaProfile::default()
        };
        let planned = expand(&image(500, 500), &profile, "raw/asset");
        assert_eq!(planned[0].key, "variants/raw/asset/thumb.webp");
    }

    #[test]
    fn rules_claim_keys_by_prefix_and_suffix() {
        let profile = MediaProfile {
            rules: vec![
                MediaRule {
                    name: "raw".to_owned(),
                    include_prefixes: vec!["raw/".to_owned()],
                    videos: vec![VideoVariant {
                        name: "360p".to_owned(),
                        max_height: Some(360),
                        ..VideoVariant::default()
                    }],
                    ..MediaRule::default()
                },
                MediaRule {
                    name: "clips".to_owned(),
                    include_suffixes: vec!["mkv".to_owned(), ".mov".to_owned()],
                    images: vec![ImageVariant::default()],
                    videos: vec![VideoVariant {
                        name: "720p".to_owned(),
                        max_height: Some(720),
                        ..VideoVariant::default()
                    }],
                    ..MediaRule::default()
                },
            ],
            ..MediaProfile::default()
        };

        assert_eq!(profile.rule_for("raw/camera/a.mov").map(|r| r.name.as_str()), Some("raw"));
        assert_eq!(profile.rule_for("media/b.mkv").map(|r| r.name.as_str()), Some("clips"));
        // Neither prefix nor suffix matches, so no rule claims the key.
        assert!(profile.rule_for("docs/readme.pdf").is_none());
    }

    #[test]
    fn the_first_matching_rule_wins() {
        let profile = MediaProfile {
            rules: vec![
                MediaRule {
                    name: "first".to_owned(),
                    include_prefixes: vec!["media/".to_owned()],
                    images: vec![ImageVariant::default()],
                    ..MediaRule::default()
                },
                MediaRule {
                    name: "second".to_owned(),
                    include_prefixes: vec!["media/".to_owned()],
                    images: vec![ImageVariant::default()],
                    ..MediaRule::default()
                },
            ],
            ..MediaProfile::default()
        };
        assert_eq!(profile.rule_for("media/x.png").map(|r| r.name.as_str()), Some("first"));
    }

    #[test]
    fn excludes_win_over_includes() {
        let rule = MediaRule {
            exclude_prefixes: vec!["private/".to_owned()],
            include_prefixes: vec!["media/".to_owned()],
            include_suffixes: vec!["png".to_owned()],
            exclude_suffixes: vec!["raw".to_owned()],
            ..MediaRule::default()
        };
        assert!(rule.accepts("media/a.png"));
        assert!(!rule.accepts("private/media/a.png"));
        assert!(!rule.accepts("media/a.raw"));
        assert!(!rule.accepts("other/a.png"));
    }

    #[test]
    fn global_filters_apply_before_the_rules() {
        let profile = MediaProfile {
            include: vec![KeyPattern {
                pattern: "media/**".to_owned(),
                negate: false,
            }],
            exclude: vec![KeyPattern {
                pattern: "**/*.tmp.png".to_owned(),
                negate: false,
            }],
            ..MediaProfile::default()
        };
        assert!(profile.accepts_key("media/2024/a.png"));
        assert!(!profile.accepts_key("other/a.png"));
        assert!(!profile.accepts_key("media/2024/a.tmp.png"));
    }

    #[test]
    fn operator_written_camel_case_is_honoured_not_silently_dropped() {
        // The admin API is S3-shaped, so camelCase is what people write. A
        // filter that parses to "no filter" is worse than a parse error: it
        // looks configured while matching everything.
        let profile: MediaProfile = serde_json::from_str(
            r#"{
                "enabled": true,
                "variantPrefix": "derived",
                "minVariantRatio": 0.5,
                "rules": [{
                    "name": "clips",
                    "includeSuffixes": ["mkv"],
                    "excludePrefixes": ["private/"],
                    "deleteSource": true,
                    "videos": [{
                        "name": "720p", "maxHeight": 720, "maxWidth": 1280, "crf": 20,
                        "preset": "fast", "audioBitrate": "96k",
                        "videoCodec": "h265", "audioCodec": "opus",
                        "pixelFormat": "yuv420p10le", "container": "webm", "maxFps": 30,
                        "fit": "cover"
                    }],
                    "images": [{
                        "name": "thumb", "maxWidth": 320, "maxHeight": 320,
                        "format": "webp", "quality": "80", "fit": "cover", "pixelFormat": "gray"
                    }]
                }]
            }"#,
        )
        .expect("camelCase config parses");

        let rule = &profile.rules[0];
        assert_eq!(rule.include_suffixes, vec!["mkv".to_owned()]);
        assert_eq!(rule.exclude_prefixes, vec!["private/".to_owned()]);
        assert!(rule.delete_source);

        let video = &rule.videos[0];
        assert_eq!(video.max_height, Some(720));
        assert_eq!(video.max_width, Some(1280));
        assert_eq!(video.crf, 20);
        assert_eq!(video.audio_bitrate, "96k");
        assert_eq!(video.video_codec, VideoCodec::H265);
        assert_eq!(video.audio_codec, AudioCodec::Opus);
        assert_eq!(video.fit, FitMode::Cover);
        assert_eq!(video.max_fps, Some(30.0));

        let image = &rule.images[0];
        assert_eq!(image.format, ImageFormat::Webp);
        assert_eq!(image.quality.as_deref(), Some("80"));
        assert_eq!(image.fit, FitMode::Cover);
        assert_eq!(image.output_pixel_format().as_deref(), Some("gray"));
        assert_eq!(profile.variant_prefix, "derived");
        assert!((profile.min_variant_ratio - 0.5).abs() < f64::EPSILON);

        // And the behaviour those filters describe actually holds.
        assert!(rule.accepts("clips/a.mkv"));
        assert!(!rule.accepts("private/clips/a.mkv"));
        assert!(!rule.accepts("clips/a.mp4"));
    }

    #[test]
    fn glob_matching_handles_stars_and_double_stars() {
        // A single `*` is scoped to one path segment, like a shell glob.
        assert!(glob_match("*.png", "a.png"));
        assert!(!glob_match("*.png", "dir/a.png"));
        assert!(glob_match("media/*.png", "media/a.png"));
        assert!(!glob_match("media/*.png", "media/2024/a.png"));
        assert!(glob_match("*", "single"));
        assert!(!glob_match("*", "anything/at/all"));

        // `**` crosses separators.
        assert!(glob_match("**", "anything/at/all"));
        assert!(glob_match("**/thumb.png", "a/b/c/thumb.png"));
        assert!(glob_match("**/thumb.png", "thumb.png"));

        // `**/` matches whole segments, including none.
        assert!(glob_match("media/**/*.png", "media/a.png"));
        assert!(glob_match("media/**/*.png", "media/2024/a.png"));
        assert!(glob_match("media/**/*.png", "media/2024/03/a.png"));
        assert!(!glob_match("media/**/*.png", "other/a.png"));
        assert!(!glob_match("media/**/*.png", "media/a.jpg"));

        assert!(glob_match("a?c.png", "abc.png"));
        assert!(!glob_match("a?c.png", "ac.png"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
    }

    #[test]
    fn admission_requires_enabled_size_key_and_a_claiming_rule() {
        let mut profile = MediaProfile::default();
        assert!(profile.accepts("media/a.mp4", 10_000));

        profile.enabled = false;
        assert!(!profile.accepts("media/a.mp4", 10_000));
        profile.enabled = true;

        assert!(!profile.accepts("media/a.mp4", 16), "too small to decode");
        assert!(!profile.accepts("media/a.mp4", u64::MAX), "too large to be worth decoding");

        // A catch-all rule claims every key, but a rule restricted to video
        // extensions must not swallow unrelated uploads.
        profile.rules = vec![MediaRule {
            include_suffixes: vec!["mp4".to_owned(), "mkv".to_owned()],
            videos: vec![VideoVariant {
                name: "720p".to_owned(),
                max_height: Some(720),
                ..VideoVariant::default()
            }],
            ..MediaRule::default()
        }];
        assert!(!profile.accepts("docs/a.pdf", 10_000));
        assert!(profile.accepts("docs/a.mp4", 10_000));
    }

    #[test]
    fn a_variant_only_counts_as_useful_when_it_saves_space() {
        // A 10 MB source re-encoded to 1 MB is worth storing.
        assert!(is_useful_variant(10_000_000, 1_000_000, 0.9, false));
        // Re-encoding that made it bigger is noise, not a rendition.
        assert!(!is_useful_variant(1_000_000, 1_200_000, 0.9, false));
        assert!(!is_useful_variant(1_000_000, 1_000_000, 0.9, false));
        // An unknown source size must not block the write.
        assert!(is_useful_variant(0, 5_000, 0.9, false));
        // A rendition that exists to convert keeps its place even when larger:
        // an HEVC source needs an H.264 copy whether or not it is smaller.
        assert!(is_useful_variant(1_000_000, 1_200_000, 0.9, true));
    }

    #[test]
    fn only_a_real_conversion_is_exempt_from_the_size_check() {
        let hevc_source = hevc(1920, 1080);
        let h264 = VariantKind::Video(VideoVariant {
            name: "1080p".to_owned(),
            ..VideoVariant::default()
        });
        assert!(is_conversion(&h264, &hevc_source, "mkv"));

        // An mp4 h264 source converted to an identical mp4 h264 rendition is
        // not a conversion — it is a copy, and a bigger copy is still not useful.
        let identity = VariantKind::Video(VideoVariant {
            name: "copy".to_owned(),
            ..VideoVariant::default()
        });
        assert!(!is_conversion(&identity, &video(1920, 1080), "mp4"));

        // A png turned into a webp is the whole point of an image host.
        let webp = VariantKind::Image(ImageVariant {
            name: "thumb".to_owned(),
            format: ImageFormat::Webp,
            ..ImageVariant::default()
        });
        assert!(is_conversion(&webp, &image(400, 300), "png"));
    }
}
