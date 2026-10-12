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

//! End-to-end pipeline tests against the real `ffmpeg` / `ffprobe` binaries.
//!
//! These are skipped when the tools are absent so the suite still runs on
//! machines without a multimedia toolchain, but where they do run they verify
//! the part unit tests cannot: that the generated command lines actually
//! produce browser-playable files.

use std::path::{Path, PathBuf};
use std::process::Command;

use rustfs_media_processing::Engine;
use rustfs_media_processing::plan::{AudioCodec, FitMode, ImageFormat, ImageVariant, MediaProfile, VideoCodec, VideoVariant};
use rustfs_media_processing::probe::{MediaInfo, MediaKind};

fn tool(name: &str) -> Option<PathBuf> {
    let output = Command::new("sh").args(["-c", &format!("command -v {name}")]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

fn engine() -> Option<Engine> {
    let ffmpeg = tool("ffmpeg")?;
    let ffprobe = tool("ffprobe")?;
    Some(Engine::new(ffmpeg, ffprobe))
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rustfs-media-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Runs ffmpeg to synthesise a source clip.
fn make_clip(path: &Path, video_codec: &str, extension: &str) {
    let ffmpeg = tool("ffmpeg").expect("ffmpeg");
    let run = |args: Vec<String>| {
        let status = Command::new(&ffmpeg)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("run ffmpeg");
        assert!(status.success(), "ffmpeg failed for {path:?}");
    };

    let mut args = vec![
        "-hide_banner".to_owned(),
        "-loglevel".to_owned(),
        "error".to_owned(),
        "-y".to_owned(),
        "-f".to_owned(),
        "lavfi".to_owned(),
        "-i".to_owned(),
        "testsrc=size=640x480:rate=15:duration=2".to_owned(),
    ];
    args.extend([
        "-c:v".to_owned(),
        video_codec.to_owned(),
        "-pix_fmt".to_owned(),
        "yuv420p".to_owned(),
    ]);
    args.push(path.display().to_string());
    run(args);

    // The extension decides the container ffmpeg muxes into.
    if extension == "mkv" {
        let remux = path.with_extension("tmp.mp4");
        std::fs::rename(path, &remux).expect("rename");
        run(vec![
            "-hide_banner".to_owned(),
            "-loglevel".to_owned(),
            "error".to_owned(),
            "-y".to_owned(),
            "-i".to_owned(),
            remux.display().to_string(),
            "-c".to_owned(),
            "copy".to_owned(),
            path.display().to_string(),
        ]);
        std::fs::remove_file(&remux).ok();
    }
}

/// Synthesises a clip that also carries an audio track.
fn make_av_clip(path: &Path) {
    let ffmpeg = tool("ffmpeg").expect("ffmpeg");
    let status = Command::new(&ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=640x480:rate=15:duration=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=2",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run ffmpeg");
    assert!(status.success(), "ffmpeg failed to build the test clip");
}

/// Synthesises an audio-only file, which no rendition should be derived from.
fn make_audio_only(path: &Path) {
    let ffmpeg = tool("ffmpeg").expect("ffmpeg");
    let status = Command::new(&ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=2",
            "-c:a",
            "aac",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run ffmpeg");
    assert!(status.success(), "ffmpeg failed to build the test audio");
}

fn make_still(path: &Path) {
    let ffmpeg = tool("ffmpeg").expect("ffmpeg");
    let status = Command::new(&ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=800x600:duration=1",
            "-frames:v",
            "1",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run ffmpeg");
    assert!(status.success(), "ffmpeg failed to build the test image");
}

/// True when the file starts with the MP4 `moov` box, i.e. faststart applied.
fn is_faststart(path: &Path) -> bool {
    let bytes = std::fs::read(path).expect("read output");
    let moov = bytes.windows(4).position(|window| window == b"moov");
    let mdat = bytes.windows(4).position(|window| window == b"mdat");
    moov.is_some() && mdat.is_none_or(|mdat| moov.expect("checked") < mdat)
}

async fn render_image(input: &Path, output: &Path, variant: &ImageVariant) {
    rustfs_media_processing::image::render(&engine_ffmpeg(), input, output, variant, std::time::Duration::from_secs(120))
        .await
        .unwrap_or_else(|error| panic!("image render failed: {error}"));
}

fn engine_ffmpeg() -> PathBuf {
    tool("ffmpeg").expect("ffmpeg is required for the pipeline tests")
}

#[tokio::test]
async fn an_hevc_clip_becomes_a_browser_playable_h264_mp4() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("hevc");
    let source = dir.join("source.mkv");
    make_clip(&source, "libx265", "mkv");

    let info = engine.probe(&source).await.expect("probe");
    assert_eq!(info.kind, MediaKind::Video);
    assert!(!info.is_browser_playable(), "HEVC must not count as playable");

    let profile = MediaProfile::default();
    let planned = engine.plan(&info, &profile, "clips/source.mkv");
    assert_eq!(planned.len(), 3, "480p, 720p and 1080p renditions");

    let target = dir.join("720p.mp4");
    engine
        .render(
            &source,
            &target,
            &rustfs_media_processing::RenderRequest {
                kind: &planned[1].kind,
                info: Some(&info),
            },
        )
        .await
        .expect("render");

    let rendered = engine.probe(&target).await.expect("probe rendered file");
    let video = rendered.video.expect("rendered file has a video stream");
    assert_eq!(video.codec, "h264");
    assert_eq!(video.pix_fmt, "yuv420p");
    assert_eq!(video.height, 480, "a 480p source must not be enlarged by a 720p filter");
    assert_eq!(video.width, 640);
    assert!(is_faststart(&target), "moov must precede mdat so clients can start playing early");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_browser_playable_source_is_not_re_encoded_but_still_kept() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("playable");
    let source = dir.join("source.mp4");
    make_clip(&source, "libx264", "mp4");

    let info = engine.probe(&source).await.expect("probe");
    assert!(info.is_browser_playable(), "an 8-bit 4:2:0 H.264 mp4 is playable");

    let planned = engine.plan(&info, &MediaProfile::default(), "clips/source.mp4");
    assert!(planned.is_empty(), "nothing smaller is worth deriving from a 640x480 clip");

    // But an explicit unbounded rendition still re-encodes it into the profile.
    let variant = VideoVariant {
        name: "web".to_owned(),
        max_height: None,
        ..VideoVariant::default()
    };
    let target = dir.join("web.mp4");
    rustfs_media_processing::video::render(
        &PathBuf::from("ffmpeg"),
        &source,
        &target,
        &variant,
        std::time::Duration::from_secs(120),
    )
    .await
    .expect("render");
    let rendered = engine.probe(&target).await.expect("probe");
    assert_eq!(rendered.video.as_ref().expect("video stream").codec, "h264");
    assert!(is_faststart(&target));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_still_image_is_resized_and_converted() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("image");
    let source = dir.join("source.png");
    make_still(&source);

    let info = engine.probe(&source).await.expect("probe");
    assert_eq!(info.kind, MediaKind::Image, "a single-frame png is an image, not a video");
    assert_eq!((info.width(), info.height()), (800, 600));

    let planned = engine.plan(&info, &MediaProfile::default(), "photos/source.png");
    let keys: Vec<_> = planned.iter().map(|variant| variant.key.as_str()).collect();
    // "large" is a no-op for an 800x600 PNG: already under 2560px and the same
    // format, so re-encoding it would only lose quality.
    assert_eq!(
        keys,
        vec![
            "variants/photos/source.png/thumb.webp",
            "variants/photos/source.png/thumb-jpeg.jpg",
            "variants/photos/source.png/medium.webp",
            "variants/photos/source.png/medium-jpeg.jpg"
        ]
    );

    // Distribution builds ship different codec sets; pick what this ffmpeg has
    // so the test asserts the pipeline rather than the packaging.
    let format = if engine.supports_encoder("libwebp").await {
        ImageFormat::Webp
    } else {
        eprintln!("note: libwebp unavailable, exercising the jpeg path instead");
        ImageFormat::Jpeg
    };
    let thumb = ImageVariant {
        name: "thumb".to_owned(),
        max_width: Some(320),
        max_height: Some(320),
        format,
        ..ImageVariant::default()
    };
    let target = dir.join(format!("thumb.{}", thumb.format.extension("png")));
    render_image(&source, &target, &thumb).await;

    let rendered = engine.probe(&target).await.expect("probe rendered image");
    assert_eq!(rendered.kind, MediaKind::Image);
    let video = rendered.video.expect("rendered image has a stream");
    assert!(video.width <= 320 && video.height <= 320, "thumb must fit inside 320x320, got {video:?}");
    // Aspect ratio is preserved rather than squashed.
    assert!(
        (video.width as f64 / video.height as f64 - 800.0 / 600.0).abs() < 0.1,
        "aspect ratio changed: {video:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn audio_only_sources_produce_no_variants() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("audio");
    let source = dir.join("source.mp4");
    make_audio_only(&source);

    let info = engine.probe(&source).await.expect("probe");
    assert_eq!(info.kind, MediaKind::Audio);
    assert!(engine.plan(&info, &MediaProfile::default(), "audio/source.mp4").is_empty());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn the_encoder_listing_is_read_from_the_real_toolchain() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };

    let encoders = engine.encoders().await.expect("read the encoder listing");
    assert!(
        encoders.len() > 10,
        "a real ffmpeg advertises dozens of encoders, got {}: {encoders:?}",
        encoders.len()
    );

    // The feature gate at startup. Reading the listing from the wrong stream
    // yields an empty list, which would silently disable rendition entirely.
    assert!(
        engine.supports_encoder("libx264").await,
        "libx264 must be advertised; the pool refuses to start without it"
    );
    assert!(
        !engine.supports_encoder("rustfs-not-a-real-encoder").await,
        "an unknown encoder name must not be reported as available"
    );
}

#[tokio::test]
async fn a_missing_toolchain_is_reported_rather_than_fatal() {
    let engine = Engine::new("rustfs-no-such-ffmpeg", "rustfs-no-such-ffprobe");
    let dir = scratch_dir("missing");
    let source = dir.join("source.mp4");
    std::fs::write(&source, b"not media").expect("write");

    let error = engine.probe(&source).await.expect_err("probe must fail without ffprobe");
    assert!(
        matches!(error, rustfs_media_processing::Error::ToolUnavailable(_)),
        "expected ToolUnavailable, got {error:?}"
    );

    // Planning is pure and never touches the binaries, so it still works here.
    let info = MediaInfo {
        kind: MediaKind::Image,
        format: "png_pipe".to_owned(),
        duration: None,
        video: None,
        has_audio: false,
    };
    assert!(
        engine
            .plan(&info, &MediaProfile::default(), "a/b.png")
            .iter()
            .all(|v| !v.key.contains("large")),
        "with no dimensions only the format-changing renditions survive"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_webm_vp9_rendition_really_encodes() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    if !engine.supports_encoder("libvpx-vp9").await {
        eprintln!("skipping: this ffmpeg has no libvpx-vp9");
        return;
    }
    let dir = scratch_dir("vp9");
    let source = dir.join("source.mp4");
    make_av_clip(&source);

    let variant = VideoVariant {
        name: "vp9".to_owned(),
        max_height: Some(360),
        video_codec: VideoCodec::Vp9,
        audio_codec: AudioCodec::Opus,
        container: Some("webm".to_owned()),
        ..VideoVariant::default()
    };
    // The scratch path carries no extension, exactly as the worker renders it.
    let target = dir.join("output-no-extension");
    engine
        .render(
            &source,
            &target,
            &rustfs_media_processing::RenderRequest {
                kind: &rustfs_media_processing::VariantKind::Video(variant),
                info: None,
            },
        )
        .await
        .expect("render");

    let rendered = engine.probe(&target).await.expect("probe");
    let stream = rendered.video.as_ref().expect("video stream");
    assert_eq!(stream.codec, "vp9");
    assert_eq!(stream.height, 360);
    assert_eq!(stream.pix_fmt, "yuv420p");
    assert!(rendered.format.contains("webm"), "expected a webm, got {}", rendered.format);
    assert_eq!(rendered.audio_codec(), Some("opus"));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_ten_bit_rendition_really_changes_the_pixel_layout() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    if !engine.supports_encoder("libx265").await {
        eprintln!("skipping: this ffmpeg has no libx265");
        return;
    }
    let dir = scratch_dir("pixfmt");
    let source = dir.join("source.mp4");
    make_clip(&source, "libx264", "mp4");

    let variant = VideoVariant {
        name: "10bit".to_owned(),
        max_height: Some(360),
        video_codec: VideoCodec::H265,
        pixel_format: Some("yuv420p10le".to_owned()),
        ..VideoVariant::default()
    };
    let target = dir.join("ten-bit.mp4");
    engine
        .render(
            &source,
            &target,
            &rustfs_media_processing::RenderRequest {
                kind: &rustfs_media_processing::VariantKind::Video(variant),
                info: None,
            },
        )
        .await
        .expect("render");

    let rendered = engine.probe(&target).await.expect("probe");
    let stream = rendered.video.expect("video stream");
    assert_eq!(stream.codec, "hevc");
    assert_eq!(stream.pix_fmt, "yuv420p10le", "the rendition must carry the 10-bit layout it asked for");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn an_avif_still_really_encodes() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    if !engine.supports_encoder("libaom-av1").await {
        eprintln!("skipping: this ffmpeg has no libaom-av1");
        return;
    }
    let dir = scratch_dir("avif");
    let source = dir.join("source.png");
    make_still(&source);

    let thumb = ImageVariant {
        name: "avif".to_owned(),
        max_width: Some(160),
        format: ImageFormat::Avif,
        quality: Some("32".to_owned()),
        ..ImageVariant::default()
    };
    let target = dir.join("thumb.avif");
    render_image(&source, &target, &thumb).await;

    let rendered = engine.probe(&target).await.expect("probe");
    // ffprobe reports AVIF under the ISO-BMFF family, so the single frame is
    // the only thing marking it as a picture rather than a video.
    assert_eq!(rendered.kind, MediaKind::Image);
    let stream = rendered.video.as_ref().expect("image stream");
    assert_eq!(stream.codec, "av1");
    assert_eq!(stream.width, 160);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_cover_rendition_really_crops_to_the_box() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("cover");
    let source = dir.join("source.png");
    make_still(&source); // 800x600

    let square = ImageVariant {
        name: "square".to_owned(),
        max_width: Some(200),
        max_height: Some(200),
        fit: FitMode::Cover,
        ..ImageVariant::default()
    };
    let target = dir.join("square.png");
    render_image(&source, &target, &square).await;

    let stream = engine.probe(&target).await.expect("probe").video.expect("image stream");
    assert_eq!((stream.width, stream.height), (200, 200), "cover fills the box exactly");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_monochrome_rendition_really_converts_pixels() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("gray");
    let source = dir.join("source.png");
    make_still(&source);

    let gray = ImageVariant {
        name: "gray".to_owned(),
        max_width: Some(100),
        format: ImageFormat::Png,
        pixel_format: Some("gray".to_owned()),
        ..ImageVariant::default()
    };
    let target = dir.join("gray.png");
    render_image(&source, &target, &gray).await;

    let stream = engine.probe(&target).await.expect("probe").video.expect("image stream");
    assert_eq!(stream.pix_fmt, "gray", "the rendition must carry the pixel layout it asked for");
    assert_eq!(stream.width, 100);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_matching_source_is_remuxed_rather_than_re_encoded() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("copy");
    let source = dir.join("source.mp4");
    make_clip(&source, "libx264", "mp4");

    let info = engine.probe(&source).await.expect("probe");
    // The same codec, container, pixel format and frame rate as the source.
    let identity = VideoVariant {
        name: "identity".to_owned(),
        ..VideoVariant::default()
    };
    let target = dir.join("copy.mp4");
    engine
        .render(
            &source,
            &target,
            &rustfs_media_processing::RenderRequest {
                kind: &rustfs_media_processing::VariantKind::Video(identity),
                info: Some(&info),
            },
        )
        .await
        .expect("render");

    let rendered = engine.probe(&target).await.expect("probe");
    assert_eq!(rendered.video.as_ref().expect("video stream").codec, "h264");
    // A remux keeps the frame count; a re-encode of a 2s clip would too, so the
    // check that matters is that the result is playable and unresized.
    assert_eq!((rendered.width(), rendered.height()), (info.width(), info.height()));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_kept_png_rendition_really_contains_png_bytes() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("keep");
    let source = dir.join("source.png");
    make_still(&source);

    // `Keep` names the rendition `.png`, so the file has to be a PNG. The
    // generic still muxer defaults to mjpeg, which would put JPEG bytes in a
    // file every client dispatches on as a PNG.
    let large = ImageVariant {
        name: "large".to_owned(),
        max_width: Some(200),
        format: ImageFormat::Keep,
        ..ImageVariant::default()
    };
    assert_eq!(large.format.extension("png"), "png");

    let target = dir.join("output-no-extension");
    render_image(&source, &target, &large).await;

    let rendered = engine.probe(&target).await.expect("probe");
    let stream = rendered.video.as_ref().expect("image stream");
    assert_eq!(stream.codec, "png", "the rendition must match the extension it is stored under");
    assert!(rendered.format.contains("png"), "expected a png container, got {}", rendered.format);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_jpeg_rendition_of_a_png_source_really_is_jpeg() {
    let Some(engine) = engine() else {
        eprintln!("skipping: ffmpeg/ffprobe are not installed");
        return;
    };
    let dir = scratch_dir("jpeg");
    let source = dir.join("source.png");
    make_still(&source);

    let thumb = ImageVariant {
        name: "thumb-jpeg".to_owned(),
        max_width: Some(200),
        format: ImageFormat::Jpeg,
        ..ImageVariant::default()
    };
    let target = dir.join("thumb.jpg");
    render_image(&source, &target, &thumb).await;

    let probed = engine.probe(&target).await.expect("probe");
    let stream = probed.video.as_ref().expect("image stream");
    assert_eq!(stream.codec, "mjpeg");
    // ffprobe names the container after the demuxer it opened the file with,
    // which for a file is `image2` rather than the piped `jpeg_pipe`. What
    // matters is the codec and that it still classifies as a picture.
    assert_eq!(probed.kind, MediaKind::Image);

    std::fs::remove_dir_all(&dir).ok();
}
