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

//! Video renditions: re-encode, re-pixel and remux to a browser-playable
//! resolution.

use std::path::Path;
use std::time::Duration;

use crate::plan::{AudioCodec, FitMode, VideoCodec, VideoVariant};
use crate::probe::MediaInfo;
use crate::{Engine, Result};

pub async fn render(ffmpeg: &Path, input: &Path, output: &Path, variant: &VideoVariant, timeout: Duration) -> Result<()> {
    let engine = Engine::new(ffmpeg, ffmpeg).with_timeout(timeout);
    render_with_engine(&engine, input, output, variant, None).await
}

pub(crate) async fn render_with_engine(
    engine: &Engine,
    input: &Path,
    output: &Path,
    variant: &VideoVariant,
    info: Option<&MediaInfo>,
) -> Result<()> {
    let args = encode_args(input, output, variant, info);
    engine.run_ffmpeg(&args).await?;
    Ok(())
}

/// Builds the ffmpeg invocation for one rendition.
///
/// `info` enables the stream-copy fast path: when the source already carries
/// exactly what the rendition asks for, the video is remuxed instead of
/// re-encoded, which is orders of magnitude cheaper. Without it the caller pays
/// for a full encode.
#[must_use]
pub fn encode_args(input: &Path, output: &Path, variant: &VideoVariant, info: Option<&MediaInfo>) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-hide_banner".into(),
        "-nostdin".into(),
        "-y".into(),
        "-i".into(),
        input.display().to_string(),
    ];

    // Only the first video and audio track survive; extra renditions cost
    // encode time and nothing plays them.
    args.extend(["-map".to_owned(), "0:v:0?".to_owned()]);
    if variant.audio_codec == AudioCodec::None {
        args.extend(["-an".to_owned()]);
    } else {
        args.extend(["-map".to_owned(), "0:a:0?".to_owned()]);
    }

    // Carry rotation and title through so phone footage is not sideways.
    args.extend(["-map_metadata".to_owned(), "0".to_owned()]);

    let copy = can_stream_copy(variant, info);
    if copy {
        args.extend(["-c".to_owned(), "copy".to_owned()]);
    } else {
        args.extend(encode_video_args(variant));
        args.extend(encode_audio_args(variant));
    }

    // Without +faststart the moov atom lands after the media data and clients
    // buffer the whole file before the first frame. Only the mp4 family has it.
    if matches!(variant.muxer(), "mp4" | "mov") {
        args.extend(["-movflags".to_owned(), "+faststart".to_owned()]);
    }
    // The output path is a scratch file whose extension says nothing useful,
    // and ffmpeg refuses to guess a muxer from a bare name.
    args.extend(["-f".to_owned(), variant.muxer().to_owned()]);
    args.push(output.display().to_string());
    args
}

fn encode_video_args(variant: &VideoVariant) -> Vec<String> {
    let mut args = Vec::new();

    // Pixel format is named explicitly: every encoder has a default, but the
    // defaults differ, and a rendition that asked for one must get it even when
    // the source already matches (the source has to be re-encoded to change it).
    if let Some(pixel_format) = variant.output_pixel_format() {
        args.extend(["-pix_fmt".to_owned(), pixel_format]);
    }

    let filter = scale_filter(variant);
    if !filter.is_empty() {
        args.extend(["-vf".to_owned(), filter]);
    }
    if let Some(bound) = variant.max_fps.filter(|bound| bound.is_finite() && *bound > 0.0) {
        args.extend(["-r".to_owned(), format!("{bound}")]);
    }

    if let Some(encoder) = variant.video_codec.encoder() {
        args.extend(["-c:v".to_owned(), encoder.to_owned()]);
        if variant.video_codec.supports_crf() {
            args.extend(["-crf".to_owned(), variant.crf.to_string()]);
        }
        args.extend(variant.video_codec.speed_args(&variant.preset));
        // Every video client decodes 8-bit 4:2:0; anything else silently fails
        // on part of the audience, so a deeper format is opt-in per rendition
        // and the matching profile has to be named with it. VP9 and AV1 name
        // their own profiles and reject `-profile:v main10` outright.
        if matches!(variant.video_codec, VideoCodec::H264 | VideoCodec::H265)
            && variant.output_pixel_format().as_deref() == Some("yuv420p10le")
        {
            args.extend(["-profile:v".to_owned(), "main10".to_owned()]);
        }
    }
    args
}

fn encode_audio_args(variant: &VideoVariant) -> Vec<String> {
    let Some(encoder) = variant.audio_codec.encoder() else {
        return Vec::new();
    };
    let mut args = vec!["-c:a".to_owned(), encoder.to_owned()];
    if variant.audio_codec.accepts_bitrate() {
        args.extend(["-b:a".to_owned(), variant.audio_bitrate.clone()]);
    }
    args
}

fn scale_filter(variant: &VideoVariant) -> String {
    let algorithm = "bicubic";
    match (variant.max_width, variant.max_height) {
        (None, None) => String::new(),
        (Some(width), None) => format!("scale=w=min(iw\\,{width}):h=-2:flags={algorithm}"),
        (None, Some(height)) => format!("scale=w=-2:h=min(ih\\,{height}):flags={algorithm}"),
        (Some(width), Some(height)) => {
            let scale = match variant.fit {
                FitMode::Contain => {
                    format!("scale=w={width}:h={height}:flags={algorithm}:force_original_aspect_ratio=decrease")
                }
                FitMode::Cover => {
                    format!("scale=w={width}:h={height}:flags={algorithm}:force_original_aspect_ratio=increase")
                }
                FitMode::Fill => format!("scale=w={width}:h={height}:flags={algorithm}"),
            };
            if variant.fit.crops() {
                format!("{scale},crop={width}:{height}")
            } else {
                scale
            }
        }
    }
}

/// True when the source already matches the rendition and can be copied.
///
/// Copying is only safe when the rendition asks for nothing at all: any
/// rescale, codec change, pixel-format change or frame-rate change has to
/// actually re-encode, and silently producing an unscaled "rendition" is the
/// failure mode this guards.
#[must_use]
pub fn can_stream_copy(variant: &VideoVariant, info: Option<&MediaInfo>) -> bool {
    if variant.video_codec == VideoCodec::Copy {
        // An explicit request to remux. Checked before the probe result, since
        // a copy applies nothing that could fail.
        return true;
    }
    let Some(info) = info else { return false };
    let Some(video) = info.video.as_ref() else { return false };

    // A bound the source already satisfies needs no scale filter at all, which
    // is what makes a matching source worth copying rather than re-encoding.
    if variant.max_width.is_some_and(|width| info.width() != width)
        || variant.max_height.is_some_and(|height| info.height() != height)
        || variant.max_fps.is_some_and(|bound| info.fps().is_some_and(|fps| fps > bound))
    {
        return false;
    }
    if variant.audio_codec == AudioCodec::None {
        return false;
    }

    let codec_ok = match variant.video_codec {
        VideoCodec::H264 => video.codec == "h264" || video.codec == "avc1",
        VideoCodec::H265 => video.codec == "hevc" || video.codec == "h265",
        VideoCodec::Vp9 => video.codec == "vp9",
        VideoCodec::Av1 => video.codec == "av1",
        VideoCodec::Mpeg4 => video.codec == "mpeg4",
        VideoCodec::Copy => true,
    };
    if !codec_ok {
        return false;
    }

    // Copying carries the source's pixel layout across untouched, so it is only
    // safe when that layout is the one the rendition wants. An unset
    // `pixelFormat` still resolves to the codec's default, so a 10-bit source
    // copied into an 8-bit rendition would come out 10-bit and unplayable.
    if let Some(wanted) = variant.output_pixel_format() {
        let actual = match video.pix_fmt.trim() {
            // ffprobe's placeholder for a container that does not say.
            "" | "unknown" => None,
            // Full-range and studio-range 4:2:0 are the same layout to a decoder.
            "yuvj420p" => Some("yuv420p".to_owned()),
            other => Some(other.to_owned()),
        };
        if actual.is_some_and(|actual| actual != wanted) {
            return false;
        }
    }

    // The muxer has to accept the codecs already in the file.
    let muxer = variant.muxer();
    let container_ok = info
        .container_extensions()
        .iter()
        .any(|name| name.eq_ignore_ascii_case(muxer));
    if !container_ok {
        return false;
    }

    audio_ok(info, variant.audio_codec)
}

/// True when the source's audio already is what the rendition asks for.
///
/// Remuxing an mp4's AAC into a webm, or an mp3 into either, produces a file
/// that decodes audio as silence on some clients — so a mismatch forces a real
/// re-encode rather than a copy.
fn audio_ok(info: &MediaInfo, target: AudioCodec) -> bool {
    if target == AudioCodec::None {
        return !info.has_audio;
    }
    if !info.has_audio {
        // A silent source already satisfies any audio target: there is nothing
        // to add, so remuxing is correct and cheaper than re-encoding.
        return true;
    }
    match (info.audio_codec(), target) {
        (Some("aac"), AudioCodec::Aac) => true,
        (Some("opus"), AudioCodec::Opus) => true,
        (Some("vorbis"), AudioCodec::Vorbis) => true,
        (Some("mp3"), AudioCodec::Mp3) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::{MediaKind, VideoStream};

    fn info(height: u32, codec: &str, pix_fmt: &str, audio: Option<&str>) -> MediaInfo {
        MediaInfo {
            kind: MediaKind::Video,
            format: "mov,mp4,m4a,3gp,3g2,mj2".to_owned(),
            duration: None,
            video: Some(VideoStream {
                codec: codec.to_owned(),
                width: height * 16 / 9,
                height,
                pix_fmt: pix_fmt.to_owned(),
                fps: Some(30.0),
                audio_codec: audio.map(str::to_owned),
            }),
            has_audio: audio.is_some(),
        }
    }

    fn variant(max_height: Option<u32>) -> VideoVariant {
        VideoVariant {
            name: "720p".to_owned(),
            max_height,
            ..VideoVariant::default()
        }
    }

    #[test]
    fn encoding_uses_x264_aac_and_a_height_bound() {
        let args = encode_args(Path::new("/in.mkv"), Path::new("/out.mp4"), &variant(Some(720)), None);
        let joined = args.join(" ");

        assert!(joined.contains("-c:v libx264"), "{joined}");
        assert!(joined.contains("-pix_fmt yuv420p"), "{joined}");
        assert!(
            joined.contains("min(ih\\,720)"),
            "the height bound must clamp against the source: {joined}"
        );
        assert!(joined.contains("+faststart"), "{joined}");
        assert!(joined.contains("-c:a aac"), "{joined}");
        assert!(joined.contains("-map 0:v:0?"), "{joined}");
    }

    #[test]
    fn the_muxer_is_named_so_a_scratch_path_needs_no_extension() {
        let args = encode_args(Path::new("/in.mkv"), Path::new("/scratch/output-ab12"), &variant(Some(720)), None);
        let joined = args.join(" ");
        assert!(joined.contains("-f mp4"), "a rendition defaults to mp4 whatever the source was: {joined}");
        assert!(joined.ends_with("/scratch/output-ab12"), "{joined}");
    }

    #[test]
    fn a_matching_source_is_copied_instead_of_re_encoded() {
        let source = info(720, "h264", "yuv420p", None);
        assert!(can_stream_copy(&variant(Some(720)), Some(&source)));
    }

    #[test]
    fn copying_is_refused_when_the_result_would_still_be_unplayable() {
        // HEVC in an mp4 copies fine but no browser decodes it.
        assert!(!can_stream_copy(&variant(Some(720)), Some(&info(720, "hevc", "yuv420p", None))));
        // 10-bit H.264 is not universally decodable either.
        assert!(!can_stream_copy(&variant(Some(720)), Some(&info(720, "h264", "yuv420p10le", None))));
        // A source whose pixel layout is not the one the rendition resolves to
        // cannot be copied, even when the rendition names no format at all.
        assert!(!can_stream_copy(&variant(Some(720)), Some(&info(720, "h264", "yuv420p10le", None))));
        // Full-range 4:2:0 is the same layout and may be carried across.
        assert!(can_stream_copy(&variant(Some(720)), Some(&info(720, "h264", "yuvj420p", None))));
        // Wrong height, or no probe result at all.
        assert!(!can_stream_copy(&variant(Some(720)), Some(&info(1080, "h264", "yuv420p", None))));
        assert!(!can_stream_copy(&variant(Some(720)), None));
    }

    #[test]
    fn a_rescale_or_frame_rate_change_always_re_encodes() {
        let source = info(720, "h264", "yuv420p", None);
        // Matching height, but the rendition still has to shrink a wider source.
        let wide = VideoVariant {
            name: "wide".to_owned(),
            max_height: Some(720),
            // The source is 1280 wide, so 640 is a bound it does not meet.
            max_width: Some(640),
            ..VideoVariant::default()
        };
        assert!(!can_stream_copy(&wide, Some(&source)), "a width bound needs a real scale filter");

        // A bound the source already meets is exactly the case that makes a copy
        // worthwhile in the first place.
        let already = VideoVariant {
            name: "already".to_owned(),
            max_width: Some(1280),
            ..VideoVariant::default()
        };
        assert!(can_stream_copy(&already, Some(&source)), "a satisfied bound needs no filter");

        // The source is 30fps, so a 15fps bound is below it.
        let capped = VideoVariant {
            name: "15".to_owned(),
            max_fps: Some(15.0),
            ..VideoVariant::default()
        };
        assert!(!can_stream_copy(&capped, Some(&source)), "a frame-rate cap needs a real resample");

        // A bound the source already meets is a no-op, like a satisfied width.
        let kept = VideoVariant {
            name: "30".to_owned(),
            max_fps: Some(30.0),
            ..VideoVariant::default()
        };
        assert!(can_stream_copy(&kept, Some(&source)));

        let dropped_audio = VideoVariant {
            name: "silent".to_owned(),
            audio_codec: AudioCodec::None,
            ..VideoVariant::default()
        };
        assert!(!can_stream_copy(&dropped_audio, Some(&info(720, "h264", "yuv420p", Some("aac")))));
    }

    #[test]
    fn an_explicit_pixel_format_forces_a_re_encode() {
        let source = info(720, "h264", "yuv420p", None);
        let ten_bit = VideoVariant {
            name: "10bit".to_owned(),
            pixel_format: Some("yuv420p10le".to_owned()),
            ..VideoVariant::default()
        };
        assert!(!can_stream_copy(&ten_bit, Some(&source)));

        let gray = VideoVariant {
            name: "gray".to_owned(),
            pixel_format: Some("gray".to_owned()),
            ..VideoVariant::default()
        };
        assert!(!can_stream_copy(&gray, Some(&source)));
    }

    #[test]
    fn every_video_codec_renders_its_own_invocation() {
        for (codec, encoder) in [
            (VideoCodec::H264, "libx264"),
            (VideoCodec::H265, "libx265"),
            (VideoCodec::Vp9, "libvpx-vp9"),
            (VideoCodec::Av1, "libsvtav1"),
            (VideoCodec::Mpeg4, "mpeg4"),
        ] {
            let rendition = VideoVariant {
                name: "v".to_owned(),
                video_codec: codec,
                container: Some("webm".to_owned()),
                ..VideoVariant::default()
            };
            let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &rendition, None).join(" ");
            assert!(joined.contains(&format!("-c:v {encoder}")), "{codec:?}: {joined}");
            assert!(joined.contains("-f webm"), "{codec:?}: {joined}");
            assert!(joined.contains("-pix_fmt"), "{codec:?} must pin a pixel format: {joined}");
        }
    }

    #[test]
    fn the_main10_profile_is_only_named_for_the_codecs_that_have_it() {
        let ten_bit_h265 = VideoVariant {
            name: "10bit".to_owned(),
            video_codec: VideoCodec::H265,
            pixel_format: Some("yuv420p10le".to_owned()),
            ..VideoVariant::default()
        };
        assert!(
            encode_args(Path::new("/in.mp4"), Path::new("/out"), &ten_bit_h265, None)
                .join(" ")
                .contains("-profile:v main10")
        );

        // VP9 reaches 10-bit through the pixel format alone and rejects the
        // x264-style profile name.
        let ten_bit_vp9 = VideoVariant {
            name: "10bit".to_owned(),
            video_codec: VideoCodec::Vp9,
            pixel_format: Some("yuv420p10le".to_owned()),
            container: Some("webm".to_owned()),
            ..VideoVariant::default()
        };
        let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &ten_bit_vp9, None).join(" ");
        assert!(!joined.contains("-profile:v"), "{joined}");
        assert!(joined.contains("-pix_fmt yuv420p10le"), "{joined}");
    }

    #[test]
    fn a_preset_word_is_dropped_for_codecs_that_take_a_number() {
        // `veryfast` is x264 naming; libvpx would refuse to parse it and abort
        // the render, so it is not forwarded.
        let vp9 = VideoVariant {
            name: "v".to_owned(),
            video_codec: VideoCodec::Vp9,
            container: Some("webm".to_owned()),
            ..VideoVariant::default()
        };
        assert!(
            !encode_args(Path::new("/in.mp4"), Path::new("/out"), &vp9, None)
                .join(" ")
                .contains("cpu-used")
        );

        // A numeric speed level is what libvpx actually wants, and it passes.
        let numbered = VideoVariant {
            name: "v".to_owned(),
            video_codec: VideoCodec::Vp9,
            container: Some("webm".to_owned()),
            preset: "4".to_owned(),
            ..VideoVariant::default()
        };
        assert!(
            encode_args(Path::new("/in.mp4"), Path::new("/out"), &numbered, None)
                .join(" ")
                .contains("-cpu-used 4")
        );
    }

    #[test]
    fn dropping_the_audio_uses_an_for_removes_the_mapping() {
        let silent = VideoVariant {
            name: "silent".to_owned(),
            audio_codec: AudioCodec::None,
            ..VideoVariant::default()
        };
        let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &silent, None).join(" ");
        assert!(joined.contains("-an"), "{joined}");
        assert!(!joined.contains("0:a:0?"), "{joined}");
        assert!(!joined.contains("-c:a"), "{joined}");
    }

    #[test]
    fn the_audio_codec_drives_the_audio_encoder() {
        for (codec, encoder) in [
            (AudioCodec::Aac, "aac"),
            (AudioCodec::Opus, "libopus"),
            (AudioCodec::Mp3, "libmp3lame"),
            (AudioCodec::Vorbis, "libvorbis"),
        ] {
            let rendition = VideoVariant {
                name: "v".to_owned(),
                audio_codec: codec,
                ..VideoVariant::default()
            };
            let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &rendition, None).join(" ");
            assert!(joined.contains(&format!("-c:a {encoder}")), "{codec:?}: {joined}");
        }
    }

    #[test]
    fn audio_matches_are_decided_on_the_codec_actually_present() {
        // The source's audio is already what the rendition wants.
        assert!(audio_ok(&info(720, "h264", "yuv420p", Some("aac")), AudioCodec::Aac));
        // A different target forces a re-encode: an mp4's AAC cannot simply be
        // wrapped into a webm.
        assert!(!audio_ok(&info(720, "h264", "yuv420p", Some("aac")), AudioCodec::Opus));
        // A silent source satisfies any audio target — there is nothing to add.
        assert!(audio_ok(&info(720, "h264", "yuv420p", None), AudioCodec::Aac));
        // Dropping audio needs there to be some.
        assert!(!audio_ok(&info(720, "h264", "yuv420p", Some("aac")), AudioCodec::None));
        assert!(audio_ok(&info(720, "h264", "yuv420p", None), AudioCodec::None));
    }

    #[test]
    fn a_frame_rate_bound_is_passed_to_ffmpeg() {
        let capped = VideoVariant {
            name: "30".to_owned(),
            max_fps: Some(30.0),
            ..VideoVariant::default()
        };
        let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &capped, None).join(" ");
        assert!(joined.contains("-r 30"), "{joined}");

        // A nonsense bound must not reach ffmpeg as `-r NaN`.
        let broken = VideoVariant {
            name: "bad".to_owned(),
            max_fps: Some(f64::NAN),
            ..VideoVariant::default()
        };
        assert!(
            !encode_args(Path::new("/in.mp4"), Path::new("/out"), &broken, None)
                .join(" ")
                .contains("-r ")
        );
    }

    #[test]
    fn cover_fills_the_box_and_crops_the_overflow() {
        let cover = VideoVariant {
            name: "square".to_owned(),
            max_width: Some(200),
            max_height: Some(200),
            fit: FitMode::Cover,
            ..VideoVariant::default()
        };
        let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &cover, None).join(" ");
        assert!(joined.contains("force_original_aspect_ratio=increase"), "{joined}");
        assert!(joined.contains("crop=200:200"), "{joined}");
    }

    #[test]
    fn a_copy_rendition_forces_remuxing_regardless_of_the_source() {
        let copy = VideoVariant {
            name: "copy".to_owned(),
            video_codec: VideoCodec::Copy,
            container: Some("mkv".to_owned()),
            ..VideoVariant::default()
        };
        assert!(can_stream_copy(&copy, None), "an explicit copy needs no probe result");
        let joined = encode_args(Path::new("/in.mp4"), Path::new("/out"), &copy, None).join(" ");
        assert!(joined.contains("-c copy"), "{joined}");
        assert!(!joined.contains("-c:v"), "{joined}");
        assert!(joined.contains("-f matroska"), "{joined}");
    }
}
