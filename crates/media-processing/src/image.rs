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

//! Image renditions: resize, re-encode and re-pixel a still into a stored
//! variant.

use std::path::Path;
use std::time::Duration;

use crate::plan::{ImageFormat, ImageVariant};
use crate::{Engine, Result};

/// Renders `input` into `output` according to `variant`.
///
/// ffmpeg is used rather than a native image crate so images and videos share
/// one codec pipeline: an operator installs one toolchain, and a source format
/// that is already in the container plays (or displays) still gets resized.
pub async fn render(ffmpeg: &Path, input: &Path, output: &Path, variant: &ImageVariant, timeout: Duration) -> Result<()> {
    // Only ffmpeg is invoked here, so the probe slot stays unfilled.
    let engine = Engine::new(ffmpeg, ffmpeg).with_timeout(timeout);
    render_with_engine(&engine, input, output, variant).await
}

pub(crate) async fn render_with_engine(engine: &Engine, input: &Path, output: &Path, variant: &ImageVariant) -> Result<()> {
    engine.run_ffmpeg(&encode_args(input, output, variant)).await?;
    Ok(())
}

/// Builds the ffmpeg invocation for one rendition.
#[must_use]
pub fn encode_args(input: &Path, output: &Path, variant: &ImageVariant) -> Vec<String> {
    let source_extension = input.extension().and_then(|extension| extension.to_str()).unwrap_or_default();
    let mut args: Vec<String> = vec![
        "-hide_banner".into(),
        "-nostdin".into(),
        "-y".into(),
        "-i".into(),
        input.display().to_string(),
    ];

    // A still image is one frame; asking for more would duplicate it.
    args.extend(["-frames:v".to_owned(), "1".to_owned()]);

    let filters = variant.filters();
    if !filters.is_empty() {
        args.extend(["-vf".to_owned(), filters]);
    }
    if let Some(encoder) = variant.encoder(source_extension) {
        args.extend(["-c:v".to_owned(), encoder.to_owned()]);
    }
    args.extend(variant.quality_args(source_extension));

    // Strip the container metadata that would otherwise be copied verbatim.
    args.extend(["-map_metadata".to_owned(), "-1".to_owned()]);
    // The output path is a scratch file whose extension says nothing useful,
    // and ffmpeg refuses to guess a muxer from a bare name.
    args.extend(["-f".to_owned(), variant.muxer(source_extension).to_owned()]);
    args.push(output.display().to_string());
    args
}

/// Convenience wrapper mirroring [`render`] but using the engine's configured
/// timeout for both tools.
pub async fn render_with(engine: &Engine, input: &Path, output: &Path, variant: &ImageVariant) -> Result<()> {
    render_with_engine(engine, input, output, variant).await
}

#[must_use]
pub fn extension_for(format: ImageFormat, source_extension: &str) -> String {
    format.extension(source_extension)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::FitMode;

    fn variant() -> ImageVariant {
        ImageVariant::default()
    }

    #[test]
    fn every_still_format_names_the_encoder_matching_its_container() {
        assert_eq!(ImageFormat::Jpeg.default_encoder(), Some("mjpeg"));
        assert_eq!(ImageFormat::Png.default_encoder(), Some("png"));
        assert_eq!(ImageFormat::Webp.default_encoder(), Some("libwebp"));
        assert_eq!(ImageFormat::Avif.default_encoder(), Some("libaom-av1"));
        assert_eq!(ImageFormat::Tiff.default_encoder(), Some("tiff"));
        assert_eq!(ImageFormat::Bmp.default_encoder(), Some("bmp"));
        assert_eq!(ImageFormat::Gif.default_encoder(), Some("gif"));
        // `Keep` is the only one that waits to see what the source is.
        assert_eq!(ImageFormat::Keep.default_encoder(), None);
    }

    #[test]
    fn a_kept_rendition_encodes_into_the_format_it_is_named_for() {
        // The generic still muxer defaults to mjpeg whatever the source was, so
        // an unnamed encoder writes JPEG bytes into a file named `.png`.
        assert_eq!(ImageFormat::source_encoder("png"), "png");
        assert_eq!(ImageFormat::source_encoder(".PNG"), "png");
        assert_eq!(ImageFormat::source_encoder("bmp"), "bmp");
        assert_eq!(ImageFormat::source_encoder("tiff"), "tiff");
        assert_eq!(ImageFormat::source_encoder("gif"), "gif");
        assert_eq!(ImageFormat::source_encoder("webp"), "libwebp");
        assert_eq!(ImageFormat::source_encoder("avif"), "libaom-av1");
        assert_eq!(ImageFormat::source_encoder("jpg"), "mjpeg");
        // Anything unrecognised falls back to the codec the muxer defaults to.
        assert_eq!(ImageFormat::source_encoder("xyz"), "mjpeg");

        let keep = ImageVariant {
            name: "large".to_owned(),
            format: ImageFormat::Keep,
            ..ImageVariant::default()
        };
        assert_eq!(keep.encoder("png"), Some("png"));
        let args = encode_args(Path::new("/in/a.png"), Path::new("/out"), &keep);
        let joined = args.join(" ");
        assert!(joined.contains("-c:v png"), "{joined}");
    }

    #[test]
    fn an_explicit_encoder_overrides_the_format_default() {
        let variant = ImageVariant {
            name: "thumb".to_owned(),
            format: ImageFormat::Webp,
            encoder: Some("webp".to_owned()),
            ..ImageVariant::default()
        };
        assert_eq!(variant.encoder("png"), Some("webp"));
    }

    #[test]
    fn quality_uses_the_flag_that_matches_the_target() {
        let jpeg = ImageVariant {
            name: "j".to_owned(),
            format: ImageFormat::Jpeg,
            quality: Some("4".to_owned()),
            ..ImageVariant::default()
        };
        assert_eq!(jpeg.quality_args("png"), vec!["-q:v".to_owned(), "4".to_owned()]);

        let webp = ImageVariant {
            name: "w".to_owned(),
            format: ImageFormat::Webp,
            quality: Some("80".to_owned()),
            ..ImageVariant::default()
        };
        assert_eq!(webp.quality_args("png"), vec!["-quality".to_owned(), "80".to_owned()]);

        let avif = ImageVariant {
            name: "a".to_owned(),
            format: ImageFormat::Avif,
            quality: Some("30".to_owned()),
            ..ImageVariant::default()
        };
        assert_eq!(
            avif.quality_args("png"),
            vec!["-crf".to_owned(), "30".to_owned(), "-b:v".to_owned(), "0".to_owned()],
            "constant-quality AV1 needs the bitrate pinned or the crf is ignored"
        );

        assert!(variant().quality_args("png").is_empty());
    }

    #[test]
    fn a_target_without_a_quality_knob_is_not_handed_one() {
        // bmp and gif have no quality option; passing one aborts the render
        // rather than being ignored.
        let bmp = ImageVariant {
            name: "b".to_owned(),
            format: ImageFormat::Bmp,
            quality: Some("5".to_owned()),
            ..ImageVariant::default()
        };
        assert!(bmp.quality_args("png").is_empty());
    }

    #[test]
    fn extensions_follow_the_requested_format() {
        assert_eq!(extension_for(ImageFormat::Keep, "PNG"), "png");
        assert_eq!(extension_for(ImageFormat::Jpeg, ".png"), "jpg");
        assert_eq!(extension_for(ImageFormat::Webp, "png"), "webp");
        assert_eq!(extension_for(ImageFormat::Avif, "png"), "avif");
        assert_eq!(extension_for(ImageFormat::Gif, "png"), "gif");
    }

    #[test]
    fn the_muxer_is_named_so_a_scratch_path_needs_no_extension() {
        // The output file the worker renders into is called `output-<hash>`;
        // without an explicit `-f` ffmpeg cannot pick a muxer and the render
        // fails before it starts.
        let args = encode_args(Path::new("/in/source.png"), Path::new("/scratch/output-ab12"), &variant());
        let joined = args.join(" ");
        assert!(joined.contains("-f image2"), "{joined}");
        assert!(joined.ends_with("/scratch/output-ab12"), "{joined}");

        let webp = ImageVariant {
            name: "w".to_owned(),
            format: ImageFormat::Webp,
            ..ImageVariant::default()
        };
        // webp and avif have dedicated muxers; the generic image2 muxer cannot
        // carry them.
        let args = encode_args(Path::new("/in/source.png"), Path::new("/scratch/out"), &webp).join(" ");
        assert!(args.contains("-f webp"), "{args}");
        let avif = ImageVariant {
            name: "a".to_owned(),
            format: ImageFormat::Avif,
            ..ImageVariant::default()
        };
        assert!(
            encode_args(Path::new("/in/x.png"), Path::new("/scratch/out"), &avif)
                .join(" ")
                .contains("-f avif")
        );
    }

    #[test]
    fn a_still_renders_as_exactly_one_frame() {
        let args = encode_args(Path::new("/in/a.png"), Path::new("/out"), &variant());
        let joined = args.join(" ");
        assert!(joined.contains("-frames:v 1"), "{joined}");
        assert!(joined.contains("-map_metadata -1"), "{joined}");
    }

    #[test]
    fn a_pixel_format_request_becomes_a_format_filter() {
        let gray = ImageVariant {
            name: "gray".to_owned(),
            max_width: Some(64),
            pixel_format: Some("GRAY".to_owned()),
            ..ImageVariant::default()
        };
        let args = encode_args(Path::new("/in/a.png"), Path::new("/out"), &gray);
        let filter = args.join(" ").contains("format=gray").then_some(true);
        assert_eq!(filter, Some(true), "the pixel format is normalised to lowercase");
        // Scale and format chain in one pass rather than two -vf flags.
        assert_eq!(args.iter().filter(|arg| *arg == "-vf").count(), 1);
    }

    #[test]
    fn cover_fills_the_box_and_crops_the_overflow() {
        let cover = ImageVariant {
            name: "square".to_owned(),
            max_width: Some(200),
            max_height: Some(200),
            fit: FitMode::Cover,
            ..ImageVariant::default()
        };
        let joined = encode_args(Path::new("/in/a.png"), Path::new("/out"), &cover).join(" ");
        assert!(joined.contains("force_original_aspect_ratio=increase"), "{joined}");
        assert!(joined.contains("crop=200:200"), "{joined}");
    }

    #[test]
    fn contain_never_upscales_past_the_bound() {
        let contain = ImageVariant {
            name: "thumb".to_owned(),
            max_width: Some(320),
            max_height: Some(320),
            ..ImageVariant::default()
        };
        let joined = encode_args(Path::new("/in/a.png"), Path::new("/out"), &contain).join(" ");
        assert!(joined.contains("force_original_aspect_ratio=decrease"), "{joined}");
        assert!(!joined.contains("crop="), "contain must not crop: {joined}");
    }

    #[test]
    fn fill_stretches_without_touching_the_aspect_ratio() {
        let fill = ImageVariant {
            name: "banner".to_owned(),
            max_width: Some(1200),
            max_height: Some(300),
            fit: FitMode::Fill,
            ..ImageVariant::default()
        };
        let joined = encode_args(Path::new("/in/a.png"), Path::new("/out"), &fill).join(" ");
        assert!(joined.contains("scale=w=1200:h=300"), "{joined}");
        assert!(!joined.contains("force_original_aspect_ratio"), "{joined}");
    }
}
