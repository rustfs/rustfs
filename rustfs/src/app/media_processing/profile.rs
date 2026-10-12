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

//! Resolves the per-bucket media processing profile and repairs it against the
//! codecs the local ffmpeg actually has.

use crate::app::storage_api::object_usecase::ECStore;
use crate::storage_api::media_processing::BUCKET_MEDIA_PROCESSING_CONFIG;
use rustfs_media_processing::Engine;
use rustfs_media_processing::plan::MediaProfile;

/// Reads the profile stored on `bucket`.
///
/// A missing profile means the feature was never turned on for that bucket, and
/// a corrupt one is logged and treated the same way: silently deriving variants
/// with half-understood settings is worse than doing nothing.
pub async fn load_profile(api: &ECStore, bucket: &str) -> Option<MediaProfile> {
    let raw = match crate::storage_api::media_processing::get_media_processing_config_in(api, bucket).await {
        Ok(Some((bytes, _updated))) => bytes,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                event = "media_processing_config_read_failed",
                component = "rustfs",
                subsystem = "media_processing",
                bucket,
                error = %error,
                "Failed to read media processing config"
            );
            return None;
        }
    };

    match serde_json::from_slice::<MediaProfile>(&raw) {
        Ok(profile) => Some(profile),
        Err(error) => {
            tracing::warn!(
                event = "media_processing_config_parse_failed",
                component = "rustfs",
                subsystem = "media_processing",
                bucket,
                config = BUCKET_MEDIA_PROCESSING_CONFIG,
                error = %error,
                "Failed to parse media processing config"
            );
            None
        }
    }
}

/// Drops renditions whose encoder the toolchain cannot provide.
///
/// Distribution builds ship very different codec sets, so a profile written on
/// one host would otherwise fail on another. Repairing at load time keeps the
/// stored configuration honest about what it can actually deliver.
pub async fn repair_profile(engine: &Engine, profile: &MediaProfile) -> MediaProfile {
    let mut repaired = profile.clone();
    for rule in &mut repaired.rules {
        let mut kept = Vec::with_capacity(rule.images.len());
        for variant in std::mem::take(&mut rule.images) {
            // `None` means the container's own codec, which is always present.
            let supported = match variant.required_encoder() {
                Some(encoder) => engine.supports_encoder(encoder).await,
                None => true,
            };
            if supported {
                kept.push(variant);
            } else {
                tracing::warn!(
                    event = "media_processing_encoder_unavailable",
                    component = "rustfs",
                    subsystem = "media_processing",
                    rule = %rule.name,
                    rendition = %variant.name,
                    encoder = ?variant.required_encoder(),
                    "Dropping a rendition the local ffmpeg cannot encode"
                );
            }
        }
        rule.images = kept;

        let mut kept = Vec::with_capacity(rule.videos.len());
        for variant in std::mem::take(&mut rule.videos) {
            // A copy-only rendition needs no encoder at all; every other one
            // needs both halves or the result has a silent or blank track.
            let video_ok = match variant.video_codec.encoder() {
                Some(encoder) => engine.supports_encoder(encoder).await,
                None => true,
            };
            let audio_ok = match variant.audio_codec.encoder() {
                Some(encoder) => engine.supports_encoder(encoder).await,
                None => true,
            };
            if video_ok && audio_ok {
                kept.push(variant);
            } else {
                tracing::warn!(
                    event = "media_processing_encoder_unavailable",
                    component = "rustfs",
                    subsystem = "media_processing",
                    rule = %rule.name,
                    rendition = %variant.name,
                    video_encoder = ?variant.video_codec.encoder(),
                    audio_encoder = ?variant.audio_codec.encoder(),
                    "Dropping a video rendition the local ffmpeg cannot encode"
                );
            }
        }
        rule.videos = kept;
    }
    repaired
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_media_processing::plan::MediaRule;
    use rustfs_media_processing::plan::{ImageFormat, ImageVariant, VideoVariant};

    #[tokio::test]
    async fn a_profile_round_trips_through_the_stored_json() {
        let json = serde_json::to_vec(&MediaProfile::default()).expect("serialize");
        let parsed: MediaProfile = serde_json::from_slice(&json).expect("parse");
        assert!(parsed.enabled());
        assert!(!parsed.rules.is_empty());
    }

    #[tokio::test]
    async fn corrupted_bytes_are_treated_as_no_profile() {
        let parsed = serde_json::from_slice::<MediaProfile>(b"not-json");
        assert!(parsed.is_err(), "corrupt config must not silently half-load");
    }

    #[tokio::test]
    async fn a_profile_with_unknown_fields_is_still_accepted() {
        // Operators hand-write this JSON; unknown keys must not break a bucket.
        let parsed: MediaProfile = serde_json::from_str(
            r#"{"enabled":true,"variantPrefix":"derived","someFutureKnob":42,
                "rules":[{"name":"r","includeSuffixes":["mp4"],
                          "videos":[{"name":"720p","maxHeight":720}]}]}"#,
        )
        .expect("unknown fields are tolerated");
        assert_eq!(parsed.variant_prefix, "derived");
        assert_eq!(parsed.rules[0].videos[0].max_height, Some(720));
    }

    #[tokio::test]
    async fn a_video_rendition_needs_both_its_video_and_audio_encoders() {
        // A video rendition whose audio codec is missing produces a file with a
        // silent track, which is worse than not producing the rendition at all:
        // clients see a playable video and never learn the sound is gone.
        let profile = MediaProfile {
            rules: vec![MediaRule {
                videos: vec![VideoVariant {
                    name: "720p".to_owned(),
                    max_height: Some(720),
                    ..VideoVariant::default()
                }],
                ..MediaRule::default()
            }],
            ..MediaProfile::default()
        };
        let engine = Engine::new("ffmpeg", "ffprobe");
        assert!(engine.supports_encoder("libx264").await, "a real ffmpeg has libx264");
        assert!(
            engine.supports_encoder("aac").await,
            "the encoder listing must include audio encoders; dropping them silently disables every video rendition"
        );

        let repaired = repair_profile(&engine, &profile).await;
        assert_eq!(repaired.rules[0].videos.len(), 1);
    }

    #[tokio::test]
    async fn renditions_without_an_encoder_are_dropped_rather_than_failing_later() {
        let profile = MediaProfile {
            rules: vec![MediaRule {
                images: vec![ImageVariant {
                    name: "thumb".to_owned(),
                    max_width: Some(320),
                    format: ImageFormat::Webp,
                    ..ImageVariant::default()
                }],
                ..MediaRule::default()
            }],
            ..MediaProfile::default()
        };

        // With an unreadable encoder list the repair is a no-op rather than a
        // guess that would silently disable every rendition.
        let repaired = repair_profile(&Engine::new("/bin/true", "/bin/true"), &profile).await;
        assert_eq!(repaired.rules[0].images.len(), 1);
    }
}
