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

//! Server-side QR rendering for TOTP enrollment.
//!
//! Rendered here, not in the clients, so there is one QR encoder for the whole
//! product instead of a JavaScript one in the console and a Rust one in the
//! CLI. The enrollment response carries both an SVG (for a browser `<img>`) and
//! Unicode block art (for a terminal), and the console needs no new npm
//! dependency to show a code.

use qrcode_rs::{EcLevel, QrCode, render::svg, render::unicode};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QrError {
    #[error("the payload does not fit in a QR code")]
    PayloadTooLarge,
}

/// Rendered forms of one provisioning URI.
#[derive(Debug, Clone)]
pub struct RenderedQr {
    /// A standalone SVG document.
    ///
    /// Consumers must embed it as an image source (a `data:` URI) rather than
    /// injecting the markup into the page: it is server-generated today, but
    /// treating it as data keeps that from becoming a same-origin script sink
    /// if the payload ever becomes attacker-influenced.
    pub svg: String,
    /// Unicode half-block art, two QR rows per text row, for terminals.
    pub utf8: String,
}

/// Render `payload` as a QR code.
///
/// Uses medium error correction: the code is displayed on a screen and scanned
/// immediately, so the higher levels only make the symbol denser and harder for
/// a phone camera to resolve.
pub fn render(payload: &str) -> Result<RenderedQr, QrError> {
    let code = QrCode::with_error_correction_level(payload.as_bytes(), EcLevel::M).map_err(|_| QrError::PayloadTooLarge)?;

    let svg = code
        .render()
        .min_dimensions(200, 200)
        // Explicit colours rather than the default: an SVG with no declared
        // colours inherits the page's, and a QR code rendered dark-on-dark by a
        // dark-mode viewer is unscannable.
        .dark_color(svg::Color("#000000"))
        .light_color(svg::Color("#ffffff"))
        .build();

    // `Dense1x2` packs two QR rows into one text row, which is what makes the
    // symbol square in a terminal where cells are twice as tall as they are
    // wide. A 1x1 rendering comes out stretched and scans poorly.
    let utf8 = code
        .render::<unicode::Dense1x2>()
        .dark_color(unicode::Dense1x2::Light)
        .light_color(unicode::Dense1x2::Dark)
        .build();

    Ok(RenderedQr { svg, utf8 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mfa::totp::{TotpSecret, provisioning_uri};

    fn sample_uri() -> String {
        provisioning_uri("RustFS", "sinan", &TotpSecret::generate())
    }

    #[test]
    fn renders_both_forms_for_a_provisioning_uri() {
        let rendered = render(&sample_uri()).expect("render");

        assert!(
            rendered.svg.starts_with("<?xml") || rendered.svg.starts_with("<svg"),
            "{}",
            &rendered.svg[..40]
        );
        assert!(rendered.svg.contains("svg"));
        assert!(!rendered.utf8.is_empty());
    }

    #[test]
    fn the_svg_declares_its_own_colours() {
        // A QR code that inherits the page's colours is unscannable in dark
        // mode, which is the failure this pins.
        let rendered = render(&sample_uri()).expect("render");

        assert!(rendered.svg.contains("#000000"), "dark modules must be explicit");
        assert!(rendered.svg.contains("#ffffff"), "light modules must be explicit");
    }

    #[test]
    fn the_terminal_rendering_is_block_art_and_roughly_square() {
        let rendered = render(&sample_uri()).expect("render");
        let lines: Vec<&str> = rendered.utf8.lines().collect();

        assert!(lines.len() > 10, "expected a multi-row symbol, got {}", lines.len());
        assert!(
            rendered
                .utf8
                .chars()
                .any(|c| matches!(c, '\u{2580}' | '\u{2584}' | '\u{2588}' | ' ')),
            "expected half-block characters"
        );

        // Dense1x2 packs two module rows per text row, so the character width
        // should be about twice the row count. Allow slack for the quiet zone.
        let width = lines.iter().map(|line| line.chars().count()).max().unwrap_or(0);
        assert!(
            width >= lines.len() && width <= lines.len() * 3,
            "aspect looks wrong: {width} columns for {} rows",
            lines.len()
        );
    }

    #[test]
    fn distinct_payloads_render_distinct_symbols() {
        let first = render("otpauth://totp/RustFS:a?secret=AAAA").expect("render");
        let second = render("otpauth://totp/RustFS:b?secret=BBBB").expect("render");

        assert_ne!(first.svg, second.svg);
        assert_ne!(first.utf8, second.utf8);
    }

    #[test]
    fn rendering_is_deterministic_for_a_given_payload() {
        let payload = "otpauth://totp/RustFS:sinan?secret=JBSWY3DPEHPK3PXP";
        assert_eq!(render(payload).expect("render").svg, render(payload).expect("render").svg);
    }

    #[test]
    fn an_oversized_payload_is_reported_rather_than_panicking() {
        // A QR code caps out around 2953 bytes; the error path must be a
        // returned error, because the payload includes an access key whose
        // length the server does not control.
        let oversized = "a".repeat(4096);
        assert_eq!(render(&oversized).unwrap_err(), QrError::PayloadTooLarge);
    }

    #[test]
    fn a_long_but_realistic_access_key_still_renders() {
        // Access keys can be long; enrollment must not fail for a legitimate one.
        let long_account = "a".repeat(128);
        let uri = provisioning_uri("RustFS", &long_account, &TotpSecret::generate());
        render(&uri).expect("a realistic provisioning URI must render");
    }
}
