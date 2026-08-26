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

//! Storage-level Object Lock types (rustfs/backlog#1842).
//!
//! The engine evaluates WORM state from persisted object metadata and the
//! bucket default retention; none of that needs S3 wire/DTO types. The
//! serving layer converts to/from its wire DTOs at its own boundary, and the
//! bucket-metadata module converts the persisted `ObjectLockConfiguration`
//! into [`DefaultRetention`] when handing it to the evaluation code here.

use std::fmt;
use time::OffsetDateTime;

/// Object Lock retention mode. Persisted metadata and the bucket default
/// retention only ever carry these two values; anything else is either
/// malformed metadata (fail-closed at the parse site) or an inactive
/// configuration (ignored at the conversion site).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

impl RetentionMode {
    pub const GOVERNANCE: &'static str = "GOVERNANCE";
    pub const COMPLIANCE: &'static str = "COMPLIANCE";

    /// Parse the canonical S3 wire spelling, case-insensitively (matching the
    /// historical `parse_ret_mode` behavior). Returns `None` for anything
    /// that is not GOVERNANCE/COMPLIANCE.
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case(Self::GOVERNANCE) {
            Some(Self::Governance)
        } else if value.eq_ignore_ascii_case(Self::COMPLIANCE) {
            Some(Self::Compliance)
        } else {
            None
        }
    }

    /// Parse only the exact canonical wire spelling. Use this for a mode a
    /// caller supplies in a *request*: the retention-modification gate has
    /// always compared the requested mode literally against the canonical
    /// persisted mode, so a non-canonical spelling must stay "not the same
    /// mode" (and therefore blocked), not be normalized into a match.
    pub fn parse_exact(value: &str) -> Option<Self> {
        match value {
            Self::GOVERNANCE => Some(Self::Governance),
            Self::COMPLIANCE => Some(Self::Compliance),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Governance => Self::GOVERNANCE,
            Self::Compliance => Self::COMPLIANCE,
        }
    }
}

impl fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Object Lock legal hold status (ON/OFF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegalHoldStatus {
    On,
    Off,
}

impl LegalHoldStatus {
    pub const ON: &'static str = "ON";
    pub const OFF: &'static str = "OFF";

    /// Parse the canonical S3 wire spelling, case-insensitively (matching the
    /// historical `parse_legalhold_status` behavior).
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case(Self::ON) {
            Some(Self::On)
        } else if value.eq_ignore_ascii_case(Self::OFF) {
            Some(Self::Off)
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::On => Self::ON,
            Self::Off => Self::OFF,
        }
    }
}

impl fmt::Display for LegalHoldStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An object version's retention as read from persisted metadata. `mode` is
/// `None` when the metadata carries no (or an unparsable) retention mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectRetention {
    pub mode: Option<RetentionMode>,
    pub retain_until_date: Option<OffsetDateTime>,
}

/// An object version's legal hold as read from persisted metadata. `status`
/// is `None` when the metadata carries no (or an unparsable) legal hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectLegalHold {
    pub status: Option<LegalHoldStatus>,
}

impl ObjectLegalHold {
    pub fn is_on(&self) -> bool {
        self.status == Some(LegalHoldStatus::On)
    }
}

/// The bucket's default Object Lock retention, converted from the persisted
/// configuration. Conversion only yields a value for an active default
/// retention (a valid GOVERNANCE/COMPLIANCE mode); a rule without a usable
/// mode converts to `None`, matching how the evaluation code has always
/// ignored such rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub days: Option<i32>,
    pub years: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The modification gate compares a *requested* mode against the canonical
    /// persisted mode literally: a non-canonical spelling must not normalize
    /// into a match, or a client could shorten GOVERNANCE retention without
    /// bypass by spelling the mode differently. `parse_exact` is that pin.
    #[test]
    fn parse_exact_accepts_only_canonical_spellings() {
        assert_eq!(RetentionMode::parse_exact("GOVERNANCE"), Some(RetentionMode::Governance));
        assert_eq!(RetentionMode::parse_exact("COMPLIANCE"), Some(RetentionMode::Compliance));
        for non_canonical in ["governance", "Governance", "compliance", "Compliance", "", "INVALID"] {
            assert_eq!(RetentionMode::parse_exact(non_canonical), None, "{non_canonical:?} must not parse");
        }
    }

    /// Persisted metadata parsing stays case-insensitive (the historical
    /// `parse_ret_mode` / `parse_legalhold_status` behavior): on-disk values
    /// written by older builds must keep locking.
    #[test]
    fn parse_is_case_insensitive_for_persisted_values() {
        assert_eq!(RetentionMode::parse("governance"), Some(RetentionMode::Governance));
        assert_eq!(RetentionMode::parse("Compliance"), Some(RetentionMode::Compliance));
        assert_eq!(LegalHoldStatus::parse("on"), Some(LegalHoldStatus::On));
        assert_eq!(LegalHoldStatus::parse("Off"), Some(LegalHoldStatus::Off));
        assert_eq!(RetentionMode::parse("INVALID"), None);
        assert_eq!(LegalHoldStatus::parse("MAYBE"), None);
    }
}
