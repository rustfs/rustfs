use crate::helpers::{Boolean, DeserializeMapField, FlattenFilter, Timestamp};
use crate::types::localized::split_language_tag_key;
use crate::{
    AddressCountry, AddressLocality, AddressPostalCode, AddressRegion, EndUserBirthday,
    EndUserEmail, EndUserFamilyName, EndUserGivenName, EndUserMiddleName, EndUserName,
    EndUserNickname, EndUserPhoneNumber, EndUserPictureUrl, EndUserProfileUrl, EndUserTimezone,
    EndUserUsername, EndUserWebsiteUrl, FormattedAddress, LanguageTag, LocalizedClaim,
    StreetAddress, SubjectIdentifier,
};

use chrono::{DateTime, Utc};
use serde::de::{DeserializeOwned, Deserializer, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::skip_serializing_none;

use std::fmt::{Debug, Formatter, Result as FormatterResult};
use std::marker::PhantomData;
use std::str;

/// Additional claims beyond the set of Standard Claims defined by OpenID Connect Core.
pub trait AdditionalClaims: Debug + DeserializeOwned + Serialize + 'static {}

/// No additional claims.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
// In order to support serde flatten, this must be an empty struct rather than an empty
// tuple struct.
pub struct EmptyAdditionalClaims {}
impl AdditionalClaims for EmptyAdditionalClaims {}

/// Address claims.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct AddressClaim {
    /// Full mailing address, formatted for display or use on a mailing label.
    ///
    /// This field MAY contain multiple lines, separated by newlines. Newlines can be represented
    /// either as a carriage return/line feed pair (`\r\n`) or as a single line feed character
    /// (`\n`).
    pub formatted: Option<FormattedAddress>,
    /// Full street address component, which MAY include house number, street name, Post Office Box,
    /// and multi-line extended street address information.
    ///
    /// This field MAY contain multiple lines, separated by newlines. Newlines can be represented
    /// either as a carriage return/line feed pair (`\r\n`) or as a single line feed character
    /// (`\n`).
    pub street_address: Option<StreetAddress>,
    /// City or locality component.
    pub locality: Option<AddressLocality>,
    /// State, province, prefecture, or region component.
    pub region: Option<AddressRegion>,
    /// Zip code or postal code component.
    pub postal_code: Option<AddressPostalCode>,
    /// Country name component.
    pub country: Option<AddressCountry>,
}

/// Gender claim.
pub trait GenderClaim: Clone + Debug + DeserializeOwned + Serialize + 'static {}

/// Standard Claims defined by OpenID Connect Core.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandardClaims<GC>
where
    GC: GenderClaim,
{
    pub(crate) sub: SubjectIdentifier,
    pub(crate) name: Option<LocalizedClaim<EndUserName>>,
    pub(crate) given_name: Option<LocalizedClaim<EndUserGivenName>>,
    pub(crate) family_name: Option<LocalizedClaim<EndUserFamilyName>>,
    pub(crate) middle_name: Option<LocalizedClaim<EndUserMiddleName>>,
    pub(crate) nickname: Option<LocalizedClaim<EndUserNickname>>,
    pub(crate) preferred_username: Option<EndUserUsername>,
    pub(crate) profile: Option<LocalizedClaim<EndUserProfileUrl>>,
    pub(crate) picture: Option<LocalizedClaim<EndUserPictureUrl>>,
    pub(crate) website: Option<LocalizedClaim<EndUserWebsiteUrl>>,
    pub(crate) email: Option<EndUserEmail>,
    pub(crate) email_verified: Option<bool>,
    pub(crate) gender: Option<GC>,
    pub(crate) birthday: Option<EndUserBirthday>,
    pub(crate) birthdate: Option<EndUserBirthday>,
    pub(crate) zoneinfo: Option<EndUserTimezone>,
    pub(crate) locale: Option<LanguageTag>,
    pub(crate) phone_number: Option<EndUserPhoneNumber>,
    pub(crate) phone_number_verified: Option<bool>,
    pub(crate) address: Option<AddressClaim>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
}
impl<GC> StandardClaims<GC>
where
    GC: GenderClaim,
{
    /// Initializes a set of Standard Claims.
    ///
    /// The Subject (`sub`) claim is the only required Standard Claim.
    pub fn new(subject: SubjectIdentifier) -> Self {
        Self {
            sub: subject,
            name: None,
            given_name: None,
            family_name: None,
            middle_name: None,
            nickname: None,
            preferred_username: None,
            profile: None,
            picture: None,
            website: None,
            email: None,
            email_verified: None,
            gender: None,
            birthday: None,
            birthdate: None,
            zoneinfo: None,
            locale: None,
            phone_number: None,
            phone_number_verified: None,
            address: None,
            updated_at: None,
        }
    }

    /// Returns the Subject (`sub`) claim.
    pub fn subject(&self) -> &SubjectIdentifier {
        &self.sub
    }

    /// Sets the Subject (`sub`) claim.
    pub fn set_subject(mut self, subject: SubjectIdentifier) -> Self {
        self.sub = subject;
        self
    }

    field_getters_setters![
        pub self [self] ["claim"] {
            set_name -> name[Option<LocalizedClaim<EndUserName>>],
            set_given_name -> given_name[Option<LocalizedClaim<EndUserGivenName>>],
            set_family_name ->
                family_name[Option<LocalizedClaim<EndUserFamilyName>>],
            set_middle_name ->
                middle_name[Option<LocalizedClaim<EndUserMiddleName>>],
            set_nickname -> nickname[Option<LocalizedClaim<EndUserNickname>>],
            set_preferred_username -> preferred_username[Option<EndUserUsername>],
            set_profile -> profile[Option<LocalizedClaim<EndUserProfileUrl>>],
            set_picture -> picture[Option<LocalizedClaim<EndUserPictureUrl>>],
            set_website -> website[Option<LocalizedClaim<EndUserWebsiteUrl>>],
            set_email -> email[Option<EndUserEmail>],
            set_email_verified -> email_verified[Option<bool>],
            set_gender -> gender[Option<GC>],
            set_birthday -> birthday[Option<EndUserBirthday>],
            set_birthdate -> birthdate[Option<EndUserBirthday>],
            set_zoneinfo -> zoneinfo[Option<EndUserTimezone>],
            set_locale -> locale[Option<LanguageTag>],
            set_phone_number -> phone_number[Option<EndUserPhoneNumber>],
            set_phone_number_verified -> phone_number_verified[Option<bool>],
            set_address -> address[Option<AddressClaim>],
            set_updated_at -> updated_at[Option<DateTime<Utc>>],
        }
    ];
}
impl<GC> FlattenFilter for StandardClaims<GC>
where
    GC: GenderClaim,
{
    // When another struct (i.e., additional claims) is co-flattened with this one, only include
    // fields in that other struct which are not part of this struct.
    fn should_include(field_name: &str) -> bool {
        !matches!(
            split_language_tag_key(field_name),
            ("sub", None)
                | ("name", _)
                | ("given_name", _)
                | ("family_name", _)
                | ("middle_name", _)
                | ("nickname", _)
                | ("preferred_username", None)
                | ("profile", _)
                | ("picture", _)
                | ("website", _)
                | ("email", None)
                | ("email_verified", None)
                | ("gender", None)
                | ("birthday", None)
                | ("birthdate", None)
                | ("zoneinfo", None)
                | ("locale", None)
                | ("phone_number", None)
                | ("phone_number_verified", None)
                | ("address", None)
                | ("updated_at", None)
        )
    }
}
impl<'de, GC> Deserialize<'de> for StandardClaims<GC>
where
    GC: GenderClaim,
{
    /// Special deserializer that supports [RFC 5646](https://tools.ietf.org/html/rfc5646) language
    /// tags associated with human-readable client metadata fields.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ClaimsVisitor<GC: GenderClaim>(PhantomData<GC>);
        impl<'de, GC> Visitor<'de> for ClaimsVisitor<GC>
        where
            GC: GenderClaim,
        {
            type Value = StandardClaims<GC>;

            fn expecting(&self, formatter: &mut Formatter) -> FormatterResult {
                formatter.write_str("struct StandardClaims")
            }
            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                // NB: The non-localized fields are actually Option<Option<_>> here so that we can
                // distinguish between omitted fields and fields explicitly set to `null`. The
                // latter is necessary so that we can detect duplicate fields (e.g., if a key is
                // present both with a null value and a non-null value, that's an error).
                let mut sub = None;
                let mut name = None;
                let mut given_name = None;
                let mut family_name = None;
                let mut middle_name = None;
                let mut nickname = None;
                let mut preferred_username = None;
                let mut profile = None;
                let mut picture = None;
                let mut website = None;
                let mut email = None;
                let mut email_verified = None;
                let mut gender = None;
                let mut birthday = None;
                let mut birthdate = None;
                let mut zoneinfo = None;
                let mut locale = None;
                let mut phone_number = None;
                let mut phone_number_verified = None;
                let mut address = None;
                let mut updated_at = None;

                macro_rules! field_case {
                    ($field:ident, $typ:ty, $language_tag:ident) => {{
                        $field = Some(<$typ>::deserialize_map_field(
                            &mut map,
                            stringify!($field),
                            $language_tag,
                            $field,
                        )?);
                    }};
                }

                while let Some(key) = map.next_key::<String>()? {
                    let (field_name, language_tag) = split_language_tag_key(&key);
                    match field_name {
                        "sub" => field_case!(sub, SubjectIdentifier, language_tag),
                        "name" => field_case!(name, LocalizedClaim<Option<_>>, language_tag),
                        "given_name" => {
                            field_case!(given_name, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "family_name" => {
                            field_case!(family_name, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "middle_name" => {
                            field_case!(middle_name, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "nickname" => {
                            field_case!(nickname, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "preferred_username" => {
                            field_case!(preferred_username, Option<_>, language_tag)
                        }
                        "profile" => {
                            field_case!(profile, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "picture" => {
                            field_case!(picture, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "website" => {
                            field_case!(website, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "email" => field_case!(email, Option<_>, language_tag),
                        "email_verified" => {
                            field_case!(email_verified, Option<Boolean>, language_tag)
                        }
                        "gender" => field_case!(gender, Option<_>, language_tag),
                        "birthday" => field_case!(birthday, Option<_>, language_tag),
                        "birthdate" => field_case!(birthdate, Option<_>, language_tag),
                        "zoneinfo" => field_case!(zoneinfo, Option<_>, language_tag),
                        "locale" => field_case!(locale, Option<_>, language_tag),
                        "phone_number" => field_case!(phone_number, Option<_>, language_tag),
                        "phone_number_verified" => {
                            field_case!(phone_number_verified, Option<Boolean>, language_tag)
                        }
                        "address" => field_case!(address, Option<_>, language_tag),
                        "updated_at" => field_case!(updated_at, Option<Timestamp>, language_tag),
                        // Ignore unknown fields.
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                            continue;
                        }
                    };
                }

                Ok(StandardClaims {
                    sub: sub.ok_or_else(|| serde::de::Error::missing_field("sub"))?,
                    name: name.and_then(LocalizedClaim::flatten_or_none),
                    given_name: given_name.and_then(LocalizedClaim::flatten_or_none),
                    family_name: family_name.and_then(LocalizedClaim::flatten_or_none),
                    middle_name: middle_name.and_then(LocalizedClaim::flatten_or_none),
                    nickname: nickname.and_then(LocalizedClaim::flatten_or_none),
                    preferred_username: preferred_username.flatten(),
                    profile: profile.and_then(LocalizedClaim::flatten_or_none),
                    picture: picture.and_then(LocalizedClaim::flatten_or_none),
                    website: website.and_then(LocalizedClaim::flatten_or_none),
                    email: email.flatten(),
                    email_verified: email_verified.flatten().map(Boolean::into_inner),
                    gender: gender.flatten(),
                    birthday: birthday.flatten(),
                    birthdate: birthdate.flatten(),
                    zoneinfo: zoneinfo.flatten(),
                    locale: locale.flatten(),
                    phone_number: phone_number.flatten(),
                    phone_number_verified: phone_number_verified.flatten().map(Boolean::into_inner),
                    address: address.flatten(),
                    updated_at: updated_at
                        .flatten()
                        .map(|sec| {
                            sec.to_utc().map_err(|_| {
                                serde::de::Error::custom(format!(
                                    "failed to parse `{sec}` as UTC datetime (in seconds) for key \
                                     `updated_at`"
                                ))
                            })
                        })
                        .transpose()?,
                })
            }
        }
        deserializer.deserialize_map(ClaimsVisitor(PhantomData))
    }
}
impl<GC> Serialize for StandardClaims<GC>
where
    GC: GenderClaim,
{
    #[allow(clippy::cognitive_complexity)]
    fn serialize<SE>(&self, serializer: SE) -> Result<SE::Ok, SE::Error>
    where
        SE: Serializer,
    {
        serialize_fields! {
            self -> serializer {
                [sub]
                [LanguageTag(name)]
                [LanguageTag(given_name)]
                [LanguageTag(family_name)]
                [LanguageTag(middle_name)]
                [LanguageTag(nickname)]
                [Option(preferred_username)]
                [LanguageTag(profile)]
                [LanguageTag(picture)]
                [LanguageTag(website)]
                [Option(email)]
                [Option(email_verified)]
                [Option(gender)]
                [Option(birthday)]
                [Option(birthdate)]
                [Option(zoneinfo)]
                [Option(locale)]
                [Option(phone_number)]
                [Option(phone_number_verified)]
                [Option(address)]
                [Option(DateTime(Seconds(updated_at)))]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::CoreGenderClaim;
    use crate::StandardClaims;

    // The spec states (https://openid.net/specs/openid-connect-core-1_0.html#UserInfoResponse):
    //   "If a Claim is not returned, that Claim Name SHOULD be omitted from the JSON object
    //   representing the Claims; it SHOULD NOT be present with a null or empty string value."
    // However, we still aim to support identity providers that disregard this suggestion.
    #[test]
    fn test_null_optional_claims() {
        let claims = serde_json::from_str::<StandardClaims<CoreGenderClaim>>(
            r#"{
                "sub": "24400320",
                "name": null,
                "given_name": null,
                "family_name": null,
                "middle_name": null,
                "nickname": null,
                "preferred_username": null,
                "profile": null,
                "picture": null,
                "website": null,
                "email": null,
                "email_verified": null,
                "gender": null,
                "birthday": null,
                "birthdate": null,
                "zoneinfo": null,
                "locale": null,
                "phone_number": null,
                "phone_number_verified": null,
                "address": null,
                "updated_at": null
            }"#,
        )
        .expect("should deserialize successfully");

        assert_eq!(claims.subject().as_str(), "24400320");
        assert_eq!(claims.name(), None);
    }

    fn expect_err_prefix(
        result: Result<StandardClaims<CoreGenderClaim>, serde_json::Error>,
        expected_prefix: &str,
    ) {
        let err_str = result.expect_err("deserialization should fail").to_string();
        assert!(
            err_str.starts_with(expected_prefix),
            "error message should begin with `{expected_prefix}`: {err_str}",
        )
    }

    #[test]
    fn test_duplicate_claims() {
        expect_err_prefix(
            serde_json::from_str(
                r#"{
                    "sub": "24400320",
                    "sub": "24400321"
                }"#,
            ),
            "duplicate field `sub` at line",
        );

        expect_err_prefix(
            serde_json::from_str(
                r#"{
                    "name": null,
                    "sub": "24400320",
                    "name": "foo",
                }"#,
            ),
            "duplicate field `name` at line",
        );

        expect_err_prefix(
            serde_json::from_str(
                r#"{
                    "name#en": null,
                    "sub": "24400320",
                    "name#en": "foo",
                }"#,
            ),
            "duplicate field `name#en` at line",
        );
    }

    #[test]
    fn test_err_field_name() {
        expect_err_prefix(
            serde_json::from_str(
                r#"{
                    "sub": 24400320
                }"#,
            ),
            "sub: invalid type: integer `24400320`, expected a string at line",
        );
    }
}
