use serde::{Deserialize, Serialize};

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;

new_type![
    /// Language tag adhering to RFC 5646 (e.g., `fr` or `fr-CA`).
    #[derive(Deserialize, Hash, Ord, PartialOrd, Serialize)]
    LanguageTag(String)
];
impl AsRef<str> for LanguageTag {
    fn as_ref(&self) -> &str {
        self
    }
}
impl Display for LanguageTag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_ref())
    }
}

pub(crate) fn split_language_tag_key(key: &str) -> (&str, Option<LanguageTag>) {
    let mut lang_tag_sep = key.splitn(2, '#');

    // String::splitn(2) always returns at least one element.
    let field_name = lang_tag_sep.next().unwrap();

    let language_tag = lang_tag_sep
        .next()
        .filter(|language_tag| !language_tag.is_empty())
        .map(|language_tag| LanguageTag::new(language_tag.to_string()));

    (field_name, language_tag)
}

pub(crate) fn join_language_tag_key<'a>(
    field_name: &'a str,
    language_tag: Option<&LanguageTag>,
) -> Cow<'a, str> {
    if let Some(language_tag) = language_tag {
        Cow::Owned(format!("{field_name}#{language_tag}"))
    } else {
        Cow::Borrowed(field_name)
    }
}

/// A [locale-aware](https://openid.net/specs/openid-connect-core-1_0.html#IndividualClaimsLanguages)
/// claim.
///
/// This structure associates one more `Option<LanguageTag>` locales with the corresponding
/// claims values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalizedClaim<T>(HashMap<LanguageTag, T>, Option<T>);
impl<T> LocalizedClaim<T> {
    /// Initialize an empty claim.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the claim contains a value for the specified locale.
    pub fn contains_key(&self, locale: Option<&LanguageTag>) -> bool {
        if let Some(l) = locale {
            self.0.contains_key(l)
        } else {
            self.1.is_some()
        }
    }

    /// Returns the entry for the specified locale or `None` if there is no such entry.
    pub fn get(&self, locale: Option<&LanguageTag>) -> Option<&T> {
        if let Some(l) = locale {
            self.0.get(l)
        } else {
            self.1.as_ref()
        }
    }

    /// Returns an iterator over the locales and claim value entries.
    pub fn iter(&self) -> impl Iterator<Item = (Option<&LanguageTag>, &T)> {
        self.1
            .iter()
            .map(|value| (None, value))
            .chain(self.0.iter().map(|(locale, value)| (Some(locale), value)))
    }

    /// Inserts or updates an entry for the specified locale.
    ///
    /// Returns the current value associated with the given locale, or `None` if there is no
    /// such entry.
    pub fn insert(&mut self, locale: Option<LanguageTag>, value: T) -> Option<T> {
        if let Some(l) = locale {
            self.0.insert(l, value)
        } else {
            self.1.replace(value)
        }
    }

    /// Removes an entry for the specified locale.
    ///
    /// Returns the current value associated with the given locale, or `None` if there is no
    /// such entry.
    pub fn remove(&mut self, locale: Option<&LanguageTag>) -> Option<T> {
        if let Some(l) = locale {
            self.0.remove(l)
        } else {
            self.1.take()
        }
    }
}
impl<T> LocalizedClaim<Option<T>> {
    pub(crate) fn flatten_or_none(self) -> Option<LocalizedClaim<T>> {
        let flattened_tagged = self
            .0
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect::<HashMap<_, _>>();
        let flattened_default = self.1.flatten();

        if flattened_tagged.is_empty() && flattened_default.is_none() {
            None
        } else {
            Some(LocalizedClaim(flattened_tagged, flattened_default))
        }
    }
}

impl<T> Default for LocalizedClaim<T> {
    fn default() -> Self {
        Self(HashMap::new(), None)
    }
}
impl<T> From<T> for LocalizedClaim<T> {
    fn from(default: T) -> Self {
        Self(HashMap::new(), Some(default))
    }
}
impl<T> FromIterator<(Option<LanguageTag>, T)> for LocalizedClaim<T> {
    fn from_iter<I: IntoIterator<Item = (Option<LanguageTag>, T)>>(iter: I) -> Self {
        let mut temp: HashMap<Option<LanguageTag>, T> = iter.into_iter().collect();
        let default = temp.remove(&None);
        Self(
            temp.into_iter()
                .filter_map(|(locale, value)| locale.map(|l| (l, value)))
                .collect(),
            default,
        )
    }
}
impl<T> IntoIterator for LocalizedClaim<T>
where
    T: 'static,
{
    type Item = <LocalizedClaimIterator<T> as Iterator>::Item;
    type IntoIter = LocalizedClaimIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        LocalizedClaimIterator {
            inner: Box::new(
                self.1.into_iter().map(|value| (None, value)).chain(
                    self.0
                        .into_iter()
                        .map(|(locale, value)| (Some(locale), value)),
                ),
            ),
        }
    }
}

/// Owned iterator over a LocalizedClaim.
pub struct LocalizedClaimIterator<T> {
    inner: Box<dyn Iterator<Item = (Option<LanguageTag>, T)>>,
}
impl<T> Iterator for LocalizedClaimIterator<T> {
    type Item = (Option<LanguageTag>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
