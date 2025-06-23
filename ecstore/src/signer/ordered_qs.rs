//! Ordered query strings

use crate::signer::utils::stable_sort_by_first;

/// Immutable query string container
#[derive(Debug, Default, Clone)]
pub struct OrderedQs {
    /// Ascending query strings
    qs: Vec<(String, String)>,
}

/// [`OrderedQs`]
#[derive(Debug, thiserror::Error)]
#[error("ParseOrderedQsError: {inner}")]
pub struct ParseOrderedQsError {
    /// url decode error
    inner: serde_urlencoded::de::Error,
}

impl OrderedQs {
    /// Constructs [`OrderedQs`] from vec
    ///
    /// + strings must be url-decoded
    #[cfg(test)]
    #[must_use]
    pub fn from_vec_unchecked(mut v: Vec<(String, String)>) -> Self {
        stable_sort_by_first(&mut v);
        Self { qs: v }
    }

    /// Parses [`OrderedQs`] from query
    ///
    /// # Errors
    /// Returns [`ParseOrderedQsError`] if query cannot be decoded
    pub fn parse(query: &str) -> Result<Self, ParseOrderedQsError> {
        let result = serde_urlencoded::from_str::<Vec<(String, String)>>(query);
        let mut v = result.map_err(|e| ParseOrderedQsError { inner: e })?;
        stable_sort_by_first(&mut v);
        Ok(Self { qs: v })
    }

    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        self.qs.binary_search_by_key(&name, |x| x.0.as_str()).is_ok()
    }

    /// Gets query values by name. Time `O(logn)`
    pub fn get_all(&self, name: &str) -> impl Iterator<Item = &str> + use<'_> {
        let qs = self.qs.as_slice();

        let lower_bound = qs.partition_point(|x| x.0.as_str() < name);
        let upper_bound = qs.partition_point(|x| x.0.as_str() <= name);

        qs[lower_bound..upper_bound].iter().map(|x| x.1.as_str())
    }

    pub fn get_unique(&self, name: &str) -> Option<&str> {
        let qs = self.qs.as_slice();
        let lower_bound = qs.partition_point(|x| x.0.as_str() < name);

        let mut iter = qs[lower_bound..].iter();
        let pair = iter.next()?;

        if let Some(following) = iter.next() {
            if following.0 == name {
                return None;
            }
        }

        (pair.0.as_str() == name).then_some(pair.1.as_str())
    }
}

impl AsRef<[(String, String)]> for OrderedQs {
    fn as_ref(&self) -> &[(String, String)] {
        self.qs.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag() {
        {
            let query = "tagging";
            let qs = OrderedQs::parse(query).unwrap();
            assert_eq!(qs.as_ref(), &[("tagging".to_owned(), String::new())]);

            assert_eq!(qs.get_unique("taggin"), None);
            assert_eq!(qs.get_unique("tagging"), Some(""));
            assert_eq!(qs.get_unique("taggingg"), None);
        }

        {
            let query = "tagging&tagging";
            let qs = OrderedQs::parse(query).unwrap();
            assert_eq!(
                qs.as_ref(),
                &[("tagging".to_owned(), String::new()), ("tagging".to_owned(), String::new())]
            );

            assert_eq!(qs.get_unique("taggin"), None);
            assert_eq!(qs.get_unique("tagging"), None);
            assert_eq!(qs.get_unique("taggingg"), None);
        }
    }
}
