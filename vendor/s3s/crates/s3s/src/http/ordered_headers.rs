//! Ordered headers

use std::str::Utf8Error;

use hyper::HeaderMap;

use crate::utils::stable_sort_by_first;

/// Immutable http header container
#[derive(Debug, Default)]
pub struct OrderedHeaders<'a> {
    /// Ascending headers (header names are lowercase)
    headers: Vec<(&'a str, &'a str)>,
}

impl<'a> OrderedHeaders<'a> {
    /// Constructs [`OrderedHeaders`] from slice
    ///
    /// + header names must be lowercase
    /// + header values must be valid
    #[cfg(test)]
    #[must_use]
    pub fn from_slice_unchecked(slice: &[(&'a str, &'a str)]) -> Self {
        for (name, _) in slice {
            let is_valid = |c: u8| c == b'-' || c.is_ascii_lowercase() || c.is_ascii_digit();
            assert!(name.as_bytes().iter().copied().all(is_valid));
        }
        let mut headers = Vec::new();
        headers.extend_from_slice(slice);
        stable_sort_by_first(&mut headers);
        Self { headers }
    }

    /// Constructs [`OrderedHeaders`] from a header map
    ///
    /// # Errors
    /// Returns [`ToStrError`] if header value cannot be converted to string slice
    pub fn from_headers(map: &'a HeaderMap) -> Result<Self, Utf8Error> {
        let mut headers: Vec<(&'a str, &'a str)> = Vec::with_capacity(map.len());

        for (name, value) in map {
            let value = std::str::from_utf8(value.as_bytes())?;
            headers.push((name.as_str(), value));
        }
        stable_sort_by_first(&mut headers);

        Ok(Self { headers })
    }

    fn get_all_pairs(&self, name: &str) -> impl Iterator<Item = (&'a str, &'a str)> + '_ + use<'a, '_> {
        let slice = self.headers.as_slice();

        let lower_bound = slice.partition_point(|x| x.0 < name);
        let upper_bound = slice.partition_point(|x| x.0 <= name);

        slice[lower_bound..upper_bound].iter().copied()
    }

    pub fn get_all(&self, name: impl AsRef<str>) -> impl Iterator<Item = &'a str> + '_ {
        self.get_all_pairs(name.as_ref()).map(|x| x.1)
    }

    fn get_unique_pair(&self, name: &'_ str) -> Option<(&'a str, &'a str)> {
        let slice = self.headers.as_slice();
        let lower_bound = slice.partition_point(|x| x.0 < name);

        let mut iter = slice[lower_bound..].iter().copied();
        let pair = iter.next()?;

        if let Some(following) = iter.next()
            && following.0 == name
        {
            return None;
        }

        (pair.0 == name).then_some(pair)
    }

    /// Gets header value by name. Time `O(logn)`
    pub fn get_unique(&self, name: impl AsRef<str>) -> Option<&'a str> {
        self.get_unique_pair(name.as_ref()).map(|(_, v)| v)
    }

    // /// Finds headers by names. Time `O(mlogn)`
    // #[must_use]
    // pub fn find_multiple(&self, names: &[impl AsRef<str>]) -> Self {
    //     let mut headers: Vec<(&'a str, &'a str)> = Vec::new();
    //     for name in names {
    //         for pair in self.get_all_pairs(name.as_ref()) {
    //             headers.push(pair);
    //         }
    //     }
    //     Self { headers }
    // }

    /// Finds headers by names. Time `O(mlogn)`
    #[must_use]
    pub fn find_multiple_with_on_missing(
        &self,
        names: &'a [impl AsRef<str>],
        on_missing: impl Fn(&'a str) -> Option<&'a str>,
    ) -> Self {
        let mut headers: Vec<(&'a str, &'a str)> = Vec::new();
        for name in names {
            let mut has_value = false;
            for pair in self.get_all_pairs(name.as_ref()) {
                headers.push(pair);
                has_value = true;
            }
            if !has_value && let Some(value) = on_missing(name.as_ref()) {
                headers.push((name.as_ref(), value));
            }
        }
        Self { headers }
    }
}

impl<'a> AsRef<[(&'a str, &'a str)]> for OrderedHeaders<'a> {
    fn as_ref(&self) -> &[(&'a str, &'a str)] {
        self.headers.as_ref()
    }
}
