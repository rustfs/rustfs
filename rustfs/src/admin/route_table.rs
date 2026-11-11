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

//! The admin route table, free of any s3s type.
//!
//! Responsible for: the `matchit` table keyed `METHOD|path`, route insertion,
//! admin path canonicalisation (the `/minio/admin` compatibility prefix onto
//! `/rustfs/admin`), the `/v4` fallback test and the lookup key.
//!
//! Not responsible for: request authentication, dispatch, response mapping or
//! any s3s type. The value type `T` is chosen by the caller.
//!
//! Upstream: `router::S3Router` registers routes and dispatches through it.

use crate::server::{ADMIN_PREFIX, MINIO_ADMIN_PREFIX, is_admin_path};
use hyper::Method;
use matchit::Router;

pub(crate) struct RouteTable<T> {
    router: Router<T>,
    #[cfg(test)]
    registered_routes: Vec<String>,
}

pub(crate) fn canonicalize_admin_path(path: &str) -> std::borrow::Cow<'_, str> {
    if is_admin_path(path)
        && let Some(suffix) = path.strip_prefix(MINIO_ADMIN_PREFIX)
    {
        return std::borrow::Cow::Owned(format!("{ADMIN_PREFIX}{suffix}"));
    }

    std::borrow::Cow::Borrowed(path)
}

pub(crate) fn is_admin_v4_fallback_path(path: &str) -> bool {
    path.strip_prefix(ADMIN_PREFIX)
        .or_else(|| path.strip_prefix(MINIO_ADMIN_PREFIX))
        .is_some_and(|suffix| suffix == "/v4" || suffix.starts_with("/v4/"))
}

/// The key a route is registered under: `METHOD|path`, with no canonicalisation.
pub(crate) fn route_key(method: &Method, path: &str) -> String {
    format!("{}|{}", method.as_str(), path)
}

/// The lookup key for a request: `METHOD|canonical(path)`.
pub(crate) fn route_key_canonical(method: &Method, path: &str) -> String {
    route_key(method, canonicalize_admin_path(path).as_ref())
}

impl<T> RouteTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            router: Router::new(),
            #[cfg(test)]
            registered_routes: Vec::new(),
        }
    }

    pub(crate) fn insert(&mut self, method: Method, path: &str, operation: T) -> std::io::Result<()> {
        let path = route_key(&method, path);
        #[cfg(test)]
        let registered_path = path.clone();

        // warn!("set uri {}", &path);

        self.router.insert(path, operation).map_err(std::io::Error::other)?;

        #[cfg(test)]
        self.registered_routes.push(registered_path);

        Ok(())
    }

    /// Looks up a key built by [`route_key_canonical`] or [`route_key`].
    pub(crate) fn at<'m, 'k>(&'m self, key: &'k str) -> Result<matchit::Match<'m, 'k, &'m T>, matchit::MatchError> {
        self.router.at(key)
    }
}

#[cfg(test)]
impl<T> RouteTable<T> {
    pub(crate) fn contains_route(&self, method: Method, path: &str) -> bool {
        self.router.at(&route_key(&method, path)).is_ok()
    }

    pub(crate) fn contains_compatible_route(&self, method: Method, path: &str) -> bool {
        self.router.at(&route_key_canonical(&method, path)).is_ok()
    }

    pub(crate) fn registered_routes(&self) -> &[String] {
        &self.registered_routes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RouteTable<&'static str> {
        let mut t = RouteTable::new();
        t.insert(Method::GET, "/rustfs/admin/v3/info", "info").unwrap();
        t.insert(Method::POST, "/rustfs/admin/v3/info", "post-info").unwrap();
        t.insert(Method::GET, "/rustfs/admin/v3/users/{name}", "user").unwrap();
        t
    }

    fn lookup(t: &RouteTable<&'static str>, method: Method, path: &str) -> Option<&'static str> {
        let key = route_key_canonical(&method, path);
        t.at(&key).ok().map(|m| *m.value)
    }

    // Negative cases first.

    #[test]
    fn canonicalize_leaves_non_admin_minio_paths_alone() {
        for path in [
            "/minio/administrator/x",
            "/minio/adminx",
            "/minio",
            "/minio/",
            "/minio/health/live",
            "/bucket/minio/admin/v3/info",
        ] {
            assert_eq!(canonicalize_admin_path(path).as_ref(), path, "{path}");
        }
    }

    #[test]
    fn canonicalize_is_case_and_dot_segment_sensitive() {
        for path in [
            "/MINIO/admin/v3/info",
            "/minio/Admin/v3/info",
            "/minio/./admin/v3/info",
            "/minio/../minio/admin/v3/info",
        ] {
            assert_eq!(canonicalize_admin_path(path).as_ref(), path, "{path}");
        }
    }

    #[test]
    fn canonicalize_does_not_decode_percent_encoding() {
        // Pinned: a percent-encoded prefix is not the compat prefix.
        assert_eq!(canonicalize_admin_path("/minio/%61dmin/v3/info").as_ref(), "/minio/%61dmin/v3/info");
        assert_eq!(canonicalize_admin_path("/%6dinio/admin/v3/info").as_ref(), "/%6dinio/admin/v3/info");
    }

    #[test]
    fn canonicalize_does_not_collapse_double_slashes() {
        // Pinned: `//` after the prefix is preserved and the compat prefix itself must be exact.
        assert_eq!(canonicalize_admin_path("//minio/admin/v3/info").as_ref(), "//minio/admin/v3/info");
        assert_eq!(canonicalize_admin_path("/minio//admin/v3/info").as_ref(), "/minio//admin/v3/info");
        assert_eq!(canonicalize_admin_path("/minio/admin//v3/info").as_ref(), "/rustfs/admin//v3/info");
    }

    #[test]
    fn canonicalize_keeps_trailing_slash() {
        assert_eq!(canonicalize_admin_path("/minio/admin/").as_ref(), "/rustfs/admin/");
        assert_eq!(canonicalize_admin_path("/minio/admin/v3/info/").as_ref(), "/rustfs/admin/v3/info/");
    }

    #[test]
    fn canonicalize_does_not_rewrite_table_catalog_compat_prefix() {
        // Only the admin compat prefix is rewritten; table catalog paths pass through.
        let compat = format!("{}/config", crate::server::TABLE_CATALOG_COMPAT_PREFIX);
        assert_eq!(canonicalize_admin_path(&compat).as_ref(), compat);
    }

    #[test]
    fn v4_fallback_rejects_near_misses() {
        for path in [
            "/rustfs/admin/v4x",
            "/rustfs/admin/V4",
            "/rustfs/admin/v3/v4",
            "/rustfs/admin/",
            "/rustfs/admin",
            "/rustfs/adminv4",
            "/minio/admin/v4x",
            "/v4",
            "/rustfs/v4",
            "//rustfs/admin/v4",
            "/bucket/rustfs/admin/v4",
            "/rustfs/admin/%76%34",
            "",
        ] {
            assert!(!is_admin_v4_fallback_path(path), "{path}");
        }
    }

    #[test]
    fn v4_fallback_accepts_trailing_slash_and_double_slash_after_v4() {
        assert!(is_admin_v4_fallback_path("/rustfs/admin/v4/"));
        assert!(is_admin_v4_fallback_path("/minio/admin/v4//x"));
    }

    #[test]
    fn lookup_misses_on_method_mismatch() {
        let t = table();
        assert_eq!(lookup(&t, Method::PUT, "/rustfs/admin/v3/info"), None);
        assert_eq!(lookup(&t, Method::DELETE, "/minio/admin/v3/info"), None);
        assert_eq!(lookup(&t, Method::HEAD, "/rustfs/admin/v3/info"), None);
    }

    #[test]
    fn lookup_misses_on_trailing_slash_double_slash_and_encoded_segments() {
        let t = table();
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/info/"), None);
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3//info"), None);
        assert_eq!(lookup(&t, Method::GET, "//rustfs/admin/v3/info"), None);
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/%69nfo"), None);
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/info%2f"), None);
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/users"), None);
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/users/"), None);
    }

    #[test]
    fn lookup_does_not_match_the_method_through_the_path() {
        let t = table();
        // The key separator is `|`; a path cannot select another method's route.
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/info|x"), None);
    }

    #[test]
    fn insert_rejects_a_duplicate_route() {
        let mut t = table();
        assert!(t.insert(Method::GET, "/rustfs/admin/v3/info", "again").is_err());
        // The same path under another method is a different key.
        assert!(t.insert(Method::PUT, "/rustfs/admin/v3/info", "put").is_ok());
    }

    #[test]
    fn insert_rejects_conflicting_parameter_names() {
        let mut t = table();
        assert!(t.insert(Method::GET, "/rustfs/admin/v3/users/{other}", "x").is_err());
    }

    // Positive cases.

    #[test]
    fn canonicalize_maps_compat_prefix() {
        assert_eq!(canonicalize_admin_path("/minio/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
        assert_eq!(canonicalize_admin_path("/minio/admin").as_ref(), "/rustfs/admin");
        assert_eq!(canonicalize_admin_path("/rustfs/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
    }

    #[test]
    fn lookup_matches_both_prefixes_and_binds_params() {
        let t = table();
        assert_eq!(lookup(&t, Method::GET, "/rustfs/admin/v3/info"), Some("info"));
        assert_eq!(lookup(&t, Method::GET, "/minio/admin/v3/info"), Some("info"));
        assert_eq!(lookup(&t, Method::POST, "/minio/admin/v3/info"), Some("post-info"));

        let key = route_key_canonical(&Method::GET, "/minio/admin/v3/users/alice");
        let m = t.at(&key).unwrap();
        assert_eq!(*m.value, "user");
        assert_eq!(m.params.get("name"), Some("alice"));
    }

    #[test]
    fn route_key_is_method_pipe_path_without_canonicalisation() {
        assert_eq!(route_key(&Method::GET, "/minio/admin/v3/info"), "GET|/minio/admin/v3/info");
        assert_eq!(route_key_canonical(&Method::GET, "/minio/admin/v3/info"), "GET|/rustfs/admin/v3/info");
    }

    #[test]
    fn registered_routes_records_insertion_order() {
        let t = table();
        assert_eq!(t.registered_routes()[0], "GET|/rustfs/admin/v3/info");
        assert_eq!(t.registered_routes().len(), 3);
        assert!(t.contains_route(Method::GET, "/rustfs/admin/v3/info"));
        assert!(!t.contains_route(Method::GET, "/minio/admin/v3/info"));
        assert!(t.contains_compatible_route(Method::GET, "/minio/admin/v3/info"));
    }
}
