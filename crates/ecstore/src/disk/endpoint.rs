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

use super::error::{Error, Result};
use path_absolutize::Absolutize;
use rustfs_utils::{is_local_host, is_socket_addr};
use std::{fmt::Display, path::Path};
use tracing::debug;
use url::{ParseError, Url};

/// enum for endpoint type.
#[derive(PartialEq, Eq, Debug)]
pub enum EndpointType {
    /// path style endpoint type enum.
    Path,

    /// URL style endpoint type enum.
    Url,
}

/// any type of endpoint.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct Endpoint {
    pub url: Url,
    pub is_local: bool,

    pub pool_idx: i32,
    pub set_idx: i32,
    pub disk_idx: i32,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.url.scheme() == "file" {
            write!(f, "{}", self.get_file_path())
        } else {
            write!(f, "{}", self.url)
        }
    }
}

impl TryFrom<&str> for Endpoint {
    /// The type returned in the event of a conversion error.
    type Error = Error;

    /// Performs the conversion.
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        // check whether given path is not empty.
        if ["", "/", "\\"].iter().any(|&v| v.eq(value)) {
            return Err(Error::other("empty or root endpoint is not supported"));
        }

        let mut is_local = false;
        let url = match Url::parse(value) {
            #[allow(unused_mut)]
            Ok(mut url) if url.has_host() => {
                // URL style of endpoint.
                // Valid URL style endpoint is
                // - Scheme field must contain "http" or "https"
                // - All field should be empty except Host and Path.
                if !((url.scheme() == "http" || url.scheme() == "https")
                    && url.username().is_empty()
                    && url.fragment().is_none()
                    && url.query().is_none())
                {
                    return Err(Error::other("invalid URL endpoint format"));
                }

                let path = url.path().to_string();

                #[cfg(not(windows))]
                let path = Path::new(&path).absolutize()?;

                // On windows having a preceding SlashSeparator will cause problems, if the
                // command line already has C:/<export-folder/ in it. Final resulting
                // path on windows might become C:/C:/ this will cause problems
                // of starting rustfs server properly in distributed mode on windows.
                // As a special case make sure to trim the separator.
                #[cfg(windows)]
                let path = Path::new(&path[1..]).absolutize()?;

                debug!("endpoint try_from: path={}", path.display());

                if path.parent().is_none() || Path::new("").eq(&path) {
                    return Err(Error::other("empty or root path is not supported in URL endpoint"));
                }

                match path.to_str() {
                    Some(v) => url.set_path(v),
                    None => return Err(Error::other("invalid path")),
                }

                url
            }
            Ok(_) => {
                // like d:/foo
                is_local = true;
                url_parse_from_file_path(value)?
            }
            Err(e) => match e {
                ParseError::InvalidPort => {
                    return Err(Error::other("invalid URL endpoint format: port number must be between 1 to 65535"));
                }
                ParseError::EmptyHost => return Err(Error::other("invalid URL endpoint format: empty host name")),
                ParseError::RelativeUrlWithoutBase => {
                    // like /foo
                    is_local = true;
                    url_parse_from_file_path(value)?
                }
                _ => return Err(Error::other(format!("invalid URL endpoint format: {e}"))),
            },
        };

        Ok(Endpoint {
            url,
            is_local,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        })
    }
}

impl Endpoint {
    /// returns type of endpoint.
    pub fn get_type(&self) -> EndpointType {
        if self.url.scheme() == "file" {
            EndpointType::Path
        } else {
            EndpointType::Url
        }
    }

    /// sets a specific pool number to this node
    pub fn set_pool_index(&mut self, idx: usize) {
        self.pool_idx = idx as i32
    }

    /// sets a specific set number to this node
    pub fn set_set_index(&mut self, idx: usize) {
        self.set_idx = idx as i32
    }

    /// sets a specific disk number to this node
    pub fn set_disk_index(&mut self, idx: usize) {
        self.disk_idx = idx as i32
    }

    /// resolves the host and updates if it is local or not.
    pub fn update_is_local(&mut self, local_port: u16) -> Result<()> {
        match (self.url.scheme(), self.url.host()) {
            (v, Some(host)) if v != "file" => {
                self.is_local = is_local_host(host, self.url.port().unwrap_or_default(), local_port)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// returns the host to be used for grid connections.
    pub fn grid_host(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => {
                debug!("grid_host scheme={}: host={}, port={}", self.url.scheme(), host, port);
                format!("{}://{}:{}", self.url.scheme(), host, port)
            }
            (Some(host), None) => {
                debug!("grid_host scheme={}: host={}", self.url.scheme(), host);
                format!("{}://{}", self.url.scheme(), host)
            }
            _ => String::new(),
        }
    }

    pub fn host_port(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => {
                debug!("host_port host={}, port={}", host, port);
                format!("{host}:{port}")
            }
            (Some(host), None) => {
                debug!("host_port host={}, port={}", host, self.url.port().unwrap_or(0));
                format!("{host}")
            }
            _ => String::new(),
        }
    }

    pub fn get_file_path(&self) -> String {
        let path: &str = self.url.path();
        let decoded: std::borrow::Cow<'_, str> = match urlencoding::decode(path) {
            Ok(decoded) => decoded,
            Err(e) => {
                debug!("Failed to decode path '{}': {}, using original path", path, e);
                std::borrow::Cow::Borrowed(path)
            }
        };
        #[cfg(windows)]
        if self.url.scheme() == "file" {
            let stripped: &str = decoded.strip_prefix('/').unwrap_or(&decoded);
            debug!("get_file_path windows: path={}", stripped);
            return stripped.to_string();
        }
        decoded.into_owned()
    }
}

/// parse a file path into a URL.
fn url_parse_from_file_path(value: &str) -> Result<Url> {
    // Only check if the arg is an ip address and ask for scheme since its absent.
    // localhost, example.com, any FQDN cannot be disambiguated from a regular file path such as
    // /mnt/export1. So we go ahead and start the rustfs server in FS modes in these cases.
    let addr: Vec<&str> = value.splitn(2, '/').collect();
    if is_socket_addr(addr[0]) {
        return Err(Error::other("invalid URL endpoint format: missing scheme http or https"));
    }

    let file_path = match Path::new(value).absolutize() {
        Ok(path) => path,
        Err(err) => return Err(Error::other(format!("absolute path failed: {err}"))),
    };

    match Url::from_file_path(file_path) {
        Ok(url) => Ok(url),
        Err(_) => Err(Error::other("Convert a file path into an URL failed")),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_endpoint() {
        #[derive(Default)]
        struct TestCase<'a> {
            arg: &'a str,
            expected_endpoint: Option<Endpoint>,
            expected_type: Option<EndpointType>,
            expected_err: Option<Error>,
        }

        let u2 = Url::parse("https://example.org/path").unwrap();
        let u4 = Url::parse("http://192.168.253.200/path").unwrap();
        let u6 = Url::parse("http://server:/path").unwrap();
        let root_slash_foo = Url::from_file_path("/foo").unwrap();

        let test_cases = [
            TestCase {
                arg: "/foo",
                expected_endpoint: Some(Endpoint {
                    url: root_slash_foo,
                    is_local: true,
                    pool_idx: -1,
                    set_idx: -1,
                    disk_idx: -1,
                }),
                expected_type: Some(EndpointType::Path),
                expected_err: None,
            },
            TestCase {
                arg: "https://example.org/path",
                expected_endpoint: Some(Endpoint {
                    url: u2,
                    is_local: false,
                    pool_idx: -1,
                    set_idx: -1,
                    disk_idx: -1,
                }),
                expected_type: Some(EndpointType::Url),
                expected_err: None,
            },
            TestCase {
                arg: "http://192.168.253.200/path",
                expected_endpoint: Some(Endpoint {
                    url: u4,
                    is_local: false,
                    pool_idx: -1,
                    set_idx: -1,
                    disk_idx: -1,
                }),
                expected_type: Some(EndpointType::Url),
                expected_err: None,
            },
            TestCase {
                arg: "",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "/",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "\\",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "c://foo",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format")),
            },
            TestCase {
                arg: "ftp://foo",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format")),
            },
            TestCase {
                arg: "http://server/path?location",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format")),
            },
            TestCase {
                arg: "http://:/path",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format: empty host name")),
            },
            TestCase {
                arg: "http://:8080/path",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format: empty host name")),
            },
            TestCase {
                arg: "http://server:/path",
                expected_endpoint: Some(Endpoint {
                    url: u6,
                    is_local: false,
                    pool_idx: -1,
                    set_idx: -1,
                    disk_idx: -1,
                }),
                expected_type: Some(EndpointType::Url),
                expected_err: None,
            },
            TestCase {
                arg: "https://93.184.216.34:808080/path",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format: port number must be between 1 to 65535")),
            },
            TestCase {
                arg: "http://server:8080//",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("empty or root path is not supported in URL endpoint")),
            },
            TestCase {
                arg: "http://server:8080/",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("empty or root path is not supported in URL endpoint")),
            },
            TestCase {
                arg: "192.168.1.210:9000",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::other("invalid URL endpoint format: missing scheme http or https")),
            },
        ];

        for test_case in test_cases {
            let ret = Endpoint::try_from(test_case.arg);
            if test_case.expected_err.is_none() && ret.is_err() {
                panic!("{}: error: expected = <nil>, got = {:?}", test_case.arg, ret);
            }
            if test_case.expected_err.is_some() && ret.is_ok() {
                panic!("{}: error: expected = {:?}, got = <nil>", test_case.arg, test_case.expected_err);
            }
            match (test_case.expected_err, ret) {
                (None, Err(e)) => panic!("{}: error: expected = <nil>, got = {}", test_case.arg, e),
                (None, Ok(mut ep)) => {
                    let _ = ep.update_is_local(9000);
                    if test_case.expected_type != Some(ep.get_type()) {
                        panic!(
                            "{}: type: expected = {:?}, got = {:?}",
                            test_case.arg,
                            test_case.expected_type,
                            ep.get_type()
                        );
                    }

                    assert_eq!(test_case.expected_endpoint, Some(ep), "{}: endpoint", test_case.arg);
                }
                (Some(e), Ok(_)) => panic!("{}: error: expected = {}, got = <nil>", test_case.arg, e),
                (Some(e), Err(e2)) => {
                    assert_eq!(e.to_string(), e2.to_string(), "{}: error: expected = {}, got = {}", test_case.arg, e, e2)
                }
            }
        }
    }

    #[test]
    fn test_endpoint_display() {
        // Test file path display
        let file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        let display_str = format!("{file_endpoint}");
        assert_eq!(display_str, "/tmp/data");

        // Test URL display
        let url_endpoint = Endpoint::try_from("http://example.com:9000/path").unwrap();
        let display_str = format!("{url_endpoint}");
        assert_eq!(display_str, "http://example.com:9000/path");
    }

    #[test]
    fn test_endpoint_type() {
        let file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        assert_eq!(file_endpoint.get_type(), EndpointType::Path);

        let url_endpoint = Endpoint::try_from("http://example.com:9000/path").unwrap();
        assert_eq!(url_endpoint.get_type(), EndpointType::Url);
    }

    #[test]
    fn test_endpoint_indexes() {
        let mut endpoint = Endpoint::try_from("/tmp/data").unwrap();

        // Test initial values
        assert_eq!(endpoint.pool_idx, -1);
        assert_eq!(endpoint.set_idx, -1);
        assert_eq!(endpoint.disk_idx, -1);

        // Test setting indexes
        endpoint.set_pool_index(2);
        endpoint.set_set_index(3);
        endpoint.set_disk_index(4);

        assert_eq!(endpoint.pool_idx, 2);
        assert_eq!(endpoint.set_idx, 3);
        assert_eq!(endpoint.disk_idx, 4);
    }

    #[test]
    fn test_endpoint_grid_host() {
        let endpoint = Endpoint::try_from("http://example.com:9000/path").unwrap();
        assert_eq!(endpoint.grid_host(), "http://example.com:9000");

        let endpoint_no_port = Endpoint::try_from("https://example.com/path").unwrap();
        assert_eq!(endpoint_no_port.grid_host(), "https://example.com");

        let file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        assert_eq!(file_endpoint.grid_host(), "");
    }

    #[test]
    fn test_endpoint_host_port() {
        let endpoint = Endpoint::try_from("http://example.com:9000/path").unwrap();
        assert_eq!(endpoint.host_port(), "example.com:9000");

        let endpoint_no_port = Endpoint::try_from("https://example.com/path").unwrap();
        assert_eq!(endpoint_no_port.host_port(), "example.com");

        let file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        assert_eq!(file_endpoint.host_port(), "");
    }

    #[test]
    fn test_endpoint_get_file_path() {
        let file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        assert_eq!(file_endpoint.get_file_path(), "/tmp/data");

        let url_endpoint = Endpoint::try_from("http://example.com:9000/path/to/data").unwrap();
        assert_eq!(url_endpoint.get_file_path(), "/path/to/data");
    }

    #[test]
    fn test_endpoint_clone_and_equality() {
        let endpoint1 = Endpoint::try_from("/tmp/data").unwrap();
        let endpoint2 = endpoint1.clone();

        assert_eq!(endpoint1, endpoint2);
        assert_eq!(endpoint1.url, endpoint2.url);
        assert_eq!(endpoint1.is_local, endpoint2.is_local);
        assert_eq!(endpoint1.pool_idx, endpoint2.pool_idx);
        assert_eq!(endpoint1.set_idx, endpoint2.set_idx);
        assert_eq!(endpoint1.disk_idx, endpoint2.disk_idx);
    }

    #[test]
    fn test_endpoint_with_special_paths() {
        // Test with complex paths
        let complex_path = "/var/lib/rustfs/data/bucket1";
        let endpoint = Endpoint::try_from(complex_path).unwrap();
        assert_eq!(endpoint.get_file_path(), complex_path);
        assert!(endpoint.is_local);
        assert_eq!(endpoint.get_type(), EndpointType::Path);
    }

    #[test]
    fn test_endpoint_with_spaces_in_path() {
        let path_with_spaces = "/Users/test/Library/Application Support/rustfs/data";
        let endpoint = Endpoint::try_from(path_with_spaces).unwrap();
        assert_eq!(endpoint.get_file_path(), path_with_spaces);
        assert!(endpoint.is_local);
        assert_eq!(endpoint.get_type(), EndpointType::Path);
    }

    #[test]
    fn test_endpoint_percent_encoding_roundtrip() {
        let path_with_spaces = "/Users/test/Library/Application Support/rustfs/data";
        let endpoint = Endpoint::try_from(path_with_spaces).unwrap();

        // Verify that the URL internally stores percent-encoded path
        assert!(
            endpoint.url.path().contains("%20"),
            "URL path should contain percent-encoded spaces: {}",
            endpoint.url.path()
        );

        // Verify that get_file_path() decodes the percent-encoded path correctly
        assert_eq!(
            endpoint.get_file_path(),
            "/Users/test/Library/Application Support/rustfs/data",
            "get_file_path() should decode percent-encoded spaces"
        );
    }

    #[test]
    fn test_endpoint_with_various_special_characters() {
        // Test path with multiple special characters that get percent-encoded
        let path_with_special = "/tmp/test path/data[1]/file+name&more";
        let endpoint = Endpoint::try_from(path_with_special).unwrap();

        // get_file_path() should return the original path with decoded characters
        assert_eq!(endpoint.get_file_path(), path_with_special);
    }

    #[test]
    fn test_endpoint_update_is_local() {
        let mut endpoint = Endpoint::try_from("http://localhost:9000/path").unwrap();
        let result = endpoint.update_is_local(9000);
        assert!(result.is_ok());

        let mut file_endpoint = Endpoint::try_from("/tmp/data").unwrap();
        let result = file_endpoint.update_is_local(9000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_url_parse_from_file_path() {
        let result = url_parse_from_file_path("/tmp/test");
        assert!(result.is_ok());

        let url = result.unwrap();
        assert_eq!(url.scheme(), "file");
    }

    #[test]
    fn test_endpoint_hash() {
        use std::collections::HashSet;

        let endpoint1 = Endpoint::try_from("/tmp/data1").unwrap();
        let endpoint2 = Endpoint::try_from("/tmp/data2").unwrap();
        let endpoint3 = endpoint1.clone();

        let mut set = HashSet::new();
        set.insert(endpoint1);
        set.insert(endpoint2);
        set.insert(endpoint3); // Should not be added as it's equal to endpoint1

        assert_eq!(set.len(), 2);
    }
}
