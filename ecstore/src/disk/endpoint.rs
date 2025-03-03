use crate::error::{Error, Result};
use crate::utils::net;
use path_absolutize::Absolutize;
use path_clean::PathClean;
use std::{fmt::Display, path::Path};
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
    pub url: url::Url,
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
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // check whether given path is not empty.
        if ["", "/", "\\"].iter().any(|&v| v.eq(value)) {
            return Err(ErrorCode::EmptyPath().with_message("empty or root endpoint is not supported"));
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
                    return Err(Error::from_string("invalid URL endpoint format"));
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

                if path.parent().is_none() || Path::new("").eq(&path) {
                    return Err(Error::from_string("empty or root path is not supported in URL endpoint"));
                }

                match path.to_str() {
                    Some(v) => url.set_path(v),
                    None => return Err(Error::from_string("invalid path")),
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
                    return Err(Error::from_string("invalid URL endpoint format: port number must be between 1 to 65535"))
                }
                ParseError::EmptyHost => return Err(Error::from_string("invalid URL endpoint format: empty host name")),
                ParseError::RelativeUrlWithoutBase => {
                    // like /foo
                    is_local = true;
                    url_parse_from_file_path(value)?
                }
                _ => return Err(Error::from_string(format!("invalid URL endpoint format: {}", e))),
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
                self.is_local = net::is_local_host(host, self.url.port().unwrap_or_default(), local_port)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// returns the host to be used for grid connections.
    pub fn grid_host(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => format!("{}://{}:{}", self.url.scheme(), host, port),
            (Some(host), None) => format!("{}://{}", self.url.scheme(), host),
            _ => String::new(),
        }
    }

    pub fn host_port(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            (Some(host), None) => format!("{}", host),
            _ => String::new(),
        }
    }

    pub fn get_file_path(&self) -> &str {
        let path = self.url.path();

        #[cfg(windows)]
        let path = &path[1..];

        path
    }
}

/// parse a file path into an URL.
fn url_parse_from_file_path(value: &str) -> Result<url::Url> {
    // Only check if the arg is an ip address and ask for scheme since its absent.
    // localhost, example.com, any FQDN cannot be disambiguated from a regular file path such as
    // /mnt/export1. So we go ahead and start the rustfs server in FS modes in these cases.
    let addr: Vec<&str> = value.splitn(2, '/').collect();
    if net::is_socket_addr(addr[0]) {
        return Err(Error::from_string("invalid URL endpoint format: missing scheme http or https"));
    }

    let file_path = match Path::new(value).absolutize() {
        Ok(path) => path,
        Err(err) => return Err(Error::from_string(format!("absolute path failed: {}", err))),
    };

    match Url::from_file_path(file_path) {
        Ok(url) => Ok(url),
        Err(_) => Err(Error::from_string("Convert a file path into an URL failed")),
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

        let u2 = url::Url::parse("https://example.org/path").unwrap();
        let u4 = url::Url::parse("http://192.168.253.200/path").unwrap();
        let u6 = url::Url::parse("http://server:/path").unwrap();
        let root_slash_foo = url::Url::from_file_path("/foo").unwrap();

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
                expected_err: Some(Error::from_string("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "/",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "\\",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("empty or root endpoint is not supported")),
            },
            TestCase {
                arg: "c://foo",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format")),
            },
            TestCase {
                arg: "ftp://foo",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format")),
            },
            TestCase {
                arg: "http://server/path?location",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format")),
            },
            TestCase {
                arg: "http://:/path",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format: empty host name")),
            },
            TestCase {
                arg: "http://:8080/path",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format: empty host name")),
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
                expected_err: Some(Error::from_string("invalid URL endpoint format: port number must be between 1 to 65535")),
            },
            TestCase {
                arg: "http://server:8080//",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("empty or root path is not supported in URL endpoint")),
            },
            TestCase {
                arg: "http://server:8080/",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("empty or root path is not supported in URL endpoint")),
            },
            TestCase {
                arg: "192.168.1.210:9000",
                expected_endpoint: None,
                expected_type: None,
                expected_err: Some(Error::from_string("invalid URL endpoint format: missing scheme http or https")),
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
}
