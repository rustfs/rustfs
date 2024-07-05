use super::disks_layout::DisksLayout;
use super::error::{Error, Result};
use super::utils::net;
use path_absolutize::Absolutize;
use path_clean::PathClean;
use std::collections::HashSet;
use std::fmt::Display;
use std::{collections::HashMap, path::Path, usize};
use url::{ParseError, Url};

/// enum for endpoint type.
#[derive(PartialEq, Eq, Debug)]
pub enum EndpointType {
    /// path style endpoint type enum.
    Path,

    /// URL style endpoint type enum.
    Url,
}

/// enum for setup type.
#[derive(PartialEq, Eq)]
pub enum SetupType {
    /// FS setup type enum.
    FS,

    /// Erasure single drive setup enum.
    ErasureSD,

    /// Erasure setup type enum.
    Erasure,

    /// Distributed Erasure setup type enum.
    DistErasure,
}

/// holds information about a node in this cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub url: url::Url,
    pub pools: Vec<usize>,
    pub is_local: bool,
    pub grid_host: String,
}

/// any type of endpoint.
#[derive(Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub url: url::Url,
    pub is_local: bool,

    pub pool_idx: Option<usize>,
    pub set_idx: Option<usize>,
    pub disk_idx: Option<usize>,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.url.scheme() == "file" {
            write!(f, "{}", self.url.path())
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
        /// check whether given path is not empty.
        fn is_empty_path(path: impl AsRef<Path>) -> bool {
            ["", "/", "\\"].iter().any(|&v| Path::new(v).eq(path.as_ref()))
        }

        if is_empty_path(value) {
            return Err(Error::from_string("empty or root endpoint is not supported"));
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

                let path = Path::new(url.path()).clean();
                if is_empty_path(&path) {
                    return Err(Error::from_string("empty or root path is not supported in URL endpoint"));
                }

                if let Some(v) = path.to_str() {
                    url.set_path(v)
                }

                // On windows having a preceding SlashSeparator will cause problems, if the
                // command line already has C:/<export-folder/ in it. Final resulting
                // path on windows might become C:/C:/ this will cause problems
                // of starting rustfs server properly in distributed mode on windows.
                // As a special case make sure to trim the separator.

                // NOTE: It is also perfectly fine for windows users to have a path
                // without C:/ since at that point we treat it as relative path
                // and obtain the full filesystem path as well. Providing C:/
                // style is necessary to provide paths other than C:/,
                // such as F:/, D:/ etc.
                //
                // Another additional benefit here is that this style also
                // supports providing \\host\share support as well.
                #[cfg(windows)]
                {
                    let path = url.path().to_owned();
                    if Path::new(&path[1..]).is_absolute() {
                        url.set_path(&path[1..]);
                    }
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
            pool_idx: None,
            set_idx: None,
            disk_idx: None,
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
        self.pool_idx = Some(idx)
    }

    /// sets a specific set number to this node
    pub fn set_set_index(&mut self, idx: usize) {
        self.set_idx = Some(idx)
    }

    /// sets a specific disk number to this node
    pub fn set_disk_index(&mut self, idx: usize) {
        self.disk_idx = Some(idx)
    }

    /// resolves the host and updates if it is local or not.
    fn update_is_local(&mut self, local_port: u16) -> Result<()> {
        match (self.url.scheme(), self.url.host()) {
            (v, Some(host)) if v != "file" => {
                self.is_local = net::is_local_host(host, self.url.port().unwrap_or_default(), local_port)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// returns the host to be used for grid connections.
    fn grid_host(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => format!("{}://{}:{}", self.url.scheme(), host, port),
            (Some(host), None) => format!("{}://{}", self.url.scheme(), host),
            _ => String::new(),
        }
    }

    fn host_port(&self) -> String {
        match (self.url.host(), self.url.port()) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            (Some(host), None) => format!("{}", host),
            _ => String::new(),
        }
    }
}

/// list of same type of endpoint.
#[derive(Debug, Default)]
pub struct Endpoints(Vec<Endpoint>);

impl AsRef<Vec<Endpoint>> for Endpoints {
    fn as_ref(&self) -> &Vec<Endpoint> {
        &self.0
    }
}

impl AsMut<Vec<Endpoint>> for Endpoints {
    fn as_mut(&mut self) -> &mut Vec<Endpoint> {
        &mut self.0
    }
}

impl From<Vec<Endpoint>> for Endpoints {
    fn from(v: Vec<Endpoint>) -> Self {
        Self(v)
    }
}

impl TryFrom<&[String]> for Endpoints {
    type Error = Error;

    /// returns new endpoint list based on input args.
    fn try_from(args: &[String]) -> Result<Self> {
        let mut endpoint_type = None;
        let mut schema = None;
        let mut endpoints = Vec::with_capacity(args.len());
        let mut uniq_set = HashSet::with_capacity(args.len());

        // Loop through args and adds to endpoint list.
        for (i, arg) in args.iter().enumerate() {
            let endpoint = match Endpoint::try_from(arg.as_str()) {
                Ok(ep) => ep,
                Err(e) => return Err(Error::from_string(format!("'{}': {}", arg, e))),
            };

            // All endpoints have to be same type and scheme if applicable.
            if i == 0 {
                endpoint_type = Some(endpoint.get_type());
                schema = Some(endpoint.url.scheme().to_owned());
            } else if Some(endpoint.get_type()) != endpoint_type {
                return Err(Error::from_string("mixed style endpoints are not supported"));
            } else if Some(endpoint.url.scheme()) != schema.as_deref() {
                return Err(Error::from_string("mixed scheme is not supported"));
            }

            // Check for duplicate endpoints.
            let endpoint_str = endpoint.to_string();
            if uniq_set.contains(&endpoint_str) {
                return Err(Error::from_string("duplicate endpoints found"));
            }

            uniq_set.insert(endpoint_str);
            endpoints.push(endpoint);
        }

        Ok(Endpoints(endpoints))
    }
}

/// a temporary type to holds the list of endpoints
pub struct PoolEndpointList {
    inner: Vec<Endpoints>,
    setup_type: SetupType,
}

impl AsRef<Vec<Endpoints>> for PoolEndpointList {
    fn as_ref(&self) -> &Vec<Endpoints> {
        &self.inner
    }
}

impl AsMut<Vec<Endpoints>> for PoolEndpointList {
    fn as_mut(&mut self) -> &mut Vec<Endpoints> {
        &mut self.inner
    }
}

impl PoolEndpointList {
    /// creates a list of endpoints per pool, resolves their relevant
    /// hostnames and discovers those are local or remote.
    pub fn create_pool_endpoints(server_addr: &str, disks_layout: &DisksLayout) -> Result<Self> {
        if disks_layout.is_empty_layout() {
            return Err(Error::from_string("invalid number of endpoints"));
        }

        let server_addr = net::check_local_server_addr(server_addr)?;

        // For single arg, return single drive EC setup.
        if disks_layout.is_single_drive_layout() {
            let mut endpoint = Endpoint::try_from(disks_layout.get_single_drive_layout())?;
            endpoint.update_is_local(server_addr.port())?;

            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(0);

            // TODO Check for cross device mounts if any.

            return Ok(Self {
                inner: vec![Endpoints::from(vec![endpoint])],
                setup_type: SetupType::ErasureSD,
            });
        }

        let mut pool_endpoints = Vec::<Endpoints>::with_capacity(disks_layout.as_ref().len());
        for (pool_idx, pool) in disks_layout.as_ref().iter().enumerate() {
            let mut endpoints = Endpoints::default();
            for (set_idx, set_layout) in pool.as_ref().iter().enumerate() {
                // Convert args to endpoints
                let mut eps = Endpoints::try_from(set_layout.as_slice())?;

                // TODO Check for cross device mounts if any.

                for (disk_idx, ep) in eps.as_mut().iter_mut().enumerate() {
                    ep.set_pool_index(pool_idx);
                    ep.set_set_index(set_idx);
                    ep.set_disk_index(disk_idx);
                }

                endpoints.as_mut().append(eps.as_mut());
            }

            if endpoints.as_ref().is_empty() {
                return Err(Error::from_string("invalid number of endpoints"));
            }

            pool_endpoints.push(endpoints);
        }

        // setup type
        let mut unique_args = HashSet::new();
        for pool in pool_endpoints.iter() {
            for ep in pool.as_ref() {
                if let Some(host) = ep.url.host_str() {
                    unique_args.insert(host.to_owned());
                } else {
                    unique_args.insert(format!("localhost:{}", server_addr.port()));
                }
            }
        }

        let setup_type = match pool_endpoints[0].as_ref()[0].get_type() {
            EndpointType::Path => SetupType::Erasure,
            EndpointType::Url => match unique_args.len() {
                1 => SetupType::Erasure,
                _ => SetupType::DistErasure,
            },
        };

        let mut pool_endpoint_list = Self {
            inner: pool_endpoints,
            setup_type,
        };

        pool_endpoint_list.update_is_local()?;

        Ok(pool_endpoint_list)
    }

    fn update_is_local(&mut self) -> Result<()> {
        unimplemented!()
    }
}

/// represent endpoints in a given pool
/// along with its setCount and setDriveCount.
#[derive(Debug)]
pub struct PoolEndpoints {
    // indicates if endpoints are provided in non-ellipses style
    pub legacy: bool,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoints: Endpoints,
    pub cmd_line: String,
    pub platform: String,
}

/// list of list of endpoints
#[derive(Debug)]
pub struct EndpointServerPools(Vec<PoolEndpoints>);

impl From<Vec<PoolEndpoints>> for EndpointServerPools {
    fn from(v: Vec<PoolEndpoints>) -> Self {
        Self(v)
    }
}

impl AsRef<Vec<PoolEndpoints>> for EndpointServerPools {
    fn as_ref(&self) -> &Vec<PoolEndpoints> {
        &self.0
    }
}

impl AsMut<Vec<PoolEndpoints>> for EndpointServerPools {
    fn as_mut(&mut self) -> &mut Vec<PoolEndpoints> {
        &mut self.0
    }
}

impl EndpointServerPools {
    /// validates and creates new endpoints from input args, supports
    /// both ellipses and without ellipses transparently.
    pub fn create_server_endpoints(server_addr: &str, disks_layout: &DisksLayout) -> Result<(EndpointServerPools, SetupType)> {
        if disks_layout.as_ref().is_empty() {
            return Err(Error::from_string("Invalid arguments specified"));
        }

        let pool_eps = PoolEndpointList::create_pool_endpoints(server_addr, disks_layout)?;

        let mut ret: EndpointServerPools = Vec::with_capacity(pool_eps.as_ref().len()).into();
        for (i, eps) in pool_eps.inner.into_iter().enumerate() {
            let layout = disks_layout.get_layout(i);
            let set_count = layout.map_or(0, |v| v.count());
            let drives_per_set = layout.map_or(0, |v| v.as_ref().first().map_or(0, |v| v.len()));
            let cmd_line = layout.map_or(String::new(), |v| v.get_cmd_line().to_owned());

            let ep = PoolEndpoints {
                legacy: disks_layout.legacy,
                set_count,
                drives_per_set,
                endpoints: eps,
                cmd_line,
                platform: String::new(),
            };

            ret.add(ep)?;
        }

        Ok((ret, pool_eps.setup_type))
    }

    /// add pool endpoints
    pub fn add(&mut self, eps: PoolEndpoints) -> Result<()> {
        let mut exits = HashSet::new();
        for peps in self.0.iter() {
            for ep in peps.endpoints.as_ref() {
                exits.insert(ep.to_string());
            }
        }

        for ep in eps.endpoints.as_ref() {
            if exits.contains(&ep.to_string()) {
                return Err(Error::from_string("duplicate endpoints found"));
            }
        }

        self.0.push(eps);

        Ok(())
    }

    /// returns a sorted list of nodes in this cluster
    pub fn get_nodes(&self) -> Vec<Node> {
        let mut node_map = HashMap::new();

        for pool in self.0.iter() {
            for ep in pool.endpoints.as_ref() {
                let n = node_map.entry(ep.host_port()).or_insert(Node {
                    url: ep.url.clone(),
                    pools: vec![],
                    is_local: ep.is_local,
                    grid_host: ep.grid_host(),
                });

                if let Some(pool_idx) = ep.pool_idx {
                    if !n.pools.contains(&pool_idx) {
                        n.pools.push(pool_idx);
                    }
                }
            }
        }

        let mut nodes: Vec<Node> = node_map.into_values().collect();

        nodes.sort_by(|a, b| a.grid_host.cmp(&b.grid_host));

        nodes
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
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
                }),
                expected_type: Some(EndpointType::Path),
                expected_err: None,
            },
            TestCase {
                arg: "https://example.org/path",
                expected_endpoint: Some(Endpoint {
                    url: u2,
                    is_local: false,
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
                }),
                expected_type: Some(EndpointType::Url),
                expected_err: None,
            },
            TestCase {
                arg: "http://192.168.253.200/path",
                expected_endpoint: Some(Endpoint {
                    url: u4,
                    is_local: false,
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
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
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
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

    #[test]
    fn test_new_endpoints() {
        let test_cases = [
            (vec!["/d1", "/d2", "/d3", "/d4"], None, 1),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d3",
                    "http://localhost/d4",
                ],
                None,
                2,
            ),
            (
                vec![
                    "http://example.org/d1",
                    "http://example.com/d1",
                    "http://example.net/d1",
                    "http://example.edu/d1",
                ],
                None,
                3,
            ),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://example.org/d1",
                    "http://example.org/d2",
                ],
                None,
                4,
            ),
            (
                vec![
                    "https://localhost:9000/d1",
                    "https://localhost:9001/d2",
                    "https://localhost:9002/d3",
                    "https://localhost:9003/d4",
                ],
                None,
                5,
            ),
            // It is valid WRT endpoint list that same path is expected with different port on same server.
            (
                vec![
                    "https://127.0.0.1:9000/d1",
                    "https://127.0.0.1:9001/d1",
                    "https://127.0.0.1:9002/d1",
                    "https://127.0.0.1:9003/d1",
                ],
                None,
                6,
            ),
            (vec!["d1", "d2", "d3", "d1"], Some(Error::from_string("duplicate endpoints found")), 7),
            (vec!["d1", "d2", "d3", "./d1"], Some(Error::from_string("duplicate endpoints found")), 8),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d1",
                    "http://localhost/d4",
                ],
                Some(Error::from_string("duplicate endpoints found")),
                9,
            ),
            (
                vec!["ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"],
                Some(Error::from_string("'ftp://server/d1': invalid URL endpoint format")),
                10,
            ),
            (
                vec!["d1", "http://localhost/d2", "d3", "d4"],
                Some(Error::from_string("mixed style endpoints are not supported")),
                11,
            ),
            (
                vec![
                    "http://example.org/d1",
                    "https://example.com/d1",
                    "http://example.net/d1",
                    "https://example.edut/d1",
                ],
                Some(Error::from_string("mixed scheme is not supported")),
                12,
            ),
            (
                vec![
                    "192.168.1.210:9000/tmp/dir0",
                    "192.168.1.210:9000/tmp/dir1",
                    "192.168.1.210:9000/tmp/dir2",
                    "192.168.110:9000/tmp/dir3",
                ],
                Some(Error::from_string(
                    "'192.168.1.210:9000/tmp/dir0': invalid URL endpoint format: missing scheme http or https",
                )),
                13,
            ),
        ];

        for test_case in test_cases {
            let args: Vec<String> = test_case.0.iter().map(|v| v.to_string()).collect();
            let ret = Endpoints::try_from(args.as_slice());

            match (test_case.1, ret) {
                (None, Err(e)) => panic!("{}: error: expected = <nil>, got = {}", test_case.2, e),
                (None, Ok(_)) => {}
                (Some(e), Ok(_)) => panic!("{}: error: expected = {}, got = <nil>", test_case.2, e),
                (Some(e), Err(e2)) => {
                    assert_eq!(e.to_string(), e2.to_string(), "{}: error: expected = {}, got = {}", test_case.2, e, e2)
                }
            }
        }
    }
}
