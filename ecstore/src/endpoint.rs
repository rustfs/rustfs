use super::disks_layout::DisksLayout;
use super::error::{Error, Result};
use super::utils::net;
use path_absolutize::Absolutize;
use path_clean::PathClean;
use std::collections::HashSet;
use std::fmt::Display;
use std::net::IpAddr;
use std::sync::Arc;
use std::{
    collections::{hash_map::Entry, HashMap},
    path::Path,
    usize,
};
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
#[derive(PartialEq, Eq, Debug)]
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

        pool_endpoint_list.update_is_local(server_addr.port())?;

        for endpoints in pool_endpoint_list.inner.iter() {
            // Check whether same path is not used in endpoints of a host on different port.
            let mut path_ip_map: HashMap<&str, Arc<HashSet<IpAddr>>> = HashMap::new();
            let mut host_ip_cache = HashMap::new();
            for ep in endpoints.as_ref() {
                let mut host_ip_set = Arc::new(HashSet::<IpAddr>::new());

                if let Some(host) = ep.url.host() {
                    if let Entry::Vacant(e) = host_ip_cache.entry(host.clone()) {
                        host_ip_set = Arc::new(
                            net::get_host_ip(host.clone())
                                .map_err(|e| Error::from_string(format!("host '{}' cannot resolve: {}", host, e)))?,
                        );
                        e.insert(host_ip_set.clone());
                    }
                }

                let path = ep.url.path();
                match path_ip_map.entry(path) {
                    Entry::Occupied(e) => {
                        if e.get().intersection(host_ip_set.as_ref()).count() > 0 {
                            return Err(Error::from_string(format!(
                                "same path '{}' can not be served by different port on same address",
                                path
                            )));
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(host_ip_set.clone());
                    }
                }
            }

            // Check whether same path is used for more than 1 local endpoints.
            let mut local_path_set = HashSet::new();
            for ep in endpoints.as_ref() {
                if !ep.is_local {
                    continue;
                }

                let path = ep.url.path();
                if local_path_set.contains(path) {
                    return Err(Error::from_string(format!(
                        "path '{}' cannot be served by different address on same server",
                        path
                    )));
                }
                local_path_set.insert(path);
            }

            // Here all endpoints are URL style.
            let mut ep_path_set = HashSet::new();
            let mut local_server_host_set = HashSet::new();
            let mut local_port_set = HashSet::new();
            let mut local_endpoint_count = 0;

            for ep in endpoints.as_ref() {
                ep_path_set.insert(ep.url.path());
                if ep.is_local && ep.url.has_host() {
                    local_server_host_set.insert(ep.url.host());
                    local_port_set.insert(ep.url.port());
                    local_endpoint_count += 1;
                }
            }

            // All endpoints are pointing to local host
            if endpoints.as_ref().len() == local_endpoint_count {
                // If all endpoints have same port number, Just treat it as local erasure setup
                // using URL style endpoints.
                if local_port_set.len() == 1 && local_server_host_set.len() > 1 {
                    return Err(Error::from_string("all local endpoints should not have different hostnames/ips"));
                }
            }
        }

        Ok(pool_endpoint_list)
    }

    /// resolves all hosts and discovers which are local
    fn update_is_local(&mut self, local_port: u16) -> Result<()> {
        for endpoints in self.inner.iter_mut() {
            for ep in endpoints.as_mut() {
                match ep.url.host() {
                    None => {
                        ep.is_local = true;
                    }
                    Some(host) => {
                        ep.is_local = net::is_local_host(host, ep.url.port().unwrap_or_default(), local_port)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// resolves all hosts and discovers which are local
    fn _update_is_local(&mut self, local_port: u16) -> Result<()> {
        let mut eps_resolved = 0;
        let mut found_local = false;
        let mut resolved_set: HashSet<(usize, usize)> = HashSet::new();
        let ep_count: usize = self.inner.iter().map(|v| v.as_ref().len()).sum();

        loop {
            // Break if the local endpoint is found already Or all the endpoints are resolved.
            if found_local || eps_resolved == ep_count {
                break;
            }

            for (i, endpoints) in self.inner.iter_mut().enumerate() {
                for (j, ep) in endpoints.as_mut().iter_mut().enumerate() {
                    if resolved_set.contains(&(i, j)) {
                        // Continue if host is already resolved.
                        continue;
                    }

                    match ep.url.host() {
                        None => {
                            if !found_local {
                                found_local = true;
                            }
                            ep.is_local = true;
                            eps_resolved += 1;
                            resolved_set.insert((i, j));
                            continue;
                        }
                        Some(host) => match net::is_local_host(host, ep.url.port().unwrap_or_default(), local_port) {
                            Ok(is_local) => {
                                if !found_local {
                                    found_local = is_local;
                                }
                                ep.is_local = is_local;
                                eps_resolved += 1;
                                resolved_set.insert((i, j));
                            }
                            Err(err) => {
                                // TODO Retry infinitely on Kubernetes and Docker swarm?
                                return Err(err);
                            }
                        },
                    }
                }
            }
        }

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

    #[test]
    fn test_create_pool_endpoints() {
        #[derive(Default)]
        struct TestCase<'a> {
            num: usize,
            server_addr: &'a str,
            args: Vec<&'a str>,
            expected_endpoints: Option<Endpoints>,
            expected_setup_type: Option<SetupType>,
            expected_err: Option<Error>,
        }

        // Filter ipList by IPs those do not start with '127.'.
        let non_loop_back_i_ps =
            net::must_get_local_ips().map_or(vec![], |v| v.into_iter().filter(|ip| ip.is_ipv4() && ip.is_loopback()).collect());
        if non_loop_back_i_ps.is_empty() {
            panic!("No non-loop back IP address found for this host");
        }
        let non_loop_back_ip = non_loop_back_i_ps[0];

        let case1_endpoint1 = format!("http://{}/d1", non_loop_back_ip);
        let case1_endpoint2 = format!("http://{}/d2", non_loop_back_ip);
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:10000/d2", non_loop_back_ip),
            "http://example.org:10000/d3".to_string(),
            "http://example.com:10000/d4".to_string(),
        ];
        let (case1_ur_ls, case1_local_flags) = get_expected_endpoints(args, format!("http://{}:10000/", non_loop_back_ip));

        let case2_endpoint1 = format!("http://{}/d1", non_loop_back_ip);
        let case2_endpoint2 = format!("http://{}:9000/d2", non_loop_back_ip);
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:9000/d2", non_loop_back_ip),
            "http://example.org:10000/d3".to_string(),
            "http://example.com:10000/d4".to_string(),
        ];
        let (case2_ur_ls, case2_local_flags) = get_expected_endpoints(args, format!("http://{}:10000/", non_loop_back_ip));

        let case3_endpoint1 = format!("http://{}/d1", non_loop_back_ip);
        let args = vec![
            format!("http://{}:80/d1", non_loop_back_ip),
            "http://example.org:9000/d2".to_string(),
            "http://example.com:80/d3".to_string(),
            "http://example.net:80/d4".to_string(),
        ];
        let (case3_ur_ls, case3_local_flags) = get_expected_endpoints(args, format!("http://{}:80/", non_loop_back_ip));

        let case4_endpoint1 = format!("http://{}/d1", non_loop_back_ip);
        let args = vec![
            format!("http://{}:9000/d1", non_loop_back_ip),
            "http://example.org:9000/d2".to_string(),
            "http://example.com:9000/d3".to_string(),
            "http://example.net:9000/d4".to_string(),
        ];
        let (case4_ur_ls, case4_local_flags) = get_expected_endpoints(args, format!("http://{}:9000/", non_loop_back_ip));

        let case5_endpoint1 = format!("http://{}:9000/d1", non_loop_back_ip);
        let case5_endpoint2 = format!("http://{}:9001/d2", non_loop_back_ip);
        let case5_endpoint3 = format!("http://{}:9002/d3", non_loop_back_ip);
        let case5_endpoint4 = format!("http://{}:9003/d4", non_loop_back_ip);
        let args = vec![
            case5_endpoint1.clone(),
            case5_endpoint2.clone(),
            case5_endpoint3.clone(),
            case5_endpoint4.clone(),
        ];
        let (case5_ur_ls, case5_local_flags) = get_expected_endpoints(args, format!("http://{}:9000/", non_loop_back_ip));

        let case6_endpoint1 = format!("http://{}:9003/d4", non_loop_back_ip);
        let args = vec![
            "http://localhost:9000/d1".to_string(),
            "http://localhost:9001/d2".to_string(),
            "http://127.0.0.1:9002/d3".to_string(),
            case6_endpoint1.clone(),
        ];
        let (case6_ur_ls, case6_local_flags) = get_expected_endpoints(args, format!("http://{}:9003/", non_loop_back_ip));

        let case7_endpoint1 = format!("http://{}:9001/export", non_loop_back_ip);
        let case7_endpoint2 = format!("http://{}:9000/export", non_loop_back_ip);

        let test_cases = [
            TestCase {
                num: 1,
                server_addr: "localhost",
                expected_err: Some(Error::from_string("address localhost: missing port in address")),
                ..Default::default()
            },
            // Erasure Single Drive
            TestCase {
                num: 2,
                server_addr: "localhost:9000",
                args: vec!["http://localhost/d1"],
                expected_err: Some(Error::from_string("use path style endpoint for SD setup")),
                ..Default::default()
            },
            TestCase {
                num: 3,
                server_addr: ":443",
                args: vec!["/d1"],
                expected_endpoints: Some(Endpoints(vec![Endpoint {
                    url: must_file_path("/d1"),
                    is_local: true,
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
                }])),
                expected_setup_type: Some(SetupType::ErasureSD),
                ..Default::default()
            },
            TestCase {
                num: 4,
                server_addr: "localhost:10000",
                args: vec!["/d1"],
                expected_endpoints: Some(Endpoints(vec![Endpoint {
                    url: must_file_path("/d1"),
                    is_local: true,
                    pool_idx: None,
                    set_idx: None,
                    disk_idx: None,
                }])),
                expected_setup_type: Some(SetupType::ErasureSD),
                ..Default::default()
            },
            TestCase {
                num: 5,
                server_addr: "localhost:9000",
                args: vec![
                    "https://127.0.0.1:9000/d1",
                    "https://localhost:9001/d1",
                    "https://example.com/d1",
                    "https://example.com/d2",
                ],
                expected_err: Some(Error::from_string("path '/d1' can not be served by different port on same address")),
                ..Default::default()
            },
            // Erasure Setup with PathEndpointType
            TestCase {
                num: 6,
                server_addr: ":1234",
                args: vec!["/d1", "/d2", "/d3", "/d4"],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: must_file_path("/d1"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_file_path("/d2"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_file_path("/d3"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_file_path("/d4"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::Erasure),
                ..Default::default()
            },
            // DistErasure Setup with URLEndpointType
            TestCase {
                num: 7,
                server_addr: ":9000",
                args: vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d3",
                    "http://localhost/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: must_url("http://localhost:9000/d1"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d2"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d3"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d4"),
                        is_local: true,
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::Erasure),
                ..Default::default()
            },
            // DistErasure Setup with URLEndpointType having mixed naming to local host.
            TestCase {
                num: 8,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://127.0.0.1/d3",
                    "http://127.0.0.1/d4",
                ],
                expected_err: Some(Error::from_string("all local endpoints should not have different hostnames/ips")),
                ..Default::default()
            },
            TestCase {
                num: 9,
                server_addr: ":9001",
                args: vec![
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                    case7_endpoint1.as_str(),
                    "http://10.0.0.2:9001/export",
                ],
                expected_err: Some(Error::from_string("path '/export' can not be served by different port on same address")),
                ..Default::default()
            },
            TestCase {
                num: 10,
                server_addr: ":9000",
                args: vec![
                    "http://127.0.0.1:9000/export",
                    case7_endpoint2.as_str(),
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                ],
                expected_err: Some(Error::from_string("path '/export' cannot be served by different address on same server")),
                ..Default::default()
            },
            // DistErasure type
            TestCase {
                num: 11,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    case1_endpoint1.as_str(),
                    case1_endpoint2.as_str(),
                    "http://example.org/d3",
                    "http://example.com/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case1_ur_ls[0].clone(),
                        is_local: case1_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case1_ur_ls[1].clone(),
                        is_local: case1_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case1_ur_ls[2].clone(),
                        is_local: case1_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case1_ur_ls[3].clone(),
                        is_local: case1_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 12,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    case2_endpoint1.as_str(),
                    case2_endpoint2.as_str(),
                    "http://example.org/d3",
                    "http://example.com/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case2_ur_ls[0].clone(),
                        is_local: case2_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case2_ur_ls[1].clone(),
                        is_local: case2_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case2_ur_ls[2].clone(),
                        is_local: case2_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case2_ur_ls[3].clone(),
                        is_local: case2_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 13,
                server_addr: ":80",
                args: vec![
                    case3_endpoint1.as_str(),
                    "http://example.org:9000/d2",
                    "http://example.com/d3",
                    "http://example.net/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case3_ur_ls[0].clone(),
                        is_local: case3_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case3_ur_ls[1].clone(),
                        is_local: case3_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case3_ur_ls[2].clone(),
                        is_local: case3_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case3_ur_ls[3].clone(),
                        is_local: case3_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 14,
                server_addr: ":9000",
                args: vec![
                    case4_endpoint1.as_str(),
                    "http://example.org/d2",
                    "http://example.com/d3",
                    "http://example.net/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case4_ur_ls[0].clone(),
                        is_local: case4_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case4_ur_ls[1].clone(),
                        is_local: case4_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case4_ur_ls[2].clone(),
                        is_local: case4_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case4_ur_ls[3].clone(),
                        is_local: case4_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 15,
                server_addr: ":9000",
                args: vec![
                    case5_endpoint1.as_str(),
                    case5_endpoint2.as_str(),
                    case5_endpoint3.as_str(),
                    case5_endpoint4.as_str(),
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case5_ur_ls[0].clone(),
                        is_local: case5_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case5_ur_ls[1].clone(),
                        is_local: case5_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case5_ur_ls[2].clone(),
                        is_local: case5_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case5_ur_ls[3].clone(),
                        is_local: case5_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 16,
                server_addr: ":9003",
                args: vec![
                    "http://localhost:9000/d1",
                    "http://localhost:9001/d2",
                    "http://127.0.0.1:9002/d3",
                    case6_endpoint1.as_str(),
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case6_ur_ls[0].clone(),
                        is_local: case6_local_flags[0],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case6_ur_ls[1].clone(),
                        is_local: case6_local_flags[1],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case6_ur_ls[2].clone(),
                        is_local: case6_local_flags[2],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                    Endpoint {
                        url: case6_ur_ls[3].clone(),
                        is_local: case6_local_flags[3],
                        pool_idx: None,
                        set_idx: None,
                        disk_idx: None,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
        ];

        for test_case in test_cases {
            let disks_layout = match DisksLayout::try_from(test_case.args.as_slice()) {
                Ok(v) => v,
                Err(e) => panic!("Test {}: unexpected error: {}", test_case.num, e),
            };

            match (
                test_case.expected_err,
                PoolEndpointList::create_pool_endpoints(test_case.server_addr, &disks_layout),
            ) {
                (None, Err(err)) => panic!("Test {}: error: expected = <nil>, got = {}", test_case.num, err),
                (Some(err), Ok(_)) => panic!("Test {}: error: expected = {}, got = <nil>", test_case.num, err),
                (Some(e), Err(e2)) => {
                    assert_eq!(
                        e.to_string(),
                        e2.to_string(),
                        "Test {}: error: expected = {}, got = {}",
                        test_case.num,
                        e,
                        e2
                    )
                }
                (None, Ok(pools)) => {
                    if Some(&pools.setup_type) != test_case.expected_setup_type.as_ref() {
                        panic!(
                            "Test {}: setupType: expected = {:?}, got = {:?}",
                            test_case.num, test_case.expected_setup_type, &pools.setup_type
                        )
                    }

                    let left_len = test_case.expected_endpoints.as_ref().map(|v| v.as_ref().len());
                    let right_len = pools.as_ref().first().map(|v| v.as_ref().len());

                    if left_len != right_len {
                        panic!("Test {}: endpoints: expected = {:?}, got = {:?}", test_case.num, left_len, right_len);
                    }

                    for (i, ep) in pools.as_ref()[0].as_ref().iter().enumerate() {
                        assert_eq!(
                            ep.to_string(),
                            test_case.expected_endpoints.as_ref().unwrap().as_ref()[i].to_string(),
                            "Test {}: endpoints: expected = {}, got = {}",
                            test_case.num,
                            test_case.expected_endpoints.as_ref().unwrap().as_ref()[i],
                            ep
                        )
                    }
                }
            }
        }
    }

    fn must_file_path(s: impl AsRef<Path>) -> url::Url {
        let url = url::Url::from_file_path(s.as_ref());

        assert!(url.is_ok(), "failed to convert path to URL: {}", s.as_ref().display());

        url.unwrap()
    }

    fn must_url(s: &str) -> url::Url {
        url::Url::parse(s).unwrap()
    }

    fn get_expected_endpoints(args: Vec<String>, prefix: String) -> (Vec<url::Url>, Vec<bool>) {
        let mut urls = vec![];
        let mut local_flags = vec![];
        for arg in args {
            urls.push(url::Url::parse(&arg).unwrap());
            local_flags.push(arg.starts_with(&prefix));
        }

        (urls, local_flags)
    }
}
