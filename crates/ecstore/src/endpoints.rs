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

use rustfs_utils::{XHost, check_local_server_addr, get_host_ip, is_local_host};
use tracing::{error, info, instrument, warn};

use crate::{
    disk::endpoint::{Endpoint, EndpointType},
    disks_layout::DisksLayout,
    global::global_rustfs_port,
};
use std::io::{Error, Result};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::IpAddr,
};

/// enum for setup type.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum SetupType {
    /// starts with unknown setup type.
    Unknown,

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

/// list of same type of endpoint.
#[derive(Debug, Default, Clone)]
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

impl<T: AsRef<str>> TryFrom<&[T]> for Endpoints {
    type Error = Error;

    /// returns new endpoint list based on input args.
    fn try_from(args: &[T]) -> Result<Self> {
        let mut endpoint_type = None;
        let mut schema = None;
        let mut endpoints = Vec::with_capacity(args.len());
        let mut uniq_set = HashSet::with_capacity(args.len());

        // Loop through args and adds to endpoint list.
        for (i, arg) in args.iter().enumerate() {
            let endpoint = match Endpoint::try_from(arg.as_ref()) {
                Ok(ep) => ep,
                Err(e) => return Err(Error::other(format!("'{}': {}", arg.as_ref(), e))),
            };

            // All endpoints have to be same type and scheme if applicable.
            if i == 0 {
                endpoint_type = Some(endpoint.get_type());
                schema = Some(endpoint.url.scheme().to_owned());
            } else if Some(endpoint.get_type()) != endpoint_type {
                return Err(Error::other("mixed style endpoints are not supported"));
            } else if Some(endpoint.url.scheme()) != schema.as_deref() {
                return Err(Error::other("mixed scheme is not supported"));
            }

            // Check for duplicate endpoints.
            let endpoint_str = endpoint.to_string();
            if uniq_set.contains(&endpoint_str) {
                return Err(Error::other("duplicate endpoints found"));
            }

            uniq_set.insert(endpoint_str);
            endpoints.push(endpoint);
        }

        Ok(Endpoints(endpoints))
    }
}

impl Endpoints {
    /// Converts `self` into its inner representation.
    ///
    /// This method consumes the `self` object and returns its inner `Vec<Endpoint>`.
    /// It is useful for when you need to take the endpoints out of their container
    /// without needing a reference to the container itself.
    pub fn into_inner(self) -> Vec<Endpoint> {
        self.0
    }

    pub fn into_ref(&self) -> &Vec<Endpoint> {
        &self.0
    }

    // GetString - returns endpoint string of i-th endpoint (0-based),
    // and empty string for invalid indexes.
    pub fn get_string(&self, i: usize) -> String {
        if i >= self.0.len() {
            return "".to_string();
        }

        self.0[i].to_string()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug)]
/// a temporary type to holds the list of endpoints
struct PoolEndpointList {
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
    async fn create_pool_endpoints(server_addr: &str, disks_layout: &DisksLayout) -> Result<Self> {
        if disks_layout.is_empty_layout() {
            return Err(Error::other("invalid number of endpoints"));
        }

        let server_addr = check_local_server_addr(server_addr)?;

        // For single arg, return single drive EC setup.
        if disks_layout.is_single_drive_layout() {
            let mut endpoint = Endpoint::try_from(disks_layout.get_single_drive_layout())?;
            endpoint.update_is_local(server_addr.port())?;

            if endpoint.get_type() != EndpointType::Path {
                return Err(Error::other("use path style endpoint for single node setup"));
            }

            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(0);

            // TODO Check for cross device mounts if any.

            return Ok(Self {
                inner: vec![Endpoints::from(vec![endpoint])],
                setup_type: SetupType::ErasureSD,
            });
        }

        let mut pool_endpoints = Vec::<Endpoints>::with_capacity(disks_layout.pools.len());
        for (pool_idx, pool) in disks_layout.pools.iter().enumerate() {
            let mut endpoints = Endpoints::default();
            for (set_idx, set_layout) in pool.iter().enumerate() {
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
                return Err(Error::other("invalid number of endpoints"));
            }

            pool_endpoints.push(endpoints);
        }

        // setup type
        let mut unique_args = HashSet::new();
        let mut pool_endpoint_list = Self {
            inner: pool_endpoints,
            setup_type: SetupType::Unknown,
        };

        pool_endpoint_list.update_is_local(server_addr.port())?;

        for endpoints in pool_endpoint_list.inner.iter_mut() {
            // Check whether same path is not used in endpoints of a host on different port.
            let mut path_ip_map: HashMap<&str, HashSet<IpAddr>> = HashMap::new();
            let mut host_ip_cache = HashMap::new();
            for ep in endpoints.as_ref() {
                if !ep.url.has_host() {
                    continue;
                }

                let host = ep.url.host().unwrap();
                let host_ip_set = if let Some(set) = host_ip_cache.get(&host) {
                    info!(
                        target: "rustfs::ecstore::endpoints",
                        host = %host,
                        endpoint = %ep.to_string(),
                        from = "cache",
                        "Create pool endpoints host '{}' found in cache for endpoint '{}'", host, ep.to_string()
                    );
                    set
                } else {
                    let ips = match get_host_ip(host.clone()).await {
                        Ok(ips) => ips,
                        Err(e) => {
                            error!("Create pool endpoints host {} not found, error:{}", host, e);
                            return Err(Error::other(format!("host '{host}' cannot resolve: {e}")));
                        }
                    };
                    info!(
                        target: "rustfs::ecstore::endpoints",
                        host = %host,
                        endpoint = %ep.to_string(),
                        from = "get_host_ip",
                        "Create pool endpoints host '{}' resolved to ips {:?} for endpoint '{}'",
                        host,
                        ips,
                        ep.to_string()
                    );
                    host_ip_cache.insert(host.clone(), ips);
                    host_ip_cache.get(&host).unwrap()
                };

                let path = ep.get_file_path();
                match path_ip_map.entry(path) {
                    Entry::Occupied(mut e) => {
                        if e.get().intersection(host_ip_set).count() > 0 {
                            return Err(Error::other(format!(
                                "same path '{path}' can not be served by different port on same address"
                            )));
                        }
                        e.get_mut().extend(host_ip_set.iter());
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

                let path = ep.get_file_path();
                if local_path_set.contains(path) {
                    return Err(Error::other(format!(
                        "path '{path}' cannot be served by different address on same server"
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
                ep_path_set.insert(ep.get_file_path());
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
                    return Err(Error::other("all local endpoints should not have different hostnames/ips"));
                }
            }

            // Add missing port in all endpoints.
            for ep in endpoints.as_mut() {
                if !ep.url.has_host() {
                    unique_args.insert(format!("localhost:{}", server_addr.port()));
                    continue;
                }
                match ep.url.port() {
                    None => {
                        let _ = ep.url.set_port(Some(server_addr.port()));
                    }
                    Some(port) => {
                        // If endpoint is local, but port is different than serverAddrPort, then make it as remote.
                        if ep.is_local && server_addr.port() != port {
                            ep.is_local = false;
                        }
                    }
                }
                unique_args.insert(ep.host_port());
            }
        }

        let setup_type = match pool_endpoint_list.as_ref()[0].as_ref()[0].get_type() {
            EndpointType::Path => SetupType::Erasure,
            EndpointType::Url => match unique_args.len() {
                1 => SetupType::Erasure,
                _ => SetupType::DistErasure,
            },
        };

        pool_endpoint_list.setup_type = setup_type;

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
                        ep.is_local = is_local_host(host, ep.url.port().unwrap_or_default(), local_port)?;
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
                        Some(host) => match is_local_host(host, ep.url.port().unwrap_or_default(), local_port) {
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
#[derive(Debug, Clone)]
pub struct PoolEndpoints {
    // indicates if endpoints are provided in non-ellipses style
    pub legacy: bool,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoints: Endpoints,
    pub cmd_line: String,
    pub platform: String,
}

/// list of endpoints
#[derive(Debug, Clone, Default)]
pub struct EndpointServerPools(pub Vec<PoolEndpoints>);

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
    pub fn reset(&mut self, eps: Vec<PoolEndpoints>) {
        self.0 = eps;
    }
    pub fn legacy(&self) -> bool {
        self.0.len() == 1 && self.0[0].legacy
    }
    pub fn get_pool_idx(&self, cmd_line: &str) -> Option<usize> {
        for (idx, eps) in self.0.iter().enumerate() {
            if eps.cmd_line.as_str() == cmd_line {
                return Some(idx);
            }
        }
        None
    }
    pub async fn from_volumes(server_addr: &str, endpoints: Vec<String>) -> Result<(EndpointServerPools, SetupType)> {
        let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        Self::create_server_endpoints(server_addr, &layouts).await
    }
    /// validates and creates new endpoints from input args, supports
    /// both ellipses and without ellipses transparently.
    pub async fn create_server_endpoints(
        server_addr: &str,
        disks_layout: &DisksLayout,
    ) -> Result<(EndpointServerPools, SetupType)> {
        if disks_layout.pools.is_empty() {
            return Err(Error::other("Invalid arguments specified"));
        }

        let pool_eps = PoolEndpointList::create_pool_endpoints(server_addr, disks_layout).await?;

        let mut ret: EndpointServerPools = Vec::with_capacity(pool_eps.as_ref().len()).into();
        for (i, eps) in pool_eps.inner.into_iter().enumerate() {
            let ep = PoolEndpoints {
                legacy: disks_layout.legacy,
                set_count: disks_layout.get_set_count(i),
                drives_per_set: disks_layout.get_drives_per_set(i),
                endpoints: eps,
                cmd_line: disks_layout.get_cmd_line(i),
                platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
            };

            ret.add(ep)?;
        }

        Ok((ret, pool_eps.setup_type))
    }

    pub fn es_count(&self) -> usize {
        self.0.iter().map(|v| v.set_count).sum()
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
                return Err(Error::other("duplicate endpoints found"));
            }
        }

        self.0.push(eps);

        Ok(())
    }

    /// returns true if the first endpoint is local.
    pub fn first_local(&self) -> bool {
        self.0
            .first()
            .and_then(|v| v.endpoints.as_ref().first())
            .is_some_and(|v| v.is_local)
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

                if !n.pools.contains(&(ep.pool_idx as usize)) {
                    n.pools.push(ep.pool_idx as usize);
                }
            }
        }

        let mut nodes: Vec<Node> = node_map.into_values().collect();

        nodes.sort_by(|a, b| a.grid_host.cmp(&b.grid_host));

        nodes
    }

    #[instrument]
    pub fn hosts_sorted(&self) -> Vec<Option<XHost>> {
        let (mut peers, local) = self.peers();

        let mut ret = vec![None; peers.len()];

        peers.sort();

        for (i, peer) in peers.iter().enumerate() {
            if &local == peer {
                continue;
            }

            let host = match XHost::try_from(peer.clone()) {
                Ok(res) => res,
                Err(err) => {
                    warn!("Xhost parse failed {:?}", err);
                    continue;
                }
            };

            ret[i] = Some(host);
        }

        ret
    }
    pub fn peers(&self) -> (Vec<String>, String) {
        let mut local = None;
        let mut set = HashSet::new();
        for ep in self.0.iter() {
            for endpoint in ep.endpoints.0.iter() {
                if endpoint.get_type() != EndpointType::Url {
                    continue;
                }
                let host = endpoint.host_port();
                if endpoint.is_local && endpoint.url.port() == Some(global_rustfs_port()) && local.is_none() {
                    local = Some(host.clone());
                }

                set.insert(host);
            }
        }

        let hosts: Vec<String> = set.iter().cloned().collect();

        (hosts, local.unwrap_or_default())
    }

    pub fn find_grid_hosts_from_peer(&self, host: &XHost) -> Option<String> {
        for ep in self.0.iter() {
            for endpoint in ep.endpoints.0.iter() {
                if endpoint.is_local {
                    continue;
                }
                let xhost = match XHost::try_from(endpoint.host_port()) {
                    Ok(res) => res,
                    Err(_) => {
                        continue;
                    }
                };

                if xhost.to_string() == host.to_string() {
                    return Some(endpoint.grid_host());
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use rustfs_utils::must_get_local_ips;

    use super::*;
    use std::path::Path;

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
            (vec!["d1", "d2", "d3", "d1"], Some(Error::other("duplicate endpoints found")), 7),
            (vec!["d1", "d2", "d3", "./d1"], Some(Error::other("duplicate endpoints found")), 8),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d1",
                    "http://localhost/d4",
                ],
                Some(Error::other("duplicate endpoints found")),
                9,
            ),
            (
                vec!["ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"],
                Some(Error::other("'ftp://server/d1': io error invalid URL endpoint format")),
                10,
            ),
            (
                vec!["d1", "http://localhost/d2", "d3", "d4"],
                Some(Error::other("mixed style endpoints are not supported")),
                11,
            ),
            (
                vec![
                    "http://example.org/d1",
                    "https://example.com/d1",
                    "http://example.net/d1",
                    "https://example.edut/d1",
                ],
                Some(Error::other("mixed scheme is not supported")),
                12,
            ),
            (
                vec![
                    "192.168.1.210:9000/tmp/dir0",
                    "192.168.1.210:9000/tmp/dir1",
                    "192.168.1.210:9000/tmp/dir2",
                    "192.168.110:9000/tmp/dir3",
                ],
                Some(Error::other("'192.168.1.210:9000/tmp/dir0': io error")),
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
                    assert!(
                        e2.to_string().starts_with(&e.to_string()),
                        "{}: error: expected = {}, got = {}",
                        test_case.2,
                        e,
                        e2
                    )
                }
            }
        }
    }

    #[tokio::test]
    async fn test_create_pool_endpoints() {
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
            must_get_local_ips().map_or(vec![], |v| v.into_iter().filter(|ip| ip.is_ipv4() && ip.is_loopback()).collect());
        if non_loop_back_i_ps.is_empty() {
            panic!("No non-loop back IP address found for this host");
        }
        let non_loop_back_ip = non_loop_back_i_ps[0];

        let case1_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let case1_endpoint2 = format!("http://{non_loop_back_ip}/d2");
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:10000/d2", non_loop_back_ip),
            "http://example.org:10000/d3".to_string(),
            "http://example.com:10000/d4".to_string(),
        ];
        let (case1_ur_ls, case1_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:10000/"));

        let case2_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let case2_endpoint2 = format!("http://{non_loop_back_ip}:9000/d2");
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:9000/d2", non_loop_back_ip),
            "http://example.org:10000/d3".to_string(),
            "http://example.com:10000/d4".to_string(),
        ];
        let (case2_ur_ls, case2_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:10000/"));

        let case3_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let args = vec![
            format!("http://{}:80/d1", non_loop_back_ip),
            "http://example.org:9000/d2".to_string(),
            "http://example.com:80/d3".to_string(),
            "http://example.net:80/d4".to_string(),
        ];
        let (case3_ur_ls, case3_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:80/"));

        let case4_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let args = vec![
            format!("http://{}:9000/d1", non_loop_back_ip),
            "http://example.org:9000/d2".to_string(),
            "http://example.com:9000/d3".to_string(),
            "http://example.net:9000/d4".to_string(),
        ];
        let (case4_ur_ls, case4_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9000/"));

        let case5_endpoint1 = format!("http://{non_loop_back_ip}:9000/d1");
        let case5_endpoint2 = format!("http://{non_loop_back_ip}:9001/d2");
        let case5_endpoint3 = format!("http://{non_loop_back_ip}:9002/d3");
        let case5_endpoint4 = format!("http://{non_loop_back_ip}:9003/d4");
        let args = vec![
            case5_endpoint1.clone(),
            case5_endpoint2.clone(),
            case5_endpoint3.clone(),
            case5_endpoint4.clone(),
        ];
        let (case5_ur_ls, case5_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9000/"));

        let case6_endpoint1 = format!("http://{non_loop_back_ip}:9003/d4");
        let args = vec![
            "http://localhost:9000/d1".to_string(),
            "http://localhost:9001/d2".to_string(),
            "http://127.0.0.1:9002/d3".to_string(),
            case6_endpoint1.clone(),
        ];
        let (case6_ur_ls, case6_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9003/"));

        let case7_endpoint1 = format!("http://{non_loop_back_ip}:9001/export");
        let case7_endpoint2 = format!("http://{non_loop_back_ip}:9000/export");

        let test_cases = [
            TestCase {
                num: 1,
                server_addr: "localhost",
                expected_err: Some(Error::other("address localhost: missing port in address")),
                ..Default::default()
            },
            // Erasure Single Drive
            TestCase {
                num: 2,
                server_addr: "localhost:9000",
                args: vec!["http://localhost/d1"],
                expected_err: Some(Error::other("use path style endpoint for single node setup")),
                ..Default::default()
            },
            TestCase {
                num: 3,
                server_addr: "0.0.0.0:443",
                args: vec!["/d1"],
                expected_endpoints: Some(Endpoints(vec![Endpoint {
                    url: must_file_path("/d1"),
                    is_local: true,
                    pool_idx: 0,
                    set_idx: 0,
                    disk_idx: 0,
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
                    pool_idx: 0,
                    set_idx: 0,
                    disk_idx: 0,
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
                expected_err: Some(Error::other("same path '/d1' can not be served by different port on same address")),
                ..Default::default()
            },
            // Erasure Setup with PathEndpointType
            TestCase {
                num: 6,
                server_addr: "0.0.0.0:1234",
                args: vec!["/d1", "/d2", "/d3", "/d4"],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: must_file_path("/d1"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d2"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d3"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d4"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::Erasure),
                ..Default::default()
            },
            // DistErasure Setup with URLEndpointType
            TestCase {
                num: 7,
                server_addr: "0.0.0.0:9000",
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d2"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d3"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://localhost:9000/d4"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
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
                expected_err: Some(Error::other("all local endpoints should not have different hostnames/ips")),
                ..Default::default()
            },
            TestCase {
                num: 9,
                server_addr: "0.0.0.0:9001",
                args: vec![
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                    case7_endpoint1.as_str(),
                    "http://10.0.0.2:9001/export",
                ],
                expected_err: Some(Error::other("same path '/export' can not be served by different port on same address")),
                ..Default::default()
            },
            TestCase {
                num: 10,
                server_addr: "0.0.0.0:9000",
                args: vec![
                    "http://127.0.0.1:9000/export",
                    case7_endpoint2.as_str(),
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                ],
                expected_err: Some(Error::other("path '/export' cannot be served by different address on same server")),
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[1].clone(),
                        is_local: case1_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[2].clone(),
                        is_local: case1_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[3].clone(),
                        is_local: case1_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[1].clone(),
                        is_local: case2_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[2].clone(),
                        is_local: case2_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[3].clone(),
                        is_local: case2_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 13,
                server_addr: "0.0.0.0:80",
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[1].clone(),
                        is_local: case3_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[2].clone(),
                        is_local: case3_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[3].clone(),
                        is_local: case3_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 14,
                server_addr: "0.0.0.0:9000",
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[1].clone(),
                        is_local: case4_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[2].clone(),
                        is_local: case4_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[3].clone(),
                        is_local: case4_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 15,
                server_addr: "0.0.0.0:9000",
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[1].clone(),
                        is_local: case5_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[2].clone(),
                        is_local: case5_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[3].clone(),
                        is_local: case5_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 16,
                server_addr: "0.0.0.0:9003",
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
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[1].clone(),
                        is_local: case6_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[2].clone(),
                        is_local: case6_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[3].clone(),
                        is_local: case6_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
        ];

        for test_case in test_cases {
            let disks_layout = match DisksLayout::from_volumes(test_case.args.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    if test_case.expected_err.is_none() {
                        panic!("Test {}: unexpected error: {}", test_case.num, e);
                    }
                    continue;
                }
            };

            match (
                test_case.expected_err,
                PoolEndpointList::create_pool_endpoints(test_case.server_addr, &disks_layout).await,
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
                        panic!("Test {}: endpoints len: expected = {:?}, got = {:?}", test_case.num, left_len, right_len);
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

    #[tokio::test]
    async fn test_create_server_endpoints() {
        let test_cases = [
            // Invalid input.
            ("", vec![], false),
            // Range cannot be negative.
            ("0.0.0.0:9000", vec!["/export1{-1...1}"], false),
            // Range cannot start bigger than end.
            ("0.0.0.0:9000", vec!["/export1{64...1}"], false),
            // Range can only be numeric.
            ("0.0.0.0:9000", vec!["/export1{a...z}"], false),
            // Duplicate disks not allowed.
            ("0.0.0.0:9000", vec!["/export1{1...32}", "/export1{1...32}"], false),
            // Same host cannot export same disk on two ports - special case localhost.
            ("0.0.0.0:9001", vec!["http://localhost:900{1...2}/export{1...64}"], false),
            // Valid inputs.
            ("0.0.0.0:9000", vec!["/export1"], true),
            ("0.0.0.0:9000", vec!["/export1", "/export2", "/export3", "/export4"], true),
            ("0.0.0.0:9000", vec!["/export1{1...64}"], true),
            ("0.0.0.0:9000", vec!["/export1{01...64}"], true),
            ("0.0.0.0:9000", vec!["/export1{1...32}", "/export1{33...64}"], true),
            ("0.0.0.0:9001", vec!["http://localhost:9001/export{1...64}"], true),
            ("0.0.0.0:9001", vec!["http://localhost:9001/export{01...64}"], true),
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let disks_layout = match DisksLayout::from_volumes(test_case.1.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    if test_case.2 {
                        panic!("Test {}: unexpected error: {}", i + 1, e);
                    }
                    continue;
                }
            };

            let ret = EndpointServerPools::create_server_endpoints(test_case.0, &disks_layout).await;

            if let Err(err) = ret {
                if test_case.2 {
                    panic!("Test {}: Expected success but failed instead {}", i + 1, err)
                }
            } else if !test_case.2 {
                panic!("Test {}: expected failure but passed instead", i + 1);
            }
        }
    }
}
