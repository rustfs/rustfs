use super::disks_layout::PoolDisksLayout;
use super::utils::{
    net::{is_local_host, split_host_port},
    string::new_string_set,
};
use anyhow::{Error, Result};
use std::fmt::Display;
use std::{collections::HashMap, net::IpAddr, path::Path, usize};
use url::{ParseError, Url};

pub const DEFAULT_PORT: u16 = 9000;

/// enum for endpoint type.
#[derive(PartialEq, Eq)]
pub enum EndpointType {
    /// path style endpoint type enum.
    Path,

    /// URL style endpoint type enum.
    Url,

    /// Unknown endpoint type enum.
    UnKnow,
}

/// holds information about a node in this cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub url: url::Url,
    pub pools: Vec<i32>,
    pub is_local: bool,
    pub grid_host: String, // TODO "scheme://host:port"
}

// impl PartialOrd for Node {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         self.grid_host.partial_cmp(&other.grid_host)
//     }
// }

/// any type of endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub url: url::Url,
    pub is_local: bool,

    pub pool_idx: i32,
    pub set_idx: i32,
    pub disk_idx: i32,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.url.has_host() {
            write!(f, "{}", self.url)
        } else {
            write!(f, "{}", self.url.path())
        }
    }
}

impl TryFrom<&str> for Endpoint {
    /// The type returned in the event of a conversion error.
    type Error = String;

    /// Performs the conversion.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if is_empty_path(value) {
            return Err("empty or root endpoint is not supported".into());
        }

        // match Url::parse(value) {
        //     Ok(u) => u,
        //     Err(e) => match e {
        //         ParseError::EmptyHost => Err("")
        //     },
        // }

        unimplemented!()
    }
}

/// check whether given path is not empty.
fn is_empty_path(path: &str) -> bool {
    ["", "/", "\\"].iter().any(|&v| v.eq(path))
}

// 检查给定字符串是否是IP地址
fn is_host_ip(ip_str: &str) -> bool {
    ip_str.parse::<IpAddr>().is_ok()
}

impl Endpoint {
    pub fn new(arg: &str) -> Result<Self> {
        if is_empty_path(arg) {
            return Err(Error::msg("不支持空或根endpoint"));
        }

        let url = Url::parse(arg).or_else(|e| match e {
            ParseError::EmptyHost => Err(Error::msg("远程地址，域名不能为空")),
            ParseError::IdnaError => Err(Error::msg("域名格式不正确")),
            ParseError::InvalidPort => Err(Error::msg("端口格式不正确")),
            ParseError::InvalidIpv4Address => Err(Error::msg("IP格式不正确")),
            ParseError::InvalidIpv6Address => Err(Error::msg("IP格式不正确")),
            ParseError::InvalidDomainCharacter => Err(Error::msg("域名字符格式不正确")),
            // url::ParseError::RelativeUrlWithoutBase => todo!(),
            // url::ParseError::RelativeUrlWithCannotBeABaseBase => todo!(),
            // url::ParseError::SetHostOnCannotBeABaseUrl => todo!(),
            ParseError::Overflow => Err(Error::msg("长度过长")),
            _ => {
                if is_host_ip(arg) {
                    return Err(Error::msg("无效的URL endpoint格式: 缺少 http 或 https"));
                }

                let abs_arg = Path::new(arg).canonicalize()?;

                let abs = abs_arg.to_str().ok_or(Error::msg("绝对路径错误"))?;
                let url = Url::from_file_path(abs).unwrap();
                Ok(url)
            }
        })?;

        if url.scheme() == "file" {
            return Ok(Endpoint {
                url: url,
                is_local: true,
                pool_idx: -1,
                set_idx: -1,
                disk_idx: -1,
            });
        }

        if url.port().is_none() {
            return Err(Error::msg("必须提供端口号"));
        }

        if !(url.scheme() == "http" || url.scheme() == "https") {
            return Err(Error::msg("URL endpoint格式无效: Scheme字段必须包含'http'或'https'"));
        }

        // 检查路径
        let path = url.path();
        if is_empty_path(path) {
            return Err(Error::msg("URL endpoint不支持空或根路径"));
        }

        // TODO: Windows 系统上的路径处理
        #[cfg(windows)]
        {
            use std::env;
            if env::consts::OS == "windows" {
                // 处理 Windows 路径的特殊逻辑
            }
        }

        Ok(Endpoint {
            url: url,
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        })
    }

    // pub fn host_port_str(&self) -> String {
    //     if self.url.has_host() && self.port() > 0 {
    //         return format!("{}:{}", self.hostname(), self.port());
    //     } else if self.url.has_host() && self.port() == 0 {
    //         return self.hostname().to_string();
    //     } else if !self.url.has_host() && self.port() > 0 {
    //         return format!(":{}", self.port());
    //     } else {
    //         return String::new();
    //     }
    // }

    // pub fn port(&self) -> u16 {
    //     self.url.port().unwrap_or(0)
    // }
    // pub fn hostname(&self) -> &str {
    //     self.url.host_str().unwrap_or("")
    // }

    pub fn get_type(&self) -> EndpointType {
        if self.url.has_host() {
            EndpointType::Url
        } else {
            EndpointType::Path
        }
    }

    // pub fn get_scheme(&self) -> String {
    //     self.url.scheme().to_string()
    // }

    pub fn set_pool_index(&mut self, idx: i32) {
        self.pool_idx = idx
    }

    pub fn set_set_index(&mut self, idx: i32) {
        self.set_idx = idx
    }

    pub fn set_disk_index(&mut self, idx: i32) {
        self.disk_idx = idx
    }

    fn update_islocal(&mut self) -> Result<()> {
        if self.url.has_host() {
            self.is_local = is_local_host(self.url.host().unwrap(), self.url.port().unwrap(), DEFAULT_PORT);
        }

        Ok(())
    }

    fn grid_host(&self) -> String {
        let host = self.url.host_str().unwrap_or("");
        let port = self.url.port().unwrap_or(0);
        if port > 0 {
            format!("{}://{}:{}", self.url.scheme(), host, port)
        } else {
            format!("{}://{}", self.url.scheme(), host)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Endpoints(Vec<Endpoint>);

impl Endpoints {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> core::slice::Iter<Endpoint> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> core::slice::IterMut<Endpoint> {
        self.0.iter_mut()
    }

    pub fn slice(&self, start: usize, end: usize) -> Vec<Endpoint> {
        self.0.as_slice()[start..end].to_vec()
    }
    pub fn from_args(args: Vec<String>) -> Result<Self> {
        let mut ep_type = EndpointType::UnKnow;
        let mut scheme = String::new();
        let mut eps = Vec::new();
        let mut uniq_args = new_string_set();
        for (i, arg) in args.iter().enumerate() {
            let endpoint = Endpoint::new(arg)?;
            if i == 0 {
                ep_type = endpoint.get_type();
                scheme = endpoint.url.scheme().to_string();
            } else if endpoint.get_type() != ep_type {
                return Err(Error::msg("不支持多种endpoints风格"));
            } else if endpoint.url.scheme().to_string() != scheme {
                return Err(Error::msg("不支持多种scheme"));
            }

            let arg_str = endpoint.to_string();

            if uniq_args.contains(arg_str.as_str()) {
                return Err(Error::msg("发现重复 endpoints"));
            }

            uniq_args.add(arg_str);

            eps.push(endpoint.clone());
        }

        Ok(Endpoints(eps))
    }
}

#[warn(dead_code)]
pub struct PoolEndpointList(Vec<Endpoints>);

impl PoolEndpointList {
    fn from_vec(v: Vec<Endpoints>) -> Self {
        Self(v)
    }

    pub fn push(&mut self, es: Endpoints) {
        self.0.push(es)
    }

    // TODO: 解析域名，判断哪个是本地地址
    fn update_is_local(&mut self) -> Result<()> {
        for eps in self.0.iter_mut() {
            for ep in eps.iter_mut() {
                // TODO:
                ep.update_islocal()?
            }
        }

        Ok(())
    }
}

// PoolEndpoints represent endpoints in a given pool
// along with its setCount and setDriveCount.
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

// EndpointServerPools - list of list of endpoints
#[derive(Debug)]
pub struct EndpointServerPools(Vec<PoolEndpoints>);

impl EndpointServerPools {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    // create_server_endpoints
    pub fn create_server_endpoints(
        server_addr: String,
        pool_args: &Vec<PoolDisksLayout>,
        legacy: bool,
    ) -> Result<(EndpointServerPools, SetupType)> {
        if pool_args.is_empty() {
            return Err(Error::msg("无效参数"));
        }

        let (pooleps, setup_type) = create_pool_endpoints(server_addr, pool_args)?;

        let mut ret = EndpointServerPools::new();

        for (i, eps) in pooleps.iter().enumerate() {
            let ep = PoolEndpoints {
                legacy: legacy,
                set_count: pool_args[i].layout.len(),
                drives_per_set: pool_args[i].layout[0].len(),
                endpoints: eps.clone(),
                cmd_line: pool_args[i].cmd_line.clone(),
                platform: String::new(),
            };

            ret.add(ep)?;
        }

        Ok((ret, setup_type))
    }

    pub fn first_is_local(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }
        return self.0[0].endpoints.0[0].is_local;
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> core::slice::Iter<'_, PoolEndpoints> {
        return self.0.iter();
    }

    pub fn push(&mut self, pes: PoolEndpoints) {
        self.0.push(pes)
    }

    pub fn add(&mut self, eps: PoolEndpoints) -> Result<()> {
        let mut exits = new_string_set();
        for peps in self.0.iter() {
            for ep in peps.endpoints.0.iter() {
                exits.add(ep.to_string());
            }
        }

        for ep in eps.endpoints.0.iter() {
            if exits.contains(&ep.to_string()) {
                return Err(Error::msg("endpoints exists"));
            }
        }

        self.0.push(eps);
        Ok(())
    }

    pub fn get_nodes(&self) -> Vec<Node> {
        let mut node_map = HashMap::new();

        for pool in self.iter() {
            for ep in pool.endpoints.iter() {
                let mut node = Node {
                    url: ep.url.clone(),
                    pools: vec![],
                    is_local: ep.is_local,
                    grid_host: ep.grid_host(),
                };
                if !node.pools.contains(&ep.pool_idx) {
                    node.pools.push(ep.pool_idx)
                }

                node_map.insert(node.grid_host.clone(), node);
            }
        }

        let mut nodes: Vec<Node> = node_map.into_iter().map(|(_, n)| n).collect();

        // nodes.sort_by(|a, b| a.cmp(b));

        nodes
    }
}

#[derive(Debug)]
pub enum SetupType {
    // UnknownSetupType - starts with unknown setup type.
    UnknownSetupType,

    // FSSetupType - FS setup type enum.
    FSSetupType,

    // ErasureSDSetupType - Erasure single drive setup enum.
    ErasureSDSetupType,

    // ErasureSetupType - Erasure setup type enum.
    ErasureSetupType,

    // DistErasureSetupType - Distributed Erasure setup type enum.
    DistErasureSetupType,
}

fn is_empty_layout(pools_layout: &Vec<PoolDisksLayout>) -> bool {
    if pools_layout.is_empty() {
        return true;
    }
    let first_layout = &pools_layout[0];
    if first_layout.layout.is_empty() || first_layout.layout[0].is_empty() || first_layout.layout[0][0].is_empty() {
        return true;
    }
    false
}

// 检查是否是单驱动器布局
fn is_single_drive_layout(pools_layout: &Vec<PoolDisksLayout>) -> bool {
    if pools_layout.len() == 1 && pools_layout[0].layout.len() == 1 && pools_layout[0].layout[0].len() == 1 {
        true
    } else {
        false
    }
}

pub fn create_pool_endpoints(server_addr: String, pools: &Vec<PoolDisksLayout>) -> Result<(Vec<Endpoints>, SetupType)> {
    if is_empty_layout(pools) {
        return Err(Error::msg("empty layout"));
    }

    // TODO: CheckLocalServerAddr

    if is_single_drive_layout(pools) {
        let mut endpoint = Endpoint::new(pools[0].layout[0][0].as_str())?;
        endpoint.update_islocal()?;

        if endpoint.get_type() != EndpointType::Path {
            return Err(Error::msg("use path style endpoint for single node setup"));
        }

        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        let mut endpoints = Vec::new();
        endpoints.push(endpoint);

        // TODO: checkCrossDeviceMounts

        return Ok((vec![Endpoints(endpoints)], SetupType::ErasureSDSetupType));
    }

    let mut ret = Vec::with_capacity(pools.len());

    for (pool_idx, pool) in pools.iter().enumerate() {
        let mut endpoints = Endpoints::new();
        for (set_idx, set_layout) in pool.layout.iter().enumerate() {
            let mut eps = Endpoints::from_args(set_layout.to_owned())?;
            // TODO: checkCrossDeviceMounts
            for (disk_idx, ep) in eps.0.iter_mut().enumerate() {
                ep.set_pool_index(pool_idx as i32);
                ep.set_set_index(set_idx as i32);
                ep.set_disk_index(disk_idx as i32);

                endpoints.0.push(ep.to_owned());
            }
        }

        if endpoints.0.is_empty() {
            return Err(Error::msg("invalid number of endpoints"));
        }

        ret.push(endpoints);
    }

    // TODO:
    PoolEndpointList::from_vec(ret.clone()).update_is_local()?;

    let mut setup_type = SetupType::UnknownSetupType;

    // TODO: parse server port
    let (_, server_port) = split_host_port(server_addr.as_str())?;

    let mut uniq_host = new_string_set();

    for (_i, eps) in ret.iter_mut().enumerate() {
        // TODO: 一些验证，参考原m

        for ep in eps.0.iter() {
            if !ep.url.has_host() {
                uniq_host.add(format!("localhost:{}", server_port));
            } else {
                // uniq_host.add(ep.url.domain().)
            }
        }
    }

    let erasure_type = uniq_host.to_slice().len() == 1;

    for eps in ret.iter() {
        if eps.0[0].get_type() == EndpointType::Path {
            setup_type = SetupType::ErasureSetupType;
            break;
        }

        if eps.0[0].get_type() == EndpointType::Url {
            if erasure_type {
                setup_type = SetupType::ErasureSetupType;
            } else {
                setup_type = SetupType::DistErasureSetupType;
            }

            break;
        }
    }

    Ok((ret, setup_type))
}

// create_server_endpoints
fn create_server_endpoints(
    server_addr: String,
    pool_args: &Vec<PoolDisksLayout>,
    legacy: bool,
) -> Result<(EndpointServerPools, SetupType)> {
    if pool_args.is_empty() {
        return Err(Error::msg("无效参数"));
    }

    let (pooleps, setup_type) = create_pool_endpoints(server_addr, pool_args)?;

    let mut ret = EndpointServerPools::new();

    for (i, eps) in pooleps.iter().enumerate() {
        let ep = PoolEndpoints {
            legacy: legacy,
            set_count: pool_args[i].layout.len(),
            drives_per_set: pool_args[i].layout[0].len(),
            endpoints: eps.clone(),
            cmd_line: pool_args[i].cmd_line.clone(),
            platform: String::new(),
        };

        ret.add(ep)?;
    }

    Ok((ret, setup_type))
}

#[cfg(test)]
mod test {

    use crate::disks_layout::DisksLayout;

    use super::*;

    #[test]
    fn test_url() {
        let path = "/dir/sss";

        let u = url::Url::parse(path);

        println!("{:#?}", u)
    }

    #[test]
    fn test_new_endpont() {
        let arg = "./data";
        let ep = Endpoint::new(arg).unwrap();

        println!("{:?}", ep);
    }

    #[test]
    fn test_create_server_endpoints() {
        let cases = vec![(":9000", vec!["http://localhost:900{1...2}/export{1...64}".to_string()])];

        for (addr, args) in cases {
            let layouts = DisksLayout::try_from(args.as_slice()).unwrap();

            println!("layouts:{:?},{}", &layouts.pools, &layouts.legacy);

            let (server_pool, setup_type) = create_server_endpoints(addr.to_string(), &layouts.pools, layouts.legacy).unwrap();

            println!("setup_type -- {:?}", setup_type);
            println!("server_pool == {:?}", server_pool);
        }

        // create_server_endpoints(server_addr, pool_args, legacy)
    }
}
