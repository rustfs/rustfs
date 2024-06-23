pub struct EndpointServerPools(Vec<PoolEndpoints>);

pub struct PoolEndpoints {
    // indicates if endpoints are provided in non-ellipses style
    pub legacy: bool,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoints: Endpoints,
    pub cmd_line: String,
    pub platform: String,
}

pub struct Endpoints(Vec<Endpoint>);

pub struct Endpoint {
    pub url: url::Url,

    pub is_local: bool,
    pub pool_idx: i32,
    pub set_idx: i32,
    pub disk_idx: i32,
}
