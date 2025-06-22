use http::request;

pub fn get_host_addr(req: &request::Builder) -> String {
    let host = req.headers_ref().expect("err").get("host");
    let uri = req.uri_ref().unwrap();
    let req_host;
    if let Some(port) = uri.port() {
        req_host = format!("{}:{}", uri.host().unwrap(), port);
    } else {
        req_host = uri.host().unwrap().to_string();
    }
    if host.is_some() && req_host != host.unwrap().to_str().unwrap().to_string() {
        return host.unwrap().to_str().unwrap().to_string();
    }
    /*if req.uri_ref().unwrap().host().is_some() {
        return req.uri_ref().unwrap().host().unwrap();
    }*/
    req_host
}

pub fn sign_v4_trim_all(input: &str) -> String {
    let ss = input.split_whitespace().collect::<Vec<_>>();
    ss.join(" ")
}

pub fn stable_sort_by_first<T>(v: &mut [(T, T)])
where
    T: Ord,
{
    v.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
}