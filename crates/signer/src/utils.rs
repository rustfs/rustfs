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

use http::request;

use s3s::Body;

pub fn get_host_addr(req: &request::Request<Body>) -> String {
    let host = req.headers().get("host");
    let uri = req.uri();
    let req_host;
    if let Some(port) = uri.port() {
        req_host = format!("{}:{}", uri.host().unwrap(), port);
    } else {
        req_host = uri.host().unwrap().to_string();
    }
    if let Some(host) = host
        && req_host != *host.to_str().unwrap()
    {
        return (*host.to_str().unwrap()).to_string();
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
