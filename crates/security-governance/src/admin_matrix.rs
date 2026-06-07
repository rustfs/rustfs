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

use std::collections::BTreeSet;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HttpMethod {
    Delete,
    Get,
    Head,
    Post,
    Put,
}

impl HttpMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdminActionRef(&'static str);

impl AdminActionRef {
    pub const fn new(action: &'static str) -> Self {
        Self(action)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicRouteKind {
    ConsoleAsset,
    Health,
    OidcBootstrap,
    StsFormPost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteRiskLevel {
    Normal,
    Sensitive,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdminRouteAccess {
    AdminAction(AdminActionRef),
    Public(PublicRouteKind),
}

impl AdminRouteAccess {
    pub const fn admin_action(self) -> Option<AdminActionRef> {
        match self {
            Self::AdminAction(action) => Some(action),
            Self::Public(_) => None,
        }
    }

    pub const fn public_kind(self) -> Option<PublicRouteKind> {
        match self {
            Self::AdminAction(_) => None,
            Self::Public(kind) => Some(kind),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdminRouteSpec {
    method: HttpMethod,
    path: &'static str,
    access: AdminRouteAccess,
    risk_level: RouteRiskLevel,
}

impl AdminRouteSpec {
    pub const fn admin(method: HttpMethod, path: &'static str, action: AdminActionRef, risk_level: RouteRiskLevel) -> Self {
        Self {
            method,
            path,
            access: AdminRouteAccess::AdminAction(action),
            risk_level,
        }
    }

    pub const fn public(method: HttpMethod, path: &'static str, kind: PublicRouteKind, risk_level: RouteRiskLevel) -> Self {
        Self {
            method,
            path,
            access: AdminRouteAccess::Public(kind),
            risk_level,
        }
    }

    pub const fn method(self) -> HttpMethod {
        self.method
    }

    pub const fn path(self) -> &'static str {
        self.path
    }

    pub const fn access(self) -> AdminRouteAccess {
        self.access
    }

    pub const fn risk_level(self) -> RouteRiskLevel {
        self.risk_level
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdminRouteMatrixError {
    #[error("admin route spec at index {index} has an empty path")]
    EmptyPath { index: usize },

    #[error("admin route spec at index {index} for {method} {path} has an empty admin action")]
    EmptyAdminAction {
        index: usize,
        method: &'static str,
        path: &'static str,
    },

    #[error("duplicate admin route spec for {method} {path}")]
    DuplicateRoute { method: &'static str, path: &'static str },
}

pub fn validate_admin_route_specs(specs: &[AdminRouteSpec]) -> Result<(), AdminRouteMatrixError> {
    let mut routes = BTreeSet::new();

    for (index, spec) in specs.iter().copied().enumerate() {
        if spec.path.trim().is_empty() {
            return Err(AdminRouteMatrixError::EmptyPath { index });
        }

        if let Some(action) = spec.access.admin_action()
            && action.as_str().trim().is_empty()
        {
            return Err(AdminRouteMatrixError::EmptyAdminAction {
                index,
                method: spec.method.as_str(),
                path: spec.path,
            });
        }

        if !routes.insert((spec.method, spec.path)) {
            return Err(AdminRouteMatrixError::DuplicateRoute {
                method: spec.method.as_str(),
                path: spec.path,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER_INFO: AdminActionRef = AdminActionRef::new("ServerInfoAdminAction");

    #[test]
    fn validates_admin_and_public_route_specs() {
        let specs = [
            AdminRouteSpec::admin(HttpMethod::Get, "/rustfs/admin/v3/info", SERVER_INFO, RouteRiskLevel::Sensitive),
            AdminRouteSpec::public(HttpMethod::Post, "/", PublicRouteKind::StsFormPost, RouteRiskLevel::Sensitive),
        ];

        assert!(validate_admin_route_specs(&specs).is_ok());
        assert_eq!(specs[0].access().admin_action().expect("admin spec must expose action"), SERVER_INFO);
        assert_eq!(
            specs[1].access().public_kind().expect("public spec must expose public kind"),
            PublicRouteKind::StsFormPost
        );
    }

    #[test]
    fn rejects_duplicate_method_and_path() {
        let specs = [
            AdminRouteSpec::admin(HttpMethod::Get, "/rustfs/admin/v3/info", SERVER_INFO, RouteRiskLevel::Sensitive),
            AdminRouteSpec::admin(
                HttpMethod::Get,
                "/rustfs/admin/v3/info",
                AdminActionRef::new("StorageInfoAdminAction"),
                RouteRiskLevel::Sensitive,
            ),
        ];

        let err = validate_admin_route_specs(&specs).expect_err("duplicate route spec should fail validation");

        assert_eq!(
            err,
            AdminRouteMatrixError::DuplicateRoute {
                method: "GET",
                path: "/rustfs/admin/v3/info"
            }
        );
    }

    #[test]
    fn rejects_empty_paths() {
        let specs = [AdminRouteSpec::admin(
            HttpMethod::Get,
            " ",
            SERVER_INFO,
            RouteRiskLevel::Sensitive,
        )];

        let err = validate_admin_route_specs(&specs).expect_err("empty path should fail validation");

        assert_eq!(err, AdminRouteMatrixError::EmptyPath { index: 0 });
    }

    #[test]
    fn rejects_empty_admin_actions() {
        let specs = [AdminRouteSpec::admin(
            HttpMethod::Get,
            "/rustfs/admin/v3/info",
            AdminActionRef::new(" "),
            RouteRiskLevel::Sensitive,
        )];

        let err = validate_admin_route_specs(&specs).expect_err("empty admin action should fail validation");

        assert_eq!(
            err,
            AdminRouteMatrixError::EmptyAdminAction {
                index: 0,
                method: "GET",
                path: "/rustfs/admin/v3/info"
            }
        );
    }
}
