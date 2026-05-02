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

use crate::error::TargetError;
use crate::target::TargetType;

/// Arguments for configuring a MySQL notification target.
///
/// Contains all configuration values needed to connect to a MySQL/TiDB
/// database and write event notification records.
#[derive(Debug, Clone)]
pub struct MySqlArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// MySQL data source name in format: `<user>:<password>@tcp(<host>:<port>)/<database>`
    pub dsn_string: String,
    /// Target table name, accepts `identifier` or `database.identifier`
    pub table: String,
    /// Directory for persistent queue storage; must be an absolute path if non-empty
    pub queue_dir: String,
    /// Maximum number of events stored in the local queue
    pub queue_limit: u64,
    /// Maximum number of open MySQL connections in the pool (0 = unlimited)
    pub max_open_connections: usize,
    /// The target type (notify or audit)
    pub target_type: TargetType,
}

impl MySqlArgs {
    /// Validates the MySQL target configuration.
    pub fn validate(&self) -> Result<(), TargetError> {
        // If the target is disabled, validation is skipped.
        if !self.enable {
            return Ok(());
        }

        if self.dsn_string.trim().is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string cannot be empty".to_string()));
        }

        let _ = parse_mysql_dsn(&self.dsn_string)?;

        validate_table_name(&self.table)?;

        if !self.queue_dir.is_empty() {
            let path = std::path::Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("MySQL queue_dir must be an absolute path".to_string()));
            }
        }

        Ok(())
    }
}

/// Parsed representation of a MySQL DSN string.
///
/// Produced by [`parse_mysql_dsn`] and consumed by the MySQL
/// target runtime to build connection options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MySqlDsn {
    /// MySQL user name
    pub user: String,
    /// MySQL password (plaintext, must be redacted before logging)
    pub password: String,
    /// MySQL server hostname or IP address
    pub host: String,
    /// MySQL server TCP port
    pub port: u16,
    /// Target database name
    pub database: String,
    /// Whether TLS is enabled
    pub tls: bool,
}

/// Parses a MySQL DSN string into its components.
///
/// Supported formats:
/// ```text
/// <user>:<password>@tcp(<host>:<port>)/<database>
/// mysql://<user>:<password>@tcp(<host>:<port>)/<database>
/// ```
///
/// Only `?tls=true` is accepted as a query parameter.
pub(crate) fn parse_mysql_dsn(dsn_string: &str) -> Result<MySqlDsn, TargetError> {
    let input = dsn_string.trim();
    if input.is_empty() {
        return Err(TargetError::Configuration("MySQL dsn_string cannot be empty".to_string()));
    }

    let remainder = input
        .strip_prefix("mysql://")
        .or_else(|| input.strip_prefix("MYSQL://"))
        .unwrap_or(input);

    let (body, query) = match remainder.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (remainder, None),
    };

    let mut tls = false;
    if let Some(query) = query {
        for param in query.split('&') {
            let param = param.trim();
            if param.is_empty() {
                continue;
            }
            let (key, value) = param.split_once('=').unwrap_or((param, ""));
            match key.trim().to_ascii_lowercase().as_str() {
                "tls" => {
                    let val = value.trim().to_ascii_lowercase();
                    if val == "true" || val == "tls" || val.is_empty() {
                        tls = true;
                    } else if val == "false" {
                        tls = false;
                    } else {
                        return Err(TargetError::Configuration(format!(
                            "unsupported value '{}' for TLS query parameter; use tls=true",
                            val
                        )));
                    }
                }
                _ => {
                    return Err(TargetError::Configuration(format!("unsupported MySQL DSN query parameter '{}'", key)));
                }
            }
        }
    }

    let Some((credentials, host_part)) = body.split_once('@') else {
        return Err(TargetError::Configuration(
            "MySQL dsn_string must contain user:password@tcp(host:port)/database".to_string(),
        ));
    };

    let Some((user, password)) = credentials.split_once(':') else {
        return Err(TargetError::Configuration("MySQL dsn_string must contain user:password".to_string()));
    };

    let user = user.trim();
    let password = password.trim();

    if user.is_empty() {
        return Err(TargetError::Configuration("MySQL dsn_string user is empty".to_string()));
    }

    let host_part = host_part.trim();

    let Some(host_part_rest) = host_part.strip_prefix("tcp(") else {
        return Err(TargetError::Configuration("MySQL dsn_string must use tcp(host:port) format".to_string()));
    };

    let Some((host_port, rest)) = host_part_rest.split_once(')') else {
        return Err(TargetError::Configuration(
            "MySQL dsn_string missing closing ')' after host:port".to_string(),
        ));
    };

    let (host, port_str) = host_port
        .split_once(':')
        .ok_or_else(|| TargetError::Configuration("MySQL dsn_string host:port is required".to_string()))?;

    let host = host.trim();
    let port_str = port_str.trim();

    if host.is_empty() {
        return Err(TargetError::Configuration("MySQL dsn_string host is empty".to_string()));
    }

    let port: u16 = port_str
        .parse()
        .map_err(|_| TargetError::Configuration(format!("MySQL dsn_string port '{}' is not a valid u16", port_str)))?;

    let database = rest
        .strip_prefix('/')
        .ok_or_else(|| TargetError::Configuration("MySQL dsn_string must include /database after host:port".to_string()))?
        .trim();

    if database.is_empty() {
        return Err(TargetError::Configuration("MySQL dsn_string database is empty".to_string()));
    }

    Ok(MySqlDsn {
        user: user.to_string(),
        password: password.to_string(),
        host: host.to_string(),
        port,
        database: database.to_string(),
        tls,
    })
}

/// Returns a redacted version of the DSN string with the password replaced by `***`.
#[allow(dead_code)]
pub(crate) fn redact_mysql_dsn(dsn_string: &str) -> String {
    let input = dsn_string.trim();
    if input.is_empty() {
        return String::new();
    }

    let remainder = input
        .strip_prefix("mysql://")
        .or_else(|| input.strip_prefix("MYSQL://"))
        .unwrap_or(input);

    let prefix = if input.starts_with("mysql://") {
        "mysql://"
    } else if input.starts_with("MYSQL://") {
        "MYSQL://"
    } else {
        ""
    };

    // This is a best-effort redaction that looks for the first '@' and then the last ':' before it to identify the password.
    match remainder.split_once('@') {
        Some((credentials, host_part)) => match credentials.split_once(':') {
            Some((user, _)) => format!("{}{}:***@{}", prefix, user.trim(), host_part.trim()),
            None => format!("{prefix}***:{host_part}"),
        },
        None => format!("{prefix}***"),
    }
}

fn is_valid_identifier_segment(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }

    let mut chars = segment.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }

    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return false;
        }
    }

    true
}

pub(crate) fn validate_table_name(table: &str) -> Result<(), TargetError> {
    let table = table.trim();

    if table.is_empty() {
        return Err(TargetError::Configuration("MySQL table name is empty".to_string()));
    }

    if table.contains('.') {
        let parts: Vec<&str> = table.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(TargetError::Configuration(format!(
                "MySQL table name '{}' is invalid; use identifier or database.identifier",
                table
            )));
        }

        if !is_valid_identifier_segment(parts[0]) {
            return Err(TargetError::Configuration(format!(
                "MySQL database name '{}' in '{}' is not a valid identifier",
                parts[0], table
            )));
        }

        if !is_valid_identifier_segment(parts[1]) {
            return Err(TargetError::Configuration(format!(
                "MySQL table name '{}' in '{}' is not a valid identifier",
                parts[1], table
            )));
        }
    } else {
        if !is_valid_identifier_segment(table) {
            return Err(TargetError::Configuration(format!(
                "MySQL table name '{}' is not a valid identifier",
                table
            )));
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn quote_table_name(table: &str) -> Result<String, TargetError> {
    let table = table.trim();

    if table.contains('.') {
        let parts: Vec<&str> = table.splitn(2, '.').collect();
        Ok(format!("`{}`.`{}`", parts[0].trim(), parts[1].trim()))
    } else {
        Ok(format!("`{}`", table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dsn_format() {
        let dsn = parse_mysql_dsn("rustfs:secret123@tcp(mysql.example.com:3306)/rustfs_events").expect("valid DSN");
        assert_eq!(dsn.user, "rustfs");
        assert_eq!(dsn.password, "secret123");
        assert_eq!(dsn.host, "mysql.example.com");
        assert_eq!(dsn.port, 3306);
        assert_eq!(dsn.database, "rustfs_events");
        assert!(!dsn.tls);
    }

    #[test]
    fn parse_dsn_with_mysql_prefix() {
        let dsn = parse_mysql_dsn("mysql://rustfs:password@tcp(127.0.0.1:3306)/mydb").expect("valid DSN with prefix");
        assert_eq!(dsn.user, "rustfs");
        assert_eq!(dsn.password, "password");
        assert_eq!(dsn.host, "127.0.0.1");
        assert_eq!(dsn.port, 3306);
        assert_eq!(dsn.database, "mydb");
    }

    #[test]
    fn parse_dsn_with_tls_true() {
        let dsn = parse_mysql_dsn("rustfs:password@tcp(127.0.0.1:3306)/mydb?tls=true").expect("valid DSN with TLS");
        assert!(dsn.tls);
    }

    #[test]
    fn parse_dsn_with_tls_bare() {
        let dsn = parse_mysql_dsn("rustfs:password@tcp(127.0.0.1:3306)/mydb?tls").expect("bare tls param");
        assert!(dsn.tls);
    }

    #[test]
    fn parse_dsn_rejects_unsupported_tls_params() {
        let err =
            parse_mysql_dsn("rustfs:password@tcp(127.0.0.1:3306)/mydb?verify_ca=true").expect_err("verify_ca should be rejected");
        assert!(err.to_string().contains("verify_ca"));

        let err = parse_mysql_dsn("rustfs:password@tcp(127.0.0.1:3306)/mydb?verify_identity=true")
            .expect_err("verify_identity should be rejected");
        assert!(err.to_string().contains("verify_identity"));

        let err = parse_mysql_dsn("rustfs:password@tcp(127.0.0.1:3306)/mydb?built_in_roots=true")
            .expect_err("built_in_roots should be rejected");
        assert!(err.to_string().contains("built_in_roots"));
    }

    #[test]
    fn parse_dsn_rejects_empty() {
        let err = parse_mysql_dsn("").expect_err("empty DSN");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn parse_dsn_rejects_missing_at() {
        let err = parse_mysql_dsn("rustfs:password").expect_err("missing @");
        assert!(err.to_string().contains("must contain user:password@"));
    }

    #[test]
    fn parse_dsn_rejects_non_tcp() {
        let err = parse_mysql_dsn("rustfs:password@unix(/tmp/mysql.sock)/mydb").expect_err("non-tcp should be rejected");
        assert!(err.to_string().contains("tcp("));
    }

    #[test]
    fn redact_dsn_masks_password() {
        let redacted = redact_mysql_dsn("rustfs:secret123@tcp(mysql.example.com:3306)/rustfs_events");
        assert_eq!(redacted, "rustfs:***@tcp(mysql.example.com:3306)/rustfs_events");
    }

    #[test]
    fn redact_dsn_with_mysql_prefix() {
        let redacted = redact_mysql_dsn("mysql://rustfs:secret123@tcp(127.0.0.1:3306)/mydb");
        assert_eq!(redacted, "mysql://rustfs:***@tcp(127.0.0.1:3306)/mydb");
    }

    #[test]
    fn validate_table_name_accepts_valid_identifier() {
        validate_table_name("rustfs_events").expect("valid table name");
        validate_table_name("my_db.events").expect("valid db.table");
        validate_table_name("_events").expect("valid starting underscore");
        validate_table_name("table_2").expect("valid with numbers");
    }

    #[test]
    fn validate_table_name_rejects_invalid() {
        let err = validate_table_name("").expect_err("empty");
        assert!(err.to_string().contains("empty"));

        let err = validate_table_name("1table").expect_err("starts with digit");
        assert!(err.to_string().contains("not a valid identifier"));

        let err = validate_table_name("my-table").expect_err("contains dash");
        assert!(err.to_string().contains("not a valid identifier"));

        let err = validate_table_name(".table").expect_err("empty db part");
        assert!(err.to_string().contains("invalid"));

        let err = validate_table_name("db.").expect_err("empty table part");
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn quote_table_name_quotes_simple() {
        let quoted = quote_table_name("rustfs_events").expect("valid");
        assert_eq!(quoted, "`rustfs_events`");
    }

    #[test]
    fn quote_table_name_quotes_database_table() {
        let quoted = quote_table_name("my_db.events").expect("valid");
        assert_eq!(quoted, "`my_db`.`events`");
    }
}
