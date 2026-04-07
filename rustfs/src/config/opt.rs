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

//! Parsed server options.
//!
//! This module contains the `Opt` struct which holds parsed server options
//! and methods for parsing command line arguments.

use super::Config;
use super::cli::{Cli, CommandResult, Commands, ServerOpts, default_server_opts, preprocess_args_for_legacy};
use crate::apply_external_env_compat;
use CommandResult::Server;
use clap::Parser;
use std::path::PathBuf;

/// Parsed server options. Public for tests and backward compatibility.
/// Use `Opt::parse_from` or `Config::parse()` to obtain.
#[derive(Clone)]
pub struct Opt {
    pub volumes: Vec<String>,
    pub address: String,
    pub server_domains: Vec<String>,
    pub access_key: Option<String>,
    pub access_key_file: Option<PathBuf>,
    pub secret_key: Option<String>,
    pub secret_key_file: Option<PathBuf>,
    pub console_enable: bool,
    pub console_address: String,
    pub obs_endpoint: String,
    pub tls_path: Option<String>,
    pub license: Option<String>,
    pub region: Option<String>,
    pub kms_enable: bool,
    pub kms_backend: String,
    pub kms_key_dir: Option<String>,
    pub kms_vault_address: Option<String>,
    pub kms_vault_token: Option<String>,
    pub kms_vault_mount_path: Option<String>,
    pub kms_default_key_id: Option<String>,
    pub buffer_profile_disable: bool,
    pub buffer_profile: String,
}

impl Opt {
    /// Create Opt from ServerOpts
    pub(super) fn from_server_opts(o: ServerOpts) -> Self {
        Self {
            volumes: o.volumes,
            address: o.address,
            server_domains: o.server_domains,
            access_key: o.access_key,
            access_key_file: o.access_key_file,
            secret_key: o.secret_key,
            secret_key_file: o.secret_key_file,
            console_enable: o.console_enable,
            console_address: o.console_address,
            obs_endpoint: o.obs_endpoint,
            tls_path: o.tls_path,
            license: o.license,
            region: o.region,
            kms_enable: o.kms_enable,
            kms_backend: o.kms_backend,
            kms_key_dir: o.kms_key_dir,
            kms_vault_address: o.kms_vault_address,
            kms_vault_token: o.kms_vault_token,
            kms_vault_mount_path: o.kms_vault_mount_path,
            kms_default_key_id: o.kms_default_key_id,
            buffer_profile_disable: o.buffer_profile_disable,
            buffer_profile: o.buffer_profile,
        }
    }

    /// Parse from preprocessed args. Supports both `rustfs <volume>` and `rustfs server <volume>`.
    #[allow(dead_code)] // used in config_test
    pub fn parse_from<I, T>(args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let _ = apply_external_env_compat();
        let args: Vec<String> = args.into_iter().map(|a| a.into().to_string_lossy().into_owned()).collect();
        let args = preprocess_args_for_legacy(args);
        let cli = Cli::parse_from(args);
        match cli.command {
            Some(Commands::Server(opts)) => Self::from_server_opts(*opts),
            Some(Commands::Info(_)) => {
                // This should not happen in parse_from, as it's handled by parse_command
                panic!("Info command should be handled by parse_command");
            }
            None => {
                // Default to server with empty volumes (will be filled from env)
                Self::from_server_opts(default_server_opts())
            }
        }
    }

    /// Parse from preprocessed args and return the command type.
    /// Returns Ok(Info(opts)) if info command, Ok(Server(opts)) if server command.
    pub fn parse_command<I, T>(args: I) -> Result<CommandResult, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let _ = apply_external_env_compat();
        let args: Vec<String> = args.into_iter().map(|a| a.into().to_string_lossy().into_owned()).collect();
        let args = preprocess_args_for_legacy(args);
        let cli = match Cli::try_parse_from(args) {
            Ok(cli) => cli,
            Err(e) => {
                // Handle help and version display - these are not real errors
                if e.kind() == clap::error::ErrorKind::DisplayHelp || e.kind() == clap::error::ErrorKind::DisplayVersion {
                    // Print the help/version message and exit successfully
                    e.print().ok();
                    std::process::exit(0);
                }
                return Err(e);
            }
        };
        match cli.command {
            Some(Commands::Info(opts)) => Ok(CommandResult::Info(opts)),
            Some(Commands::Server(opts)) => Self::server_command_result(Self::from_server_opts(*opts)),
            None => {
                // Default to server with empty volumes (will be filled from env)
                Self::server_command_result(Self::from_server_opts(default_server_opts()))
            }
        }
    }

    // Helper to convert Opt to CommandResult::Server with error handling
    fn server_command_result(opt: Opt) -> Result<CommandResult, clap::Error> {
        Ok(Server(Box::new(Config::from_opt(opt).map_err(|e| {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, e.to_string())
        })?)))
    }

    /// Try parse from args, returns error on invalid input.
    #[allow(dead_code)] // used in config_test
    pub fn try_parse_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let _ = apply_external_env_compat();
        let args: Vec<String> = args.into_iter().map(|a| a.into().to_string_lossy().into_owned()).collect();
        let args = preprocess_args_for_legacy(args);
        let cli = Cli::try_parse_from(args)?;
        match cli.command {
            Some(Commands::Server(opts)) => Ok(Self::from_server_opts(*opts)),
            Some(Commands::Info(_)) => Err(clap::Error::new(clap::error::ErrorKind::DisplayHelp)),
            None => {
                // Default to server with empty volumes
                Ok(Self::from_server_opts(default_server_opts()))
            }
        }
    }

    /// Parse from env::args(). Used by Config::parse().
    #[allow(dead_code)] // used in config_test
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        Self::parse_from(args)
    }
}
