# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{ defaultPackage }:

{ config, lib, pkgs, ... }:

let
  cfg = config.services.rustfs;
  volumeArguments = lib.concatStringsSep " " cfg.volumes;
in
{
  options.services.rustfs = {
    enable = lib.mkEnableOption "the RustFS object storage server";

    package = lib.mkOption {
      type = lib.types.package;
      default = defaultPackage pkgs.stdenv.hostPlatform.system;
      defaultText = lib.literalExpression "inputs.rustfs.packages.\${pkgs.system}.rustfs";
      description = "RustFS server package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "rustfs";
      description = "User account under which RustFS runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "rustfs";
      description = "Group under which RustFS runs.";
    };

    volumes = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ "/var/lib/rustfs" ];
      description = "Data volumes passed to RustFS via RUSTFS_VOLUMES.";
    };

    address = lib.mkOption {
      type = lib.types.str;
      default = ":9000";
      description = "Address on which the S3 API listens.";
    };

    consoleEnable = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Whether to enable the management console.";
    };

    consoleAddress = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:9001";
      description = "Address on which the management console listens.";
    };

    accessKeyFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/rustfs-access-key";
      description = "Runtime file containing the root access key.";
    };

    secretKeyFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/rustfs-secret-key";
      description = "Runtime file containing the root secret key.";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      description = "Rust log filter passed through RUST_LOG.";
    };

    extraEnvironmentVariables = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Additional environment variables for the service.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.volumes != [ ];
        message = "services.rustfs.volumes must contain at least one path.";
      }
      {
        assertion = builtins.all (volume: lib.hasPrefix "/" volume) cfg.volumes;
        message = "services.rustfs.volumes entries must be absolute local paths.";
      }
      {
        assertion = cfg.accessKeyFile != null;
        message = "services.rustfs.accessKeyFile must be set; default credentials are not enabled by this module.";
      }
      {
        assertion = cfg.secretKeyFile != null;
        message = "services.rustfs.secretKeyFile must be set; default credentials are not enabled by this module.";
      }
      {
        assertion = !(builtins.hasAttr "RUSTFS_ACCESS_KEY" cfg.extraEnvironmentVariables);
        message = "services.rustfs.extraEnvironmentVariables must not set RUSTFS_ACCESS_KEY; use accessKeyFile.";
      }
      {
        assertion = !(builtins.hasAttr "RUSTFS_SECRET_KEY" cfg.extraEnvironmentVariables);
        message = "services.rustfs.extraEnvironmentVariables must not set RUSTFS_SECRET_KEY; use secretKeyFile.";
      }
    ];

    users.groups.${cfg.group} = { };
    users.users.${cfg.user} = {
      group = cfg.group;
      isSystemUser = true;
      description = "RustFS service user";
    };

    systemd.tmpfiles.rules = lib.map (volume: "d ${volume} 0750 ${cfg.user} ${cfg.group} -") cfg.volumes;

    systemd.services.rustfs = {
      description = "RustFS Object Storage Server";
      documentation = [ "https://docs.rustfs.com/" ];
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      environment = cfg.extraEnvironmentVariables // {
        RUSTFS_VOLUMES = volumeArguments;
        RUSTFS_ADDRESS = cfg.address;
        RUSTFS_CONSOLE_ENABLE = lib.boolToString cfg.consoleEnable;
        RUSTFS_CONSOLE_ADDRESS = cfg.consoleAddress;
        RUSTFS_ACCESS_KEY_FILE = "%d/access-key";
        RUSTFS_SECRET_KEY_FILE = "%d/secret-key";
        RUST_LOG = cfg.logLevel;
      };

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/rustfs";
        User = cfg.user;
        Group = cfg.group;
        LoadCredential = lib.optionals (cfg.accessKeyFile != null && cfg.secretKeyFile != null) [
          "access-key:${toString cfg.accessKeyFile}"
          "secret-key:${toString cfg.secretKeyFile}"
        ];
        Restart = "on-failure";
        RestartSec = "5s";
        LimitNOFILE = 1048576;
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectHome = true;
        ProtectSystem = "strict";
        ReadWritePaths = cfg.volumes;
        UMask = "0077";
      };
    };
  };
}
