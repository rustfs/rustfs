{
  config,
  lib,
  pkgs,
  rustfsPkg,
  ...
}:
let
  dataDir = "/srv/rustfs";
  stateDir = "/var/lib/rustfs";
  logDir = "/var/log/rustfs";
in
{
  users.groups.rustfs = { };

  users.users.rustfs = {
    isSystemUser = true;
    group = "rustfs";
    home = stateDir;
    createHome = false;
    description = "RustFS service user";
  };

  systemd.services.rustfs = {
    description = "RustFS Object Storage Server";
    wantedBy = [ "multi-user.target" ];
    after = [ "network-online.target" ];
    wants = [ "network-online.target" ];

    # Keep non-secret runtime settings in the unit environment. For secrets,
    # prefer runtime files so credentials do not land in the Nix store.
    environment = {
      RUSTFS_ADDRESS = "0.0.0.0:9000";
      RUSTFS_CONSOLE_ENABLE = "true";
      RUSTFS_CONSOLE_ADDRESS = "0.0.0.0:9001";
      RUSTFS_VOLUMES = "${dataDir}/vol{1...4}";
    };

    serviceConfig = {
      Type = "notify";
      NotifyAccess = "main";

      User = "rustfs";
      Group = "rustfs";

      # Use a state directory instead of a log directory as the cwd so
      # relative paths do not accidentally resolve under /var/log.
      WorkingDirectory = stateDir;
      StateDirectory = "rustfs";
      LogsDirectory = "rustfs";

      # Explicitly use the server entrypoint instead of relying on legacy
      # CLI compatibility shims in service management.
      ExecStart = "${rustfsPkg}/bin/rustfs server";

      LimitNOFILE = 1048576;
      LimitNPROC = 32768;
      TasksMax = "infinity";

      Restart = "on-failure";
      RestartSec = "10s";
      StartLimitIntervalSec = "5min";
      StartLimitBurst = 5;

      # RustFS publishes READY=1 after runtime readiness succeeds. These
      # values leave room for cold starts without hiding permanently wedged
      # instances for too long.
      TimeoutStartSec = "5min";
      TimeoutStopSec = "45s";
      KillMode = "control-group";
      SendSIGKILL = true;

      # Keep the example neutral by default. If your deployment explicitly
      # wants RustFS to outlive other workloads during OOM, tune this in your
      # local NixOS configuration.
      OOMScoreAdjust = 0;

      NoNewPrivileges = true;
      PrivateTmp = true;
      ProtectHome = true;
      ProtectSystem = "strict";
      ProtectClock = true;
      ProtectControlGroups = true;
      ProtectKernelModules = true;
      ProtectKernelTunables = true;
      RestrictRealtime = true;
      RestrictSUIDSGID = true;
      UMask = "0077";

      ReadWritePaths = [
        dataDir
        stateDir
        logDir
      ];

      StandardOutput = "journal";
      StandardError = "journal";
    };
  };

  # Example secret wiring for sops-nix / agenix style runtime files:
  #
  # systemd.services.rustfs.environment = {
  #   RUSTFS_ACCESS_KEY_FILE = config.age.secrets.rustfs-access.path;
  #   RUSTFS_SECRET_KEY_FILE = config.age.secrets.rustfs-secret.path;
  # };
}
