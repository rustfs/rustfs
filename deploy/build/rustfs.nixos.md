# RustFS NixOS Service Guide

This guide explains how to use [rustfs.nixos.service.nix](./rustfs.nixos.service.nix) as a starting point for managing RustFS with `systemd` on NixOS.

## 1. What the example does

The NixOS example is designed for the current RustFS startup model:

- `Type = "notify"` so `systemd` waits for RustFS to publish `READY=1`
- a dedicated `rustfs` system user instead of running the service as `root`
- `WorkingDirectory = "/var/lib/rustfs"` instead of using a log directory as the working directory
- `KillMode = "control-group"` and `SendSIGKILL = true` so stuck shutdowns do not leave the unit hanging forever
- `StandardOutput = "journal"` and `StandardError = "journal"` so logs stay in `journald`
- runtime configuration through `RUSTFS_*` environment variables
- secret file wiring through `RUSTFS_ACCESS_KEY_FILE` and `RUSTFS_SECRET_KEY_FILE`

## 2. Import the example into your NixOS configuration

You can either copy the contents into your own module, or import it directly and provide `rustfsPkg`.

Example:

```nix
{
  imports = [
    ./deploy/build/rustfs.nixos.service.nix
  ];

  _module.args.rustfsPkg = pkgs.callPackage ./path/to/rustfs/package.nix { };
}
```

If you already package RustFS through a flake output or overlay, point `rustfsPkg` at that package instead.

## 3. Adjust the default paths

The example uses these defaults:

- data directory: `/srv/rustfs`
- state directory: `/var/lib/rustfs`
- log directory: `/var/log/rustfs`

Before enabling the service, make sure your RustFS volumes and any TLS material match your real deployment layout.

The example currently sets:

```nix
RUSTFS_VOLUMES = "/srv/rustfs/vol{1...4}";
RUSTFS_ADDRESS = "0.0.0.0:9000";
RUSTFS_CONSOLE_ENABLE = "true";
RUSTFS_CONSOLE_ADDRESS = "0.0.0.0:9001";
```

Update these values to match your storage topology and exposure policy.

## 4. Wire secrets through runtime files

Do not put credentials directly into `environment` if they would be stored in the Nix store.

RustFS supports file-based secrets, so on NixOS the preferred pattern is:

```nix
systemd.services.rustfs.environment = {
  RUSTFS_ACCESS_KEY_FILE = config.age.secrets.rustfs-access.path;
  RUSTFS_SECRET_KEY_FILE = config.age.secrets.rustfs-secret.path;
};
```

The same shape also works with `sops-nix`, `agenix`, or any other runtime secret manager that exposes files under `/run`.

## 5. Rebuild and enable the service

After integrating the unit into your NixOS configuration:

```bash
sudo nixos-rebuild switch
sudo systemctl enable --now rustfs
```

## 6. Verify readiness and logs

Check whether `systemd` sees the service as fully ready:

```bash
systemctl status rustfs
systemctl show rustfs -p Type -p ActiveState -p SubState
```

Follow the service logs:

```bash
journalctl -u rustfs -f
```

Look for the point where RustFS reports that startup is complete and `systemd` transitions the unit into the running state.

## 7. Common NixOS-specific notes

- If you previously used `SendSIGKILL = false`, expect shutdown behavior to change. The example favors reliable service recovery over indefinite graceful waits.
- If your service really needs more startup time, increase `TimeoutStartSec`, but keep it bounded. Very large values can hide broken startup states.
- If you must write file logs instead of journald logs, change `StandardOutput` and `StandardError`, then make sure the target path is writable by the `rustfs` user.
- If you decide to run as `root`, review the hardening flags again. The example is written for a dedicated unprivileged service user.

## 8. Validation note

If your deployment depends on this example directly, validate the final module in your own NixOS environment before rollout. A common check is:

```bash
nix-instantiate --parse /path/to/your/module.nix
```

If you integrate the example into a host configuration or flake, prefer validating it through your normal `nixos-rebuild`, flake check, or CI pipeline as well.
