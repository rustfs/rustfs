"""KMS backends: what each can do, the process configurations beyond the basic ones, and the admin calls that drive them.

A Vault development server is started once per run, as a child of the driver, the first time a configuration that
needs it is prepared; it is stopped when the run ends. Its root token and unseal key are generated or captured at
run time, live only in this process and in the environment of the servers it starts, and are redacted from the
log the suite keeps.
"""

from __future__ import annotations

import atexit
import base64
import json
import os
import re
import secrets
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import framework as fw
import harness as h
import observe

# ---------------------------------------------------------------------------
# What each backend can do. Cases decide a skip from this table, never from an error they received.
# ---------------------------------------------------------------------------

LOCAL, VAULT_TRANSIT, VAULT_KV2, STATIC = "local", "vault-transit", "vault-kv2", "static"

CAPABILITIES = {
    LOCAL: {
        "manage_keys": True,          # keys are created and changed through the admin API
        "disable": True,              # a key can be disabled and enabled again
        "read_while_disabled": True,  # objects under a disabled key stay readable
        "rotate": False,              # a key can be rotated
        "destroy": True,              # a key can be destroyed
        "admin_configure": True,      # the backend can be configured through the admin API
    },
    VAULT_TRANSIT: {"manage_keys": True, "disable": True, "read_while_disabled": True, "rotate": True, "destroy": True,
                    "admin_configure": True},
    VAULT_KV2: {"manage_keys": True, "disable": True, "read_while_disabled": True, "rotate": True, "destroy": True,
                "admin_configure": True},
    STATIC: {"manage_keys": False, "disable": False, "read_while_disabled": False, "rotate": False, "destroy": False,
             "admin_configure": False},
}

# The backend behind each process configuration that runs a KMS.
CONFIG_BACKEND = {"e2": LOCAL, "e2e": LOCAL, "e2m": LOCAL, "e3": VAULT_TRANSIT, "e4": VAULT_KV2, "e8": STATIC}

STATIC_KEY = "bb-static"


def backend_of(config: str) -> str:
    return CONFIG_BACKEND[config]


def can(config: str, capability: str) -> bool:
    return CAPABILITIES[CONFIG_BACKEND[config]][capability]


def require(config: str, capability: str) -> None:
    """Skip the case when the configuration's backend lacks `capability`."""
    if not can(config, capability):
        raise fw.Skip(f"the {CONFIG_BACKEND[config]} backend does not support {capability!r}")


# ---------------------------------------------------------------------------
# Vault development server
# ---------------------------------------------------------------------------

VAULT_BIN_ENV = "SSE_BB_VAULT_BIN"
VAULT_START_TIMEOUT = 30.0
_UNSEAL_KEY = re.compile(r"Unseal Key:\s*(\S+)")
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class _Vault:
    """The one Vault server of this run."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.token = ""
        self.unseal_key = ""
        # A failed start is remembered, so every later configuration reports the same reason without retrying.
        self.error: str | None = None
        self._pump: threading.Thread | None = None

    @property
    def address(self) -> str:
        return f"http://127.0.0.1:{h.VAULT_PORT}"

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


_vault = _Vault()


def _vault_binary() -> str:
    configured = os.environ.get(VAULT_BIN_ENV)
    if configured:
        if not Path(configured).is_file():
            raise h.EnvError(f"no vault binary at {VAULT_BIN_ENV}={configured!r}")
        return configured
    found = shutil.which("vault")
    if not found:
        raise h.EnvError(f"no vault binary on PATH; install it or set {VAULT_BIN_ENV}")
    return found


def _redacting_pump(stream, log_path: Path) -> None:
    """Copy the server's output to the run's log without the root token and the unseal key it prints at startup."""
    with log_path.open("a") as log:
        for raw in iter(stream.readline, b""):
            line = raw.decode(errors="replace")
            match = _UNSEAL_KEY.search(line)
            if match:
                _vault.unseal_key = match.group(1)
            for secret in (_vault.token, _vault.unseal_key):
                if secret:
                    line = line.replace(secret, "<redacted>")
            log.write(line)
            log.flush()


def vault_request(method: str, path: str, payload: dict | None = None, timeout: float = 10.0) -> tuple[int, dict]:
    """One call to the Vault HTTP API as root. Returns (status, parsed body); status 0 is a transport failure."""
    request = urllib.request.Request(f"{_vault.address}/v1/{path.lstrip('/')}", method=method,
                                     data=json.dumps(payload).encode() if payload is not None else None)
    request.add_header("X-Vault-Token", _vault.token)
    try:
        with _NO_PROXY.open(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as err:
        status, raw = err.code, err.read()
    except (urllib.error.URLError, OSError, TimeoutError):
        return 0, {}
    try:
        return status, json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return status, {}


def ensure_vault() -> None:
    """Start the Vault development server unless it already runs; `EnvError` when it cannot be had."""
    if _vault.running():
        return
    if _vault.error:
        raise h.EnvError(_vault.error)
    try:
        binary = _vault_binary()
        # A development server listens on the API port and, for its cluster address, on the next one.
        for port in (h.VAULT_PORT, h.VAULT_PORT + 1):
            if h.port_in_use(port):
                raise h.EnvError(f"Vault needs port {port}, which is already in use; "
                                 f"find the holder with: lsof -nP -iTCP:{port} -sTCP:LISTEN")
        log_dir = (h.RUN_DIR or h.STATE_DIR) / "vault"
        log_dir.mkdir(parents=True, exist_ok=True)
        _vault.token = secrets.token_urlsafe(24)
        _vault.unseal_key = ""
        env = {k: v for k, v in h.clean_env().items() if not k.startswith("VAULT_")}
        # The token travels in the environment: a command line is visible to every local user.
        env["VAULT_DEV_ROOT_TOKEN_ID"] = _vault.token
        _vault.proc = subprocess.Popen(
            [binary, "server", "-dev", f"-dev-listen-address=127.0.0.1:{h.VAULT_PORT}", "-dev-no-store-token"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        _vault._pump = threading.Thread(target=_redacting_pump, args=(_vault.proc.stdout, log_dir / "vault.log"), daemon=True)
        _vault._pump.start()
        atexit.register(stop_vault)
        _wait_vault(sealed=False, timeout=VAULT_START_TIMEOUT)
        # The banner with the unseal key follows shortly after the listener answers. Without it the server still
        # serves every case that does not seal it, so its absence is reported only by `seal_vault`.
        deadline = time.monotonic() + 5.0
        while not _vault.unseal_key and time.monotonic() < deadline:
            time.sleep(0.1)
        # A development server mounts KV version 2 at `secret/`; the Transit engine has to be enabled.
        status, _ = vault_request("POST", "sys/mounts/transit", {"type": "transit"})
        if status not in (200, 204):
            raise h.EnvError(f"enabling the Vault Transit engine answered HTTP {status}")
    except h.EnvError as err:
        _vault.error = f"Vault is not available: {err}"
        stop_vault()
        raise h.EnvError(_vault.error) from None


def _wait_vault(sealed: bool, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if not _vault.running():
            raise h.EnvError("the Vault server exited")
        # sys/health answers 200 when unsealed and 503 when sealed; both carry the `sealed` field.
        status, body = vault_request("GET", "sys/health", timeout=2.0)
        if status in (200, 503) and body.get("sealed") is sealed:
            return
        if time.monotonic() > deadline:
            raise h.EnvError(f"Vault did not become {'sealed' if sealed else 'unsealed'} within {timeout:.0f}s (HTTP {status})")
        time.sleep(0.2)


def stop_vault() -> None:
    """Stop the Vault server this driver started. Nothing else is ever signalled."""
    proc, _vault.proc = _vault.proc, None
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=h.STOP_GRACE)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=h.STOP_GRACE)
    if _vault._pump is not None:
        _vault._pump.join(timeout=5.0)


def seal_vault() -> None:
    """Seal Vault: it keeps answering, and refuses every key operation until it is unsealed."""
    if not _vault.unseal_key:
        raise h.EnvError("the Vault unseal key was not captured at startup, so a sealed Vault could not be recovered")
    status, _ = vault_request("PUT", "sys/seal")
    if status not in (200, 204):
        raise h.EnvError(f"sealing Vault answered HTTP {status}")
    _wait_vault(sealed=True)


def unseal_vault() -> None:
    status, body = vault_request("PUT", "sys/unseal", {"key": _vault.unseal_key})
    if status != 200 or body.get("sealed") is not False:
        raise h.EnvError(f"unsealing Vault answered HTTP {status} sealed={body.get('sealed')}")
    _wait_vault(sealed=False)


def _prepare_vault(_site: h.Site) -> None:
    ensure_vault()


def _provision_vault(site: h.Site) -> None:
    """Vault starts empty, so the default key has to be created too; the Local backend finds its key file."""
    h.wait_kms_healthy(site)
    for name in (h.DEFAULT_KEY, h.KEY_1, h.KEY_2):
        h.ensure_key(site, name)


def _vault_env(backend: str) -> dict[str, str]:
    if not _vault.running():
        raise h.EnvError(_vault.error or "the Vault server is not running")
    return {
        "RUSTFS_KMS_ENABLE": "true",
        "RUSTFS_KMS_BACKEND": backend,
        "RUSTFS_KMS_VAULT_ADDRESS": _vault.address,
        "RUSTFS_KMS_VAULT_TOKEN": _vault.token,
        "RUSTFS_KMS_DEFAULT_KEY_ID": h.DEFAULT_KEY,
        "RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS": "true",
        "RUSTFS_KMS_PROBE_INTERVAL_SECS": str(h.KMS_PROBE_INTERVAL),
    }


def _vault_transit_env(_site: h.Site) -> dict[str, str]:
    # The key records keep the product's default location: the admin configure request cannot name another one,
    # so only the default lets "configure again with the same settings" mean the same settings.
    return {**_vault_env(VAULT_TRANSIT), "RUSTFS_KMS_VAULT_MOUNT_PATH": "transit"}


def _vault_kv2_env(_site: h.Site) -> dict[str, str]:
    return {**_vault_env(VAULT_KV2), "RUSTFS_KMS_VAULT_KV_MOUNT": "secret"}


def vault_configure_payload(backend: str, default_key_id: str = h.DEFAULT_KEY) -> dict:
    """The admin configure request equal to the environment configuration of `backend`, but for the default key.

    The token is left empty, which asks the server to keep the credentials it already has: the run's token never
    travels in a request.
    """
    payload = {
        "backend_type": {VAULT_TRANSIT: "VaultTransit", VAULT_KV2: "VaultKV2"}[backend],
        "address": _vault.address,
        "auth_method": {"Token": {"token": ""}},
        "default_key_id": default_key_id,
        "allow_insecure_dev_defaults": True,
    }
    if backend == VAULT_TRANSIT:
        payload["mount_path"] = "transit"
    else:
        payload["kv_mount"] = "secret"
    return payload


# ---------------------------------------------------------------------------
# Process configurations
# ---------------------------------------------------------------------------


def _local_kms_with_master_key(site: h.Site) -> dict[str, str]:
    return {**fw.CONFIGS["e2"](site), "RUSTFS_SSE_S3_MASTER_KEY": fw.master_key()}


def _static_env(_site: h.Site) -> dict[str, str]:
    secret = base64.b64encode(h.derived_key("static kms key")).decode()
    return {
        "RUSTFS_KMS_ENABLE": "true",
        "RUSTFS_KMS_BACKEND": "static",
        "RUSTFS_KMS_STATIC_SECRET_KEY": f"{STATIC_KEY}:{secret}",
        "RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS": "true",
        "RUSTFS_KMS_PROBE_INTERVAL_SECS": str(h.KMS_PROBE_INTERVAL),
    }


def _provision_static(site: h.Site) -> None:
    h.wait_kms_healthy(site)


def local_kms_env(site: h.Site, **overrides: str | None) -> dict[str, str]:
    """The Local KMS environment with variables replaced, or removed when the override is None."""
    env = dict(fw.CONFIGS["e2"](site))
    for name, value in overrides.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    return env


# Local KMS plus a loaded local master key.
fw.register_config("e2m", _local_kms_with_master_key, prepare=fw.CONFIG_PREPARE["e2"], provision=fw.CONFIG_PROVISION["e2"])
# Vault Transit: Vault wraps and unwraps the data keys; the key records live in KV.
fw.register_config("e3", _vault_transit_env, prepare=_prepare_vault, provision=_provision_vault, teardown=stop_vault)
# Vault KV version 2: Vault stores the key material, the server wraps the data keys itself.
fw.register_config("e4", _vault_kv2_env, prepare=_prepare_vault, provision=_provision_vault, teardown=stop_vault)
# Static: one key taken from the environment, nothing to manage.
fw.register_config("e8", _static_env, provision=_provision_static)


# ---------------------------------------------------------------------------
# Admin API: KMS service control and key lifecycle
# ---------------------------------------------------------------------------


def kms_configure(s: h.Site, payload: dict):
    return h.admin(s, "POST", "kms/configure", payload=payload, label="KmsConfigure")


def kms_reconfigure(s: h.Site, payload: dict):
    return h.admin(s, "POST", "kms/reconfigure", payload=payload, label="KmsReconfigure")


def kms_reload(s: h.Site):
    return h.admin(s, "POST", "kms/reload", label="KmsReload")


def kms_restart(s: h.Site):
    """Start with `force`: a running service is replaced by a new one built from the same configuration."""
    return h.admin(s, "POST", "kms/start", payload={"force": True}, label="KmsForceStart")


def kms_service_status(s: h.Site):
    return h.admin(s, "GET", "kms/service-status", label="KmsServiceStatus")


def key_describe(s: h.Site, key_id: str):
    return h.admin(s, "GET", f"kms/keys/{urllib.parse.quote(key_id, safe='')}", label="KmsDescribeKey")


def key_disable(s: h.Site, key_id: str):
    return h.admin(s, "POST", "kms/keys/disable", payload={"key_id": key_id}, label="KmsDisableKey")


def key_enable(s: h.Site, key_id: str):
    return h.admin(s, "POST", "kms/keys/enable", payload={"key_id": key_id}, label="KmsEnableKey")


def key_rotate(s: h.Site, key_id: str):
    return h.admin(s, "POST", "kms/keys/rotate", payload={"key_id": key_id}, label="KmsRotateKey")


def key_schedule_deletion(s: h.Site, key_id: str):
    """Move the key to pending deletion; nothing is destroyed yet."""
    return h.admin(s, "DELETE", "kms/keys/delete", payload={"key_id": key_id}, label="KmsScheduleKeyDeletion")


def key_destroy(s: h.Site, key_id: str):
    """Destroy a key that is pending deletion. The server must run with immediate deletion allowed."""
    payload = {"key_id": key_id, "force_immediate": True, "confirm_key_id": key_id}
    return h.admin(s, "DELETE", "kms/keys/delete", payload=payload, label="KmsDestroyKey")


def key_state(response) -> str | None:
    """The key state in an admin response that carries key metadata."""
    status, body = response
    if status != 200 or not isinstance(body, dict):
        return None
    metadata = body.get("key_metadata") or {}
    return metadata.get("key_state")


def succeeded(response) -> bool:
    """An admin call answered 200 and says it did what was asked; the service-control routes answer 200 either way."""
    status, body = response
    return status == 200 and isinstance(body, dict) and body.get("success") is True


def kms_not_running(s: h.Site, timeout: float = 30.0) -> str:
    """Wait for the KMS service to run with a healthy backend. Returns "" once it does, else what it reports.

    Unlike `harness.wait_kms_healthy` this does not raise: whether the service comes up after a restart or a
    reconfiguration is what some cases judge, so a service that stays down must be a failure, not an environment error.
    """
    deadline = time.monotonic() + timeout
    with observe.quiet():
        while True:
            status, body = kms_service_status(s)
            if status == 200 and isinstance(body, dict) and body.get("status") == "Running" and body.get("healthy") is True:
                return ""
            if time.monotonic() > deadline:
                state = {k: body.get(k) for k in ("status", "backend_type", "healthy")} if isinstance(body, dict) else body
                return f"the KMS service is not running with a healthy backend after {timeout:.0f}s: HTTP {status} {str(state)[:200]}"
            time.sleep(0.5)
