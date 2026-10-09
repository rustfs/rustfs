#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec python3 - "$ROOT_DIR" "$@" <<'PY'
import json
import re
import sys
from pathlib import Path


root = Path(sys.argv[1])
arguments = sys.argv[2:]
if arguments not in ([], ["--self-test"]):
    raise SystemExit("usage: check_oidc_architecture_boundaries.sh [--self-test]")

fixture_file = root / "scripts/fixtures/architecture_migration_rules/oidc/cases.json"
raw_string = re.compile(r'r(#+)?"')
char_literal = re.compile(r"'(?:\\.|[^'\\\n])'")
required_modules = (
    "crates/iam/src/oidc/mod.rs",
    "crates/iam/src/oidc/config.rs",
    "crates/iam/src/oidc/provider.rs",
    "crates/iam/src/oidc/runtime.rs",
    "crates/iam/src/oidc/state.rs",
    "crates/iam/src/oidc/transport.rs",
)
removed_modules = (
    "crates/iam/src/oidc.rs",
    "crates/iam/src/oidc_state.rs",
    "crates/iam/src/federation/registry.rs",
    "crates/iam/src/federation/oidc",
)
admin_config_service = "rustfs/src/admin/service/oidc_config.rs"
site_replication_handler = "rustfs/src/admin/handlers/site_replication.rs"
admin_oidc_handler = "rustfs/src/admin/handlers/oidc.rs"
admin_idp_compat_handler = "rustfs/src/admin/handlers/idp_compat.rs"


def mask(source, start, end):
    return "".join("\n" if character == "\n" else " " for character in source[start:end])


def code_only(source):
    """Preserve offsets while ignoring Rust comments, strings, and test items."""
    output = list(source)
    index = 0
    while index < len(source):
        start = index
        if source.startswith("//", index):
            end = source.find("\n", index)
            index = len(source) if end < 0 else end
        elif source.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
        else:
            raw = raw_string.match(source, index)
            if raw:
                delimiter = '"' + (raw.group(1) or "")
                end = source.find(delimiter, index + len(raw.group()))
                index = len(source) if end < 0 else end + len(delimiter)
            elif source[index] == '"':
                index += 1
                while index < len(source):
                    if source[index] == "\\":
                        index += 2
                    elif source[index] == '"':
                        index += 1
                        break
                    else:
                        index += 1
            elif source[index] == "'" and (literal := char_literal.match(source, index)):
                index = literal.end()
            else:
                index += 1
                continue
        output[start:index] = mask(source, start, index)

    production = "".join(output)
    test_item = re.compile(
        r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*(?:#\s*\[[^]]*\]\s*)*"
        r"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:mod|fn|impl|use|struct|enum|trait|const|static)\b"
    )
    search_from = 0
    while match := test_item.search(production, search_from):
        delimiter = re.search(r"[;{]", production[match.end():])
        if not delimiter:
            break
        end = match.end() + delimiter.end()
        if delimiter.group() == "{":
            depth = 1
            while end < len(production) and depth:
                if production[end] == "{":
                    depth += 1
                elif production[end] == "}":
                    depth -= 1
                end += 1
        production = production[: match.start()] + mask(production, match.start(), end) + production[end:]
        search_from = end
    return production


def find_matches(rule, path, source, pattern):
    return [
        (rule, path, source.count("\n", 0, match.start()) + 1)
        for match in re.finditer(pattern, source, re.MULTILINE | re.DOTALL)
    ]


def function_body(source, name):
    signature = re.search(rf"\bfn\s+{name}\s*\([^)]*\)[^{{]*\{{", source)
    if not signature:
        return ""
    depth = 1
    end = signature.end()
    while end < len(source) and depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    return source[signature.end(): end - 1]


def workflow_event_paths(source, event):
    section = re.search(rf"^  {event}:\s*\n(.*?)(?=^  [a-z_]+:|\Z)", source, re.MULTILINE | re.DOTALL)
    if not section:
        return set()
    paths = set()
    in_paths = False
    for line in section.group(1).splitlines():
        if re.fullmatch(r"    paths:\s*", line):
            in_paths = True
            continue
        if not in_paths:
            continue
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        entry = re.fullmatch(r"      -\s+(?:[\"']([^\"']+)[\"']|([^\s#]+))\s*(?:#.*)?", line)
        if not entry:
            break
        paths.add(entry.group(1) or entry.group(2))
    return paths


def source_findings(path, source):
    findings = []
    if path in removed_modules or path.startswith("crates/iam/src/federation/oidc/"):
        return [("oidc-module-owner", path, 1)]
    if path == ".github/workflows/oidc-keycloak.yml":
        expected_paths = {"crates/iam/src/oidc/**", admin_config_service}
        if any(not expected_paths.issubset(workflow_event_paths(source, event)) for event in ("pull_request", "push")):
            findings.append(("oidc-live-workflow", path, 1))
        return findings
    if not path.endswith(".rs") or path.endswith("_tests.rs") or path.endswith("test_support.rs") or "/tests/" in path:
        return findings
    if not (path.startswith("crates/iam/src/") or path.startswith("rustfs/src/")):
        return findings

    code = code_only(source)
    findings += find_matches(
        "oidc-legacy-runtime", path, code,
        r"\b(?:FederatedIdentityRegistry|FederatedIdentityProvider|FederatedCodeExchange|OIDC_SYS|get_oidc|init_oidc_sys|init_oidc_sys_with_extra_root_ca_provider)\b",
    )

    if path.startswith("crates/iam/src/federation/"):
        findings += find_matches(
            "federation-oidc-detail", path, code,
            r"\b(?:crate|super)::oidc\b|\buse\b[^;]*\boidc\b[^;]*;|\boidc::|\b(?:OidcSys|OidcClaims|OidcStateStore|OidcProviderConfig|OidcConfigSnapshot|OidcExtraRootCaProvider|ProviderRuntime|ReqwestHttpClient|openidconnect|reqwest)\b",
        )

    if path.startswith("crates/iam/src/") and path != "crates/iam/src/lib.rs" and not path.startswith(
        ("crates/iam/src/oidc/", "crates/iam/src/federation/")
    ):
        findings += find_matches(
            "oidc-runtime-owner", path, code,
            r"\b(?:OidcSys|StandardOidcAdapter|build_oidc_sys_with_extra_root_ca_provider)\b",
        )

    if path == "crates/iam/src/oidc/config.rs":
        findings += find_matches(
            "oidc-config-direction", path, code,
            r"\b(?:provider|runtime|state|transport)::|\b(?:ProviderRuntime|OidcSys|OidcStateStore|ReqwestHttpClient|CoreClient)\b",
        )
        query = re.search(r"pub\s+trait\s+OidcConfigQuery\b[^\{]*\{([^}]*)\}", code, re.DOTALL)
        narrow_query = r"\s*fn\s+site_replication_snapshot\s*\(\s*&self\s*\)\s*->\s*OidcSiteReplicationSnapshot\s*;\s*"
        if not query or not re.fullmatch(narrow_query, query.group(1)):
            findings.append(("oidc-query-shape", path, 1))
        provider = re.search(r"pub\s+struct\s+OidcSiteReplicationProvider\s*\{([^}]*)\}", code, re.DOTALL)
        provider_shape = (
            r"\s*pub\s+provider_id\s*:\s*String\s*,"
            r"\s*pub\s+claim_name\s*:\s*String\s*,"
            r"\s*pub\s+role_policy\s*:\s*String\s*,"
            r"\s*pub\s+client_id\s*:\s*String\s*,"
            r"\s*pub\s+hashed_client_secret\s*:\s*String\s*,?\s*"
        )
        if not provider or not re.fullmatch(provider_shape, provider.group(1)):
            findings.append(("oidc-site-replication-shape", path, 1))
        snapshot = re.search(r"pub\s+struct\s+OidcSiteReplicationSnapshot\s*\{([^}]*)\}", code, re.DOTALL)
        if not snapshot or not re.fullmatch(r"\s*providers\s*:\s*Vec\s*<\s*OidcSiteReplicationProvider\s*>\s*,?\s*", snapshot.group(1)):
            findings.append(("oidc-site-replication-shape", path, 1))

    if path.startswith("rustfs/src/") and path != "rustfs/src/startup_auth.rs":
        findings += find_matches(
            "oidc-runtime-owner", path, code,
            r"\b(?:OidcSys|StandardOidcAdapter|build_oidc_sys_with_extra_root_ca_provider)\b",
        )

    if path == admin_oidc_handler:
        findings += find_matches(
            "oidc-admin-config-owner", path, code,
            r"\b(?:read_admin_config_without_migrate|read_admin_server_config_snapshot|save_admin_server_config_snapshot|current_object_store_handle_for_context|ServerConfig|KVS|build_upsert_provider_config|upsert_persisted_provider_config|delete_persisted_provider_config|persisted_provider_secret|validate_oidc_provider_config_with_extra_root_ca|load_oidc_config_snapshot|current_oidc_extra_root_ca_material)\b",
        )
    if path == admin_idp_compat_handler:
        findings += find_matches(
            "oidc-admin-config-owner", path, code,
            r"\b(?:read_admin_config_without_migrate|read_admin_server_config_snapshot|save_admin_server_config_snapshot|current_object_store_handle_for_context|build_upsert_provider_config|upsert_persisted_provider_config|delete_persisted_provider_config|persisted_provider_secret|validate_oidc_provider_config_with_extra_root_ca|current_oidc_extra_root_ca_material)\b",
        )
    if path == admin_config_service:
        findings += find_matches("oidc-admin-service-layer", path, code, r"\b(?:s3s|S3Error|S3Result|s3_error)\b")
        for operation in ("list_config", "upsert_config", "delete_config", "validate_config"):
            if not re.search(rf"pub\(crate\)\s+async\s+fn\s+{operation}\s*\(", code):
                findings.append(("oidc-admin-config-owner", path, 1))
        for storage_operation in ("read_admin_server_config_snapshot", "save_admin_server_config_snapshot"):
            if not re.search(rf"\b{storage_operation}\s*\(", code):
                findings.append(("oidc-admin-config-owner", path, 1))
    if path == site_replication_handler:
        findings += find_matches(
            "oidc-site-replication-consumer", path, code,
            r"\b(?:OidcConfigSnapshot|OidcProviderConfig|load_oidc_config_snapshot)\b",
        )
        oidc_mapping = function_body(code, "open_id_settings_from_snapshot")
        if not oidc_mapping or re.search(r"\b(?:hash_client_secret|Sha256)\b|\.\s*(?:config_snapshot|client_secret)\b", oidc_mapping):
            findings.append(("oidc-site-replication-consumer", path, 1))
        if not re.search(r"\.\s*site_replication_snapshot\s*\(", code):
            findings.append(("oidc-site-replication-consumer", path, 1))

    for match in re.finditer(r"\bFederatedAuthorization\s*\{", code):
        preceding_word = re.search(r"\b(\w+)\s*$", code[: match.start()])
        if preceding_word and preceding_word.group(1) in ("struct", "impl"):
            continue
        if path != "crates/iam/src/federation/mapper.rs":
            findings.append(("federation-authorization-owner", path, code.count("\n", 0, match.start()) + 1))

    if path == "rustfs/src/app/context/interfaces.rs":
        paired_snapshot = (
            r"pub\s+type\s+FederatedIdentityRuntimeSnapshot\s*=\s*\(\s*"
            r"Arc\s*<\s*FederatedIdentityService\s*>\s*,\s*"
            r"Arc\s*<\s*dyn\s+OidcConfigQuery\s*>\s*\)\s*;"
        )
        if not re.search(paired_snapshot, code):
            findings.append(("oidc-runtime-pair", path, 1))
    if path == "rustfs/src/app/context/handles.rs":
        runtime = re.search(r"struct\s+FederatedIdentityRuntime\s*\{([^}]*)\}", code, re.DOTALL)
        if not runtime or not re.search(r"\bservice\s*:\s*Arc\s*<\s*FederatedIdentityService\s*>", runtime.group(1)) or not re.search(
            r"\boidc_config_query\s*:\s*Arc\s*<\s*dyn\s+OidcConfigQuery\s*>", runtime.group(1)
        ):
            findings.append(("oidc-runtime-pair", path, 1))
    return findings


def layout_findings(exists=None, service_modules=None):
    if exists is None:
        exists = lambda path: (root / path).exists()
    if service_modules is None:
        service_modules = (root / "rustfs/src/admin/service/mod.rs").read_text()
    findings = []
    for path in required_modules:
        if not exists(path):
            findings.append(("oidc-module-owner", path, 1))
    for path in removed_modules:
        if exists(path):
            findings.append(("oidc-module-owner", path, 1))
    if not exists(admin_config_service):
        findings.append(("oidc-admin-config-owner", admin_config_service, 1))
    if not re.search(r"\bmod\s+oidc_config\s*;", service_modules):
        findings.append(("oidc-admin-config-owner", "rustfs/src/admin/service/mod.rs", 1))
    return findings


def fixture_findings():
    cases = json.loads(fixture_file.read_text())
    actual = []
    expected = []
    for case in cases:
        case_id = case["id"]
        expected.extend(f"{rule}|{case_id}" for rule in case["expected"])
        if "layout" in case:
            layout = case["layout"]
            present = set(required_modules) | {admin_config_service}
            present.difference_update(layout.get("missing", []))
            present.update(layout.get("restored", []))
            findings = layout_findings(present.__contains__, layout.get("service_modules", "mod oidc_config;"))
        else:
            findings = source_findings(case["path"], case["source"])
        actual.extend(f"{rule}|{case_id}" for rule, _, _ in findings)
    return sorted(set(actual)), sorted(set(expected))


def repository_findings():
    findings = layout_findings()
    for base in ("crates/iam/src", "rustfs/src"):
        for file in sorted((root / base).rglob("*.rs")):
            path = file.relative_to(root).as_posix()
            findings.extend(source_findings(path, file.read_text()))
    workflow = ".github/workflows/oidc-keycloak.yml"
    findings.extend(source_findings(workflow, (root / workflow).read_text()))
    return findings


actual, expected = fixture_findings()
if actual != expected:
    print("OIDC architecture guard fixture mismatch:", file=sys.stderr)
    print(f"  expected: {expected}", file=sys.stderr)
    print(f"  actual:   {actual}", file=sys.stderr)
    raise SystemExit(1)

if arguments == ["--self-test"]:
    print("OIDC architecture guard fixtures passed.")
    raise SystemExit(0)

findings = repository_findings()
for rule, path, line in findings:
    print(f"OIDC architecture boundary failed: {rule}: {path}:{line}", file=sys.stderr)
if findings:
    raise SystemExit(1)
print("OIDC architecture boundaries passed.")
PY
