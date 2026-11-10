#!/usr/bin/env python3
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

"""Release certification gate for the public RustFS publication exits.

Consumes the shared evidence envelope contract (rustfs/backlog#2658,
schema_version=1) together with a versioned release validation plan and a
release candidate record. Publication is fail-closed: any missing, duplicate,
unplanned, stale, substituted, cancelled or non-PASS required item blocks the
release and no official channel is written.

Two entry points:

  validate          aggregate one parent candidate batch's evidence envelopes
                    and write machine JSON, JUnit and a human summary;
                    exit 0 only for a complete legal success.

  verify-promotion  re-check the exact artifact bytes about to be published
                    against the passed list captured by `validate`; a swapped
                    package or an edited plan after verification fails.

Both exit nonzero with classified findings and never print credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1

RELEASE_TYPES = ("preview", "prerelease", "stable")
STATES = ("pass", "fail", "infra_error", "unsupported", "quarantined", "skipped")
REASON_CLASSES = ("PRODUCT", "HARNESS", "ENV", "INCONCLUSIVE", "NOT_APPLICABLE")
BATCH_STATUSES = ("completed", "cancelled", "timed_out")

HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
MUTABLE_LATEST = re.compile(r"(?:^|/)latest(?:/|$)")

REQUIRED_PLAN_KEYS = (
    "schema_version",
    "plan_id",
    "release_types",
    "targets",
    "features",
    "trusted_workflows",
    "lanes",
    "upgrade_sources",
)
REQUIRED_ENVELOPE_KEYS = (
    "schema_version",
    "evidence_profile",
    "run_id",
    "run_attempt",
    "plan_sha256",
    "tier",
    "lane",
    "case_id",
    "product_commit",
    "harness_commit",
    "target_fingerprint",
    "platform",
    "started_at",
    "finished_at",
    "state",
    "reason_code",
    "reason_class",
    "evidence",
    "workflow",
)


class GateError(Exception):
    """One classified gate finding; `code` is stable and machine-readable."""

    def __init__(self, code, message, case_id=None, lane=None, platform=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.case_id = case_id
        self.lane = lane
        self.platform = platform

    def as_dict(self):
        finding = {"code": self.code, "message": self.message}
        if self.case_id is not None:
            finding["case_id"] = self.case_id
        if self.lane is not None:
            finding["lane"] = self.lane
        if self.platform is not None:
            finding["platform"] = self.platform
        return finding


def canonical_sha256(document):
    """Semantic digest of a parsed JSON document (sorted keys, no whitespace)."""
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path, code):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise GateError(code, f"missing input file: {path}") from None
    except json.JSONDecodeError as error:
        raise GateError(code, f"invalid JSON in {path}: {error.msg}") from None


def platform_key(platform):
    return (platform["os"], platform["arch"], platform["abi"])


def key_dict(lane, platform, case_id):
    return {
        "lane": lane,
        "platform": {
            "os": platform["os"],
            "arch": platform["arch"],
            "abi": platform["abi"],
        },
        "case_id": case_id,
    }


def key_label(lane, platform, case_id):
    return f"{lane}/{platform['os']}-{platform['arch']}-{platform['abi']}/{case_id}"


def parse_utc(value, code, field):
    if not isinstance(value, str) or not value:
        raise GateError(code, f"{field} must be a non-empty UTC timestamp")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise GateError(code, f"{field} is not an ISO-8601 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise GateError(code, f"{field} must be a UTC timestamp") from None
    return parsed


def require_string(document, field, code, context):
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise GateError(code, f"{context}: {field} must be a non-empty string")
    return value


def require_hex(value, pattern, code, field, context):
    if not isinstance(value, str) or not pattern.match(value):
        raise GateError(code, f"{context}: {field} must match {pattern.pattern}")
    return value


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def parse_platform(entry, code, context):
    if not isinstance(entry, dict):
        raise GateError(code, f"{context}: platform entry must be an object")
    return {
        "os": require_string(entry, "os", code, context),
        "arch": require_string(entry, "arch", code, context),
        "abi": require_string(entry, "abi", code, context),
    }


def validate_plan(plan):
    code = "plan_invalid"
    if not isinstance(plan, dict):
        raise GateError(code, "plan must be a JSON object")
    if plan.get("schema_version") != SCHEMA_VERSION:
        raise GateError(code, f"plan schema_version must be {SCHEMA_VERSION}")
    require_string(plan, "plan_id", code, "plan")

    release_types = plan.get("release_types")
    if not isinstance(release_types, list) or not release_types:
        raise GateError(code, "plan release_types must be a non-empty list")
    for release_type in release_types:
        if release_type not in RELEASE_TYPES:
            raise GateError(code, f"plan release_types contains unknown {release_type!r}")

    targets = plan.get("targets")
    if not isinstance(targets, list) or not targets:
        raise GateError(code, "plan targets must be a non-empty list")
    parsed_targets = []
    seen_targets = set()
    for entry in targets:
        platform = parse_platform(entry, code, "plan targets")
        target_key = platform_key(platform)
        if target_key in seen_targets:
            raise GateError(code, f"plan targets duplicate {target_key}")
        seen_targets.add(target_key)
        parsed_targets.append(platform)

    features = plan.get("features")
    if not isinstance(features, list) or any(
        not isinstance(feature, str) or not feature for feature in features
    ):
        raise GateError(code, "plan features must be a list of non-empty strings")

    trusted = plan.get("trusted_workflows")
    if not isinstance(trusted, list) or not trusted:
        raise GateError(
            code,
            "plan trusted_workflows must be a non-empty list; "
            "an untrusted or PR-uploaded result set can never certify a release",
        )
    trusted_pairs = []
    for entry in trusted:
        if not isinstance(entry, dict):
            raise GateError(code, "plan trusted_workflows entries must be objects")
        repository = require_string(entry, "repository", code, "plan trusted_workflows")
        workflow_id = require_string(entry, "workflow_id", code, "plan trusted_workflows")
        trusted_pairs.append((repository, workflow_id))

    lanes = plan.get("lanes")
    if not isinstance(lanes, list) or not lanes:
        raise GateError(code, "plan lanes must be a non-empty list")
    parsed_lanes = {}
    for entry in lanes:
        if not isinstance(entry, dict):
            raise GateError(code, "plan lanes entries must be objects")
        lane = require_string(entry, "lane", code, "plan lane")
        if lane in parsed_lanes:
            raise GateError(code, f"plan lanes duplicate {lane!r}")
        harness_repository = require_string(
            entry, "harness_repository", code, f"plan lane {lane}"
        )
        harness_sha = require_hex(
            entry.get("harness_sha"), HEX40, code, "harness_sha", f"plan lane {lane}"
        )
        timeout_seconds = entry.get("timeout_seconds")
        if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
            raise GateError(
                code, f"plan lane {lane}: timeout_seconds must be a positive integer"
            )
        capabilities = entry.get("environment_capabilities")
        if not isinstance(capabilities, list) or any(
            not isinstance(item, str) or not item for item in capabilities
        ):
            raise GateError(
                code,
                f"plan lane {lane}: environment_capabilities must be a list of non-empty strings",
            )
        cases = entry.get("cases")
        if not isinstance(cases, list) or not cases:
            raise GateError(code, f"plan lane {lane}: cases must be a non-empty list")
        for case_id in cases:
            if not isinstance(case_id, str) or not case_id:
                raise GateError(code, f"plan lane {lane}: case IDs must be non-empty strings")
        if len(set(cases)) != len(cases):
            raise GateError(code, f"plan lane {lane}: duplicate case IDs")

        applies = entry.get("applies_to")
        if not isinstance(applies, dict):
            raise GateError(code, f"plan lane {lane}: applies_to must be an object")
        applies_types = applies.get("release_types")
        if not isinstance(applies_types, list) or not applies_types:
            raise GateError(
                code, f"plan lane {lane}: applies_to.release_types must be a non-empty list"
            )
        for release_type in applies_types:
            if release_type not in release_types:
                raise GateError(
                    code,
                    f"plan lane {lane}: applies_to.release_types contains unknown {release_type!r}",
                )
        applies_platforms = applies.get("platforms")
        if applies_platforms == "all":
            lane_platforms = list(parsed_targets)
        elif isinstance(applies_platforms, list) and applies_platforms:
            lane_platforms = []
            for platform_entry in applies_platforms:
                platform = parse_platform(platform_entry, code, f"plan lane {lane}")
                if platform_key(platform) not in seen_targets:
                    raise GateError(
                        code,
                        f"plan lane {lane}: platform {platform_key(platform)} is not a declared target",
                    )
                lane_platforms.append(platform)
        else:
            raise GateError(
                code,
                f"plan lane {lane}: applies_to.platforms must be 'all' or a non-empty list",
            )

        parsed_lanes[lane] = {
            "lane": lane,
            "harness_repository": harness_repository,
            "harness_sha": harness_sha,
            "timeout_seconds": timeout_seconds,
            "environment_capabilities": capabilities,
            "cases": cases,
            "release_types": applies_types,
            "platforms": lane_platforms,
        }

    upgrade_sources = plan.get("upgrade_sources")
    if not isinstance(upgrade_sources, dict):
        raise GateError(code, "plan upgrade_sources must be an object")
    basis = upgrade_sources.get("basis")
    if not isinstance(basis, str) or not basis.strip():
        raise GateError(
            code,
            "plan upgrade_sources.basis must state what actually supports the listed sources",
        )
    sources = upgrade_sources.get("sources")
    if not isinstance(sources, list) or not sources:
        raise GateError(code, "plan upgrade_sources.sources must be a non-empty list")
    for source in sources:
        if not isinstance(source, dict):
            raise GateError(code, "plan upgrade_sources.sources entries must be objects")
        require_string(source, "version", code, "plan upgrade source")
        require_string(source, "role", code, "plan upgrade source")
        require_string(source, "artifact_name", code, "plan upgrade source")
        require_hex(
            source.get("artifact_sha256"),
            HEX64,
            code,
            "artifact_sha256",
            "plan upgrade source",
        )
    rollback_supported = upgrade_sources.get("rollback_supported")
    if not isinstance(rollback_supported, bool):
        raise GateError(
            code,
            "plan upgrade_sources.rollback_supported must be an explicit boolean; "
            "rollback support is never a default",
        )
    rollback_basis = upgrade_sources.get("rollback_basis")
    if not isinstance(rollback_basis, str) or not rollback_basis.strip():
        raise GateError(code, "plan upgrade_sources.rollback_basis must be explicit")

    return {
        "plan_id": plan["plan_id"],
        "release_types": release_types,
        "targets": parsed_targets,
        "features": features,
        "trusted_pairs": trusted_pairs,
        "lanes": parsed_lanes,
        "upgrade_sources": upgrade_sources,
        "plan_sha256": canonical_sha256(plan),
    }


def expand_required(lanes, release_profile):
    """Expand the plan into the frozen required (lane, platform, case) set."""
    required = []
    for lane_name, lane in lanes.items():
        if release_profile not in lane["release_types"]:
            continue
        for platform in lane["platforms"]:
            for case_id in lane["cases"]:
                required.append(key_dict(lane_name, platform, case_id))
    return required


def planned_pair(lanes, lane_name, case_id):
    lane = lanes.get(lane_name)
    return lane is not None and case_id in lane["cases"]


# ---------------------------------------------------------------------------
# Candidate
# ---------------------------------------------------------------------------


def validate_candidate(candidate, plan_info, plan_sha256):
    code = "candidate_invalid"
    if not isinstance(candidate, dict):
        raise GateError(code, "candidate must be a JSON object")
    if candidate.get("schema_version") != SCHEMA_VERSION:
        raise GateError(code, f"candidate schema_version must be {SCHEMA_VERSION}")
    product_commit = require_hex(
        candidate.get("product_commit"), HEX40, code, "product_commit", "candidate"
    )
    version = require_string(candidate, "version", code, "candidate")
    release_profile = require_string(candidate, "release_profile", code, "candidate")
    if release_profile not in RELEASE_TYPES:
        raise GateError(code, f"candidate release_profile must be one of {RELEASE_TYPES}")
    features = candidate.get("features")
    if not isinstance(features, list) or any(
        not isinstance(feature, str) or not feature for feature in features
    ):
        raise GateError(code, "candidate features must be a list of non-empty strings")

    candidate_plan_sha = require_hex(
        candidate.get("plan_sha256"), HEX64, code, "plan_sha256", "candidate"
    )
    if candidate_plan_sha != plan_sha256:
        raise GateError(
            "plan_mismatch",
            "candidate plan_sha256 does not match the validation plan",
        )

    batch = candidate.get("validation_batch")
    if not isinstance(batch, dict):
        raise GateError(code, "candidate validation_batch must be an object")
    run_id = require_string(batch, "run_id", code, "candidate validation_batch")
    run_attempt = batch.get("run_attempt")
    if not isinstance(run_attempt, int) or isinstance(run_attempt, bool) or run_attempt < 1:
        raise GateError(code, "candidate validation_batch.run_attempt must be a positive integer")

    deliverables = candidate.get("deliverables")
    if not isinstance(deliverables, list) or not deliverables:
        raise GateError(code, "candidate deliverables must be a non-empty list")
    parsed_deliverables = []
    seen_names = set()
    for entry in deliverables:
        if not isinstance(entry, dict):
            raise GateError(code, "candidate deliverables entries must be objects")
        name = require_string(entry, "name", code, "candidate deliverable")
        if name in seen_names:
            raise GateError(code, f"candidate deliverables duplicate name {name!r}")
        seen_names.add(name)
        kind = require_string(entry, "kind", code, f"candidate deliverable {name}")
        location = require_string(entry, "location", code, f"candidate deliverable {name}")
        if MUTABLE_LATEST.search(location):
            raise GateError(
                code,
                f"candidate deliverable {name}: location must be immutable, not a latest pointer",
            )
        sha256 = require_hex(
            entry.get("sha256"), HEX64, code, "sha256", f"candidate deliverable {name}"
        )
        size = entry.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 1:
            raise GateError(code, f"candidate deliverable {name}: size must be a positive integer")
        binary_sha256 = entry.get("binary_sha256")
        if binary_sha256 is not None:
            binary_sha256 = require_hex(
                binary_sha256, HEX64, code, "binary_sha256", f"candidate deliverable {name}"
            )
        if kind in ("zip", "deb", "rpm") and binary_sha256 is None:
            raise GateError(
                code,
                f"candidate deliverable {name}: packages must record the wrapped binary SHA256",
            )
        parsed_deliverables.append(
            {
                "name": name,
                "kind": kind,
                "location": location,
                "sha256": sha256,
                "size": size,
                "binary_sha256": binary_sha256,
            }
        )

    image = None
    if candidate.get("image") is not None:
        entry = candidate["image"]
        if not isinstance(entry, dict):
            raise GateError(code, "candidate image must be an object")
        ref = require_string(entry, "ref", code, "candidate image")
        if ref.endswith(":latest"):
            raise GateError(code, "candidate image ref must not target the mutable latest tag")
        digest = entry.get("digest")
        if not isinstance(digest, str) or not IMAGE_DIGEST.match(digest):
            raise GateError(code, "candidate image digest must be an immutable sha256 digest")
        image_binary = require_hex(
            entry.get("binary_sha256"), HEX64, code, "binary_sha256", "candidate image"
        )
        image = {"ref": ref, "digest": digest, "binary_sha256": image_binary}

    return {
        "product_commit": product_commit,
        "version": version,
        "release_profile": release_profile,
        "features": features,
        "validation_batch": {"run_id": run_id, "run_attempt": run_attempt},
        "deliverables": parsed_deliverables,
        "image": image,
        "candidate_sha256": canonical_sha256(candidate),
    }


# ---------------------------------------------------------------------------
# Evidence envelopes (G01, schema_version=1)
# ---------------------------------------------------------------------------


def validate_envelope(envelope, plan_info, candidate_info, source_name):
    code = "evidence_invalid"
    context = f"envelope {source_name}"
    if not isinstance(envelope, dict):
        raise GateError(code, f"{context}: must be a JSON object")
    if envelope.get("schema_version") != SCHEMA_VERSION:
        raise GateError(code, f"{context}: schema_version must be {SCHEMA_VERSION}")
    for field in REQUIRED_ENVELOPE_KEYS:
        if field not in envelope:
            raise GateError(code, f"{context}: missing required field {field}")

    evidence_profile = require_string(envelope, "evidence_profile", code, context)
    run_id = require_string(envelope, "run_id", code, context)
    run_attempt = envelope.get("run_attempt")
    if not isinstance(run_attempt, int) or isinstance(run_attempt, bool) or run_attempt < 1:
        raise GateError(code, f"{context}: run_attempt must be a positive integer")
    envelope_plan_sha = require_hex(
        envelope.get("plan_sha256"), HEX64, code, "plan_sha256", context
    )
    if envelope_plan_sha != plan_info["plan_sha256"]:
        raise GateError(
            "plan_mismatch",
            f"{context}: plan_sha256 does not match the validation plan",
        )
    require_string(envelope, "tier", code, context)
    lane = require_string(envelope, "lane", code, context)
    case_id = require_string(envelope, "case_id", code, context)
    product_commit = require_hex(
        envelope.get("product_commit"), HEX40, code, "product_commit", context
    )
    harness_commit = require_hex(
        envelope.get("harness_commit"), HEX40, code, "harness_commit", context
    )
    require_string(envelope, "target_fingerprint", code, context)
    platform = parse_platform(envelope.get("platform"), code, context)
    started_at = parse_utc(envelope.get("started_at"), code, f"{context} started_at")
    finished_at = parse_utc(envelope.get("finished_at"), code, f"{context} finished_at")
    if finished_at < started_at:
        raise GateError(code, f"{context}: finished_at precedes started_at")

    state = envelope.get("state")
    if state not in STATES:
        raise GateError(code, f"{context}: state must be one of {STATES}")
    reason_code = require_string(envelope, "reason_code", code, context)
    reason_class = envelope.get("reason_class")
    if reason_class not in REASON_CLASSES:
        raise GateError(code, f"{context}: reason_class must be one of {REASON_CLASSES}")

    evidence = envelope.get("evidence")
    if not isinstance(evidence, list):
        raise GateError(code, f"{context}: evidence must be a list")
    for entry in evidence:
        if not isinstance(entry, dict):
            raise GateError(code, f"{context}: evidence entries must be objects")
        path = require_string(entry, "path", code, f"{context} evidence")
        if path.startswith("/") or ".." in path.split("/"):
            raise GateError(
                code,
                f"{context}: evidence path {path!r} must stay inside the run's evidence root",
            )
        require_hex(entry.get("sha256"), HEX64, code, "sha256", f"{context} evidence")
        size = entry.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise GateError(code, f"{context}: evidence size must be a non-negative integer")
    if state == "pass" and not evidence:
        raise GateError(code, f"{context}: a PASS envelope must carry evidence entries")

    workflow = envelope.get("workflow")
    if not isinstance(workflow, dict):
        raise GateError(code, f"{context}: workflow must be an object")
    repository = require_string(workflow, "repository", code, f"{context} workflow")
    workflow_id = require_string(workflow, "workflow_id", code, f"{context} workflow")
    workflow_run_id = require_string(workflow, "run_id", code, f"{context} workflow")
    workflow_attempt = workflow.get("run_attempt")
    if (
        not isinstance(workflow_attempt, int)
        or isinstance(workflow_attempt, bool)
        or workflow_attempt < 1
    ):
        raise GateError(code, f"{context}: workflow.run_attempt must be a positive integer")
    workflow_sha = require_hex(
        workflow.get("workflow_sha"), HEX40, code, "workflow_sha", f"{context} workflow"
    )
    if (repository, workflow_id) not in plan_info["trusted_pairs"]:
        raise GateError(
            "untrusted_source",
            f"{context}: workflow {repository}/{workflow_id} is not a trusted verification source; "
            "PR-uploaded results cannot certify a release",
        )
    workflow_status = workflow.get("status")
    if workflow_status is not None and workflow_status not in (
        "completed",
        "cancelled",
        "timed_out",
    ):
        raise GateError(code, f"{context}: workflow.status is not a known status")

    lane_info = plan_info["lanes"].get(lane)
    if lane_info is not None:
        if harness_commit != lane_info["harness_sha"]:
            raise GateError(
                "harness_mismatch",
                f"{context}: harness_commit does not match the fixed harness SHA for lane {lane}",
                case_id=case_id,
                lane=lane,
            )
        if platform_key(platform) not in {
            platform_key(entry) for entry in lane_info["platforms"]
        }:
            raise GateError(
                "platform_mismatch",
                f"{context}: platform is not applicable to lane {lane}",
                case_id=case_id,
                lane=lane,
            )

    if product_commit != candidate_info["product_commit"]:
        raise GateError(
            "identity_mismatch",
            f"{context}: product_commit does not match the candidate",
            case_id=case_id,
            lane=lane,
        )
    batch = candidate_info["validation_batch"]
    if run_id != batch["run_id"]:
        raise GateError(
            "identity_mismatch",
            f"{context}: run_id does not identify this parent candidate batch",
            case_id=case_id,
            lane=lane,
        )
    if run_attempt != batch["run_attempt"]:
        raise GateError(
            "stale_attempt",
            f"{context}: run_attempt {run_attempt} is not the candidate batch attempt "
            f"{batch['run_attempt']}; stale evidence cannot certify a new attempt",
            case_id=case_id,
            lane=lane,
        )

    artifact = envelope.get("artifact")
    if artifact is not None:
        if not isinstance(artifact, dict):
            raise GateError(code, f"{context}: artifact must be an object")
        _validate_artifact_binding(
            artifact, candidate_info, context, case_id=case_id, lane=lane
        )

    return {
        "lane": lane,
        "platform": platform,
        "case_id": case_id,
        "state": state,
        "reason_code": reason_code,
        "reason_class": reason_class,
        "evidence_profile": evidence_profile,
        "workflow": {
            "repository": repository,
            "workflow_id": workflow_id,
            "run_id": workflow_run_id,
            "run_attempt": workflow_attempt,
            "workflow_sha": workflow_sha,
            "status": workflow_status,
        },
    }


def _validate_artifact_binding(artifact, candidate_info, context, case_id, lane):
    deliverables = candidate_info["deliverables"]
    code = "evidence_invalid"
    archive_sha = artifact.get("archive_sha256")
    if archive_sha is not None:
        archive_sha = require_hex(archive_sha, HEX64, code, "archive_sha256", context)
        if not any(entry["sha256"] == archive_sha for entry in deliverables):
            raise GateError(
                "digest_mismatch",
                f"{context}: artifact archive_sha256 does not match any candidate deliverable; "
                "same tag with different bytes cannot reuse a certification",
                case_id=case_id,
                lane=lane,
            )
    binary_sha = artifact.get("binary_sha256")
    if binary_sha is not None:
        binary_sha = require_hex(binary_sha, HEX64, code, "binary_sha256", context)
        known_binaries = {
            entry["binary_sha256"] for entry in deliverables if entry["binary_sha256"]
        }
        if candidate_info["image"] is not None:
            known_binaries.add(candidate_info["image"]["binary_sha256"])
        if binary_sha not in known_binaries:
            raise GateError(
                "digest_mismatch",
                f"{context}: artifact binary_sha256 does not match the candidate's recorded binaries",
                case_id=case_id,
                lane=lane,
            )
    image_digest = artifact.get("container_image_digest")
    if image_digest is not None:
        if not isinstance(image_digest, str) or not IMAGE_DIGEST.match(image_digest):
            raise GateError(
                "evidence_invalid",
                f"{context}: artifact container_image_digest must be a sha256 digest",
            )
        if candidate_info["image"] is None or image_digest != candidate_info["image"]["digest"]:
            raise GateError(
                "digest_mismatch",
                f"{context}: container_image_digest does not match the candidate image digest",
                case_id=case_id,
                lane=lane,
            )


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def run_validate(plan_path, candidate_path, results_dir, output_dir):
    findings = []
    plan_info = None
    candidate_info = None

    try:
        plan = load_json(plan_path, "plan_invalid")
        plan_info = validate_plan(plan)
        candidate = load_json(candidate_path, "candidate_invalid")
        candidate_info = validate_candidate(candidate, plan_info, plan_info["plan_sha256"])
    except GateError as error:
        findings.append(error)

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    if plan_info is None or candidate_info is None:
        _write_outputs(output, findings, plan_info, candidate_info, [], [])
        return 1

    required = expand_required(plan_info["lanes"], candidate_info["release_profile"])
    expansion = {
        "schema_version": SCHEMA_VERSION,
        "plan_sha256": plan_info["plan_sha256"],
        "candidate_sha256": candidate_info["candidate_sha256"],
        "release_profile": candidate_info["release_profile"],
        "validation_batch": candidate_info["validation_batch"],
        "required_cases": required,
        "trusted_workflows": [
            {"repository": repository, "workflow_id": workflow_id}
            for repository, workflow_id in plan_info["trusted_pairs"]
        ],
    }
    # Frozen before any result is read: the required set may not shrink afterwards.
    with open(output / "plan-expansion.json", "w", encoding="utf-8") as handle:
        json.dump(expansion, handle, indent=2, sort_keys=True)
        handle.write("\n")

    results = Path(results_dir)
    envelopes = []
    malformed = 0
    if results.is_dir():
        for entry in sorted(results.glob("*.json")):
            try:
                document = load_json(entry, "evidence_invalid")
                if entry.name == "batch.json":
                    batch = _validate_batch(document, candidate_info, entry.name)
                    if batch["status"] == "cancelled":
                        findings.append(
                            GateError(
                                "cancelled",
                                "parent candidate batch was cancelled; "
                                "no official publish write may run",
                            )
                        )
                    elif batch["status"] == "timed_out":
                        findings.append(
                            GateError(
                                "timeout",
                                "parent candidate batch timed out; "
                                "no official publish write may run",
                            )
                        )
                    continue
                parsed = validate_envelope(document, plan_info, candidate_info, entry.name)
            except GateError as error:
                findings.append(error)
                malformed += 1
                continue
            if parsed["workflow"]["status"] == "cancelled":
                findings.append(
                    GateError(
                        "cancelled",
                        f"envelope {entry.name}: child workflow "
                        f"{parsed['workflow']['repository']}/{parsed['workflow']['workflow_id']} "
                        f"run {parsed['workflow']['run_id']} was cancelled",
                        case_id=parsed["case_id"],
                        lane=parsed["lane"],
                    )
                )
            if parsed["workflow"]["status"] == "timed_out":
                findings.append(
                    GateError(
                        "timeout",
                        f"envelope {entry.name}: child workflow run "
                        f"{parsed['workflow']['run_id']} timed out",
                        case_id=parsed["case_id"],
                        lane=parsed["lane"],
                    )
                )
            envelopes.append(parsed)
    else:
        findings.append(GateError("evidence_invalid", f"results directory not found: {results}"))
        malformed += 1

    if not envelopes and malformed == 0:
        findings.append(
            GateError(
                "zero_tests",
                "results contain zero evidence envelopes; an empty batch cannot certify a release",
            )
        )

    seen = {}
    for envelope in envelopes:
        key = (
            envelope["lane"],
            platform_key(envelope["platform"]),
            envelope["case_id"],
        )
        if key in seen:
            findings.append(
                GateError(
                    "duplicate_key",
                    "duplicate result for "
                    + key_label(envelope["lane"], envelope["platform"], envelope["case_id"])
                    + " within one parent batch",
                    case_id=envelope["case_id"],
                    lane=envelope["lane"],
                )
            )
            continue
        seen[key] = envelope

    required_keys = {
        (entry["lane"], platform_key(entry["platform"]), entry["case_id"]): entry
        for entry in required
    }
    for key, entry in required_keys.items():
        if key not in seen:
            findings.append(
                GateError(
                    "missing_case",
                    "missing required case "
                    + key_label(entry["lane"], entry["platform"], entry["case_id"]),
                    case_id=entry["case_id"],
                    lane=entry["lane"],
                    platform=entry["platform"],
                )
            )

    for key, envelope in seen.items():
        if key in required_keys:
            if envelope["state"] != "pass":
                findings.append(
                    GateError(
                        "non_pass",
                        "required case "
                        + key_label(envelope["lane"], envelope["platform"], envelope["case_id"])
                        + f" is {envelope['state']} ({envelope['reason_code']}); "
                        "only PASS satisfies a required item",
                        case_id=envelope["case_id"],
                        lane=envelope["lane"],
                    )
                )
            continue
        lane, platform_key_value, case_id = key
        platform = {"os": platform_key_value[0], "arch": platform_key_value[1], "abi": platform_key_value[2]}
        if planned_pair(plan_info["lanes"], lane, case_id):
            findings.append(
                GateError(
                    "platform_mismatch",
                    "case "
                    + key_label(lane, platform, case_id)
                    + " ran on a platform outside the lane's applicability",
                    case_id=case_id,
                    lane=lane,
                )
            )
        else:
            findings.append(
                GateError(
                    "extra_case",
                    "unplanned case "
                    + key_label(lane, platform, case_id)
                    + " is not in the validation plan's required set",
                    case_id=case_id,
                    lane=lane,
                )
            )

    _write_outputs(output, findings, plan_info, candidate_info, required, envelopes)
    return 1 if findings else 0


def _validate_batch(document, candidate_info, source_name):
    code = "evidence_invalid"
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_VERSION:
        raise GateError(code, f"batch.json: schema_version must be {SCHEMA_VERSION}")
    run_id = require_string(document, "run_id", code, source_name)
    run_attempt = document.get("run_attempt")
    if not isinstance(run_attempt, int) or isinstance(run_attempt, bool) or run_attempt < 1:
        raise GateError(code, f"{source_name}: run_attempt must be a positive integer")
    status = document.get("status")
    if status not in BATCH_STATUSES:
        raise GateError(code, f"{source_name}: status must be one of {BATCH_STATUSES}")
    batch = candidate_info["validation_batch"]
    if run_id != batch["run_id"] or run_attempt != batch["run_attempt"]:
        raise GateError(
            "stale_attempt",
            f"{source_name}: batch identity does not match the candidate batch",
        )
    return {"run_id": run_id, "run_attempt": run_attempt, "status": status}


def _write_outputs(output, findings, plan_info, candidate_info, required, envelopes):
    state = "blocked" if findings else "pass"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "state": state,
        "publish_allowed": not findings,
        "plan_sha256": plan_info["plan_sha256"] if plan_info else None,
        "candidate_sha256": candidate_info["candidate_sha256"] if candidate_info else None,
        "product_commit": candidate_info["product_commit"] if candidate_info else None,
        "version": candidate_info["version"] if candidate_info else None,
        "release_profile": candidate_info["release_profile"] if candidate_info else None,
        "validation_batch": candidate_info["validation_batch"] if candidate_info else None,
        "required_cases": required,
        "observed": [
            {
                "lane": envelope["lane"],
                "platform": envelope["platform"],
                "case_id": envelope["case_id"],
                "state": envelope["state"],
                "reason_code": envelope["reason_code"],
            }
            for envelope in envelopes
        ],
        "findings": [finding.as_dict() for finding in findings],
    }
    with open(output / "release-gate-summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
    _write_junit(output / "release-gate.junit.xml", required, findings)
    lines = [
        "# Release gate summary",
        "",
        f"- state: {state}",
        f"- publish_allowed: {str(not findings).lower()}",
        f"- plan_sha256: {summary['plan_sha256']}",
        f"- candidate_sha256: {summary['candidate_sha256']}",
        f"- required cases: {len(required)}",
        f"- observed envelopes: {len(envelopes)}",
        f"- findings: {len(findings)}",
    ]
    for finding in findings:
        label = finding.case_id or "-"
        lines.append(f"- {finding.code}: {label}: {finding.message}")
    with open(output / "release-gate-summary.md", "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def _write_junit(path, required, findings):
    required_labels = {
        key_label(entry["lane"], entry["platform"], entry["case_id"]): entry for entry in required
    }
    cases = 0
    failures = 0
    suite = ET.Element("testsuite", {"name": "release-gate"})
    bound = set()
    for label, entry in required_labels.items():
        cases += 1
        testcase = ET.SubElement(suite, "testcase", {"classname": "release-gate", "name": label})
        for finding in findings:
            if (
                finding.case_id == entry["case_id"]
                and finding.lane == entry["lane"]
                and label not in bound
            ):
                failures += 1
                bound.add(label)
                ET.SubElement(
                    testcase,
                    "failure",
                    {"type": finding.code, "message": finding.message},
                )
                break
    for finding in findings:
        if finding.case_id is not None and finding.lane is not None:
            label = finding.case_id
            for entry in required:
                if entry["case_id"] == finding.case_id and entry["lane"] == finding.lane:
                    label = key_label(entry["lane"], entry["platform"], entry["case_id"])
                    break
            if label in bound:
                continue
            bound.add(label)
        else:
            label = finding.code
            if label in bound:
                label = f"{finding.code}:{finding.message[:40]}"
            bound.add(label)
        cases += 1
        failures += 1
        testcase = ET.SubElement(suite, "testcase", {"classname": "release-gate", "name": str(label)})
        ET.SubElement(testcase, "failure", {"type": finding.code, "message": finding.message})
    suite.set("tests", str(cases))
    suite.set("failures", str(failures))
    ET.ElementTree(suite).write(path, encoding="unicode", xml_declaration=True)


# ---------------------------------------------------------------------------
# verify-promotion
# ---------------------------------------------------------------------------


def run_verify_promotion(
    plan_path,
    candidate_path,
    summary_path,
    artifact_path,
    artifact_name,
    artifact_sha256,
    output_dir,
    artifact_role="archive",
):
    findings = []
    summary_file = Path(summary_path)
    if summary_file.is_dir():
        summary_file = summary_file / "release-gate-summary.json"
    try:
        summary = load_json(summary_file, "certification_missing")
        if summary.get("state") != "pass" or not summary.get("publish_allowed"):
            raise GateError(
                "certification_missing",
                "no passing release certification to promote against; "
                "publication stays blocked",
            )
        plan = load_json(plan_path, "plan_invalid")
        plan_info = validate_plan(plan)
        if summary.get("plan_sha256") != plan_info["plan_sha256"]:
            raise GateError(
                "plan_edited",
                "the validation plan changed after certification; the old certification cannot be reused",
            )
        candidate = load_json(candidate_path, "candidate_invalid")
        candidate_info = validate_candidate(candidate, plan_info, plan_info["plan_sha256"])

        if summary.get("candidate_sha256") != candidate_info["candidate_sha256"]:
            raise GateError(
                "candidate_edited",
                "the candidate record changed after certification; "
                "re-run validation before promotion",
            )
        current_required = {
            (
                entry["lane"],
                platform_key(entry["platform"]),
                entry["case_id"],
            )
            for entry in expand_required(plan_info["lanes"], candidate_info["release_profile"])
        }
        for entry in summary.get("required_cases", []):
            key = (entry["lane"], platform_key(entry["platform"]), entry["case_id"])
            if key not in current_required:
                raise GateError(
                    "required_set_shrunk",
                    "the plan's required set shrank after certification: "
                    + key_label(entry["lane"], entry["platform"], entry["case_id"]),
                    case_id=entry["case_id"],
                    lane=entry["lane"],
                )

        if artifact_path is not None:
            artifact_sha256 = file_sha256(artifact_path)
            if artifact_name is None:
                artifact_name = Path(artifact_path).name
        if not artifact_sha256 or not HEX64.match(artifact_sha256):
            raise GateError(
                "artifact_mismatch",
                "promotion requires the artifact bytes (or their sha256) to re-check against the passed list",
            )
        if artifact_role not in ("archive", "binary"):
            raise GateError(
                "artifact_mismatch", "artifact role must be 'archive' or 'binary'"
            )

        digest = artifact_sha256
        if artifact_name is not None and candidate_info["image"] is not None:
            if artifact_name == candidate_info["image"]["ref"] or artifact_name == "image":
                expected = candidate_info["image"]["digest"].split(":", 1)[1]
                if digest != expected:
                    raise GateError(
                        "artifact_mismatch",
                        "image digest does not match the certified candidate image",
                    )
                digest = None

        if digest is not None:
            if artifact_role == "binary":
                matches = [
                    entry
                    for entry in candidate_info["deliverables"]
                    if entry["binary_sha256"] == digest
                ]
            else:
                matches = [
                    entry
                    for entry in candidate_info["deliverables"]
                    if entry["sha256"] == digest
                ]
            if not matches:
                raise GateError(
                    "artifact_mismatch",
                    "artifact bytes do not match any certified deliverable; "
                    "a swapped package cannot be promoted",
                )
            if (
                artifact_role == "archive"
                and artifact_name is not None
                and not any(entry["name"] == artifact_name for entry in matches)
            ):
                raise GateError(
                    "artifact_mismatch",
                    f"artifact name {artifact_name!r} does not match the certified deliverable "
                    "bound to these bytes",
                )
    except GateError as error:
        findings.append(error)

    if output_dir is not None:
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": SCHEMA_VERSION,
            "state": "blocked" if findings else "pass",
            "artifact_name": artifact_name,
            "artifact_sha256": artifact_sha256,
            "artifact_role": artifact_role,
            "findings": [finding.as_dict() for finding in findings],
        }
        with open(output / "promotion-check.json", "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
            handle.write("\n")

    for finding in findings:
        print(f"{finding.code}: {finding.message}", file=sys.stderr)
    return 1 if findings else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate", help="aggregate one parent candidate batch's evidence"
    )
    validate_parser.add_argument("--plan", required=True)
    validate_parser.add_argument("--candidate", required=True)
    validate_parser.add_argument("--results", required=True)
    validate_parser.add_argument("--output", required=True)

    promote_parser = subparsers.add_parser(
        "verify-promotion", help="re-check to-be-published bytes against the passed list"
    )
    promote_parser.add_argument("--plan", required=True)
    promote_parser.add_argument("--candidate", required=True)
    promote_parser.add_argument("--summary", required=True)
    promote_parser.add_argument("--artifact", default=None)
    promote_parser.add_argument("--artifact-name", default=None)
    promote_parser.add_argument("--artifact-sha256", default=None)
    promote_parser.add_argument(
        "--artifact-role",
        default="archive",
        choices=("archive", "binary"),
        help="match the digest against certified archive digests or wrapped binary digests",
    )
    promote_parser.add_argument("--output", default=None)

    arguments = parser.parse_args(argv)
    if arguments.command == "validate":
        try:
            return run_validate(
                arguments.plan,
                arguments.candidate,
                arguments.results,
                arguments.output,
            )
        except GateError as error:
            print(f"{error.code}: {error.message}", file=sys.stderr)
            return 1
    if arguments.artifact is None and arguments.artifact_sha256 is None:
        print(
            "artifact_mismatch: verify-promotion requires --artifact or --artifact-sha256",
            file=sys.stderr,
        )
        return 1
    return run_verify_promotion(
        arguments.plan,
        arguments.candidate,
        arguments.summary,
        arguments.artifact,
        arguments.artifact_name,
        arguments.artifact_sha256,
        arguments.output,
        arguments.artifact_role,
    )


if __name__ == "__main__":
    sys.exit(main())
