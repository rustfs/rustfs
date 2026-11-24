#!/usr/bin/env python3
# Copyright 2026 RustFS Team
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
"""Compare identical signed Ceph requests on two disposable loopback servers."""

import argparse
from collections import Counter
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import re
import secrets
import socket
import sys
import tempfile
import tomllib
from unittest.mock import patch
from urllib.parse import urlsplit
from xml.etree import ElementTree

import gateway_corpus_capture as capture


MAX_WIRE_BYTES = 8 * 1024 * 1024
HEADERS = {
    "accept-ranges", "cache-control", "content-disposition", "content-encoding", "content-language",
    "content-range", "content-type", "etag", "expires", "x-amz-delete-marker", "x-amz-object-lock-legal-hold",
    "x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-request-charged", "x-amz-version-id",
    "x-amz-website-redirect-location",
}
HEADER_PREFIXES = ("x-amz-server-side-encryption", "x-amz-meta-", "x-amz-checksum-")
EXPECTED_CONTROLS = {"changed_error_code": ["error_code", "body"], "changed_object": ["body"]}


class ReplayError(Exception):
    """A public diagnostic that contains no wire data."""


def verify_digest(path, expected):
    with path.open("rb") as file:
        actual = hashlib.file_digest(file, "sha256").hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", expected) or actual != expected:
        raise ReplayError("The reference executable differs from the verified release extraction")


def xml_tree(node):
    def text(value):
        return value if value and value.strip() else ""

    return (node.tag, sorted(node.attrib.items()), text(node.text),
            [(xml_tree(child), text(child.tail)) for child in node if child.tag.rsplit("}", 1)[-1] != "RequestId"])


def project(operation, method, status, headers, body):
    heads = [(name.lower(), value) for name, value in headers]
    lengths = [value for name, value in heads if name == "content-length"]
    if lengths and method != "HEAD" and status not in (204, 304) and lengths != [str(len(body))]:
        raise ReplayError("Invalid HTTP response Content-Length")
    selected = [(k, v) for k, v in heads if k in HEADERS or k.startswith(HEADER_PREFIXES)
                or (method == "HEAD" and k == "content-length")]
    selected.sort(key=lambda pair: pair[0])
    result = {"status": status, "headers": selected, "error_code": None, "body": hashlib.sha256(body).hexdigest()}
    if body.lstrip().startswith(b"<") and (status >= 400 or operation != "GetObject"):
        try:
            root = ElementTree.fromstring(body)
        except ElementTree.ParseError:
            return result
        result["body"] = xml_tree(root)
        if root.tag.rsplit("}", 1)[-1] == "Error":
            result["error_code"] = root.findtext("{*}Code")
    return result


def differences(left, right):
    return [field for field in left if left[field] != right[field]]


class SdkBody(io.BytesIO):
    def stream(self, amt=65536, decode_content=False):
        while piece := self.read(amt):
            yield piece


class WireReplay:
    def __init__(self, reference_port, candidate_port, frozen):
        self.authority = f"127.0.0.1:{reference_port}"
        self.reference_port = reference_port
        self.candidate_port = candidate_port
        self.frozen = frozen
        self.current = None
        self.operation = None
        self.case = None
        self.counts = Counter()
        self.operations = Counter()
        self.diffs = []
        self.controls = {}

    def compare(self, method, wire, response, content):
        head, body = wire.split(b"\r\n\r\n", 1)
        lines = head.decode("latin1").split("\r\n")
        fields = {k.lower(): v.strip() for k, v in (line.split(":", 1) for line in lines[1:])}
        if (lines[0].split(" ")[0] != method or fields.get("host") != self.authority or
                "Credential=rustfsadmin-ci/" not in fields.get("authorization", "") or
                "transfer-encoding" in fields or int(fields.get("content-length", "0")) != len(body)):
            raise ReplayError("Unexpected signing identity, authority or request framing")
        # The TCP destinations differ. The signed Host, header lines, target and body are untouched.
        with socket.create_connection(("127.0.0.1", self.candidate_port), timeout=15) as sock:
            sock.sendall(wire)
            with http.client.HTTPResponse(sock, method=method) as candidate:
                candidate.begin()
                candidate_body = candidate.read(MAX_WIRE_BYTES + 1)
                if len(candidate_body) > MAX_WIRE_BYTES:
                    raise ReplayError("Candidate response exceeds the selected fixture bound")
                candidate_headers = candidate.getheaders()
                right = project(self.operation, method, candidate.status, candidate_headers, candidate_body)
        reference_headers = list(response.raw.headers.items())
        for headers in (reference_headers, candidate_headers):
            dates = [v for k, v in headers if k.lower() == "date"]
            if len(dates) != 1 or parsedate_to_datetime(dates[0]) != self.frozen:
                raise ReplayError("The server clock did not match the fixed test instant")
        left = project(self.operation, method, response.status_code, reference_headers, content)
        self.counts[self.case] += 1
        self.operations[self.operation] += 1
        fields = differences(left, right)
        if fields:
            self.diffs.append({"case": self.case, "request": self.counts[self.case], "operation": self.operation, "fields": fields})
        if "changed_error_code" not in self.controls and left["error_code"] == "NoSuchKey":
            changed = content.replace(b"<Code>NoSuchKey</Code>", b"<Code>NoSuchBucket</Code>", 1)
            headers = [(k, str(len(changed)) if k.lower() == "content-length" else v) for k, v in reference_headers]
            mutation = project(self.operation, method, response.status_code, headers, changed)
            self.controls["changed_error_code"] = differences(left, mutation)
        if ("changed_object" not in self.controls and self.operation == "GetObject"
                and response.status_code == 200 and content):
            changed = bytes([content[0] ^ 1]) + content[1:]
            mutation = project(self.operation, method, response.status_code, reference_headers, changed)
            self.controls["changed_object"] = differences(left, mutation)

    @contextmanager
    def intercept(self):
        from botocore.awsrequest import AWSResponse
        from botocore.client import BaseClient
        from botocore.httpsession import URLLib3Session

        api_call = BaseClient._make_api_call
        http_send = http.client.HTTPConnection.send
        sdk_send = URLLib3Session.send

        def operation(client, name, params):
            self.operation = name
            return api_call(client, name, params)

        def send_bytes(connection, data):
            if self.current is not None:
                if connection.host != "127.0.0.1" or connection.port != self.reference_port:
                    raise ReplayError("SDK connection is outside the reference fixture")
                if not isinstance(data, (bytes, bytearray, memoryview)) or len(self.current) + len(data) > MAX_WIRE_BYTES:
                    raise ReplayError("Request exceeds the selected byte fixture bound")
                self.current.extend(data)
            return http_send(connection, data)

        def observe(session, request):
            url = urlsplit(request.url)
            if url.scheme != "http" or url.netloc != self.authority or self.current is not None:
                raise ReplayError("Unexpected SDK destination or concurrent request")
            self.current = bytearray()
            try:
                response = sdk_send(session, request)
                body = response.content
                if len(body) > MAX_WIRE_BYTES:
                    raise ReplayError("Reference response exceeds the selected fixture bound")
                self.compare(request.method, bytes(self.current), response, body)
                return AWSResponse(response.url, response.status_code, response.headers, SdkBody(body))
            finally:
                self.current = None

        with patch.object(BaseClient, "_make_api_call", operation), patch.object(http.client.HTTPConnection, "send", send_bytes), \
                patch.object(URLLib3Session, "send", observe):
            yield


def replay_cases():
    manifest = Path(__file__).resolve().parents[1] / ".config/gateway-corpus-tests.toml"
    return tomllib.loads(manifest.read_text())["wire_replay"]["ceph"]


def run(args, temp):
    if capture.revision(args.ceph) != capture.ceph_pin() or capture.revision(args.gateway) != capture.gateway_pin():
        raise ReplayError("Ceph or gateway checkout differs from its pin")
    verify_digest(args.reference, args.reference_sha256)
    selected = replay_cases()
    if args.reference_version != tomllib.loads(Path(".config/reference-binary.toml").read_text())["version"]:
        raise ReplayError("Reference version differs from the release pin")
    nightly = set(capture.select_tests("nightly")["ceph"])
    implemented = set(Path("scripts/s3-tests/implemented_tests.txt").read_text().splitlines())
    if not selected or len(set(selected)) != len(selected) or not set(selected) <= nightly & implemented:
        raise ReplayError("Invalid release-replay Ceph selection")
    frozen = datetime.now(timezone.utc).replace(microsecond=0)
    env = dict(os.environ, FAKETIME=frozen.strftime("%Y-%m-%d %H:%M:%S"), TZ="UTC", FAKETIME_DONT_FAKE_MONOTONIC="1",
               RUSTFS_CHECK_UPDATE="false", NO_PROXY="*", no_proxy="*")
    env["DYLD_INSERT_LIBRARIES" if sys.platform == "darwin" else "LD_PRELOAD"] = str(args.clock_library.resolve(strict=True))
    secret = secrets.token_urlsafe(36)
    candidate = Path(os.environ["CARGO_BIN_EXE_rustfs"])
    if args.reference.samefile(candidate):
        raise ReplayError("Release and candidate point to the same executable")
    # The workflow reuses the already verified corpus-record build and its mandatory fixture guards.
    private_capture = temp / "capture" / "corpus"
    private_capture.mkdir(mode=0o700, parents=True)
    candidate_env = dict(env, TMPDIR=str(temp), RUSTFS_CORPUS_DIR=str(private_capture), RUSTFS_CORPUS_RECORD="1",
                         RUSTFS_CORPUS_RECORD_SRC=f"s3-tests@{capture.ceph_pin()}")
    with capture.ceph_server(args.reference, temp / "reference", env, secret) as ref_port, \
            capture.ceph_server(candidate, temp / "candidate", candidate_env, secret) as cand_port:
        replay = WireReplay(ref_port, cand_port, frozen)
        conf = capture.ceph_config(temp, ref_port, secret)
        sys.path.insert(0, str(args.ceph))
        with patch.dict(os.environ, {"S3TEST_CONF": str(conf), "NO_PROXY": "*", "no_proxy": "*"}), \
                (temp / "ceph.log").open("w") as log, redirect_stdout(log), redirect_stderr(log):
            from s3tests import functional
            from s3tests.functional import test_s3
            if Path(test_s3.__file__).resolve() != args.ceph / "s3tests/functional/test_s3.py":
                raise ReplayError("Ceph tests were loaded from a different checkout")
            functional.configure()
            with replay.intercept():
                for case in selected:
                    replay.case = case
                    try:
                        getattr(test_s3, case)()
                    except ReplayError:
                        raise
                    except Exception as error:
                        raise ReplayError(f"Reference case {case} stopped ({type(error).__name__})") from None
    capture.scan(args.gateway, private_capture.parent)
    if set(replay.counts) != set(selected) or replay.controls != EXPECTED_CONTROLS:
        raise ReplayError("Missing real traffic or failed response-difference control")
    verify_digest(args.reference, args.reference_sha256)
    receipt = json.loads(candidate.with_name(candidate.name + ".e2e.json").read_text())
    return {"scope": "identical requests from selected Ceph tests", "reference_version": args.reference_version,
            "reference_executable_sha256": args.reference_sha256, "ceph_revision": capture.ceph_pin(),
            "candidate_sha256": receipt["binary_sha256"], "candidate_revision": receipt["source"]["head"],
            "requests_by_operation": dict(replay.operations),
            "requests_by_case": dict(replay.counts), "requests_compared": sum(replay.counts.values()),
            "differences": replay.diffs, "negative_controls": replay.controls,
            "other_ceph_cases_not_replayed": sorted(nightly - set(selected)), "e2e_requests_replayed": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    check = sub.add_parser("check")
    check.add_argument("summary", type=Path)
    compare = sub.add_parser("compare")
    for option in ("ceph", "gateway", "reference", "clock-library", "summary"):
        compare.add_argument(f"--{option}", type=lambda s: Path(s).resolve(), required=True)
    compare.add_argument("--reference-sha256", required=True)
    compare.add_argument("--reference-version", required=True)
    args = parser.parse_args(argv)
    if args.cmd == "check":
        report = json.loads(args.summary.read_text())
        print(f"{report['requests_compared']} requests compared, {len(report['differences'])} differences")
        counts = report["requests_by_case"]
        complete = (set(counts) == set(replay_cases()) and all(type(n) is int and n > 0 for n in counts.values())
                    and sum(counts.values()) == report["requests_compared"])
        return int(not complete or bool(report["differences"]) or report["negative_controls"] != EXPECTED_CONTROLS)
    args.summary.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="rustfs-wire-") as directory:
        temp = Path(directory)
        capture.publish_summary(args, temp, run(args, temp))
    return 0


if __name__ == "__main__":
    sys.exit(main())
