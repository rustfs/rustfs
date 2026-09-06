#!/usr/bin/env python3
"""Signing regressions requiring the PyIceberg smoke runtime dependencies."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

from botocore.auth import S3SigV4Auth, SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from requests import Request, Response, Session
from requests.adapters import HTTPAdapter

import pyiceberg_smoke


RUSTFS_PROFILES = {"rustfs", "rustfs-compat", "rustfs-vended-credentials"}


class PyIcebergSigningTest(unittest.TestCase):
    def setUp(self) -> None:
        self.deps = SimpleNamespace(
            botocore_auth=SigV4Auth,
            botocore_s3_auth=S3SigV4Auth,
            botocore_credentials=Credentials,
            botocore_awsrequest=AWSRequest,
        )
        clock = mock.patch("botocore.auth.get_current_datetime", return_value=datetime(2026, 1, 1, tzinfo=timezone.utc))
        clock.start()
        self.addCleanup(clock.stop)

    def args(self, profile: str = "rustfs") -> object:
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            sys, "argv", ["pyiceberg_smoke.py", "--profile", profile, "--endpoint", "http://127.0.0.1:29500", "--bucket", "warehouse"]
        ):
            return pyiceberg_smoke.parse_args()

    def expected_signature(self, request: object, args: object, signer: object = S3SigV4Auth) -> str:
        headers = {key: value for key, value in request.headers.items() if key.lower() != "authorization"}
        expected = AWSRequest(method=request.method, url=request.url, data=request.body, headers=headers)
        signer(Credentials(args.access_key, args.secret_key), args.rest_signing_name, args.region).add_auth(expected)
        return expected.headers["Authorization"]

    def test_adapter_signs_the_encoded_wire_path_and_full_query(self) -> None:
        for profile in sorted(RUSTFS_PROFILES):
            args = self.args(profile)
            session = Session()
            self.addCleanup(session.close)
            catalog = SimpleNamespace(uri=f"{args.endpoint}{args.rest_path}", _session=session)
            pyiceberg_smoke.install_rustfs_rest_sigv4_adapter(catalog, args, self.deps)
            for namespace in ["sales", "sales%1Ftax", "literal%251F", "with%20space", "with%2Bplus", "a%2Fb"]:
                with self.subTest(profile=profile, namespace=namespace):
                    url = f"{catalog.uri}/v1/warehouse/namespaces/{namespace}?key=b&key=a&empty=&plus=%2B"
                    request = Request("POST", url, data=b'{"properties":{"owner":"analytics"}}').prepare()
                    session.get_adapter(url).add_headers(request)
                    self.assertEqual(request.url, url)
                    self.assertEqual(request.headers["x-amz-content-sha256"], hashlib.sha256(request.body).hexdigest())
                    self.assertEqual(request.headers["Authorization"], self.expected_signature(request, args))

    def test_signature_changes_when_method_path_or_body_changes(self) -> None:
        args = self.args()
        session = Session()
        self.addCleanup(session.close)
        catalog = SimpleNamespace(uri=f"{args.endpoint}{args.rest_path}", _session=session)
        pyiceberg_smoke.install_rustfs_rest_sigv4_adapter(catalog, args, self.deps)
        request = Request("POST", f"{catalog.uri}/v1/warehouse/namespaces/sales%1Ftax", data=b"{}").prepare()
        session.get_adapter(request.url).add_headers(request)
        for attribute, value in [("method", "DELETE"), ("url", request.url.replace("%1F", "%251F")), ("body", b'{"changed":true}')]:
            with self.subTest(attribute=attribute):
                changed = copy.copy(request)
                changed.headers = request.headers.copy()
                setattr(changed, attribute, value)
                self.assertNotEqual(request.headers["Authorization"], self.expected_signature(changed, args))

    def test_direct_rest_requests_use_the_same_path_contract(self) -> None:
        args = self.args()
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"{}"
        path = f"{args.rest_path}/v1/warehouse/namespaces/sales%1Ftax?key=b&key=a&empty="
        with mock.patch.object(pyiceberg_smoke.urllib.request, "urlopen", return_value=response) as send:
            pyiceberg_smoke.signed_rest_request(args, self.deps, "GET", path)
        wire = send.call_args.args[0]
        request = Request(wire.method, wire.full_url, headers=dict(wire.header_items())).prepare()
        self.assertEqual(request.headers["Authorization"], self.expected_signature(request, args))

    def test_vendor_profiles_keep_generic_sigv4_normalization(self) -> None:
        for profile in sorted(set(pyiceberg_smoke.PROFILE_DEFAULTS) - RUSTFS_PROFILES):
            with self.subTest(profile=profile):
                args = SimpleNamespace(profile=profile, access_key="test-access", secret_key="test-secret", rest_signing_name="s3tables", region="us-east-1")
                request = AWSRequest(method="GET", url="https://catalog.example/namespaces/sales%1Ftax")
                pyiceberg_smoke.sign_rest_request(args, self.deps, request)
                expected = AWSRequest(method="GET", url=request.url)
                SigV4Auth(Credentials(args.access_key, args.secret_key), args.rest_signing_name, args.region).add_auth(expected)
                self.assertEqual(request.headers["Authorization"], expected.headers["Authorization"])

    def test_initial_config_and_recreated_sessions_are_signed(self) -> None:
        for profile in sorted(RUSTFS_PROFILES):
            args = self.args(profile)
            seen = []

            def send(adapter: HTTPAdapter, request: object, **kwargs: object) -> Response:
                adapter.add_headers(request, **kwargs)
                self.assertEqual(request.headers["x-amz-content-sha256"], hashlib.sha256(b"").hexdigest())
                self.assertEqual(request.headers["Authorization"], self.expected_signature(request, args))
                seen.append(request.url)
                response = Response()
                response.status_code = 200
                response._content = json.dumps(
                    {"defaults": {}, "overrides": {}} if "/v1/config" in request.url else {"namespace": ["sales", "tax"], "properties": {}}
                ).encode()
                return response

            credential = pyiceberg_smoke.StorageCredential(
                prefix="s3://warehouse/tables/test/",
                config={"s3.access-key-id": "temporary-access", "s3.secret-access-key": "temporary-secret", "s3.session-token": "temporary-token"},
            )
            named_config = {"uri": "https://configured.example", "ssl": {"cabundle": "catalog-ca.pem", "client": {"cert": "client.pem", "key": "client-key.pem"}}}
            with self.subTest(profile=profile), mock.patch.object(HTTPAdapter, "send", autospec=True, side_effect=send), mock.patch(
                "pyiceberg.catalog._ENV_CONFIG.get_catalog_config", return_value=named_config
            ):
                for storage_credential in [None, credential]:
                    catalog = pyiceberg_smoke.load_rest_catalog(args, self.deps, storage_credential)
                    self.addCleanup(catalog._session.close)
                    self.assertEqual(catalog.uri, f"{args.endpoint}{args.rest_path}")
                    self.assertEqual(catalog._session.verify, "catalog-ca.pem")
                    self.assertEqual(catalog._session.cert, ("client.pem", "client-key.pem"))
                    self.assertEqual(catalog.load_namespace_properties(("sales", "tax")), {})
            self.assertEqual(len(seen), 4)
            self.assertIn("/v1/config", seen[0])
            self.assertIn("/namespaces/sales%1Ftax", seen[1])


if __name__ == "__main__":
    unittest.main()
