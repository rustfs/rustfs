#!/usr/bin/env python3
"""Adversarial namespace, version, pagination and streaming oracle checks."""

import copy
import hashlib
import io
import unittest

import scanner_version_oracle as oracle


class S3Error(Exception):
    def __init__(self, status, version):
        self.response = {"ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": {
            "x-amz-delete-marker": "true", "x-amz-version-id": version}}}


def fixture():
    rows, payloads = [], {}
    for key, versions in (("alpha", [b"old-data", b"new-data"]), ("gone", [b"retained", None]),
                          ("recreated", [None, b"current"]), ("empty", [b""])):
        for index, payload in enumerate(versions):
            version = key + "-" + str(index + 1)
            row = {"bucket": "bench-bucket", "key": key, "generation": str(index + 1), "version_id": version,
                   "is_latest": index == len(versions) - 1, "delete_marker": payload is None,
                   "bytes": 0 if payload is None else len(payload),
                   "sha256": None if payload is None else hashlib.sha256(payload).hexdigest()}
            rows.append(row)
            payloads[oracle.identity(row)] = payload
    return {"buckets": ["bench-bucket", "empty-bucket"], "versions": rows}, payloads


class Client:
    def __init__(self, inventory, payloads):
        self.rows = copy.deepcopy(inventory["versions"])
        self.payloads = dict(payloads)
        self.gets, self.streams = [], []
        self.version_round = 0
        self.change_after_read = False
        self.hidden_current = False
        self.override_current = None
        self.delete_status = None
        self.bad_length = False
        self.stream_failure = False

    def list_object_versions(self, Bucket, MaxKeys, **cursor):
        if Bucket == "bench-bucket" and not cursor:
            self.version_round += 1
        rows = [r for r in self.rows if r["bucket"] == Bucket]
        start = 0
        if cursor:
            start = next(i + 1 for i, r in enumerate(rows)
                         if (r["key"], r["version_id"]) == (cursor["KeyMarker"], cursor["VersionIdMarker"]))
        selected = rows[start:start + MaxKeys]
        page = {"IsTruncated": start + MaxKeys < len(rows), "Versions": [], "DeleteMarkers": []}
        for row in selected:
            item = {"Key": row["key"], "VersionId": row["version_id"], "IsLatest": row["is_latest"]}
            if self.change_after_read and self.version_round > 1:
                item["IsLatest"] = not item["IsLatest"]
            if not row["delete_marker"]:
                item["Size"] = row["bytes"]
            page["DeleteMarkers" if row["delete_marker"] else "Versions"].append(item)
        if page["IsTruncated"]:
            page.update(NextKeyMarker=selected[-1]["key"], NextVersionIdMarker=selected[-1]["version_id"])
        return page

    def list_objects_v2(self, Bucket, MaxKeys, **cursor):
        rows = [r for r in self.rows if r["bucket"] == Bucket and r["is_latest"] and not r["delete_marker"]]
        if self.hidden_current:
            rows = rows[1:]
        start = int(cursor.get("ContinuationToken", 0))
        page = {"IsTruncated": start + MaxKeys < len(rows),
                "Contents": [{"Key": r["key"], "Size": r["bytes"]} for r in rows[start:start + MaxKeys]]}
        if page["IsTruncated"]:
            page["NextContinuationToken"] = str(start + MaxKeys)
        return page

    def get_object(self, Bucket, Key, **kwargs):
        self.gets.append((Bucket, Key, kwargs))
        rows = [r for r in self.rows if r["bucket"] == Bucket and r["key"] == Key]
        row = next(r for r in rows if r["version_id"] == kwargs["VersionId"]) if kwargs else next(r for r in rows if r["is_latest"])
        if not kwargs and self.override_current and Key == "alpha":
            row = rows[0]
        if row["delete_marker"]:
            raise S3Error(self.delete_status or (405 if kwargs else 404), row["version_id"])
        body = io.BytesIO(self.payloads[oracle.identity(row)])
        if self.stream_failure:
            class Broken(io.BytesIO):
                def read(self, count):
                    if self.tell():
                        raise OSError("read failed after partial data")
                    return super().read(1)
            body = Broken(self.payloads[oracle.identity(row)])
        self.streams.append(body)
        return {"VersionId": row["version_id"], "ContentLength": row["bytes"] + self.bad_length, "Body": body}


class OracleTest(unittest.TestCase):
    def setUp(self):
        self.inventory, self.payloads = fixture()
        self.client = Client(self.inventory, self.payloads)

    def run_oracle(self, clients=None):
        return oracle.verify(clients or {"node-a": self.client}, self.inventory, workers=2, page_size=2)

    def test_all_versions_markers_current_reads_and_empty_bucket(self):
        other = Client(self.inventory, self.payloads)
        result = self.run_oracle({"node-a": self.client, "node-b": other})
        self.assertTrue(result["complete"])
        self.assertEqual(result["actual"]["objects"], 3)
        self.assertEqual(result["actual"]["versions"], 7)
        self.assertEqual(result["actual"]["bytes"], 31)
        self.assertEqual(result["actual"], oracle.summary(self.inventory["versions"]))
        for client in (self.client, other):
            self.assertEqual(len(client.gets), 11)
            self.assertEqual({(b, k, q["VersionId"]) for b, k, q in client.gets if q}, set(self.payloads))
            self.assertTrue(all(body.closed for body in client.streams))

    def test_corrupt_noncurrent_content_on_second_endpoint_rejects(self):
        other = Client(self.inventory, self.payloads)
        other.payloads["bench-bucket", "alpha", "alpha-1"] = b"bad-data"
        with self.assertRaisesRegex(ValueError, "content mismatch"):
            self.run_oracle({"node-a": self.client, "node-b": other})
        self.assertTrue(all(body.closed for body in other.streams))

    def test_short_long_and_wrong_length_close_stream(self):
        for body in (b"short", b"longer-than-expected"):
            with self.subTest(body=body):
                self.client.payloads["bench-bucket", "alpha", "alpha-1"] = body
                with self.assertRaises(ValueError):
                    self.run_oracle()
                self.assertTrue(all(stream.closed for stream in self.client.streams))
        self.client.bad_length = True
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            self.run_oracle()
        self.assertTrue(all(stream.closed for stream in self.client.streams))

    def test_partial_stream_error_is_not_success(self):
        self.client.stream_failure = True
        with self.assertRaises(OSError):
            self.run_oracle()
        self.assertTrue(all(stream.closed for stream in self.client.streams))

    def test_missing_and_extra_versions_reject(self):
        original = copy.deepcopy(self.client.rows)
        for rows in (original[1:], original + [dict(original[0], version_id="unexpected")]):
            self.client.rows = rows
            with self.assertRaisesRegex(ValueError, "namespace"):
                self.run_oracle()
        self.assertEqual(self.client.gets, [])

    def test_wrong_latest_and_marker_type_reject(self):
        for field, value in (("is_latest", True), ("delete_marker", True)):
            self.client.rows = copy.deepcopy(self.inventory["versions"])
            self.client.rows[0][field] = value
            with self.assertRaisesRegex(ValueError, "metadata mismatch"):
                self.run_oracle()

    def test_current_listing_and_unversioned_get_are_independent_checks(self):
        self.client.hidden_current = True
        with self.assertRaisesRegex(ValueError, "current namespace"):
            self.run_oracle()
        self.client.hidden_current = False
        self.client.override_current = True
        with self.assertRaisesRegex(ValueError, "GET version mismatch"):
            self.run_oracle()

    def test_delete_marker_service_error_is_not_an_expected_delete(self):
        self.client.delete_status = 503
        with self.assertRaisesRegex(ValueError, "delete marker GET contract"):
            self.run_oracle()
        counts = [call for call in self.client.gets if call[2].get("VersionId") == "gone-2"]
        self.assertLessEqual(len(counts), 1)

    def test_namespace_mutation_during_reads_rejects(self):
        self.client.change_after_read = True
        with self.assertRaisesRegex(ValueError, "changed during oracle"):
            self.run_oracle()

    def test_repeated_or_missing_pagination_marker_rejects(self):
        class BadPagination:
            def __init__(self):
                self.calls = 0
            def list_object_versions(self, **kwargs):
                self.calls += 1
                return {"IsTruncated": True, "NextKeyMarker": "same", "NextVersionIdMarker": "same",
                        "Versions": [{"Key": str(self.calls), "VersionId": "id", "Size": 1, "IsLatest": True}]}
            def list_objects_v2(self, **kwargs):
                return {"IsTruncated": True, "Contents": [{"Key": "key", "Size": 1}]}
        with self.assertRaisesRegex(ValueError, "version marker"):
            oracle.list_versions(BadPagination(), ["bench-bucket"], 2, 2)
        with self.assertRaisesRegex(ValueError, "current marker"):
            oracle.list_current(BadPagination(), ["bench-bucket"], 2, 2)

    def test_empty_truncated_pages_fail_without_looping(self):
        class EmptyPagination:
            def list_object_versions(self, **kwargs):
                return {"IsTruncated": True, "NextKeyMarker": "new", "NextVersionIdMarker": "new"}
            def list_objects_v2(self, **kwargs):
                return {"IsTruncated": True, "NextContinuationToken": "new"}
        for listing in (oracle.list_versions, oracle.list_current):
            with self.assertRaisesRegex(ValueError, "no progress"):
                listing(EmptyPagination(), ["bench-bucket"], 2, 100)

    def test_namespace_size_is_bounded_before_reads(self):
        with self.assertRaisesRegex(ValueError, "exceeds inventory"):
            oracle.list_versions(self.client, self.inventory["buckets"], 2, 1)
        with self.assertRaisesRegex(ValueError, "exceeds inventory"):
            oracle.list_current(self.client, self.inventory["buckets"], 2, 1)
        self.assertEqual(self.client.gets, [])

    def test_duplicate_listed_version_rejects(self):
        self.client.rows.insert(1, self.client.rows[0])
        with self.assertRaisesRegex(ValueError, "duplicate listed version"):
            self.run_oracle()

    def test_null_version_is_read_explicitly(self):
        row = self.inventory["versions"][0]
        data = self.payloads[oracle.identity(row)]
        row.update(version_id="null", is_latest=True)
        self.inventory["versions"] = [row]
        client = Client(self.inventory, {oracle.identity(row): data})
        self.run_oracle({"node": client})
        self.assertEqual(client.gets[0][2], {"VersionId": "null"})

    def test_logical_digest_stable_but_physical_binding_changes(self):
        first = self.run_oracle()
        new_payloads = {}
        for row in self.inventory["versions"]:
            data = self.payloads[oracle.identity(row)]
            row["version_id"] += "-another-leg"
            new_payloads[oracle.identity(row)] = data
        second = self.run_oracle({"node-a": Client(self.inventory, new_payloads)})
        self.assertEqual(first["actual"], second["actual"])
        self.assertNotEqual(first["endpoints"]["node-a"]["version_bindings_sha256"],
                            second["endpoints"]["node-a"]["version_bindings_sha256"])

    def test_malformed_inventory_fails_before_network(self):
        original = copy.deepcopy(self.inventory)
        for field, value in (("is_latest", 1), ("bytes", True), ("version_id", ""), ("sha256", "bad")):
            self.inventory = copy.deepcopy(original)
            self.inventory["versions"][0][field] = value
            with self.assertRaises(ValueError):
                self.run_oracle()
        self.assertEqual(self.client.version_round, 0)

    def test_duplicate_physical_or_logical_ids_reject(self):
        for field in ("version_id", "generation"):
            inventory = copy.deepcopy(self.inventory)
            inventory["versions"][1][field] = inventory["versions"][0][field]
            with self.assertRaisesRegex(ValueError, "duplicate version"):
                oracle.validate_inventory(inventory)


if __name__ == "__main__":
    unittest.main()
