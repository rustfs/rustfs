#!/usr/bin/env python3
"""Own an isolated four-node Docker lab for inline geometry validation."""

import argparse
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
BASE_COMPOSE = ROOT / ".docker/compose/docker-compose.cluster.local-build.yml"
OWNER_LABEL = "rustfs.validation.owner"
NODES = tuple(f"node{i}" for i in range(1, 5))

CLIENT = r'''
import boto3, hashlib, json, random, sys
from botocore.config import Config
cfg = json.load(sys.stdin)
client = boto3.client("s3", endpoint_url="http://" + cfg.get("coordinator", "node1") + ":9000", region_name="us-east-1",
    aws_access_key_id=cfg["access_key"], aws_secret_access_key=cfg["secret_key"],
    config=Config(signature_version="s3v4", proxies={}, s3={"addressing_style":"path"}))
if cfg["action"] == "create":
    client.create_bucket(Bucket=cfg["bucket"])
    print("{}")
elif cfg["action"] == "probe":
    records = []
    for size in (4096, 1048576, 1048577, 1310720, 1572856, 1572857, 1572864):
        key = "geometry/probe-" + str(size)
        data = random.Random(size).randbytes(size)
        client.put_object(Bucket=cfg["bucket"], Key=key, Body=data)
        head = client.head_object(Bucket=cfg["bucket"], Key=key)
        assert head["ContentLength"] == size
        body = client.get_object(Bucket=cfg["bucket"], Key=key)["Body"].read()
        assert hashlib.sha256(body).digest() == hashlib.sha256(data).digest()
        for start in (0, 1048570, size - 4096):
            if start < 0 or start >= size:
                continue
            end = min(size - 1, start + 4095)
            result = client.get_object(Bucket=cfg["bucket"], Key=key, Range=f"bytes={start}-{end}")
            assert result["Body"].read() == data[start:end + 1]
        records.append({"key":key, "size":size, "sha256":hashlib.sha256(data).hexdigest()})
    print(json.dumps(records))
elif cfg["action"] in ("compat-seed", "compat-check", "compat-finish-mpu", "compat-delete"):
    def payload(size, seed):
        return random.Random(seed).randbytes(size)

    def record(key, size, seed, response):
        return {"key": key, "size": size, "seed": seed,
                "version_id": response.get("VersionId", "null"),
                "sdk_retry_attempts": response.get("ResponseMetadata", {}).get("RetryAttempts", 0),
                "sha256": hashlib.sha256(payload(size, seed)).hexdigest()}

    manifest = cfg.get("manifest")
    if cfg["action"] == "compat-seed":
        from botocore.exceptions import ClientError
        try:
            client.head_bucket(Bucket=cfg["bucket"])
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            client.create_bucket(Bucket=cfg["bucket"])
        records = []
        for size in (0, 4096, 1048577, 1310720, 1572856, 1572857):
            key = "compat/plain-" + str(size)
            result = client.put_object(Bucket=cfg["bucket"], Key=key, Body=payload(size, size))
            records.append(record(key, size, size, result))
        client.put_bucket_versioning(Bucket=cfg["bucket"], VersioningConfiguration={"Status": "Enabled"})
        for seed, size in enumerate((0, 4096, 196608, 196609), 100):
            result = client.put_object(Bucket=cfg["bucket"], Key="compat/versions", Body=payload(size, seed))
            records.append(record("compat/versions", size, seed, result))
        key = "compat/resumed-multipart"
        upload = client.create_multipart_upload(Bucket=cfg["bucket"], Key=key)
        first = client.upload_part(Bucket=cfg["bucket"], Key=key, UploadId=upload["UploadId"],
                                   PartNumber=1, Body=payload(5 * 1024 * 1024, 200))
        manifest = {"bucket": cfg["bucket"], "records": records,
                    "multipart": {"key": key, "upload_id": upload["UploadId"], "first_etag": first["ETag"]}}
        # Capture the complete seed inventory, including additional versions
        # whose contents match a known body oracle.
        expected = {(item["key"], item["version_id"]) for item in records}
        for page in client.get_paginator("list_object_versions").paginate(Bucket=cfg["bucket"], Prefix="compat/"):
            assert not page.get("DeleteMarkers"), "unexpected seed delete marker"
            for version in page.get("Versions", []):
                identity = (version["Key"], version["VersionId"])
                if identity in expected:
                    continue
                data = client.get_object(Bucket=cfg["bucket"], Key=identity[0], VersionId=identity[1])["Body"].read()
                digest = hashlib.sha256(data).hexdigest()
                match = next((item for item in records if item["key"] == identity[0]
                              and item["sha256"] == digest and item["size"] == len(data)), None)
                assert match is not None, "unexpected seed object or content"
                records.append(dict(match, version_id=identity[1]))
                expected.add(identity)
    else:
        assert manifest["bucket"] == cfg["bucket"]
        if cfg["action"] == "compat-finish-mpu" and not manifest["multipart"].get("complete"):
            upload = manifest["multipart"]
            second = client.upload_part(Bucket=cfg["bucket"], Key=upload["key"], UploadId=upload["upload_id"],
                                        PartNumber=2, Body=payload(1024 * 1024, 201))
            result = client.complete_multipart_upload(Bucket=cfg["bucket"], Key=upload["key"], UploadId=upload["upload_id"],
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": upload["first_etag"]}, {"PartNumber": 2, "ETag": second["ETag"]}]})
            data = payload(5 * 1024 * 1024, 200) + payload(1024 * 1024, 201)
            manifest["records"].append({"key": upload["key"], "size": len(data), "seed": None,
                                        "version_id": result["VersionId"], "sha256": hashlib.sha256(data).hexdigest()})
            manifest["multipart"]["complete"] = True
        elif cfg["action"] == "compat-delete":
            selected = [item for item in manifest["records"] if item["key"] == "compat/versions"]
            if selected:
                result = client.delete_objects(Bucket=cfg["bucket"], Delete={"Objects": [
                    {"Key": item["key"], "VersionId": item["version_id"]} for item in selected]})
                assert not result.get("Errors"), result.get("Errors")
                assert len(result.get("Deleted", [])) == len(selected)
                manifest["deleted_versions"] = selected
                manifest["records"] = [item for item in manifest["records"] if item["key"] != "compat/versions"]
            else:
                assert manifest.get("deleted_versions"), "manifest has no versions to delete"
    if cfg["action"] == "compat-check":
        actual_versions = set()
        for page in client.get_paginator("list_object_versions").paginate(Bucket=cfg["bucket"], Prefix="compat/"):
            assert not page.get("DeleteMarkers"), "unexpected delete marker"
            actual_versions.update((item["Key"], item["VersionId"]) for item in page.get("Versions", []))
        expected_versions = {(item["key"], item["version_id"]) for item in manifest["records"]}
        assert actual_versions == expected_versions, {"missing": sorted(expected_versions - actual_versions), "unexpected": sorted(actual_versions - expected_versions)}
        for item in manifest["records"]:
            args = {"Bucket": cfg["bucket"], "Key": item["key"], "VersionId": item["version_id"]}
            assert client.head_object(**args)["ContentLength"] == item["size"]
            data = client.get_object(**args)["Body"].read()
            assert len(data) == item["size"] and hashlib.sha256(data).hexdigest() == item["sha256"]
            if item["size"] > 1048576:
                result = client.get_object(**args, Range="bytes=1048570-1048582")
                assert result["Body"].read() == data[1048570:1048583]
    print(json.dumps(manifest))
'''


def write_private(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DockerLab:
    def __init__(self, state, config):
        self.state = Path(state)
        self.config = config

    def docker(self, *args, input_text=None):
        result = subprocess.run(
            ["docker", "--context", self.config["context"], *args],
            input=input_text, text=True, capture_output=True,
        )
        if result.returncode:
            detail = result.stderr.strip()[-4000:]
            for field in ("access_key", "secret_key"):
                if self.config.get(field):
                    detail = detail.replace(self.config[field], "<redacted>")
            raise RuntimeError(f"Docker {args[0]} failed ({result.returncode}): {detail}")
        return result.stdout

    def docker_json(self, *args):
        return json.loads(self.docker(*args))

    def resource_ids(self, kind):
        listing = {"container": ("ps", "-aq"), "volume": ("volume", "ls", "-q"),
                   "network": ("network", "ls", "-q")}[kind]
        return self.docker(*listing, "--filter", "label=com.docker.compose.project=" + self.config["project"]).split()

    def assert_owned(self):
        for kind in ("container", "volume", "network"):
            for identifier in self.resource_ids(kind):
                command = ("inspect", identifier) if kind == "container" else (kind, "inspect", identifier)
                item = self.docker_json(*command)[0]
                labels = item["Config"].get("Labels", {}) if kind == "container" else item.get("Labels", {})
                if (labels or {}).get(OWNER_LABEL) != self.config["owner"]:
                    raise ValueError(f"refusing to alter unowned {kind} {identifier}")

    def assert_spec_owned(self, spec):
        for kind, group in (("volume", "volumes"), ("network", "networks")):
            existing = set(self.docker(kind, "ls", "--format", "{{.Name}}").splitlines())
            for resource in spec.get(group, {}).values():
                name = resource["name"]
                if name in existing:
                    labels = self.docker_json(kind, "inspect", name)[0].get("Labels", {}) or {}
                    if labels.get(OWNER_LABEL) != self.config["owner"]:
                        raise ValueError(f"refusing to use unowned {kind} {name}")

    def compose(self, *args):
        return self.docker("compose", "--project-name", self.config["project"],
                           "--file", str(self.state / "compose.json"), *args)

    def make_spec(self, shard_bytes, rpc_mode="json-compat"):
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith(("RUSTFS_", "OTEL_"))}
        environment["RUSTFS_IMAGE"] = self.config["image_id"]
        raw = subprocess.run(
            ["docker", "--context", self.config["context"], "compose", "--env-file", "/dev/null",
             "--project-name", self.config["project"], "--file", str(BASE_COMPOSE), "config", "--format", "json"],
            env=environment, check=True, capture_output=True, text=True,
        )
        return self.customize_spec(json.loads(raw.stdout), shard_bytes, rpc_mode)

    def customize_spec(self, spec, shard_bytes, rpc_mode="json-compat"):
        if rpc_mode not in ("json-compat", "msgpack-only"):
            raise ValueError("unsupported inline lab RPC mode")
        msgpack_only = str(rpc_mode == "msgpack-only").lower()
        if set(spec["services"]) != set(NODES):
            raise ValueError("inline lab requires the repository's four-node compose topology")
        if any(item.get("external") for group in ("volumes", "networks") for item in spec.get(group, {}).values()):
            raise ValueError("inline lab requires owned volumes and networks")
        labels = {OWNER_LABEL: self.config["owner"]}
        for index, name in enumerate(NODES):
            service = spec["services"][name]
            service.pop("build", None)
            service.pop("container_name", None)
            service.pop("ports", None)
            service["image"] = self.config["image_id"]
            service["command"] = ["server", "http://node{1...4}:9000/data/rustfs{0...3}"]
            service["labels"] = dict(service.get("labels", {}), **labels)
            service["mem_limit"] = self.config["memory"]
            service["cpus"] = 1
            if self.config["cpu_sets"]:
                service["cpuset"] = self.config["cpu_sets"][index]
            service["environment"].update({
                "RUSTFS_ADDRESS": ":9000",
                "RUSTFS_ACCESS_KEY": self.config["access_key"],
                "RUSTFS_SECRET_KEY": self.config["secret_key"],
                "RUSTFS_CONSOLE_ENABLE": "false",
                "RUSTFS_OBS_ENDPOINT": "",
                "NO_PROXY": "*",
                "no_proxy": "*",
                "RUSTFS_COMPRESSION_ENABLED": "false",
                "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true",
                "RUSTFS_STORAGE_CLASS_STANDARD": "EC:4",
                "RUSTFS_ERASURE_SET_DRIVE_COUNT": "16",
                "RUSTFS_STORAGE_CLASS_INLINE_BLOCK": "128KiB",
                "RUSTFS_ERASURE_SHARD_BYTES": str(shard_bytes),
                "RUSTFS_DURABILITY_MODE": "strict",
                "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY": msgpack_only,
                "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED": msgpack_only,
            })
        spec["services"]["node1"]["ports"] = [{"target": 9000, "published": str(self.config["port"]),
                                                 "host_ip": "127.0.0.1", "protocol": "tcp"}]
        for group in ("volumes", "networks"):
            for item in spec.get(group, {}).values():
                item["labels"] = dict(item.get("labels", {}), **labels)
        return spec

    def container(self, node):
        result = self.compose("ps", "--quiet", node).strip()
        if not result or "\n" in result:
            raise ValueError(f"expected one running {node} container")
        return result

    def client(self, bucket, action, manifest=None, coordinator="node1"):
        if coordinator not in NODES:
            raise ValueError("compatibility coordinator must be an owned lab node")
        spec = json.loads((self.state / "compose.json").read_text())
        network_key = next(iter(spec["services"]["node1"]["networks"]))
        network = spec["networks"][network_key]["name"]
        payload = {key: self.config[key] for key in ("access_key", "secret_key")}
        payload.update(bucket=bucket, action=action, manifest=manifest, coordinator=coordinator)
        result = self.docker("run", "--rm", "--pull", "never", "--label", OWNER_LABEL + "=" + self.config["owner"],
                             "--network", network, "--entrypoint", "python3", "-i", self.config["client_image_id"],
                             "-c", CLIENT, input_text=json.dumps(payload))
        return json.loads(result)

    def mc_proxy(self, image, arguments):
        arguments = list(arguments)
        if arguments[:1] == ["--"]:
            arguments.pop(0)
        if arguments[:2] == ["alias", "set"]:
            if len(arguments) < 6:
                raise ValueError("mc alias set needs the lab endpoint and credentials")
            endpoint = urllib.parse.urlsplit(arguments[3])
            if endpoint.scheme != "http" or endpoint.hostname not in ("127.0.0.1", "localhost") or endpoint.port != self.config["port"]:
                raise ValueError("mc proxy alias must target this lab's loopback endpoint")
            arguments[3] = "http://node1:9000"
        spec = json.loads((self.state / "compose.json").read_text())
        network_key = next(iter(spec["services"]["node1"]["networks"]))
        network = spec["networks"][network_key]["name"]
        config_dir = Path(os.environ.get("MC_CONFIG_DIR", str(self.state / "mc-config"))).resolve()
        config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        image_id = self.docker_json("image", "inspect", image)[0]["Id"]
        command = ["docker", "--context", self.config["context"], "run", "--rm", "--pull", "never",
                   "--label", OWNER_LABEL + "=" + self.config["owner"], "--network", network,
                   "--mount", f"type=bind,src={config_dir},dst=/mc-config", "--env", "MC_CONFIG_DIR=/mc-config",
                   "--env", "NO_PROXY=*", image_id, *arguments]
        result = subprocess.run(command)
        if result.returncode:
            raise RuntimeError(f"Linux mc probe exited with code {result.returncode}")

    def wait_ready(self):
        deadline = time.monotonic() + 240
        pending = list(NODES)
        while time.monotonic() < deadline:
            pending = []
            for node in NODES:
                try:
                    status = self.docker("exec", self.container(node), "curl", "--silent", "--show-error",
                                         "--connect-timeout", "2", "--max-time", "3", "--output", "/dev/null",
                                         "--write-out", "%{http_code}", "http://127.0.0.1:9000/health/ready").strip()
                    if status != "200":
                        pending.append(node)
                except RuntimeError:
                    pending.append(node)
            if not pending:
                return
            time.sleep(1)
        raise TimeoutError("inline Docker nodes did not become ready: " + ", ".join(pending))

    def archive_compat_manifests(self):
        manifests = sorted(self.state.glob("compat-*.json"))
        if not manifests:
            return
        history = self.state / "compat-history" / secrets.token_hex(8)
        history.mkdir(parents=True, mode=0o700)
        for manifest in manifests:
            manifest.rename(history / manifest.name)

    def deploy(self, bucket, keep_data, shard_bytes, rpc_mode="json-compat"):
        validate_shard_bytes(shard_bytes)
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
            raise ValueError("a valid isolated S3 bucket is required")
        if os.environ.get("HOTPATH_ABBA_DRIVE_SYNC", "true") != "true":
            raise ValueError("this lab validates strict durability cells")
        if sha256_file(self.config["binary"]) != self.config["binary_sha256"]:
            raise ValueError("benchmark binary changed after configuration")
        self.assert_owned()
        spec = self.make_spec(shard_bytes, rpc_mode)
        self.assert_spec_owned(spec)
        candidate = self.state / "compose.next.json"
        write_private(candidate, spec)
        self.docker("compose", "--project-name", self.config["project"], "--file", str(candidate), "config", "--quiet")
        if (self.state / "compose.json").exists():
            self.compose("down", *([] if keep_data else ["--volumes"]))
        if not keep_data:
            self.archive_compat_manifests()
        candidate.replace(self.state / "compose.json")
        self.compose("up", "--detach", "--no-build", "--pull", "never")
        self.wait_ready()
        actual_hashes = []
        for node in NODES:
            identifier = self.container(node)
            info = self.docker_json("inspect", identifier)[0]
            if info["Image"] != self.config["image_id"]:
                raise ValueError(f"unexpected running image for {node}")
            actual_env = dict(item.split("=", 1) for item in info["Config"]["Env"])
            for key in ("RUSTFS_ERASURE_SHARD_BYTES", "RUSTFS_STORAGE_CLASS_INLINE_BLOCK", "RUSTFS_DURABILITY_MODE",
                        "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY", "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED"):
                if actual_env.get(key) != spec["services"][node]["environment"][key]:
                    raise ValueError(f"unexpected running {key} for {node}")
            actual_hashes.append(self.docker("exec", identifier, "sha256sum", "/usr/local/bin/rustfs").split()[0])
        expected = self.config["binary_sha256"]
        if any(value != expected for value in actual_hashes):
            raise ValueError("running server binary differs from the benchmark artifact")
        if not keep_data:
            self.client(bucket, "create")
        evidence = Path(os.environ.get("HOTPATH_ABBA_DEPLOY_EVIDENCE_FILE", str(self.state / "deploy.env")))
        values = {
            "leg": os.environ.get("HOTPATH_ABBA_LEG", "manual"),
            "phase": os.environ.get("HOTPATH_ABBA_PHASE", "manual"),
            "bucket": bucket, "binary_sha256": expected,
            "dataset_reset": str(not keep_data).lower(), "shard_bytes": shard_bytes,
            "inline_block": 131072, "rpc_mode": rpc_mode, "durability": "strict", "topology": "4x4-EC12+4",
            "image_id": self.config["image_id"], "source_revision": self.config["revision"],
        }
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
        print(f"ready project={self.config['project']} shard_bytes={shard_bytes}")

    def probe(self, bucket):
        records = self.client(bucket, "probe")
        node = self.container("node1")
        spec = json.loads((self.state / "compose.json").read_text())
        shard_bytes = int(spec["services"]["node1"]["environment"]["RUSTFS_ERASURE_SHARD_BYTES"])
        expected_block = shard_bytes * 12 if shard_bytes else 1024 * 1024
        for record in records:
            path = f"/data/rustfs0/{bucket}/{record['key']}/xl.meta"
            geometry = self.docker_json("exec", node, "/usr/local/bin/inline_lab_inspect", path)
            if (geometry["data_blocks"], geometry["parity_blocks"], geometry["block_size"]) != (12, 4, expected_block):
                raise ValueError("unexpected persisted EC layout or block size")
            record["geometry"] = geometry
        write_private(self.state / "probe.json", records)
        print(json.dumps(records, indent=2))


def validate_shard_bytes(value):
    if value < 0 or value > 64 * 1024 * 1024 or (value and value % 4096):
        raise ValueError("shard bytes must be zero or a 4 KiB multiple up to 64 MiB")


def configure(args):
    state = Path(args.state_dir).resolve()
    state.mkdir(parents=True, exist_ok=True)
    os.chmod(state, 0o700)
    previous = json.loads((state / "config.json").read_text()) if (state / "config.json").exists() else None
    if previous and (previous["context"], previous["project"]) != (args.context, args.project):
        raise ValueError("state belongs to a different Docker context or project")
    config = previous or {"owner": secrets.token_hex(16), "access_key": "inline-lab-access", "secret_key": secrets.token_hex(24)}
    config.update(context=args.context, project=args.project, port=args.port, binary=str(Path(args.binary).resolve()),
                  memory=args.memory, cpu_sets=args.cpu_sets.split(";") if args.cpu_sets else [])
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,50}", args.project):
        raise ValueError("invalid Docker compose project")
    if not 1 <= args.port <= 65535 or (config["cpu_sets"] and (len(config["cpu_sets"]) != 4 or any(not re.fullmatch(r"[0-9,-]+", value) for value in config["cpu_sets"]))):
        raise ValueError("invalid port or four-node CPU set list")
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise ValueError("source revision must be a complete Git commit")
    config["binary_sha256"] = sha256_file(config["binary"])
    lab = DockerLab(state, config)
    lab.assert_owned()
    image = lab.docker_json("image", "inspect", args.image)[0]
    if image["Config"].get("Labels", {}).get("org.opencontainers.image.revision") != args.revision:
        raise ValueError("image revision does not match the selected source")
    config.update(image_id=image["Id"], image_ref=args.image, revision=args.revision,
                  client_image_id=lab.docker_json("image", "inspect", args.client_image)[0]["Id"])
    write_private(state / "config.json", config)
    print(f"configured project={args.project} endpoint=127.0.0.1:{args.port}")


def execute(args):
    if args.command == "configure":
        configure(args)
        return
    state = Path(args.state_dir).resolve()
    config = json.loads((state / "config.json").read_text())
    lab = DockerLab(state, config)
    lab.assert_owned()
    if args.command == "deploy":
        lab.deploy(args.bucket, args.keep_data, args.shard_bytes, args.rpc_mode)
    elif args.command == "probe":
        lab.probe(args.bucket)
    elif args.command.startswith("compat-"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", args.bucket):
            raise ValueError("a valid isolated compatibility bucket is required")
        manifest_path = state / f"compat-{args.bucket}.json"
        if args.command == "compat-seed":
            if manifest_path.exists():
                raise ValueError("compatibility manifest already exists")
            manifest = None
        else:
            manifest = json.loads(manifest_path.read_text())
        result = lab.client(args.bucket, args.command, manifest, args.coordinator)
        if args.command == "compat-seed":
            coordinator = lab.docker_json("inspect", lab.container(args.coordinator))[0]
            result["writer_revision"] = coordinator["Config"]["Labels"]["org.opencontainers.image.revision"]
            result["writer_coordinator"] = args.coordinator
        if args.command != "compat-check":
            # Retain the successful mutation before fallible read/geometry checks
            # so a retry can use the returned version IDs and completed MPU state.
            write_private(manifest_path, result)
            result = lab.client(args.bucket, "compat-check", result, args.coordinator)
        node = lab.container("node1")
        for record in result["records"]:
            path = f"/data/rustfs0/{args.bucket}/{record['key']}/xl.meta"
            version = "00000000-0000-0000-0000-000000000000" if record["version_id"] == "null" else record["version_id"]
            geometry = lab.docker_json("exec", node, "/usr/local/bin/inline_lab_inspect", path, version)
            if "geometry" in record and geometry != record["geometry"]:
                raise ValueError(f"persisted geometry changed for {record['key']} version {record['version_id']}")
            record["geometry"] = geometry
        write_private(manifest_path, result)
        print(f"{args.command} verified {len(result['records'])} objects/versions")
    elif args.command == "mc":
        lab.mc_proxy(args.image, args.arguments)
    elif (state / "compose.json").exists():
        lab.compose("down", *(["--volumes"] if args.purge else []))
        if args.purge:
            lab.archive_compat_manifests()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("configure")
    setup.add_argument("--context", required=True)
    setup.add_argument("--project", required=True)
    setup.add_argument("--image", required=True)
    setup.add_argument("--client-image", default="rustfs-inline-build-tools:1.100")
    setup.add_argument("--revision", required=True)
    setup.add_argument("--binary", required=True)
    setup.add_argument("--port", type=int, default=19080)
    setup.add_argument("--memory", default="4g")
    setup.add_argument("--cpu-sets", default="")
    deploy = commands.add_parser("deploy")
    deploy.add_argument("--bucket", default=os.environ.get("HOTPATH_ABBA_BUCKET", ""))
    deploy.add_argument("--shard-bytes", type=int, default=int(os.environ.get("RUSTFS_ERASURE_SHARD_BYTES", "0")))
    deploy.add_argument("--keep-data", action="store_true")
    deploy.add_argument("--rpc-mode", choices=("json-compat", "msgpack-only"),
                        default=os.environ.get("RUSTFS_INLINE_LAB_RPC_MODE", "json-compat"))
    probe = commands.add_parser("probe")
    probe.add_argument("--bucket", required=True)
    for action in ("compat-seed", "compat-check", "compat-finish-mpu", "compat-delete"):
        command = commands.add_parser(action)
        command.add_argument("--bucket", required=True)
        command.add_argument("--coordinator", choices=NODES, default="node1")
    mc = commands.add_parser("mc")
    mc.add_argument("--image", required=True)
    mc.add_argument("arguments", nargs=argparse.REMAINDER)
    stop = commands.add_parser("down")
    stop.add_argument("--purge", action="store_true")
    args = parser.parse_args()
    state = Path(args.state_dir).resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("inline Docker lab is already in use") from error
        execute(args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"inline Docker lab failed: {error}", file=sys.stderr)
        sys.exit(1)
