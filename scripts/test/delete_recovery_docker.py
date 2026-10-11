#!/usr/bin/env python3
"""Run delete recovery regressions in disposable local Docker containers.

Requires host Python 3.9+, Docker, and curl with --aws-sigv4. Supply a native
Linux binary compiled with e2e-test-hooks and a Linux image containing Python 3
and that binary's runtime libraries. All S3 requests target localhost with
synthetic credentials. Container processes run as UID 10001. The harness labels
its resources, refuses existing volumes, preserves JSON reports/server logs,
and removes only containers, networks, and volumes it created.

Example:
  python3 scripts/test/delete_recovery_docker.py     --binary /absolute/path/to/rustfs-candidate     --baseline-binary /absolute/path/to/rustfs-old     --image local-rustfs-validation:latest     --artifact-dir /absolute/path/to/new-results     smoke prepared1 applied1 decision1 restore returning untouched mixed

Available cases include single/multi-node process termination, inline/empty
objects, legacy explicit cleanup, versioned and retained objects, UUID prefix
children, corrupted journals, real permission errors, lost receipt responses,
admin bucket/object repair, and deep keys. Use --help for runtime options.
Logical disks share the Docker host filesystem; this tests process/RPC failure
and namespace safety, not independent hardware failure or power-loss durability.
"""
from pathlib import Path
import base64, hashlib, ipaddress, json, os, subprocess, tempfile, threading, time, uuid, xml.etree.ElementTree as ET
from urllib.parse import quote
ROOT = None
PREFIX = None
IMAGE = None
PORT = 19010
CANDIDATE = None
BASELINE = None
ACCESS = 'recovery-test'
SECRET = 'recovery-test-secret'

def run(*args, check=True, **kwargs):
    return subprocess.run(args, check=check, capture_output=True, **kwargs)

def docker(*args, check=True):
    return run('docker', *args, check=check)

class S3:

    def __init__(self, port):
        self.url = f'http://127.0.0.1:{port}'

    def request(self, method, path='', body=None, headers=None, expected=None, timeout=60):
        call = ['curl', '--noproxy', '*', '--silent', '--show-error', '--max-time', str(timeout), '--aws-sigv4', 'aws:amz:us-east-1:s3', '--user', ACCESS + ':' + SECRET, '--request', method]
        for (key, value) in (headers or {}).items():
            call += ['--header', key + ': ' + value]
        if body is not None:
            call += ['--data-binary', '@-']
        with tempfile.NamedTemporaryFile(dir=ROOT) as header_file:
            response = run(*call, self.url + '/' + path, '--dump-header', header_file.name, '--write-out', '\n%{http_code}', input=body, check=False)
            self.last_headers = {}
            for line in Path(header_file.name).read_text(errors='replace').splitlines():
                if ':' in line:
                    (key, value) = line.split(':', 1)
                    self.last_headers[key.lower()] = value.strip()
        if response.returncode:
            raise RuntimeError(response.stderr.decode(errors='replace'))
        (payload, status) = response.stdout.rsplit(b'\n', 1)
        code = int(status)
        if expected is not None and code not in ((expected,) if isinstance(expected, int) else expected):
            raise AssertionError(f'{method} {path}: {code}, expected {expected}: {payload[:1500]!r}')
        return (code, payload)

    def put_bucket(self, bucket, lock=False):
        headers = {'x-amz-bucket-object-lock-enabled': 'true'} if lock else {}
        return self.request('PUT', bucket, headers=headers, expected=200)

    def put(self, bucket, key, body, headers=None):
        return self.request('PUT', bucket + '/' + key, body, headers, 200)

    def get(self, bucket, key):
        return self.request('GET', bucket + '/' + key, expected=200)[1]

    def delete(self, bucket, key, expected=(204, 200)):
        return self.request('DELETE', bucket + '/' + key, expected=expected)

    def delete_bucket(self, bucket, expected=204):
        return self.request('DELETE', bucket, expected=expected)

    def list(self, bucket):
        (_, body) = self.request('GET', bucket + '?list-type=2', expected=200)
        root = ET.fromstring(body)
        return [node.text for node in root.iter() if node.tag.rsplit('}', 1)[-1] == 'Key']

    def versioning(self, bucket, state='Enabled'):
        xml = f'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>{state}</Status></VersioningConfiguration>'.encode()
        return self.request('PUT', bucket + '?versioning', xml, {'Content-Type': 'application/xml'}, 200)

    def batch_delete(self, bucket, keys):
        xml = ET.Element('Delete')
        for key in keys:
            ET.SubElement(ET.SubElement(xml, 'Object'), 'Key').text = key
        data = ET.tostring(xml)
        (status, body) = self.request('POST', bucket + '?delete', data, {'Content-Type': 'application/xml', 'Content-MD5': base64.b64encode(hashlib.md5(data).digest()).decode()}, 200)
        response = ET.fromstring(body)
        errors = [node for node in response.iter() if node.tag.rsplit('}', 1)[-1] == 'Error']
        assert not errors, f'DeleteObjects returned per-key errors: {body!r}'
        return (status, body)

    def complete_multipart(self, bucket, key, parts):
        path = bucket + '/' + key
        (_, response) = self.request('POST', path + '?uploads', expected=200)
        upload = next(node.text for node in ET.fromstring(response).iter() if node.tag.rsplit('}', 1)[-1] == 'UploadId')
        assert upload, 'native multipart initiation must return an upload ID'
        query = '?uploadId=' + quote(upload, safe='')
        complete = ET.Element('CompleteMultipartUpload')
        for number, body in enumerate(parts, 1):
            self.request('PUT', path + query + '&partNumber=' + str(number), body, expected=200)
            etag = self.last_headers.get('etag')
            assert etag, 'native UploadPart must return its ETag'
            part = ET.SubElement(complete, 'Part')
            ET.SubElement(part, 'PartNumber').text = str(number)
            ET.SubElement(part, 'ETag').text = etag
        (_, response) = self.request('POST', path + query, ET.tostring(complete), {'Content-Type': 'application/xml'}, 200)
        assert ET.fromstring(response).tag.rsplit('}', 1)[-1] == 'CompleteMultipartUploadResult', response
        return upload

class Cluster:

    def __init__(self, case, nodes=1, drives=4, extra_env=None, binary_by_node=None):
        self.case = case
        self.nodes = nodes
        self.drives = drives
        self.port = PORT
        self.binary = 'candidate'
        self.binary_by_node = binary_by_node or {}
        self.names = [f'{PREFIX}-{case}-n{i}' for i in range(1, nodes + 1)]
        self.network = f'{PREFIX}-{case}-net'
        self.env = extra_env or {}
        self.created_names = []
        self.created_network = False
        self.clients = [S3(self.port + i) for i in range(nodes)]
        self.volumes = []
        self.run_args = {}
        self.barriers = [ROOT / 'barriers' / case / f'n{i}' for i in range(1, nodes + 1)]
        for directory in self.barriers:
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(511)

    def create(self):
        docker('network', 'create', '--label', 'rustfs.delete-recovery-test=' + PREFIX, self.network)
        self.created_network = True
        configuration = json.loads(docker('network', 'inspect', self.network).stdout)[0]['IPAM']['Config']
        subnet = next(ipaddress.ip_network(value['Subnet']) for value in configuration if ':' not in value['Subnet'])
        addresses = [str(subnet[index + 2]) for index in range(self.nodes)]
        urls = []
        for name in self.names:
            for drive in range(1, self.drives + 1):
                urls.append(f'http://{name}:9000/data/d{drive}')
        for (idx, name) in enumerate(self.names):
            args = ['run', '-d', '--user', '10001:10001', '--name', name, '--network', self.network, '--hostname', name, '-p', f'127.0.0.1:{self.port + idx}:9000', '--label', 'rustfs.delete-recovery-test=' + PREFIX, '-v', str(CANDIDATE) + ':/binaries/candidate:ro', '-v', str(self.barriers[idx]) + ':/barrier']
            # Stopped peers must still resolve while the remaining quorum restarts.
            args += ['--ip', addresses[idx]]
            for peer, address in zip(self.names, addresses):
                args += ['--add-host', peer + ':' + address]
            if BASELINE is not None:
                args += ['-v', str(BASELINE) + ':/binaries/baseline:ro']
            data_mounts = []
            for drive in range(1, self.drives + 1):
                volume = f'{name}-d{drive}'
                if docker('volume', 'inspect', volume, check=False).returncode == 0:
                    raise RuntimeError('refusing existing volume: ' + volume)
                docker('volume', 'create', '--label', 'rustfs.delete-recovery-test=' + PREFIX, volume)
                self.volumes.append(volume)
                args += ['-v', f'{volume}:/data/d{drive}']
                data_mounts += ['-v', f'{volume}:/data/d{drive}']
            docker('run', '--rm', *data_mounts, IMAGE, 'python3', '-c', "from pathlib import Path; import os; [os.chown(p,10001,10001) for p in Path('/data').glob('d*')]")
            values = {'RUSTFS_VOLUMES': ' '.join(urls) if self.nodes > 1 else ' '.join((f'/data/d{d}' for d in range(1, self.drives + 1))), 'RUSTFS_ADDRESS': '0.0.0.0:9000', 'RUSTFS_CONSOLE_ENABLE': 'false', 'RUSTFS_ACCESS_KEY': ACCESS, 'RUSTFS_SECRET_KEY': SECRET, 'RUSTFS_OBS_LOG_DIRECTORY': '/tmp/logs', 'RUSTFS_OBS_LOGGER_LEVEL': 'info', 'RUSTFS_OBS_LOG_STDOUT_ENABLED': 'true', 'RUSTFS_DELETE_RECOVERY_BARRIER_DIR': '/barrier', 'RUSTFS_UNSAFE_BYPASS_DISK_CHECK': 'true', 'RUSTFS_DURABILITY_MODE': 'strict', **self.env}
            for (key, value) in values.items():
                if value is not None:
                    args += ['-e', key + '=' + value]
            self.run_args[idx] = [*args, IMAGE, '/binaries/' + self.binary_by_node.get(idx, self.binary)]
            launch = docker(*self.run_args[idx], check=False)
            if launch.returncode:
                label = docker('inspect', '--format', '{{index .Config.Labels "rustfs.delete-recovery-test"}}', name, check=False)
                if label.returncode == 0 and label.stdout.decode().strip() == PREFIX:
                    self.created_names.append(name)
                raise RuntimeError('Docker launch failed: ' + launch.stderr.decode(errors='replace'))
            self.created_names.append(name)
        self.ready()

    def ready(self, timeout=180, indices=None):
        deadline = time.monotonic() + timeout
        pending = set(range(self.nodes) if indices is None else indices)
        while pending and time.monotonic() < deadline:
            for idx in list(pending):
                try:
                    (code, _) = self.clients[idx].request('GET', '', timeout=2)
                    if code in (200, 403):
                        pending.remove(idx)
                except Exception:
                    pass
            if pending:
                for idx in pending:
                    state = docker('inspect', '--format', '{{.State.Running}}', self.names[idx], check=False)
                    if state.returncode == 0 and state.stdout.strip() != b'true':
                        self.logs()
                        raise RuntimeError('server exited during startup: ' + self.names[idx])
                time.sleep(0.2)
        if pending:
            self.logs()
            raise TimeoutError('cluster not ready: ' + str(pending))

    def kill(self, index=0):
        docker('kill', '--signal', 'KILL', self.names[index])

    def restart(self, index=None):
        for i in range(self.nodes) if index is None else [index]:
            docker('start', self.names[i])
        self.ready()

    def replace_node(self, index, binary):
        docker('kill', '--signal', 'KILL', self.names[index], check=False)
        docker('rm', self.names[index])
        args = self.run_args[index].copy()
        args[-1] = '/binaries/' + binary
        docker(*args)
        self.run_args[index] = args
        self.ready(indices=[index])

    def logs(self):
        for name in self.created_names:
            response = docker('logs', name, check=False)
            (ROOT / (name + '.log')).write_bytes(response.stdout + response.stderr)

    def inspect_data(self, code, indices=None):
        """Access only this cluster's named volumes, including stopped peers."""
        result = []
        for index, name in enumerate(self.names):
            if indices is not None and index not in indices:
                continue
            response = docker('run', '--rm', '--volumes-from', name, IMAGE, 'python3', '-c', code)
            result.append(json.loads(response.stdout))
        return result

    def remove(self):
        """Keep logs and delete only resources successfully created here."""
        self.logs()
        for name in self.created_names:
            docker('rm', '-f', name, check=False)
        for volume in self.volumes:
            docker('volume', 'rm', volume, check=False)
        if self.created_network:
            docker('network', 'rm', self.network, check=False)

def smoke():
    cluster = Cluster('smoke', drives=1)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'recovery-smoke'
        client.put_bucket(bucket)
        for (key, size) in [('zero', 0), ('inline', 4096), ('external', 2 * 1024 * 1024)]:
            payload = bytes(range(256)) * (size // 256)
            client.put(bucket, key, payload)
            assert client.get(bucket, key) == payload
        client.batch_delete(bucket, ['zero', 'inline', 'external'])
        assert client.list(bucket) == []
        client.delete_bucket(bucket)
        return {'case': 's3-smoke', 'status': 'passed', 'topology': '1 node / 1 named local volume'}
    finally:
        cluster.remove()

def seed_legacy(cluster, bucket, key, shape):
    transaction = str(uuid.uuid4())
    if shape == 'alias':
        directories = {path.split('/')[0] for node in residue_inventory(cluster, bucket, key) for drive in node for path in drive['files'] if path.endswith('/part.1')}
        assert len(directories) == 1, 'alias fixture requires one shared data-directory generation'
        transaction = directories.pop()
    code = "from pathlib import Path\nimport hashlib,json,os,uuid\nbucket=%r; key=%r; shape=%r; transaction=%r\nresult=[]\nfor drive in sorted(Path('/data').glob('d*')):\n root=drive/bucket/key\n original=root/'xl.meta'\n if not original.is_file(): continue\n data=original.read_bytes(); backup=root/transaction/'xl.meta.bkp'\n backup.parent.mkdir(exist_ok=shape=='alias'); backup.write_bytes(data); os.chown(backup.parent,10001,10001); os.chown(backup,10001,10001)\n dirs=[p for p in root.iterdir() if p.is_dir() and (p.name!=transaction or shape=='alias') and any(p.glob('part.*'))]\n if shape in ('reserved','alias'):\n  assert dirs, 'external fixture must contain erasure parts'\n  for d in dirs:\n   marker=d/('reserve-delete-data.'+transaction); marker.write_bytes(b''); os.chown(marker,10001,10001)\n elif shape=='backup-only':\n  assert not dirs, 'inline fixture must not contain external parts'\n else: raise AssertionError(shape)\n original.unlink()\n owned=[root,*root.rglob('*')]\n assert all(p.stat().st_uid==10001 and p.stat().st_gid==10001 for p in owned), [(str(p),p.stat().st_uid,p.stat().st_gid) for p in owned]\n result.append({'drive':drive.name,'transaction':transaction,'backup_sha256':hashlib.sha256(data).hexdigest(),'data_dirs':[p.name for p in dirs],'fixture_owner':'10001:10001','fixture_modes':{str(p.relative_to(root)):oct(p.stat().st_mode & 0o777) for p in owned}})\nprint(json.dumps(result))\n" % (bucket, key, shape, transaction)
    return cluster.inspect_data(code)

def residue_inventory(cluster, bucket, key):
    return cluster.inspect_data("from pathlib import Path\nimport hashlib,json\nresult=[]\nfor drive in sorted(Path('/data').glob('d*')):\n root=drive/%r/%r\n result.append({'drive':drive.name,'exists':root.exists(),'files':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}})\nprint(json.dumps(result))\n" % (bucket, key))

def legacy_retry(nodes=1):
    case = 'legacy' + str(nodes)
    cluster = Cluster(case, nodes=nodes, drives=1 if nodes == 1 else 2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'legacy-recovery'
        client.put_bucket(bucket)
        original = {key: bytes(range(256)) * (size // 256) for (key, size) in [('inline', 4096), ('reserved', 2 * 1024 * 1024)]}
        for (key, payload) in original.items():
            client.put(bucket, key, payload)
            assert client.get(bucket, key) == payload
        for i in range(nodes):
            cluster.kill(i)
        fixtures = {key: seed_legacy(cluster, bucket, key, 'backup-only' if key == 'inline' else 'reserved') for key in original}
        before = {key: residue_inventory(cluster, bucket, key) for key in original}
        cluster.restart()
        time.sleep(2)
        after = {key: residue_inventory(cluster, bucket, key) for key in original}
        assert before == after, 'automatic recovery must not guess an unjournaled historical outcome'
        client.batch_delete(bucket, list(original))
        for key in original:
            current = residue_inventory(cluster, bucket, key)
            assert all((not drive['exists'] for node in current for drive in node)), f'current explicit DELETE must reclaim {key}: {current}'
        assert client.list(bucket) == []
        client.delete_bucket(bucket)
        return {'case': case, 'status': 'passed', 'fixtures': fixtures, 'topology': f'{nodes} nodes'}
    finally:
        cluster.remove()

def arm_barrier(cluster, index, bucket, key, stage, disk=None):
    nonce = str(uuid.uuid4())
    arm = {'nonce': nonce, 'bucket': bucket, 'object': key, 'stage': stage}
    if disk is not None:
        arm['disk'] = disk
    (cluster.barriers[index] / 'arm.json').write_text(json.dumps(arm))
    return nonce

def reached_barrier(cluster, index, nonce, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for path in cluster.barriers[index].glob('reached.' + nonce + '.*.json'):
            try:
                return json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        time.sleep(0.02)
    cluster.logs()
    raise TimeoutError('crash stage not reached: ' + nonce)

def clear_barriers(cluster):
    for directory in cluster.barriers:
        (directory / 'arm.json').unlink(missing_ok=True)

def async_delete(client, bucket, key):
    result = {}

    def target():
        try:
            result['response'] = client.request('DELETE', bucket + '/' + key, timeout=90)
        except Exception as error:
            result['exception'] = str(error)
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return (thread, result)

def wait_inventory(cluster, bucket, key, predicate, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = residue_inventory(cluster, bucket, key)
        if predicate(last):
            return last
        time.sleep(0.1)
    raise AssertionError('recovery did not converge: ' + json.dumps(last))

def process_crash(stage, nodes=1, size=2 * 1024 * 1024):
    case = ('inline-' if size == 4096 else 'zero-' if size == 0 else 'kill-') + stage.lower() + '-' + str(nodes)
    cluster = Cluster(case, nodes=nodes, drives=4 if nodes == 1 else 2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'journal-crash'
        key = 'payload'
        client.put_bucket(bucket)
        payload = bytes(range(256)) * (size // 256)
        client.put(bucket, key, payload)
        assert client.get(bucket, key) == payload
        original = residue_inventory(cluster, bucket, key)
        nonce = arm_barrier(cluster, 0, bucket, key, stage, disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        for index in range(nodes):
            cluster.kill(index)
        thread.join(5)
        clear_barriers(cluster)
        cluster.restart()
        if stage == 'DECISION':
            wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)))
            assert client.list(bucket) == []
            client.delete_bucket(bucket)
        else:
            wait_inventory(cluster, bucket, key, lambda inventory: inventory == original)
            assert client.get(bucket, key) == payload
            client.delete(bucket, key)
            client.delete_bucket(bucket)
        return {'case': case, 'status': 'passed', 'observation': observation, 'request': {key: str(value) for (key, value) in request.items()}}
    finally:
        cluster.remove()

def rollback_crash():
    cluster = Cluster('kill-restore', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'rollback-restart'
        key = 'payload'
        client.put_bucket(bucket)
        payload = bytes(range(256)) * 8192
        client.put(bucket, key, payload)
        original = residue_inventory(cluster, bucket, key)
        first_nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        first = reached_barrier(cluster, 0, first_nonce)
        cluster.kill()
        thread.join(5)
        clear_barriers(cluster)
        second_nonce = arm_barrier(cluster, 0, bucket, key, 'RESTORE', disk='/data/d1')
        docker('start', cluster.names[0])
        second = reached_barrier(cluster, 0, second_nonce)
        cluster.kill()
        clear_barriers(cluster)
        cluster.restart()
        wait_inventory(cluster, bucket, key, lambda inventory: inventory == original)
        assert client.get(bucket, key) == payload
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'rollback-second-process-kill', 'status': 'passed', 'observations': [first, second]}
    finally:
        cluster.remove()

def versioned_crash(stage):
    cluster = Cluster('versions-' + stage.lower(), drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'versioned-recovery'
        key = 'history'
        client.put_bucket(bucket)
        client.versioning(bucket)
        first = b'old-version-' * 200000
        second = b'new-version-' * 300
        client.put(bucket, key, first)
        v1 = client.last_headers.get('x-amz-version-id')
        assert v1 and v1 != 'null'
        client.put(bucket, key, second)
        v2 = client.last_headers.get('x-amz-version-id')
        assert v2 and v2 != v1
        assert client.get(bucket, key + '?versionId=' + v1) == first
        assert client.get(bucket, key) == second
        original = residue_inventory(cluster, bucket, key)
        nonce = arm_barrier(cluster, 0, bucket, key, stage, disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key + '?versionId=' + v1)
        observation = reached_barrier(cluster, 0, nonce)
        cluster.kill()
        thread.join(5)
        clear_barriers(cluster)
        cluster.restart()
        if stage == 'APPLIED':
            wait_inventory(cluster, bucket, key, lambda inventory: inventory == original)
            assert client.get(bucket, key + '?versionId=' + v1) == first
            client.delete(bucket, key + '?versionId=' + v1)
        else:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                (code, _) = client.request('GET', bucket + '/' + key + '?versionId=' + v1)
                if code == 404:
                    break
                time.sleep(0.1)
            else:
                raise AssertionError('committed version deletion was resurrected')
        assert client.get(bucket, key) == second
        assert client.get(bucket, key + '?versionId=' + v2) == second
        client.delete(bucket, key + '?versionId=' + v2)
        client.delete_bucket(bucket)
        return {'case': 'versioned-' + stage.lower(), 'status': 'passed', 'observation': observation, 'deleted_version': v1, 'retained_version': v2}
    finally:
        cluster.remove()

def protected_version():
    import datetime
    cluster = Cluster('retention', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'protected-recovery'
        key = 'retained'
        client.put_bucket(bucket, lock=True)
        client.versioning(bucket)
        until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        payload = b'compliance-and-legal-hold' * 1024
        client.put(bucket, key, payload, {'x-amz-object-lock-mode': 'COMPLIANCE', 'x-amz-object-lock-retain-until-date': until, 'x-amz-object-lock-legal-hold': 'ON'})
        version = client.last_headers.get('x-amz-version-id')
        assert version and version != 'null'
        original = residue_inventory(cluster, bucket, key)
        client.delete(bucket, key + '?versionId=' + version, expected=403)
        assert residue_inventory(cluster, bucket, key) == original
        cluster.kill()
        cluster.restart()
        assert client.get(bucket, key + '?versionId=' + version) == payload
        assert residue_inventory(cluster, bucket, key) == original
        client.delete_bucket(bucket, expected=409)
        return {'case': 'compliance-legal-hold-restart', 'status': 'passed', 'retained_version': version}
    finally:
        cluster.remove()

def returning_drive_new_generation():
    cluster = Cluster('returning-new-generation', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'returning-drive'
        key = 'payload'
        client.put_bucket(bucket)
        old = b'old-payload-' * 200000
        new = b'new-payload-' * 180000
        client.put(bucket, key, old)
        assert client.get(bucket, key) == old
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        for index in range(4):
            cluster.kill(index)
        thread.join(5)
        clear_barriers(cluster)
        for index in range(3):
            docker('start', cluster.names[index])
        cluster.ready(indices=range(3))
        client.put(bucket, key, new)
        assert client.get(bucket, key) == new
        client.delete(bucket, key)
        docker('start', cluster.names[3])
        cluster.ready()
        wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)), timeout=90)
        client.request('GET', bucket + '/' + key, expected=404)
        assert client.list(bucket) == []
        client.delete_bucket(bucket)
        return {'case': 'old-prepared-new-put-new-delete-returning-peer', 'status': 'passed', 'observation': observation}
    finally:
        cluster.remove()

def mixed_versions():
    cluster = Cluster('mixed', nodes=4, drives=2, binary_by_node={3: 'baseline'})
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'mixed-recovery'
        key = 'payload'
        payload = b'mixed-version-data' * 100000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        assert client.get(bucket, key) == payload
        original = residue_inventory(cluster, bucket, key)
        results = []
        for initiator in (0, 3):
            (code, body) = cluster.clients[initiator].request('DELETE', bucket + '/' + key)
            assert code >= 400, f'mixed initiator{initiator} must fail closed: {code} {body!r}'
            after = residue_inventory(cluster, bucket, key)
            (ROOT / f'mixed-initiator-{initiator}-inventory.json').write_text(json.dumps({'before': original, 'after': after}, indent=2))
            assert after == original, 'mixed rejection must preserve exact metadata/shards after rollback'
            assert client.get(bucket, key) == payload
            results.append({'initiator': initiator, 'delete_status': code})
        cluster.replace_node(3, 'candidate')
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'three-current-one-pre-extension-peer-both-initiators', 'status': 'passed', 'results': results}
    finally:
        cluster.remove()

def permission_fault():
    cluster = Cluster('permission-error', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'permission-fault'
        key = 'payload'
        payload = b'permission-test' * 150000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        assert client.get(bucket, key) == payload
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        transaction = str(uuid.UUID(observation['transaction']))
        code = "from pathlib import Path; import json,os; p=Path('/data/d1')/%r/%r/%r; os.chmod(p,0o555); print(json.dumps({'path':str(p),'mode':oct(p.stat().st_mode&0o777)}))" % (bucket, key, transaction)
        fault = cluster.inspect_data(code)
        (cluster.barriers[0] / ('release.' + nonce)).write_bytes(b'')
        thread.join(30)
        assert not thread.is_alive(), 'permission failure must return rather than hang'
        clear_barriers(cluster)
        current = residue_inventory(cluster, bucket, key)
        assert any((drive['exists'] for node in current for drive in node))
        client.delete_bucket(bucket, expected=409)
        repair = "from pathlib import Path; import json,os; p=Path('/data/d1')/%r/%r/%r; os.chmod(p,0o755); print(json.dumps({'repaired':True}))" % (bucket, key, transaction)
        cluster.inspect_data(repair)
        cluster.kill()
        cluster.restart()
        wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)))
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket)
        return {'case': 'actual-eacces-decision-publication', 'status': 'passed', 'fault': fault, 'request': {key: str(value) for (key, value) in request.items()}, 'observation': observation}
    finally:
        cluster.remove()

def injected_error(stage):
    cluster = Cluster('injected-' + stage.lower(), drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'injected-fault'
        key = 'payload'
        payload = b'injected-error' * 170000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        nonce = arm_barrier(cluster, 0, bucket, key, stage, disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        (cluster.barriers[0] / ('release.' + nonce)).write_bytes(b'io_error')
        thread.join(30)
        assert not thread.is_alive(), 'injected error must finish the caller'
        clear_barriers(cluster)
        cluster.kill()
        cluster.restart()
        if stage == 'PREPARED':
            (code, body) = client.request('GET', bucket + '/' + key)
            if code == 200:
                assert body == payload
                client.delete(bucket, key)
            else:
                assert code == 404
        wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)))
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket)
        return {'case': 'post-persist-' + stage.lower() + '-io-error', 'status': 'passed', 'observation': observation, 'request': {key: str(value) for (key, value) in request.items()}}
    finally:
        cluster.remove()

def missing_version_returning_peer():
    cluster = Cluster('returning-specific-version', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'missing-version-recovery'
        key = 'history'
        client.put_bucket(bucket)
        client.versioning(bucket)
        old = b'first-version' * 160000
        newer = b'latest-version' * 1000
        client.put(bucket, key, old)
        v1 = client.last_headers['x-amz-version-id']
        client.put(bucket, key, newer)
        v2 = client.last_headers['x-amz-version-id']
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key + '?versionId=' + v1)
        observation = reached_barrier(cluster, 0, nonce)
        for index in range(4):
            cluster.kill(index)
        thread.join(5)
        clear_barriers(cluster)
        for index in range(3):
            docker('start', cluster.names[index])
        cluster.ready(indices=range(3))
        (code, body) = client.request('DELETE', bucket + '/' + key + '?versionId=' + v1)
        initial_delete_status = code
        assert code >= 400, f'missing version with unresolved offline journal must not acknowledge204: {code} {body!r}'
        docker('start', cluster.names[3])
        cluster.ready()
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            (code, body) = client.request('GET', bucket + '/' + key + '?versionId=' + v1)
            if code == 200:
                assert body == old
                break
            time.sleep(0.2)
        else:
            raise AssertionError('undecided historical version did not recover after peer returned')
        client.delete(bucket, key + '?versionId=' + v1)
        client.request('GET', bucket + '/' + key + '?versionId=' + v1, expected=404)
        assert client.get(bucket, key) == newer
        client.delete(bucket, key + '?versionId=' + v2)
        client.delete_bucket(bucket)
        return {'case': 'missing-specific-version-offline-fence', 'status': 'passed', 'observation': observation, 'initial_delete_status': initial_delete_status}
    finally:
        cluster.remove()

def untouched_returning_peer():
    cluster = Cluster('untouched-returning', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'untouched-peer'
        key = 'payload'
        payload = b'untouched-original' * 150000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        assert client.get(bucket, key) == payload
        original = residue_inventory(cluster, bucket, key)
        cluster.kill(3)
        client.delete(bucket, key)
        after = residue_inventory(cluster, bucket, key)
        assert after[3] == original[3], 'offline unprepared peer must remain byte-for-byte untouched'
        client.delete_bucket(bucket, expected=409)
        cluster.restart(3)
        wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)), timeout=90)
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket)
        return {'case': 'acknowledged-delete-untouched-offline-peer-return', 'status': 'passed'}
    finally:
        cluster.remove()

def uuid_prefix_crash():
    cluster = Cluster('uuid-prefix', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'uuid-prefix'
        key = 'parent'
        payload = b'parent-payload' * 160000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        inventory = residue_inventory(cluster, bucket, key)
        part = next((name for name in inventory[0][0]['files'] if name.endswith('/part.1')))
        data_dir = str(uuid.UUID(part.split('/')[0]))
        child = key + '/' + data_dir + '/child'
        child_payload = b'preserve-live-prefix-child' * 800
        client.put(bucket, child, child_payload)
        assert client.get(bucket, child) == child_payload
        nonce = arm_barrier(cluster, 0, bucket, key, 'DECISION', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        cluster.kill()
        thread.join(5)
        clear_barriers(cluster)
        cluster.restart()
        assert client.get(bucket, child) == child_payload, 'recovery must preserve actual data-dir UUID-prefix descendants'
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket, expected=409)
        client.delete(bucket, child)
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'committed-delete-live-uuid-data-prefix', 'status': 'passed', 'observation': observation, 'protected_child': child}
    finally:
        cluster.remove()

def corruption_repair():
    cluster = Cluster('corrupt-journal', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'corrupt-journal'
        key = 'payload'
        payload = b'original-protected-data' * 100000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        cluster.kill()
        thread.join(5)
        clear_barriers(cluster)
        transaction = str(uuid.UUID(observation['transaction']))
        code = "from pathlib import Path; import base64,json; p=Path('/data/d1')/%r/%r/%r/'delete-prepared'; old=p.read_bytes(); p.write_bytes(b'corrupt-record'); print(json.dumps({'original':base64.b64encode(old).decode()}))" % (bucket, key, transaction)
        saved = cluster.inspect_data(code)[0]['original']
        before = residue_inventory(cluster, bucket, key)
        cluster.restart()
        time.sleep(2)
        assert residue_inventory(cluster, bucket, key) == before, 'corrupt authority must preserve all backup/payload files'
        (code, body) = client.request('DELETE', bucket + '/' + key)
        assert code >= 400, f'corrupt hidden transaction must not acknowledge cleanup: {code} {body!r}'
        assert residue_inventory(cluster, bucket, key) == before
        repair = "from pathlib import Path; import base64,json; p=Path('/data/d1')/%r/%r/%r/'delete-prepared'; p.write_bytes(base64.b64decode(%r)); print(json.dumps({'repaired':True}))" % (bucket, key, transaction, saved)
        cluster.inspect_data(repair)
        cluster.kill()
        cluster.restart()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            (code, body) = client.request('GET', bucket + '/' + key)
            if code == 200:
                assert body == payload
                break
            time.sleep(0.1)
        else:
            raise AssertionError('valid original journal must recover original bytes after repair')
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'corrupt-journal-fails-closed-then-repair', 'status': 'passed', 'observation': observation}
    finally:
        cluster.remove()

def admin_heal(client, bucket, dry_run=False, remove=False, key=None, recursive=None):
    path = 'rustfs/admin/v3/heal/' + bucket + ('/' + key if key else '')
    options = {'recursive': key is None if recursive is None else recursive, 'dryRun': dry_run, 'remove': remove, 'recreate': False, 'scanMode': 1, 'updateParity': False, 'nolock': False, 'readRepair': False}
    (_, body) = client.request('POST', path, json.dumps(options).encode(), {'Content-Type': 'application/json'}, 200)
    token = json.loads(body)['clientToken']
    deadline = time.monotonic() + 90

    def states(value):
        if isinstance(value, dict):
            for (k, v) in value.items():
                if k == 'state' and isinstance(v, str):
                    yield v.lower()
                yield from states(v)
        elif isinstance(value, list):
            for v in value:
                yield from states(v)
    while time.monotonic() < deadline:
        (_, body) = client.request('POST', path + '?clientToken=' + token, headers={'Content-Type': 'application/json'}, expected=200)
        result = json.loads(body)
        with (ROOT / 'admin-responses.jsonl').open('a') as report:
            report.write(json.dumps({'bucket': bucket, 'key': key, 'dry_run': dry_run, 'remove': remove, 'recursive': options['recursive'], 'response': result}) + '\n')
        status = set(states(result))
        if status & {'failed', 'completed_with_errors', 'blocked'}:
            raise AssertionError('admin heal failed: ' + json.dumps(result))
        if 'completed' in status or result.get('summary') == 'finished':
            return result
        time.sleep(0.1)
    raise TimeoutError('admin heal did not complete: ' + token)

def admin_legacy_dry_run(direct=False):
    cluster = Cluster('admin-legacy', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'admin-legacy'
        client.put_bucket(bucket)
        for (key, payload) in [('inline', b'i' * 4096), ('external', b'e' * (2 * 1024 * 1024))]:
            client.put(bucket, key, payload)
        cluster.kill()
        seed_legacy(cluster, bucket, 'inline', 'backup-only')
        seed_legacy(cluster, bucket, 'external', 'reserved')
        before = {key: residue_inventory(cluster, bucket, key) for key in ('inline', 'external')}
        cluster.restart()
        time.sleep(2)
        dry = admin_heal(client, bucket, dry_run=True, remove=True)
        assert {key: residue_inventory(cluster, bucket, key) for key in before} == before, 'dry run must never mutate ambiguous residue'
        actual = [admin_heal(client, bucket, dry_run=False, remove=True, key=key) for key in before] if direct else admin_heal(client, bucket, dry_run=False, remove=True)
        for key in before:
            wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)))
        client.delete_bucket(bucket)
        return {'case': 'admin-legacy-dry-run-and-explicit-cleanup', 'status': 'passed', 'dry_run_report': dry, 'cleanup_report': actual}
    finally:
        cluster.remove()

def admin_prefix_scoped():
    cluster = Cluster('admin-prefix', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket, prefix = 'admin-prefix', 'selected/'
        selected = {'selected/inline': b'i' * 4096, 'selected/external': b'e' * (2 * 1024 * 1024)}
        outside = {'selected-neighbor/hidden': b'n' * 4096, 'outside/hidden': b'o' * 4096}
        live_key, live_body = 'selected/live', b'live-prefix-object-must-survive'
        client.put_bucket(bucket)
        for key, body in {**selected, **outside, live_key: live_body}.items():
            client.put(bucket, key, body)
            assert client.get(bucket, key) == body
        cluster.kill()
        for key in {**selected, **outside}:
            seed_legacy(cluster, bucket, key, 'reserved' if key == 'selected/external' else 'backup-only')
        before = {key: residue_inventory(cluster, bucket, key) for key in {**selected, **outside}}
        cluster.restart()
        dry = admin_heal(client, bucket, dry_run=True, remove=True, key=prefix, recursive=True)
        assert any('delete recovery (dry run)' in item.get('detail', '') and 'discovered=2' in item.get('detail', '') for item in dry.get('items', [])), 'prefix dry run must inspect the two matching hidden transactions'
        assert {key: residue_inventory(cluster, bucket, key) for key in before} == before, 'prefix dry run must preserve every hidden transaction'
        actual = admin_heal(client, bucket, remove=True, key=prefix, recursive=True)
        after = {key: residue_inventory(cluster, bucket, key) for key in before}
        (ROOT / 'prefix-inventory.json').write_text(json.dumps({'before': before, 'after': after}, indent=2))
        for key in selected:
            wait_inventory(cluster, bucket, key, lambda inventory: all(not drive['exists'] for node in inventory for drive in node))
        for key in outside:
            assert residue_inventory(cluster, bucket, key) == before[key], f'prefix recovery must preserve outside transaction {key}'
        assert client.get(bucket, live_key) == live_body, 'prefix remove must preserve the live object'
        assert client.list(bucket) == [live_key]
        client.delete(bucket, live_key)
        client.delete_bucket(bucket, expected=409)
        for key in outside:
            client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'admin-prefix-hidden-transactions-only', 'status': 'passed', 'prefix': prefix, 'selected_keys': list(selected), 'protected_keys': list(outside), 'dry_run_report': dry, 'cleanup_report': actual}
    finally:
        cluster.remove()

def deep_key_crash(large_stack=False):
    extra_env = {'RUSTFS_RUNTIME_THREAD_STACK_SIZE': str(32 * 1024 * 1024)} if large_stack else None
    cluster = Cluster('deep-key-large-stack' if large_stack else 'deep-key', drives=4, extra_env=extra_env)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'deep-recovery'
        key = '/'.join(['a'] * 300) + '/payload'
        payload = b'deep-inline-content' * 200
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        original = residue_inventory(cluster, bucket, key)
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        cluster.kill()
        thread.join(5)
        clear_barriers(cluster)
        cluster.restart()
        wait_inventory(cluster, bucket, key, lambda inventory: inventory == original)
        assert client.get(bucket, key) == payload
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'deep-key-recovery-large-stack' if large_stack else 'deep-key-recovery', 'status': 'passed', 'segments': 301, 'runtime_thread_stack_size': 32 * 1024 * 1024 if large_stack else 'default', 'observation': observation}
    finally:
        cluster.remove()

def receipt_response_loss():
    cluster = Cluster('receipt-response-loss', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'receipt-response-loss'
        key = 'payload'
        payload = b'lost-response-data' * 150000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        nonces = {index: arm_barrier(cluster, index, bucket, key, 'DECISION') for index in (1, 2)}
        (thread, request) = async_delete(client, bucket, key)
        observations = []
        for (index, nonce) in nonces.items():
            observations.append(reached_barrier(cluster, index, nonce))
            deadline = time.monotonic() + 40
            while len(list(cluster.barriers[index].glob('reached.' + nonce + '.*.json'))) < 2:
                if time.monotonic() > deadline:
                    raise TimeoutError('both drive receipts must persist before peer kill')
                time.sleep(0.02)
        for index in (1, 2):
            cluster.kill(index)
        thread.join(75)
        assert not thread.is_alive(), 'DELETE must return after peer response loss'
        if 'response' in request:
            assert request['response'][0] >= 400, 'four online responses cannot satisfy five-drive receipt quorum'
        before = residue_inventory(cluster, bucket, key)
        assert any(('delete-quorum-committed' in path for node in before for drive in node for path in drive['files'])), 'stored decisions must remain while peers are offline'
        clear_barriers(cluster)
        for index in (1, 2):
            docker('start', cluster.names[index])
        cluster.ready()
        wait_inventory(cluster, bucket, key, lambda inventory: all((not drive['exists'] for node in inventory for drive in node)), timeout=90)
        client.request('GET', bucket + '/' + key, expected=404)
        assert client.list(bucket) == []
        client.delete_bucket(bucket)
        return {'case': 'receipt-publication-response-loss', 'status': 'passed', 'observations': observations, 'request': {key: str(value) for (key, value) in request.items()}}
    finally:
        cluster.remove()

def retained_decision_newer_undecided():
    cluster = Cluster('inverse-null-history', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        (bucket, key) = ('inverse-null-history', 'payload')
        older = b'older-generation' * 150000
        newer = b'newer-generation' * 150000
        client.put_bucket(bucket)
        client.put(bucket, key, older)
        cluster.kill(3)
        client.delete(bucket, key)
        witness = residue_inventory(cluster, bucket, key)
        assert any(('delete-quorum-committed' in path for node in witness[:3] for drive in node for path in drive['files'])), 'offline old peer requires retained committed authority'
        client.put(bucket, key, newer)
        assert client.get(bucket, key) == newer
        nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
        (thread, request) = async_delete(client, bucket, key)
        observation = reached_barrier(cluster, 0, nonce)
        for index in range(3):
            cluster.kill(index)
        thread.join(5)
        clear_barriers(cluster)
        cluster.restart()
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            (status, body) = client.request('GET', bucket + '/' + key)
            if status == 200:
                assert body == newer, 'older committed nil receipt must never promote a newer undecided generation'
                break
            time.sleep(0.1)
        else:
            raise AssertionError('newer undecided generation was not restored after complete peer return')
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'retained-older-decision-newer-undecided-null', 'status': 'passed', 'observation': observation}
    finally:
        cluster.remove()


def completed_multipart_sidecars(protected=False, crash=False):
    case = 'multipart-integrity' if protected else 'multipart-sidecars'
    if crash:
        case += '-applied'
    mode = 'true' if protected else 'false'
    cluster = Cluster(case, drives=4, extra_env={'RUSTFS_SHARD_INTEGRITY_WRITE': mode, 'RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED': mode})
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'completed-multipart'
        key = 'payload'
        parts = [bytes(range(256)) * 20480, bytes(reversed(range(256))) * 4096]
        payload = b''.join(parts)
        client.put_bucket(bucket)
        upload = client.complete_multipart(bucket, key, parts)
        assert client.get(bucket, key) == payload, 'complete MPU must expose all uploaded bytes'
        original = residue_inventory(cluster, bucket, key)
        (ROOT / 'multipart-original-inventory.json').write_text(json.dumps(original, indent=2))
        for node in original:
            for drive in node:
                assert drive['exists'] and 'xl.meta' in drive['files'], 'complete MPU must publish metadata on every drive'
                for number in (1, 2):
                    sidecars = [name for name in drive['files'] if Path(name).name == f'part.{number}.meta']
                    assert len(sidecars) == 1, 'native completed MPU must retain each real part metadata sidecar'
                    prefix = f'part.{number}.integrity.'
                    proofs = [name for name in drive['files'] if Path(name).name.startswith(prefix)]
                    assert len(proofs) == (1 if protected else 0), 'native completion must retain the selected integrity format'
                    for proof in proofs:
                        assert uuid.UUID(Path(proof).name[len(prefix):]).int != 0, 'integrity proof generation must be non-nil'
        observation = None
        if crash:
            nonce = arm_barrier(cluster, 0, bucket, key, 'APPLIED', disk='/data/d1')
            (thread, request) = async_delete(client, bucket, key)
            observation = reached_barrier(cluster, 0, nonce)
            cluster.kill()
            thread.join(5)
            clear_barriers(cluster)
            cluster.restart()
            wait_inventory(cluster, bucket, key, lambda inventory: inventory == original)
            restored = residue_inventory(cluster, bucket, key)
            assert restored == original, 'rollback must preserve exact metadata, all shards, part sidecars and integrity proofs'
            (ROOT / 'multipart-restored-inventory.json').write_text(json.dumps(restored, indent=2))
            assert client.get(bucket, key) == payload, 'rolled-back native MPU must retain the complete readable body'
        client.batch_delete(bucket, [key])
        wait_inventory(cluster, bucket, key, lambda inventory: all(not drive['exists'] for node in inventory for drive in node))
        client.request('GET', bucket + '/' + key, expected=404)
        assert client.list(bucket) == []
        client.delete_bucket(bucket)
        return {'case': case, 'status': 'passed', 'native_upload_id': upload, 'integrity': protected, 'part_sizes': [len(part) for part in parts], 'bytes': len(payload), 'observation': observation, 'bucket_deletion': 'ordinary non-force204'}
    finally:
        cluster.remove()

def ordinary_missing_with_offline_peer():
    cluster = Cluster('missing-offline', nodes=4, drives=1)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'missing-offline'
        client.put_bucket(bucket)
        cluster.kill(3)
        client.delete(bucket, 'never-existed-single', expected=204)
        client.batch_delete(bucket, ['never-existed-batch-a', 'never-existed-batch-b'])
        assert client.list(bucket) == [], 'ordinary missing DELETE must leave the existing bucket valid'
        payload = b'bucket-remains-usable' * 200
        client.put(bucket, 'live', payload)
        assert client.get(bucket, 'live') == payload
        docker('start', cluster.names[3])
        cluster.ready()
        assert client.get(bucket, 'live') == payload
        client.delete(bucket, 'live')
        wait_inventory(cluster, bucket, 'live', lambda inventory: all(not drive['exists'] for node in inventory for drive in node), timeout=90)
        client.delete_bucket(bucket)
        return {'case': 'ordinary-missing-delete-offline-single-and-batch', 'status': 'passed', 'cleanup_claim': 'no residue recovery was requested'}
    finally:
        cluster.remove()

def legacy_transaction_data_dir_alias():
    cluster = Cluster('legacy-data-dir-alias', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket, key = 'legacy-data-dir-alias', 'payload'
        payload = b'legacy-data-dir-content' * 100000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        assert client.get(bucket, key) == payload
        cluster.kill()
        fixture = seed_legacy(cluster, bucket, key, 'alias')
        before = residue_inventory(cluster, bucket, key)
        cluster.restart()
        time.sleep(2)
        assert residue_inventory(cluster, bucket, key) == before, 'automatic recovery must preserve ambiguous alias residue'
        client.delete(bucket, key)
        wait_inventory(cluster, bucket, key, lambda inventory: all(not drive['exists'] for node in inventory for drive in node))
        client.delete_bucket(bucket)
        return {'case': 'legacy-transaction-equals-data-directory', 'status': 'passed', 'fixture': fixture}
    finally:
        cluster.remove()

def admin_smoke():
    cluster = Cluster('admin-smoke', drives=4)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket, key = 'admin-smoke', 'payload'
        payload = b'healthy-admin-object' * 1000
        client.put_bucket(bucket)
        client.put(bucket, key, payload)
        bucket_report = admin_heal(client, bucket, dry_run=True)
        object_report = admin_heal(client, bucket, key=key)
        assert client.get(bucket, key) == payload
        client.delete(bucket, key)
        client.delete_bucket(bucket)
        return {'case': 'admin-healthy-bucket-and-object-client', 'status': 'passed', 'bucket_report': bucket_report, 'object_report': object_report}
    finally:
        cluster.remove()

def large_batch_delete():
    cluster = Cluster('batch-1000', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket = 'batch-1000'
        client.put_bucket(bucket)
        payloads = {f'object-{index:04d}': (bytes(range(256)) * 8192 if index % 250 == 0 else f'content-{index:04d}'.encode()) for index in range(1000)}
        for key, payload in payloads.items():
            client.put(bucket, key, payload)
        for key, payload in payloads.items():
            assert client.get(bucket, key) == payload
        client.batch_delete(bucket, list(payloads))
        assert client.list(bucket) == []
        for key in ('object-0000', 'object-0500', 'object-0999'):
            assert all(not drive['exists'] for node in residue_inventory(cluster, bucket, key) for drive in node)
        client.delete_bucket(bucket)
        return {'case': 'maximum-1000-key-remote-delete-batch', 'status': 'passed', 'exact_gets': 1000, 'keys': 1000}
    finally:
        cluster.remove()

def large_version_history():
    cluster = Cluster('versions-3000', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket, key = 'versions-3000', 'history'
        client.put_bucket(bucket)
        client.versioning(bucket)
        versions = []
        for index in range(3000):
            payload = f'version-content-{index:04d}'.encode()
            client.put(bucket, key, payload)
            versions.append((client.last_headers['x-amz-version-id'], payload))
        for index in (0, 1500, 2999):
            version, payload = versions[index]
            assert client.request('GET', bucket + '/' + key + '?versionId=' + version, expected=200)[1] == payload
        oldest = versions.pop(0)[0]
        client.delete(bucket, key + '?versionId=' + oldest)
        client.request('GET', bucket + '/' + key + '?versionId=' + oldest, expected=404)
        assert client.get(bucket, key) == versions[-1][1]
        for offset in range(0, len(versions), 1000):
            group = versions[offset:offset + 1000]
            xml = ET.Element('Delete')
            for version, payload in group:
                item = ET.SubElement(xml, 'Object')
                ET.SubElement(item, 'Key').text = key
                ET.SubElement(item, 'VersionId').text = version
            data = ET.tostring(xml)
            status, body = client.request('POST', bucket + '?delete', data, {'Content-Type': 'application/xml', 'Content-MD5': base64.b64encode(hashlib.md5(data).digest()).decode()}, 200, timeout=180)
            response = ET.fromstring(body)
            assert not [node for node in response.iter() if node.tag.rsplit('}', 1)[-1] == 'Error'], body
            deleted = [child.text for node in response.iter() if node.tag.rsplit('}', 1)[-1] == 'Deleted' for child in node if child.tag.rsplit('}', 1)[-1] == 'VersionId']
            assert sorted(deleted) == sorted(version for version, payload in group), body
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket)
        return {'case': 'large-version-history-remote-single-and-batch-delete', 'status': 'passed', 'versions': 3000, 'survivor_bytes_verified': True}
    finally:
        cluster.remove()

def unknown_predecessor_fresh_delete():
    cluster = Cluster('unknown-predecessor', nodes=4, drives=2)
    try:
        cluster.create()
        client = cluster.clients[0]
        bucket, key = 'unknown-predecessor', 'payload'
        client.put_bucket(bucket)
        client.put(bucket, key, b'older-inline' * 300)
        nonce = arm_barrier(cluster, 3, bucket, key, 'APPLIED', disk='/data/d1')
        thread, request = async_delete(cluster.clients[3], bucket, key)
        observation = reached_barrier(cluster, 3, nonce)
        transaction = str(uuid.UUID(observation['transaction']))
        verify = "from pathlib import Path; import json; result=[];\nfor drive in sorted(Path('/data').glob('d*')):\n root=drive/%r/%r; tx=root/%r; result.append(not (root/'xl.meta').exists() and (tx/'xl.meta.bkp').is_file())\nprint(json.dumps(result))" % (bucket, key, transaction)
        deadline = time.monotonic() + 30
        while not all(cluster.inspect_data(verify, indices=[3])[0]):
            if time.monotonic() >= deadline:
                raise TimeoutError('both returning-peer preparations must reach metadata absence')
            time.sleep(.05)
        cluster.kill(3)
        for index in range(3):
            cluster.kill(index)
        thread.join(5)
        clear_barriers(cluster)
        restore = "from pathlib import Path; import json,os; result=[];\nfor drive in sorted(Path('/data').glob('d*')):\n root=drive/%r/%r; tx=root/%r; backup=tx/'xl.meta.bkp';\n if backup.exists(): os.replace(backup,root/'xl.meta')\n if tx.exists():\n  for path in tx.iterdir():\n   assert path.is_file() and path.name in ('delete-prepared','delete-target'), path\n   path.unlink()\n  tx.rmdir()\n assert (root/'xl.meta').is_file(); result.append(True)\nprint(json.dumps(result))" % (bucket, key, transaction)
        cluster.inspect_data(restore, indices=range(3))
        for index in range(3):
            docker('start', cluster.names[index])
        cluster.ready(indices=range(3))
        newer = b'newer-inline' * 300
        client.put(bucket, key, newer)
        assert client.get(bucket, key) == newer
        client.delete(bucket, key)
        docker('start', cluster.names[3])
        cluster.ready()
        time.sleep(2)
        pending = residue_inventory(cluster, bucket, key)
        assert all(transaction + '/xl.meta.bkp' in drive['files'] for drive in pending[3]), 'automatic recovery must preserve unknown predecessor evidence'
        client.request('GET', bucket + '/' + key, expected=404)
        client.delete_bucket(bucket, expected=409)
        client.delete(bucket, key, expected=204)
        wait_inventory(cluster, bucket, key, lambda inventory: all(not drive['exists'] for node in inventory for drive in node))
        client.delete_bucket(bucket)
        return {'case': 'unknown-predecessor-preserved-until-fresh-current-delete', 'status': 'passed', 'observation': observation, 'fixture': 'authentic prepared records retained only on stopped returning peer'}
    finally:
        cluster.remove()

def main():
    global ROOT, PREFIX, IMAGE, PORT, CANDIDATE, BASELINE
    import argparse, re, sys
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--binary', required=True, type=Path, help='Linux candidate binary built with e2e-test-hooks')
    parser.add_argument('--baseline-binary', type=Path, help='Older Linux binary, required by mixed case')
    parser.add_argument('--image', required=True, help='Local Linux image with Python 3 and the binary runtime libraries')
    parser.add_argument('--artifact-dir', required=True, type=Path, help='New directory for logs and stage handshakes')
    parser.add_argument('--namespace', default='rustfs-delete-' + uuid.uuid4().hex[:12])
    parser.add_argument('--port', type=int, default=19010, help='First of four sequential localhost ports')
    parser.add_argument('cases', nargs='*', default=['smoke'])
    options = parser.parse_args()
    if not re.fullmatch('[a-z][a-z0-9-]{0,45}', options.namespace):
        parser.error('namespace must be a short lowercase Docker name')
    if not 1024 <= options.port <= 65532:
        parser.error('port must leave room for four localhost ports')
    CANDIDATE = options.binary.resolve(strict=True)
    if not CANDIDATE.is_file():
        parser.error('binary must be a regular file')
    BASELINE = options.baseline_binary.resolve(strict=True) if options.baseline_binary else None
    if BASELINE is not None and (not BASELINE.is_file()):
        parser.error('baseline binary must be a regular file')
    ROOT = options.artifact_dir.resolve()
    ROOT.mkdir(parents=True, exist_ok=False)
    PREFIX = options.namespace
    IMAGE = options.image
    PORT = options.port
    for resource in ('container', 'volume', 'network'):
        existing = docker(resource, 'ls', '-q', '--filter', 'label=rustfs.delete-recovery-test=' + PREFIX).stdout.strip()
        if existing:
            parser.error('namespace already owns Docker resources; choose a new namespace')
    cases = {'unknown-fresh': unknown_predecessor_fresh_delete, 'batch1000': large_batch_delete, 'versions3000': large_version_history, 'admin-smoke': admin_smoke, 'legacy-alias': legacy_transaction_data_dir_alias, 'missing-offline': ordinary_missing_with_offline_peer, 'inverse': retained_decision_newer_undecided, 'deep': deep_key_crash, 'deep-large-stack': lambda : deep_key_crash(True), 'admin-prefix': admin_prefix_scoped, 'response-loss': receipt_response_loss, 'admin-object': lambda : admin_legacy_dry_run(True), 'smoke': smoke, 'legacy1': lambda : legacy_retry(1), 'legacy4': lambda : legacy_retry(4), 'restore': rollback_crash, 'retention': protected_version, 'version-applied': lambda : versioned_crash('APPLIED'), 'version-decision': lambda : versioned_crash('DECISION'), 'returning': returning_drive_new_generation, 'untouched': untouched_returning_peer, 'missing-version': missing_version_returning_peer, 'mixed': mixed_versions, 'admin': admin_legacy_dry_run, 'prefix': uuid_prefix_crash, 'corruption': corruption_repair, 'permission': permission_fault, 'error-prepared': lambda : injected_error('PREPARED'), 'error-applied': lambda : injected_error('APPLIED'), 'error-decision': lambda : injected_error('DECISION')}
    cases.update({'multipart-sidecars': completed_multipart_sidecars, 'multipart-integrity': lambda: completed_multipart_sidecars(True), 'multipart-sidecars-applied': lambda: completed_multipart_sidecars(False, True), 'multipart-integrity-applied': lambda: completed_multipart_sidecars(True, True)})
    for stage in ('PREACTIVATION', 'PREPARED', 'APPLIED', 'DECISION'):
        for nodes in (1, 4):
            cases[f'{stage.lower()}{nodes}'] = lambda stage=stage, nodes=nodes: process_crash(stage, nodes)
        cases[f'inline-{stage.lower()}'] = lambda stage=stage: process_crash(stage, 1, 4096)
        cases[f'zero-{stage.lower()}'] = lambda stage=stage: process_crash(stage, 1, 0)
    if 'mixed' in options.cases and BASELINE is None:
        parser.error('mixed requires --baseline-binary')
    unknown = set(options.cases) - set(cases)
    if unknown:
        parser.error('unknown cases: ' + ','.join(sorted(unknown)))
    results = []
    for case in options.cases:
        started = time.monotonic()
        try:
            result = cases[case]()
            result['elapsed_seconds'] = round(time.monotonic() - started, 3)
        except BaseException as error:
            result = {'case': case, 'status': 'failed', 'error': str(error), 'elapsed_seconds': round(time.monotonic() - started, 3)}
            (ROOT / (case + '.json')).write_text(json.dumps(result, indent=2, sort_keys=True))
            raise
        results.append(result)
        (ROOT / (case + '.json')).write_text(json.dumps(result, indent=2, sort_keys=True))
        print(json.dumps(result, sort_keys=True), flush=True)
    (ROOT / 'results.json').write_text(json.dumps(results, indent=2, sort_keys=True))
if __name__ == '__main__':
    main()
