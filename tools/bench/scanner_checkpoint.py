#!/usr/bin/env python3
"""Compare three checkpoint layouts on the same append-only coverage trace. No RustFS I/O."""

import argparse
from collections import Counter
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import platform
import re
from statistics import median
import subprocess
import sys
import tempfile
from time import perf_counter_ns


ROWS = 32
MAX_SEGMENTS = 32
COMMITS = 64  # Ends at the maximum chain length *after* one compaction.
FANOUT = 8
HISTORY_PAGES = (128, 1280)
LAYOUTS = ('full', 'segments', 'paged')
SCOPE = 'synthetic/set-0/source-0/plan-0'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return json.dumps(value, separators=(',', ':'), sort_keys=True).encode('ascii')


def page(prefix, ordinal):
    # Each fixture row has a unique key, version count, delete-marker count and logical bytes.
    return [[f'{prefix}/{i:08d}', 1 + i % 3, int(i % 5 == 0), i * 31 % 65536]
            for i in range(ordinal * ROWS, (ordinal + 1) * ROWS)]


class Files:
    """Single-writer fixture. Only new payloads and the root replacement cross the write meter."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.writes = Counter()
        self.read_bytes = self.reads = self.unlinks = 0
        self.live_bytes = self.peak_bytes = self.root_bytes = 0

    def read(self, name):
        data = (self.directory / name).read_bytes()
        self.read_bytes += len(data)
        self.reads += 1
        return data

    def get(self, ref):
        require(isinstance(ref, str) and re.fullmatch('[0-9a-f]{64}', ref), 'invalid reference')
        data = self.read(ref)
        require(sha256(data).hexdigest() == ref, 'blob digest mismatch')
        return json.loads(data)

    def write(self, name, kind, data):
        with (self.directory / name).open('xb') as stream:
            count = stream.write(data)
        require(count == len(data), 'short write')
        self.writes[kind] += count
        self.live_bytes += count
        self.peak_bytes = max(self.peak_bytes, self.live_bytes)

    def put(self, kind, value):
        data = encode(value)
        if kind in ('page', 'segment'):
            require(0 < len(value) <= 256 and len(data) <= 1024**2, 'work page exceeds limit')
        ref = sha256(data).hexdigest()
        self.write(ref, kind, data)
        return ref

    def commit(self, root, retired=()):
        data = encode(root)
        self.write('next', 'root', data)
        (self.directory / 'next').replace(self.directory / 'root')
        self.live_bytes -= self.root_bytes
        self.root_bytes = len(data)
        # Only this fixture's append-only right spine or superseded base/segments are retired.
        for ref in retired:
            path = self.directory / ref
            self.live_bytes -= path.stat().st_size
            path.unlink()
            self.unlinks += 1


def seed(files, layout, pages):
    root = dict(layout=layout, scope=SCOPE, revision=0, pages=len(pages))
    if layout == 'paged':
        refs = [files.put('page', p) for p in pages]
        height = 0
        while len(refs) > 1:
            refs = [files.put('index', refs[i:i + FANOUT]) for i in range(0, len(refs), FANOUT)]
            height += 1
        root.update(tree=refs[0], height=height)
    else:
        root['base'] = files.put('base', pages)
        if layout == 'segments':
            root['segments'] = []
    files.commit(root)
    return root


def append_tree(files, root, new_page, retired):
    leaf = files.put('page', new_page)
    n, h = root['pages'], root['height']

    def insert(ref, level):
        if level == 0:
            require(ref is None, 'append would replace a leaf')
            return leaf
        children = files.get(ref) if ref else []
        slot = n // FANOUT**(level - 1) % FANOUT
        require(slot <= len(children), 'hole in append path')
        if slot == len(children):
            children.append(insert(None, level - 1))
        else:
            children[slot] = insert(children[slot], level - 1)
        if ref:
            retired.append(ref)
        return files.put('index', children)

    if n == FANOUT**h:
        root.update(tree=files.put('index', [root['tree'], insert(None, h)]), height=h + 1)
    else:
        root['tree'] = insert(root['tree'], h)


def materialize(files):
    root = json.loads(files.read('root'))
    require(root['scope'] == SCOPE and root['layout'] in LAYOUTS, 'wrong fixture scope/layout')
    require(0 < root['pages'] <= max(HISTORY_PAGES) + COMMITS, 'invalid page count')
    if root['layout'] == 'paged':
        refs = [root['tree']]
        require(0 <= root['height'] <= 8, 'invalid tree height')
        for _ in range(root['height']):
            children = []
            for ref in refs:
                node = files.get(ref)
                require(0 < len(node) <= FANOUT, 'invalid index fanout')
                children.extend(node)
                require(len(children) <= root['pages'], 'index exceeds declared coverage')
            refs = children
        pages = [files.get(ref) for ref in refs]
    else:
        pages = files.get(root['base'])
        if root['layout'] == 'segments':
            require(len(root['segments']) <= MAX_SEGMENTS, 'unbounded segment replay')
            pages.extend(files.get(ref) for ref in root['segments'])
    require(len(pages) == root['pages'], 'incomplete recovery')
    return root, pages


def append(files, root, p, full_pages):
    retired = []
    if root['layout'] == 'full':
        full_pages.append(p)
        retired.append(root['base'])
        root['base'] = files.put('base', full_pages)
    elif root['layout'] == 'segments':
        segments = root['segments']
        if len(segments) == MAX_SEGMENTS:
            # Backpressure: do not acknowledge a 33rd delta while a compaction is owed.
            _, merged = materialize(files)
            retired = [root['base'], *segments]
            root['base'] = files.put('compaction', merged)
            segments.clear()
        segments.append(files.put('segment', p))
    else:
        append_tree(files, root, p, retired)
    root['revision'] += 1
    root['pages'] += 1
    files.commit(root, retired)


def cold_worker(directory):
    files = Files(directory)
    start = perf_counter_ns()
    root, pages = materialize(files)
    elapsed = perf_counter_ns() - start
    return dict(ns=elapsed, bytes=files.read_bytes, files=files.reads, revision=root['revision'],
                digest=sha256(encode(pages)).hexdigest())


def run(directory, layout, initial, deltas):
    files = Files(directory)
    # The writer cannot change the input ledger used by this or subsequent cells.
    writer_seed = deepcopy(initial)
    root = seed(files, layout, writer_seed)
    seed_writes = files.writes.copy()
    full_pages = writer_seed if layout == 'full' else None
    del writer_seed
    ordinary = []
    compactions = []
    peer = Files(directory)  # Independent reopen; correctness reads do not enter writer costs.
    for rev, p in enumerate(deltas, 1):
        before = files.writes.total()
        read_before = files.read_bytes
        compact = files.writes['compaction']
        append(files, root, deepcopy(p), full_pages)
        size = files.writes.total() - before
        if compact != files.writes['compaction']:
            compactions.append(dict(revision=rev, checkpoint_write_bytes=size, read_bytes=files.read_bytes - read_before))
        else:
            ordinary.append(size)
        observed, pages = materialize(peer)
        require(observed['revision'] == rev and pages == initial + deltas[:rev],
                f'{layout}: input ledger mismatch at revision {rev}')

    expected = initial + deltas
    digest = sha256(encode(expected)).hexdigest()
    cold = []
    for _ in range(3):
        output = subprocess.check_output([sys.executable, __file__, '--recover', str(directory)], timeout=60)
        sample = json.loads(output)
        require(sample.pop('digest') == digest and sample['revision'] == len(deltas), 'cold oracle mismatch')
        cold.append(sample)

    checkpoint_writes = files.writes - seed_writes
    checkpoint_reads = files.read_bytes
    retained = sum(p.stat().st_size for p in Path(directory).iterdir())
    require(retained == files.live_bytes, 'space accounting mismatch')
    # Common export target: the full control's exact two-file representation and reader.
    # The full control is already readable, so rollback there needs no additional write.
    if layout != 'full':
        export_dir = Path(directory) / 'export'
        export_dir.mkdir()
        export = Files(export_dir)
        observed, pages = materialize(files)
        base = export.put('base', pages)
        export.commit(dict(layout='full', scope=SCOPE, revision=observed['revision'], pages=len(pages), base=base))
        export_bytes = export.writes.total()
    else:
        export = files
        export_bytes = 0
    observed, pages = materialize(export)
    require(observed['layout'] == 'full' and observed['revision'] == len(deltas) and pages == expected,
            'full-reader rollback mismatch')
    # Include the read-back of the exported/control snapshot in the rollback cost.
    export_reads = files.read_bytes - checkpoint_reads + (export.read_bytes if export is not files else 0)
    return dict(layout=layout, history_rows=len(initial) * ROWS, final_rows=len(expected) * ROWS,
                oracle_sha256=digest, seed_writes=seed_writes, checkpoint_writes=checkpoint_writes,
                ordinary_write_bytes_p50=median(ordinary), compactions=compactions, checkpoint_read_bytes=checkpoint_reads,
                checkpoint_unlinks=files.unlinks, retained_bytes=retained, peak_checkpoint_bytes=files.peak_bytes, cold=cold,
                export_write_bytes=export_bytes, export_read_bytes=export_reads,
                total_encoded_io_bytes=files.writes.total() + checkpoint_reads + cold[0]['bytes'] + export_bytes + export_reads)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recover', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.recover is not None:
        print(json.dumps(cold_worker(args.recover)))
        return
    deltas = [page('delta', i) for i in range(COMMITS)]
    print(json.dumps(dict(runner_sha256=sha256(Path(__file__).read_bytes()).hexdigest(), python=platform.python_version(),
                          platform=platform.platform(), commits=COMMITS, rows_per_commit=ROWS, fanout=FANOUT,
                          segment_limit=MAX_SEGMENTS, delta_trace_sha256=sha256(encode(deltas)).hexdigest())), flush=True)
    for n in HISTORY_PAGES:
        initial = [page('base', i) for i in range(n)]
        for layout in LAYOUTS:
            with tempfile.TemporaryDirectory(prefix='scanner-checkpoint-') as directory:
                print(json.dumps(run(directory, layout, initial, deltas)), flush=True)


if __name__ == '__main__':
    main()
