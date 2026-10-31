"""Reconcile complete client ledgers and calculate exact nearest-rank samples."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import sys
import tempfile
import time
import uuid
from contextlib import ExitStack, closing
from decimal import Decimal, ROUND_CEILING

SCRATCH_FREE_FLOOR = 10 * 1024**3


def require(value, message):
    if not value:
        raise ValueError(message)


def clock_identity():
    info = time.get_clock_info('monotonic')
    require(info.monotonic is True and info.adjustable is False and math.isfinite(info.resolution)
            and info.resolution > 0, 'client monotonic clock unavailable')
    result = {'pid':os.getpid(), 'platform':sys.platform, 'implementation':info.implementation,
              'resolution_ns':max(1, math.ceil(info.resolution * 1_000_000_000)),
              'monotonic':True, 'adjustable':False, 'boot_id_sha256':None, 'process_start_ticks':None}
    if sys.platform == 'linux':
        boot = Path('/proc/sys/kernel/random/boot_id').read_bytes().strip()
        require(0 < len(boot) <= 128, 'client boot identity unavailable')
        raw = Path('/proc/self/stat').read_text()
        end = raw.rfind(')')
        require(end >= 0 and raw.split(' ', 1)[0] == str(result['pid']), 'invalid client process identity')
        fields = raw[end + 1:].split()
        require(len(fields) >= 20 and fields[19].isdigit() and int(fields[19]) > 0, 'invalid client process start')
        result.update(boot_id_sha256=hashlib.sha256(boot).hexdigest(), process_start_ticks=int(fields[19]))
    return result


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate JSON key in client ledger')
        value[key] = item
    return value


def source_file(root_fd, ref):
    require(type(ref) is dict and set(ref) == {'path', 'sha256', 'bytes'}, 'invalid client ledger reference')
    relative = ref['path']
    require(isinstance(relative, str) and relative and not Path(relative).is_absolute()
            and '..' not in Path(relative).parts and str(Path(relative)) == relative, 'invalid client ledger path')
    require(isinstance(ref['sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', ref['sha256'])
            and type(ref['bytes']) is int and 0 <= ref['bytes'] <= 2**63-1, 'invalid client ledger identity')
    parts = Path(relative).parts
    require(parts and all(part not in ('.', '..') for part in parts), 'invalid client ledger path')
    root = os.fstat(root_fd)
    directory = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
            current = os.fstat(directory)
            require(current.st_dev == root.st_dev and current.st_uid == os.geteuid()
                    and current.st_mode & 0o077 == 0, 'client ledger directory is not private and owned')
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            current = os.fstat(fd)
            require(stat.S_ISREG(current.st_mode) and current.st_dev == root.st_dev
                    and current.st_size == ref['bytes'], 'client ledger identity or size differs')
        except BaseException:
            os.close(fd)
            raise
        return fd
    finally:
        os.close(directory)


def records(fd, reference):
    with os.fdopen(os.dup(fd), 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_size == reference['bytes'], 'client ledger size differs')
        digest = hashlib.sha256()
        while line := stream.readline(4097):
            require(len(line) <= 4096 and line.endswith(b'\n'), 'truncated or oversized client record')
            digest.update(line)
            try:
                row = json.loads(line, object_pairs_hook=unique_object)
            except (UnicodeError, json.JSONDecodeError):
                raise ValueError('malformed client record') from None
            require(type(row) is dict, 'invalid client record')
            yield row
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                and digest.hexdigest() == reference['sha256'], 'client ledger changed or digest differs')


def request_id(row, planned):
    value = row.get('request_id')
    require(isinstance(value, str) and re.fullmatch(r'0|[1-9][0-9]{0,18}', value), 'invalid client request id')
    index = int(value)
    require(index < planned, 'client request outside plan')
    return index


def scratch_parent():
    parent = Path(tempfile.gettempdir()).resolve(strict=True)
    for directory in (parent, *parent.parents):
        info = directory.stat()
        shared = bool(info.st_mode & 0o022)
        sticky = bool(info.st_mode & stat.S_ISVTX)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid in (0, os.geteuid())
                and (not shared or sticky), 'untrusted client scratch parent')
    return parent


def identity(reference):
    clock_id = reference.get('clock_id')
    try:
        require(isinstance(clock_id, str) and str(uuid.UUID(clock_id)) == clock_id, 'invalid client clock id')
    except (ValueError, AttributeError):
        raise ValueError('invalid client clock id') from None
    first, last = reference.get('clock_before'), reference.get('clock_after')
    keys = {'pid','platform','implementation','resolution_ns','monotonic','adjustable','boot_id_sha256','process_start_ticks'}
    require(type(first) is dict and first == last and set(first) == keys
            and type(first['pid']) is int and first['pid'] > 0
            and isinstance(first['platform'], str) and 0 < len(first['platform']) <= 64
            and isinstance(first['implementation'], str) and 0 < len(first['implementation']) <= 256
            and type(first['resolution_ns']) is int and first['resolution_ns'] > 0
            and first['monotonic'] is True and first['adjustable'] is False, 'client clock changed or unavailable')
    if first['platform'] == 'linux':
        require(isinstance(first['boot_id_sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', first['boot_id_sha256'])
                and type(first['process_start_ticks']) is int and first['process_start_ticks'] > 0,
                'client process identity unavailable')
    else:
        require(first['boot_id_sha256'] is None and first['process_start_ticks'] is None,
                'unverified client process identity')
    return clock_id, first


def analyze(root, reference, request, stats, cancelled):
    """Return original-window observations. Partial input never produces release metrics.

    Each planned index must have one schedule record and each dispatch one
    matching start and completion. Percentiles include completed successes and
    failures and use nearest rank, ceil(p*n/100), in integer nanoseconds. The
    measured window includes the real post-dispatch drain; it is not aligned
    to any server or another client. Error-rate numerator/denominator are exact.
    """
    root_fd = os.open(Path(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        root_stat = os.fstat(root_fd)
        require(root_stat.st_uid == os.geteuid() and root_stat.st_mode & 0o077 == 0,
                'client evidence root is not private and owned')
        return _analyze_open_root(root_fd, reference, request, stats, cancelled)
    finally:
        os.close(root_fd)


def _analyze_open_root(root_fd, reference, request, stats, cancelled):
    require(type(reference) is dict and reference.get('schema') == 2, 'invalid client evidence schema')
    clock, clock_info = identity(reference)
    start, dispatch_end, end = (reference.get(key) for key in ('window_start_ns','dispatch_window_end_ns','window_end_ns'))
    require(all(type(v) is int and 0 <= v < 2**63 for v in (start, dispatch_end, end))
            and start < dispatch_end and start < end, 'invalid client window')
    duration = request.get('duration_seconds')
    fixed = request.get('fixed')
    require(type(duration) is int and duration > 0 and type(fixed) is dict, 'invalid client request duration')
    rate_value = fixed.get('offered_load_ops')
    require(type(rate_value) in (int,float) and math.isfinite(rate_value) and rate_value > 0, 'invalid client offered rate')
    rate = Decimal(str(rate_value))
    planned = int((rate * Decimal(duration)).to_integral_value(rounding=ROUND_CEILING))
    require(0 < planned < 2**63 and dispatch_end == start + duration * 1_000_000_000,
            'planned client window differs')
    require(type(cancelled) is bool and (cancelled or end >= dispatch_end), 'client window stopped early')
    require(type(stats) is dict and all(type(stats.get(k)) is int and stats[k] >= 0 for k in
            ('planned_requests','started','completed','errors','missed_dispatches'))
            and stats['planned_requests'] == planned, 'client counters unavailable')
    sample = {'path':reference.get('path'), 'sha256':reference.get('sha256'), 'bytes':reference.get('bytes')}
    refs = {'samples':sample, 'starts':reference.get('starts'), 'schedule':reference.get('schedule')}
    with ExitStack() as stack:
        paths = {}
        for key, value in refs.items():
            paths[key] = source_file(root_fd, value)
            stack.callback(os.close, paths[key])
        identities = {(os.fstat(fd).st_dev, os.fstat(fd).st_ino) for fd in paths.values()}
        require(len(identities) == 3, 'client ledgers alias')
        return _reduce_ledgers(paths, refs, request, stats, cancelled, clock, clock_info,
                               start, dispatch_end, end, planned, rate)


def _reduce_ledgers(paths, refs, request, stats, cancelled, clock, clock_info, start, dispatch_end, end, planned, rate):
    period = max(1, int(Decimal(1_000_000_000) / rate))
    with tempfile.TemporaryDirectory(prefix='scanner-client-reduce-', dir=scratch_parent()) as scratch:
        scratch_budget = 64 * 1024**2 + planned * 512
        require(shutil.disk_usage(scratch).free >= SCRATCH_FREE_FLOOR + scratch_budget,
                'insufficient client reduction scratch space')

        def check_space(index):
            if index % 4096 == 0:
                require(shutil.disk_usage(scratch).free >= SCRATCH_FREE_FLOOR,
                        'insufficient client reduction scratch space')

        with closing(sqlite3.connect(str(Path(scratch)/'ledger.sqlite'))) as db:
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            db.execute(f'PRAGMA max_page_count={scratch_budget // page_size}')
            db.execute('CREATE TABLE requests (id INTEGER PRIMARY KEY, scheduled INTEGER NOT NULL, observed INTEGER NOT NULL, '
                       'dispatched INTEGER NOT NULL, reason TEXT, began INTEGER, finished INTEGER, op TEXT, outcome TEXT, status INTEGER)')
            db.execute('CREATE INDEX by_latency ON requests((finished-began), id)')
            scheduled_count = misses = 0
            for index, row in enumerate(records(paths['schedule'], refs['schedule'])):
                check_space(index)
                require(set(row) == {'request_id','clock_id','scheduled_ns','observed_ns','dispatched','miss_reason'}
                        and row['clock_id'] == clock, 'invalid schedule record')
                index = request_id(row, planned)
                expect = start + int(Decimal(index) * 1_000_000_000 / rate)
                require(index == scheduled_count and type(row['scheduled_ns']) is int
                        and row['scheduled_ns'] == expect and expect < dispatch_end
                        and type(row['observed_ns']) is int and expect <= row['observed_ns'] <= end
                        and type(row['dispatched']) is bool, 'invalid schedule order or timestamp')
                reason = row['miss_reason']
                require((row['dispatched'] and reason is None) or
                        (not row['dispatched'] and reason in ('late_slot','client_capacity','after_dispatch_window')), 'invalid dispatch outcome')
                require(not row['dispatched'] or row['observed_ns'] < min(expect + period, dispatch_end),
                        'late dispatch marked on time')
                require(reason != 'late_slot' or row['observed_ns'] >= expect + period, 'inconsistent late dispatch')
                require(reason != 'after_dispatch_window' or row['observed_ns'] >= dispatch_end, 'inconsistent dispatch boundary')
                db.execute('INSERT INTO requests(id,scheduled,observed,dispatched,reason) VALUES (?,?,?,?,?)',
                           (index, expect, row['observed_ns'], int(row['dispatched']), reason))
                scheduled_count += 1; misses += not row['dispatched']
            require(cancelled or scheduled_count == planned, 'scheduled client records missing')
            for index, row in enumerate(records(paths['starts'], refs['starts'])):
                check_space(index)
                require(set(row) == {'request_id','clock_id','start_ns','scheduled_ns','operation'}
                        and row['clock_id'] == clock and row['operation'] in ('GET','PUT')
                        and type(row['start_ns']) is int and start <= row['start_ns'] <= end
                        and type(row['scheduled_ns']) is int, 'invalid client start')
                changed = db.execute('UPDATE requests SET began=?, op=? WHERE id=? AND dispatched=1 AND began IS NULL '
                                     'AND scheduled=? AND observed<=?',
                                     (row['start_ns'], row['operation'], request_id(row, planned), row['scheduled_ns'], row['start_ns'])).rowcount
                require(changed == 1, 'missing, duplicate or unmatched client start')
            for index, row in enumerate(records(paths['samples'], refs['samples'])):
                check_space(index)
                require(set(row) == {'request_id','clock_id','start_ns','end_ns','outcome','operation','error_type','http_status'}
                        and row['clock_id'] == clock and row['operation'] in ('GET','PUT')
                        and type(row['start_ns']) is int and type(row['end_ns']) is int
                        and start <= row['start_ns'] <= row['end_ns'] <= end
                        and row['outcome'] in ('success','error','timeout','cancelled'), 'invalid client completion or cross-window sample')
                error_type, status = row['error_type'], row['http_status']
                require((row['outcome'] == 'success' and error_type is None) or
                        (row['outcome'] != 'success' and isinstance(error_type,str) and 0 < len(error_type) <= 256),
                        'invalid client outcome identity')
                require(status is None or (type(status) is int and 100 <= status <= 599), 'invalid HTTP status')
                require(not (row['outcome'] == 'success' and status is not None and status >= 400), 'HTTP failure marked successful')
                changed = db.execute('UPDATE requests SET finished=?, outcome=?, status=? WHERE id=? AND began=? '
                                     'AND op=? AND finished IS NULL',
                                     (row['end_ns'], row['outcome'], status, request_id(row, planned), row['start_ns'], row['operation'])).rowcount
                require(changed == 1, 'missing, duplicate or unmatched client completion')
            counts = db.execute('SELECT COUNT(*), COUNT(began), COUNT(finished), '
                                "COUNT(CASE WHEN outcome IS NOT NULL AND outcome != 'success' THEN 1 END), "
                                'COUNT(status), COUNT(CASE WHEN status=503 THEN 1 END), '
                                'COUNT(CASE WHEN began< ? AND finished> ? THEN 1 END), '
                                'COUNT(CASE WHEN began>= ? THEN 1 END), '
                                'COUNT(CASE WHEN dispatched=1 AND observed>= ? THEN 1 END), '
                                'COUNT(CASE WHEN began-scheduled>= ? THEN 1 END) FROM requests',
                                (dispatch_end, dispatch_end, dispatch_end, dispatch_end, period)).fetchone()
            rows, started, completed, errors, status_known, known_503, crossing, after_dispatch, late_dispatch, late_starts = counts
            dispatched = db.execute('SELECT COUNT(*) FROM requests WHERE dispatched=1').fetchone()[0]
            require(rows == scheduled_count and started == completed == dispatched
                    and stats['started'] == started and stats['completed'] == completed
                    and stats['errors'] == errors and stats['missed_dispatches'] == misses,
                    'client schedule/start/completion/counter mismatch')
            complete = not cancelled and scheduled_count == planned and misses == 0 and completed == planned
            scheduler_window_complete = complete and late_dispatch == 0 and after_dispatch == 0 and late_starts == 0
            # Worker start precedes the locked start-ledger flush. It does not
            # establish when the SDK was called or whether traffic was sent.
            offered_load_complete = None if scheduler_window_complete else False
            result = {'complete':complete, 'sample_complete':complete, 'offered_load_complete':offered_load_complete,
                      'scheduler_window_complete':scheduler_window_complete,
                      'planned_requests':planned, 'scheduled_requests':scheduled_count,
                      'missed_dispatches':misses, 'observed_completed':completed,
                      'observed_errors':errors if completed else None,
                      'observed_http_503_lower_bound':known_503 if known_503 else None,
                      'http_status_known':status_known, 'http_status_unknown':completed-status_known,
                      'http_503':known_503 if complete and completed > 0 and status_known == completed else None,
                      'requests':completed if complete else None, 'errors':errors if complete else None,
                      'error_rate':{'numerator':errors, 'denominator':completed} if complete and completed else None,
                      'p95_ns':None, 'p99_ns':None, 'p95_ms':None, 'p99_ms':None,
                      'throughput_ops':None, 'sample_throughput_ops':None,
                      'quantile_method':'nearest-rank over all completed requests, including errors',
                      'clock_id':clock, 'client_clock':clock_info,
                      'window_start_ns':start, 'dispatch_window_end_ns':dispatch_end, 'window_end_ns':end,
                      'crossing_dispatch_end':crossing, 'started_after_dispatch_end':after_dispatch,
                      'dispatched_after_dispatch_end':late_dispatch, 'late_request_starts':late_starts,
                      'elapsed_seconds':(end-start)/1_000_000_000, 'same_source_window_proven':False,
                      'release_acceptance_ready':False}
            if complete and completed >= 100:
                check_space(0)
                for percentile in (95,99):
                    rank = (completed * percentile + 99) // 100
                    latency = db.execute('SELECT finished-began FROM requests ORDER BY (finished-began), id LIMIT 1 OFFSET ?', (rank-1,)).fetchone()[0]
                    result['p'+str(percentile)+'_ns'] = latency
                    result['p'+str(percentile)+'_ms'] = latency / 1_000_000
            if complete:
                result['sample_throughput_ops'] = (completed-errors) * 1_000_000_000 / (end-start)
                if offered_load_complete is True:
                    result['throughput_ops'] = result['sample_throughput_ops']
            return result
