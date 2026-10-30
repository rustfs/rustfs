import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

import scanner_client_trace as trace
import scanner_abba


class LedgerTests(unittest.TestCase):
    def setUp(self):
        disk = mock.patch.object(trace.shutil, 'disk_usage', return_value=mock.Mock(free=100 * 1024**3))
        disk.start()
        self.addCleanup(disk.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve(); self.root.chmod(0o700)
        self.clock = str(uuid.uuid4())
        self.identity = trace.clock_identity()
        self.start, self.dispatch_end, self.end = 1_000_000_000, 2_000_000_000, 2_100_000_000
        self.request = {'duration_seconds':1, 'fixed':{'offered_load_ops':100}}
        self.schedule, self.starts, self.samples = [], [], []
        for index in range(100):
            due = self.start + index * 10_000_000
            began = due + 2
            self.schedule.append({'request_id':str(index),'clock_id':self.clock,'scheduled_ns':due,'observed_ns':due+1,
                                  'dispatched':True,'miss_reason':None})
            self.starts.append({'request_id':str(index),'clock_id':self.clock,'scheduled_ns':due,'start_ns':began,'operation':'GET'})
            self.samples.append({'request_id':str(index),'clock_id':self.clock,'start_ns':began,'end_ns':began+index+1,
                                 'outcome':'success','operation':'GET','error_type':None,'http_status':200})
        self.stats = {'planned_requests':100, 'started':100, 'completed':100, 'errors':0, 'missed_dispatches':0}
        self.cancelled = False

    def tearDown(self): self.tmp.cleanup()

    def save(self):
        refs = {}
        for name, rows in (('samples',self.samples),('starts',self.starts),('schedule',self.schedule)):
            path = self.root/(name+'.ndjson')
            path.write_text(''.join(json.dumps(row,separators=(',',':'))+'\n' for row in rows))
            raw = path.read_bytes()
            refs[name] = {'path':path.name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
        return {'schema':2, **refs['samples'],'clock_id':self.clock,
                'clock_before':copy.deepcopy(self.identity),'clock_after':copy.deepcopy(self.identity),
                'window_start_ns':self.start,'dispatch_window_end_ns':self.dispatch_end,'window_end_ns':self.end,
                'starts':refs['starts'],'schedule':refs['schedule']}

    def compute(self):
        return trace.analyze(self.root, self.save(), self.request, self.stats, self.cancelled)

    def test_nearest_rank_includes_failure_and_exact_error_ratio(self):
        self.samples[0].update(outcome='error',error_type='ClientError',http_status=503)
        self.stats['errors'] = 1
        result = self.compute()
        self.assertTrue(result['complete'])
        self.assertEqual((result['p95_ns'],result['p99_ns']), (95,99))
        self.assertEqual(result['error_rate'], {'numerator':1,'denominator':100})
        self.assertEqual(result['http_503'], 1)
        self.assertEqual(result['observed_http_503_lower_bound'], 1)
        self.assertFalse(result['same_source_window_proven'])
        self.assertFalse(result['release_acceptance_ready'])
        self.assertEqual(result['dispatch_window_end_ns'], self.dispatch_end)

    def test_status_unavailable_is_not_zero_and_known_503_not_hidden(self):
        self.samples[0].update(outcome='error',error_type='ClientError',http_status=503)
        self.samples[1]['http_status'] = None
        self.stats['errors'] = 1
        result = self.compute()
        self.assertIsNone(result['http_503'])
        self.assertEqual(result['http_status_unknown'], 1)
        self.assertEqual(result['observed_http_503_lower_bound'], 1)
        self.assertEqual(result['error_rate'], {'numerator':1,'denominator':100})

    def test_low_samples_and_missed_dispatch_do_not_produce_percentiles(self):
        self.request['fixed']['offered_load_ops'] = 99
        self.schedule.pop(); self.starts.pop(); self.samples.pop()
        self.stats.update(planned_requests=99, started=99, completed=99)
        from decimal import Decimal
        for index,(sch,st,fin) in enumerate(zip(self.schedule,self.starts,self.samples)):
            due=self.start + int(Decimal(index) * 1_000_000_000 / Decimal(99))
            sch.update(scheduled_ns=due,observed_ns=due+1)
            st.update(scheduled_ns=due,start_ns=due+2)
            fin.update(start_ns=due+2,end_ns=due+3+index)
        low = self.compute()
        self.assertTrue(low['complete']); self.assertIsNone(low['p99_ns']); self.assertIsNone(low['p95_ns'])
        self.request['fixed']['offered_load_ops'] = 100
        for index,(sch,st,fin) in enumerate(zip(self.schedule,self.starts,self.samples)):
            due=self.start+index*10_000_000; sch.update(scheduled_ns=due,observed_ns=due+1)
            st.update(scheduled_ns=due,start_ns=due+2); fin.update(start_ns=due+2,end_ns=due+3+index)
        due=self.start+990_000_000
        self.schedule.append({'request_id':'99','clock_id':self.clock,'scheduled_ns':due,'observed_ns':due+1,
                              'dispatched':False,'miss_reason':'client_capacity'})
        self.stats.update(planned_requests=100, started=99, completed=99, missed_dispatches=1)
        missing = self.compute()
        self.assertFalse(missing['complete']); self.assertIsNone(missing['p99_ns']); self.assertIsNone(missing['error_rate'])

    def test_explicit_cancelled_full_or_partial_does_not_produce_percentile(self):
        self.cancelled = True
        result = self.compute()
        self.assertFalse(result['complete']); self.assertIsNone(result['p99_ns'])
        self.schedule = self.schedule[:8]; self.starts = self.starts[:8]; self.samples = self.samples[:8]
        self.stats.update(started=8,completed=8)
        result = self.compute()
        self.assertFalse(result['complete']); self.assertIsNone(result['requests'])

    def test_missed_dispatch_above_p99_sample_threshold_stays_unavailable(self):
        self.request['fixed']['offered_load_ops'] = 200
        self.schedule, self.starts, self.samples = [], [], []
        for index in range(200):
            due=self.start+index*5_000_000; dispatched=index != 199
            self.schedule.append({'request_id':str(index),'clock_id':self.clock,'scheduled_ns':due,'observed_ns':due+1,
                                  'dispatched':dispatched,'miss_reason':None if dispatched else 'client_capacity'})
            if dispatched:
                self.starts.append({'request_id':str(index),'clock_id':self.clock,'scheduled_ns':due,'start_ns':due+2,'operation':'GET'})
                self.samples.append({'request_id':str(index),'clock_id':self.clock,'start_ns':due+2,'end_ns':due+3+index,
                                     'outcome':'success','operation':'GET','error_type':None,'http_status':200})
        self.stats.update(planned_requests=200,started=199,completed=199,missed_dispatches=1)
        result=self.compute()
        self.assertFalse(result['complete']); self.assertEqual(result['observed_completed'],199)
        self.assertIsNone(result['p95_ns']); self.assertIsNone(result['p99_ns']); self.assertIsNone(result['error_rate'])

    def test_no_ledger_truncation_or_missing_completion_accepted(self):
        ref=self.save(); path=self.root/ref['path']; raw=path.read_bytes()[:-2]; path.write_bytes(raw)
        ref.update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        with self.assertRaises(ValueError): trace.analyze(self.root,ref,self.request,self.stats,False)
        self.samples.pop(); self.stats.update(started=99,completed=99)
        with self.assertRaises(ValueError): self.compute()

    def test_duplicate_mismatch_clock_and_cross_window_rejected(self):
        changes = ('duplicate-start','missing-schedule','clock','cross-window','start-mismatch','duplicate-json','invalid-status','float-schedule')
        for change in changes:
            with self.subTest(change=change):
                schedules,starts,samples=copy.deepcopy(self.schedule),copy.deepcopy(self.starts),copy.deepcopy(self.samples)
                try:
                    if change=='duplicate-start': self.starts.append(copy.deepcopy(self.starts[0]))
                    elif change=='missing-schedule': self.schedule.pop()
                    elif change=='clock': self.samples[0]['clock_id']=str(uuid.uuid4())
                    elif change=='cross-window': self.samples[0]['end_ns']=self.end+1
                    elif change=='start-mismatch': self.samples[0]['start_ns']+=1
                    elif change=='invalid-status': self.samples[0]['http_status']=True
                    elif change=='float-schedule': self.schedule[0]['scheduled_ns']=float(self.schedule[0]['scheduled_ns'])
                    if change=='duplicate-json':
                        ref=self.save(); p=self.root/ref['path']; raw=p.read_bytes().replace(b'"outcome":"success"', b'"outcome":"success","outcome":"error"', 1)
                        p.write_bytes(raw); ref.update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
                        with self.assertRaises(ValueError): trace.analyze(self.root,ref,self.request,self.stats,False)
                    else:
                        with self.assertRaises(ValueError): self.compute()
                finally:
                    self.schedule,self.starts,self.samples=schedules,starts,samples

    def test_scheduled_window_crossing_is_disclosed_not_retimestamped(self):
        self.samples[-1]['end_ns'] = self.dispatch_end + 5
        result = self.compute()
        self.assertEqual(result['crossing_dispatch_end'],1)
        self.assertEqual(result['window_end_ns'],self.end)
        self.assertEqual(result['dispatch_window_end_ns'],self.dispatch_end)
        self.assertFalse(result['same_source_window_proven'])

    def test_dispatch_after_its_slot_is_not_reported_as_on_time(self):
        self.schedule[0]['observed_ns'] = self.start + 10_000_000
        self.starts[0]['start_ns'] = self.schedule[0]['observed_ns'] + 1
        self.samples[0].update(start_ns=self.starts[0]['start_ns'], end_ns=self.starts[0]['start_ns'] + 1)
        with self.assertRaisesRegex(ValueError, 'late dispatch'):
            self.compute()

    def test_late_recorded_start_cannot_count_as_a_complete_offered_window(self):
        due = self.schedule[0]['scheduled_ns']
        self.starts[0]['start_ns'] = due + 10_000_000
        self.samples[0].update(start_ns=self.starts[0]['start_ns'], end_ns=self.starts[0]['start_ns'] + 1)
        result = self.compute()
        self.assertTrue(result['sample_complete'])
        self.assertEqual(result['late_request_starts'], 1)
        self.assertFalse(result['scheduler_window_complete'])
        self.assertIs(result['offered_load_complete'], False)

    def test_refuses_ledger_through_symlinked_parent(self):
        ref = self.save()
        with tempfile.TemporaryDirectory() as outside:
            other = Path(outside)
            (other / 'sample.ndjson').write_bytes((self.root / ref['path']).read_bytes())
            (self.root / 'alias').symlink_to(other, target_is_directory=True)
            ref['path'] = 'alias/sample.ndjson'
            with self.assertRaises((ValueError, OSError)):
                trace.analyze(self.root, ref, self.request, self.stats, False)

    def test_parent_replacement_cannot_redirect_open_ledger(self):
        ref = self.save()
        nested = self.root / 'client'
        nested.mkdir(mode=0o700)
        for name in ('samples.ndjson', 'starts.ndjson', 'schedule.ndjson'):
            (self.root / name).rename(nested / name)
        ref['path'] = 'client/' + ref['path']
        for key in ('starts', 'schedule'):
            ref[key]['path'] = 'client/' + ref[key]['path']
        with tempfile.TemporaryDirectory() as outside:
            outside_path = Path(outside)
            for name in ('samples.ndjson', 'starts.ndjson', 'schedule.ndjson'):
                (outside_path / name).write_bytes(b'wrong ledger\n')
            original_open = trace.os.open
            swapped = False

            def open_with_replacement(path, flags, *args, **kwargs):
                nonlocal swapped
                if Path(path).name == 'schedule.ndjson' and not swapped:
                    swapped = True
                    nested.rename(self.root / 'held-client')
                    nested.symlink_to(outside_path, target_is_directory=True)
                return original_open(path, flags, *args, **kwargs)

            with mock.patch.object(trace.os, 'open', side_effect=open_with_replacement):
                result = trace.analyze(self.root, ref, self.request, self.stats, False)
            self.assertTrue(swapped)
            self.assertEqual(result['p99_ns'], 99)

    def test_refuses_reduction_without_scratch_budget(self):
        with mock.patch.object(trace.shutil, 'disk_usage', return_value=mock.Mock(free=1)):
            with self.assertRaisesRegex(ValueError, 'scratch'):
                self.compute()

    def test_refuses_nonsticky_shared_scratch_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            shared = Path(tmp) / 'shared'
            shared.mkdir(mode=0o777)
            shared.chmod(0o777)
            with mock.patch.object(trace.tempfile, 'gettempdir', return_value=str(shared)):
                with self.assertRaisesRegex(ValueError, 'scratch parent'):
                    self.compute()

    def test_refuses_sticky_parent_owned_by_another_user(self):
        with tempfile.TemporaryDirectory() as tmp:
            shared = Path(tmp) / 'shared'
            shared.mkdir(mode=0o700)
            shared.chmod(0o1777)
            shared = shared.resolve()
            original_stat = Path.stat

            def foreign_owner(path, *args, **kwargs):
                info = original_stat(path, *args, **kwargs)
                if path == shared:
                    return mock.Mock(st_mode=info.st_mode, st_uid=os.geteuid() + 1)
                return info

            with mock.patch.object(trace.tempfile, 'gettempdir', return_value=str(shared)), \
                 mock.patch.object(Path, 'stat', autospec=True, side_effect=foreign_owner):
                with self.assertRaisesRegex(ValueError, 'scratch parent'):
                    self.compute()

    def test_abba_does_not_accept_ledger_without_proven_offered_load(self):
        ref = self.save()
        observed = trace.analyze(self.root, ref, self.request, self.stats, False)
        result = {'client_request_samples': ref, 'client_observation': observed,
                  'stats': self.stats, 'cancelled': False,
                  'elapsed_seconds': observed['elapsed_seconds'],
                  'metrics': {key: observed[key] for key in ('requests', 'errors', 'p95_ms', 'p99_ms', 'throughput_ops')}}
        with self.assertRaisesRegex(ValueError, 'client schedule or percentile evidence is incomplete'):
            scanner_abba.validate_client_request_samples(result, self.request, self.root)

    def test_abba_does_not_accept_missing_or_tampered_ledger(self):
        with self.assertRaisesRegex(ValueError, 'missing client request samples'):
            scanner_abba.validate_client_request_samples({}, self.request, self.root)
        ref = self.save()
        (self.root / ref['path']).write_bytes(b'tampered\n')
        with self.assertRaises(ValueError):
            scanner_abba.validate_client_request_samples(
                {'client_request_samples': ref, 'stats': self.stats, 'cancelled': False}, self.request, self.root)

    def test_abba_does_not_turn_unknown_status_into_zero_503s(self):
        ref = self.save()
        observed = trace.analyze(self.root, ref, self.request, self.stats, False)
        observed.update(offered_load_complete=True, http_503=None)
        result = {'client_request_samples': ref, 'client_observation': observed,
                  'stats': self.stats, 'cancelled': False}
        with mock.patch.object(scanner_abba, 'reconcile_client_ledger', return_value=observed):
            with self.assertRaisesRegex(ValueError, 'status unavailable'):
                scanner_abba.validate_client_request_samples(result, self.request, self.root)


if __name__ == '__main__':
    unittest.main()
