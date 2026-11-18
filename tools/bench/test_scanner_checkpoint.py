import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import scanner_checkpoint as bench


class CheckpointTests(unittest.TestCase):
    def test_unpublished_writes_do_not_advance_recovery(self):
        for layout in bench.LAYOUTS:
            with self.subTest(layout=layout), tempfile.TemporaryDirectory() as directory:
                files = bench.Files(directory)
                initial = [bench.page('base', i) for i in range(bench.FANOUT)]
                root = bench.seed(files, layout, initial)
                full = list(initial)
                expected = list(initial)
                for i in range(bench.MAX_SEGMENTS):
                    p = bench.page('delta', i)
                    bench.append(files, root, p, full)
                    expected.append(p)
                # Failure after writing the new snapshot/compaction/path but before replacing the root.
                with patch.object(Path, 'replace', side_effect=OSError('root unavailable')):
                    with self.assertRaisesRegex(OSError, 'root unavailable'):
                        bench.append(files, root, bench.page('delta', bench.MAX_SEGMENTS), full)
                visible, pages = bench.materialize(bench.Files(directory))
                self.assertEqual((visible['revision'], pages), (bench.MAX_SEGMENTS, expected))
                if layout == 'segments':
                    self.assertEqual(len(visible['segments']), bench.MAX_SEGMENTS)
                    self.assertGreater(files.writes['compaction'], 0)

    def test_cold_reader_rejects_damaged_reachable_data(self):
        for layout in bench.LAYOUTS:
            with self.subTest(layout=layout), tempfile.TemporaryDirectory() as directory:
                files = bench.Files(directory)
                initial = [bench.page('base', i) for i in range(2)]
                root = bench.seed(files, layout, initial)
                bench.append(files, root, bench.page('delta', 0), initial.copy())
                if layout == 'segments':
                    ref = root['segments'][0]
                else:
                    ref = root['tree' if layout == 'paged' else 'base']
                (Path(directory) / ref).write_bytes(b'[]')  # Valid JSON, wrong content.
                result = subprocess.run([sys.executable, bench.__file__, '--recover', directory],
                                        capture_output=True, timeout=10, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('blob digest mismatch', result.stderr)

    def test_cold_reader_rejects_a_missing_committed_page(self):
        with tempfile.TemporaryDirectory() as directory:
            files = bench.Files(directory)
            root = bench.seed(files, 'paged', [bench.page('base', i) for i in range(2)])
            refs = json.loads((Path(directory) / root['tree']).read_bytes())
            (Path(directory) / refs[-1]).unlink()
            with self.assertRaises(FileNotFoundError):
                bench.cold_worker(directory)

    def test_writer_cannot_change_input_ledger_to_match_its_output(self):
        append = bench.append

        def lose_version(files, root, p, full):
            p[0][1] -= 1
            append(files, root, p, full)

        with tempfile.TemporaryDirectory() as directory, patch.object(bench, 'append', lose_version):
            with self.assertRaisesRegex(ValueError, 'input ledger mismatch at revision 1'):
                bench.run(directory, 'full', [bench.page('base', 0)], [bench.page('delta', 0)])


if __name__ == '__main__':
    unittest.main()
