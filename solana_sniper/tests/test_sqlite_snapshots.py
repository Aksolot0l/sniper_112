from __future__ import annotations

import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sqlite_snapshots import SQLiteSnapshotWorker, backup_sqlite_atomic, snapshot_run_databases


class SQLiteSnapshotTests(unittest.TestCase):
    def _make_db(self, path: Path, values=(1,)) -> None:
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS values_table (value INTEGER NOT NULL)')
            db.executemany('INSERT INTO values_table(value) VALUES (?)', [(value,) for value in values])

    def _restore_gzip_db(self, snapshot: Path, restored: Path) -> None:
        with gzip.open(snapshot, 'rb') as source, restored.open('wb') as target:
            target.write(source.read())

    def test_sqlite_backup_is_consistent_compressed_and_no_clobber(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'telemetry.sqlite'
            self._make_db(source, (1,))
            snapshot = root / 'snapshot.sqlite.gz'
            self.assertTrue(backup_sqlite_atomic(source, snapshot))
            self._make_db(source, (2,))
            restored = root / 'restored.sqlite'
            self._restore_gzip_db(snapshot, restored)
            with sqlite3.connect(restored) as db:
                values = [row[0] for row in db.execute('SELECT value FROM values_table ORDER BY value')]
            self.assertEqual(values, [1])
            with self.assertRaises(FileExistsError):
                backup_sqlite_atomic(source, snapshot)
            self.assertFalse(any('.tmp' in p.name for p in root.iterdir()))

    def test_run_snapshot_index_is_append_only_and_includes_each_present_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / 'run-unique'
            snapshot_dir = root / 'snapshots'
            run_dir.mkdir()
            self._make_db(run_dir / 'telemetry.sqlite', (1, 2))
            self._make_db(run_dir / 'sniper.db', (3,))
            record = snapshot_run_databases(
                run_dir, snapshot_dir, session_id='session-unique', episode=1,
            )
            self.assertEqual(record['run_id'], 'run-unique')
            self.assertEqual(record['episode'], 1)
            self.assertEqual(len(record['files']), 2)
            self.assertEqual(record['errors'], [])
            index_path = snapshot_dir / 'snapshot-index.jsonl'
            index = [json.loads(line) for line in index_path.read_text().splitlines()]
            self.assertEqual(len(index), 1)
            self.assertEqual(set(index[0]['files']), set(record['files']))
            self.assertTrue(all((snapshot_dir / name).is_file() for name in record['files']))

    def test_worker_always_writes_final_snapshot_on_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / 'run'
            run_dir.mkdir()
            self._make_db(run_dir / 'telemetry.sqlite', (7,))
            worker = SQLiteSnapshotWorker(
                run_dir, root / 'snapshots', session_id='session-test',
                episode=2, interval_sec=150,
            )
            worker.start()
            final_record = worker.stop_and_snapshot()
            self.assertEqual(final_record['episode'], 2)
            self.assertEqual(len(final_record['files']), 1)
            self.assertEqual(len(worker.records), 1)


if __name__ == '__main__':
    unittest.main()
