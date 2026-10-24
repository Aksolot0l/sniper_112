"""Atomic, append-only SQLite snapshots for PAPER_ONLY A/B session archives.

Snapshots are local workspace artifacts. Remote Dropbox uploads are intentionally
handled by the connected-app tool, not by a shell process holding credentials.
"""
from __future__ import annotations

import gzip
import json
import os
import secrets
import shutil
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATABASE_NAMES = ("telemetry.sqlite", "sniper.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def backup_sqlite_atomic(source: Path, destination: Path) -> bool:
    """Write a consistent SQLite backup, gzip it, and publish with os.replace."""
    source = Path(source)
    destination = Path(destination)
    if not source.is_file():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite snapshot: {destination.name}")

    nonce = secrets.token_hex(5)
    temp_db = destination.parent / f".{destination.name}.{nonce}.tmp.sqlite"
    temp_gz = destination.parent / f".{destination.name}.{nonce}.tmp.gz"
    try:
        source_uri = source.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(source_uri, uri=True, timeout=10.0)) as src:
            with closing(sqlite3.connect(temp_db, timeout=10.0)) as dst:
                src.backup(dst, pages=256, sleep=0.02)
                dst.commit()

        with temp_db.open("rb") as raw, temp_gz.open("xb") as compressed_file:
            with gzip.GzipFile(filename="", mode="wb", fileobj=compressed_file, mtime=0) as compressed:
                shutil.copyfileobj(raw, compressed, length=1024 * 1024)
            compressed_file.flush()
            os.fsync(compressed_file.fileno())

        # Publish atomically without replacing an archive created by anyone else.
        if destination.exists():
            raise FileExistsError(f"refusing to overwrite snapshot: {destination.name}")
        os.link(temp_gz, destination)
        temp_gz.unlink()
        _fsync_directory(destination.parent)
        return True
    finally:
        for temporary in (temp_db, temp_gz):
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def snapshot_run_databases(
    run_dir: Path,
    snapshot_dir: Path,
    *,
    session_id: str,
    episode: int,
) -> dict[str, Any]:
    """Back up each present SQLite DB and append one durable index record."""
    run_dir = Path(run_dir)
    snapshot_dir = Path(snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    saved: list[str] = []
    errors: list[str] = []
    for db_name in DATABASE_NAMES:
        source = run_dir / db_name
        if not source.is_file():
            continue
        dest = snapshot_dir / f"{run_dir.name}-{stamp}-{db_name}.gz"
        try:
            if backup_sqlite_atomic(source, dest):
                saved.append(dest.name)
        except Exception as exc:
            errors.append(f"{db_name}:{type(exc).__name__}")

    record = {
        "session_id": session_id,
        "episode": episode,
        "run_id": run_dir.name,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "files": saved,
        "errors": errors,
    }
    index = snapshot_dir / "snapshot-index.jsonl"
    with index.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return record


class SQLiteSnapshotWorker:
    """Take local consistent DB snapshots every interval seconds, then one final."""

    def __init__(
        self,
        run_dir: Path,
        snapshot_dir: Path,
        *,
        session_id: str,
        episode: int,
        interval_sec: float = 150.0,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.snapshot_dir = Path(snapshot_dir)
        self.session_id = session_id
        self.episode = int(episode)
        self.interval_sec = max(30.0, float(interval_sec))
        self._stop = threading.Event()
        self._snapshot_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, name=f"sqlite-snapshot-{episode}", daemon=True,
        )
        self._started = False
        self.records: list[dict[str, Any]] = []

    def start(self) -> None:
        if self._started:
            raise RuntimeError("snapshot worker already started")
        self._thread.start()
        self._started = True

    def _take_snapshot(self) -> dict[str, Any]:
        with self._snapshot_lock:
            record = snapshot_run_databases(
                self.run_dir, self.snapshot_dir,
                session_id=self.session_id, episode=self.episode,
            )
            self.records.append(record)
            return record

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            try:
                self._take_snapshot()
            except Exception as exc:
                self.records.append({"errors": [f"snapshot_worker:{type(exc).__name__}"]})

    def stop_and_snapshot(self, join_timeout: float = 20.0) -> dict[str, Any]:
        self._stop.set()
        if self._started:
            self._thread.join(timeout=max(0.1, join_timeout))
            if self._thread.is_alive():
                record: dict[str, Any] = {"errors": ["snapshot_worker:join_timeout"]}
                self.records.append(record)
                return record
            self._started = False
        try:
            return self._take_snapshot()
        except Exception as exc:
            record = {"errors": [f"snapshot_final:{type(exc).__name__}"]}
            self.records.append(record)
            return record
