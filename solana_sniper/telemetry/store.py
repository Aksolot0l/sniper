"""Persistent sinks.

* Store      – the single SQLite writer (collector process) + raw_events.jsonl
* SpoolSink  – append-only JSONL spool written by the *bot* process; the
               collector ingests it into SQLite (so SQLite keeps one writer).

Both are used only from one dedicated writer thread, never from bot hooks.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import schema
from .model import KINDS, iter_json_lines, utc_iso

RAW_EVENTS = 'raw_events.jsonl'
DB_NAME = 'collector.sqlite'
SPOOL_DIR = 'spool'

# Files that must never appear in (or be written to) a telemetry data dir.
FORBIDDEN_NAMES = ('wallet.json', 'sniper.db', 'trades.log')


class UnsafeDataDir(RuntimeError):
    pass


def ensure_data_dir(path: os.PathLike | str) -> Path:
    """Create/validate a dedicated telemetry directory.

    Refuses directories that look like bot run archives (``runs/``) or that
    already contain wallet/trade files, so old archives are never touched.
    """
    root = Path(path).expanduser().resolve()
    if 'runs' in root.parts:
        raise UnsafeDataDir('telemetry data dir must not live inside a runs/ archive')
    if root.exists():
        for name in FORBIDDEN_NAMES:
            if (root / name).exists():
                raise UnsafeDataDir(f'{root} contains {name}: choose a dedicated empty directory')
    root.mkdir(parents=True, exist_ok=True)
    marker = root / '.telemetry_data_dir'
    if not marker.exists():
        marker.write_text('sniper telemetry collector data directory\n', encoding='utf-8')
    return root


def _encode(table: str, record: Dict[str, Any]) -> Tuple[List[str], Tuple[Any, ...]]:
    columns = schema.table_columns(table)
    known = set(columns)
    payload = {k: v for k, v in record.items() if k not in known and k != 'kind'}
    values: List[Any] = []
    for name in columns[:-1]:
        value = record.get(name)
        if name in schema.JSON_COLUMNS and value is not None and not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
        elif isinstance(value, bool):
            value = int(value)
        elif isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
        values.append(value)
    values.append(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False) if payload else None)
    return columns, tuple(values)


class Store:
    """Single-writer SQLite store with append-only JSONL mirror."""

    def __init__(
        self,
        data_dir: os.PathLike | str,
        *,
        raw_max_bytes: int = 512 * 1024 * 1024,
        write_raw_jsonl: bool = True,
    ) -> None:
        self.root = ensure_data_dir(data_dir)
        self.db_path = self.root / DB_NAME
        self.raw_path = self.root / RAW_EVENTS
        self.raw_max_bytes = raw_max_bytes
        self.write_raw_jsonl = write_raw_jsonl
        self.db = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None,
                                  check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.execute('PRAGMA busy_timeout=30000')
        self.schema_version = schema.migrate(self.db)
        self.inserted = 0
        self.ignored_duplicate_event_ids = 0
        self._lock = threading.Lock()

    # -- writing -----------------------------------------------------------
    def append(self, records: Iterable[Dict[str, Any]],
               offsets: Optional[Dict[str, int]] = None) -> Tuple[int, int]:
        """Insert a batch atomically. Returns (inserted, ignored_same_event_id).

        Idempotent by event_id: re-delivery of the *same* event is ignored and
        counted. Distinct raw events (even for the same mint/signature) have
        distinct event_ids and are all kept.
        """
        inserted: List[Dict[str, Any]] = []
        ignored = 0
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                for record in records:
                    table = record.get('kind')
                    if table not in KINDS:
                        raise ValueError(f'record without valid kind: {table!r}')
                    columns, values = _encode(table, record)
                    cur = self.db.execute(
                        f'INSERT OR IGNORE INTO {table} ({", ".join(columns)}) '
                        f'VALUES ({", ".join("?" for _ in columns)})',
                        values,
                    )
                    if cur.rowcount == 1:
                        inserted.append(record)
                    else:
                        ignored += 1
                for spool_file, offset in (offsets or {}).items():
                    self.db.execute(
                        'INSERT INTO spool_offsets(spool_file, byte_offset, updated_at_utc) '
                        'VALUES (?, ?, ?) ON CONFLICT(spool_file) DO UPDATE SET '
                        'byte_offset=excluded.byte_offset, updated_at_utc=excluded.updated_at_utc',
                        (spool_file, offset, utc_iso()),
                    )
                self.db.execute('COMMIT')
            except BaseException:
                self.db.execute('ROLLBACK')
                raise
            self.inserted += len(inserted)
            self.ignored_duplicate_event_ids += ignored
            if inserted and self.write_raw_jsonl:
                self._append_jsonl(inserted)
        return len(inserted), ignored

    def _append_jsonl(self, records: List[Dict[str, Any]]) -> None:
        self._rotate_if_needed()
        with open(self.raw_path, 'a', encoding='utf-8') as fh:
            for record in records:
                fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + '\n')
            fh.flush()

    def _rotate_if_needed(self) -> None:
        try:
            size = self.raw_path.stat().st_size
        except FileNotFoundError:
            return
        if size < self.raw_max_bytes:
            return
        archive = self.root / 'raw_archive'
        archive.mkdir(exist_ok=True)
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        target = archive / f'raw_events-{stamp}.jsonl.gz'
        with open(self.raw_path, 'rb') as src, gzip.open(target, 'wb') as dst:
            shutil.copyfileobj(src, dst)
        self.raw_path.unlink()  # content preserved in the gzip archive above

    # -- reading -----------------------------------------------------------
    def spool_offsets(self) -> Dict[str, int]:
        rows = self.db.execute('SELECT spool_file, byte_offset FROM spool_offsets').fetchall()
        return {name: int(offset) for name, offset in rows}

    def count(self, table: str) -> int:
        return int(self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0])

    def backup(self, keep: int = 4) -> Path:
        backups = self.root / 'backups'
        backups.mkdir(exist_ok=True)
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        target = backups / f'collector-{stamp}.sqlite'
        dest = sqlite3.connect(str(target))
        with self._lock:
            self.db.backup(dest)
        dest.close()
        for old in sorted(backups.glob('collector-*.sqlite'))[:-keep]:
            old.unlink()  # only our own rotated backups, never run archives
        return target

    def close(self) -> None:
        with self._lock:
            self.db.close()


class SpoolSink:
    """Bot-side append-only JSONL spool with size rotation (no SQLite in bot)."""

    def __init__(self, data_dir: os.PathLike | str, *, max_bytes: int = 64 * 1024 * 1024,
                 prefix: str = 'bot') -> None:
        self.root = ensure_data_dir(data_dir)
        self.dir = self.root / SPOOL_DIR
        self.dir.mkdir(exist_ok=True)
        self.max_bytes = max_bytes
        self.prefix = prefix
        self._fh = None
        self._path: Optional[Path] = None
        self._last_fsync = 0.0

    def _open_new(self) -> None:
        if self._fh:
            self._fh.close()
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        self._path = self.dir / f'{self.prefix}-{stamp}-{os.getpid()}-{int(time.time()*1000)%1000:03d}.jsonl'
        self._fh = open(self._path, 'a', encoding='utf-8')

    def append(self, records: Iterable[Dict[str, Any]], offsets: Any = None) -> Tuple[int, int]:
        if self._fh is None or (self._path and self._path.stat().st_size >= self.max_bytes):
            self._open_new()
        count = 0
        for record in records:
            self._fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + '\n')
            count += 1
        self._fh.flush()
        if time.time() - self._last_fsync > 2.0:
            os.fsync(self._fh.fileno())
            self._last_fsync = time.time()
        return count, 0

    def close(self) -> None:
        if self._fh:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None


def read_spool_chunk(spool_dir: Path, offsets: Dict[str, int],
                     max_records: int = 2000) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Read complete (newline-terminated) records past stored offsets."""
    records: List[Dict[str, Any]] = []
    new_offsets: Dict[str, int] = {}
    if not spool_dir.exists():
        return records, new_offsets
    for path in sorted(spool_dir.glob('*.jsonl')):
        start = offsets.get(path.name, 0)
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            continue
        if size <= start:
            continue
        with open(path, 'rb') as fh:
            fh.seek(start)
            chunk = fh.read(min(size - start, 8 * 1024 * 1024))
        end = chunk.rfind(b'\n')
        if end < 0:
            continue  # a writer is mid-line; retry later
        pos = start
        for raw in chunk[:end + 1].split(b'\n')[:-1]:
            pos += len(raw) + 1
            for item in iter_json_lines([raw.decode('utf-8', errors='replace')]):
                records.append(item)
            if len(records) >= max_records:
                break
        new_offsets[path.name] = pos
        if len(records) >= max_records:
            break
    return records, new_offsets
