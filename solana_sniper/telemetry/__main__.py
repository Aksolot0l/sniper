"""CLI: python -m telemetry {run,export,status,backup,stop} --data-dir DIR

Only `run` touches the network (read-only). export/status/backup/stop are offline.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog='python -m telemetry')
    sub = parser.add_subparsers(dest='cmd', required=True)
    for name, help_text in (
        ('run', 'start the passive collector (network, read-only) — only with explicit permission'),
        ('export', 'offline: SQLite -> CSV/README/manifest (+ ZIP with --zip)'),
        ('status', 'offline: row counts and last health record'),
        ('backup', 'offline: consistent SQLite backup into DATA_DIR/backups'),
        ('stop', 'ask a running collector to stop gracefully (creates DATA_DIR/STOP)'),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument('--data-dir', default='../telemetry_data')
        if name == 'export':
            cmd.add_argument('--zip', action='store_true')
            cmd.add_argument('--no-ingest', action='store_true', help='do not ingest bot spool first')
    args = parser.parse_args(argv)

    from .store import DB_NAME, ensure_data_dir
    root = ensure_data_dir(args.data_dir)

    if args.cmd == 'run':
        from .collector import run_collector
        from .config import CollectorConfig
        return run_collector(CollectorConfig.from_env(str(root)))
    if args.cmd == 'export':
        from .export import export_bundle
        result = export_bundle(str(root), make_zip=args.zip, ingest=not args.no_ingest)
        print(json.dumps({'row_counts': result['row_counts'], 'zip': result['zip'],
                          'ingested_spool_records': result['ingested_spool_records']}, indent=2))
        return 0
    if args.cmd == 'status':
        db = root / DB_NAME
        if not db.exists():
            print('no collector.sqlite yet')
            return 0
        con = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
        from .model import KINDS
        counts = {t: con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in KINDS}
        last = con.execute('SELECT received_at_utc, health_type, status FROM health ORDER BY row_seq DESC LIMIT 1').fetchone()
        from .collector import collector_running
        print(json.dumps({'running': collector_running(root), 'row_counts': counts, 'last_health': last}, indent=2))
        return 0
    if args.cmd == 'backup':
        from .collector import collector_running
        if collector_running(root):
            print('collector is running: it makes periodic backups itself (backups/)')
            return 1
        from .store import Store
        store = Store(root)
        print(store.backup())
        store.close()
        return 0
    if args.cmd == 'stop':
        (Path(root) / 'STOP').write_text('stop requested\n', encoding='utf-8')
        print(f'stop requested: {root / "STOP"} (collector exits within ~1s and flushes)')
        return 0
    return 1


if __name__ == '__main__':
    sys.exit(main())
