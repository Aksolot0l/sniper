"""CLI: python -m telemetry {run,export,status,backup,stop} --data-dir DIR
     python -m telemetry {watch,watch-stop,watch-report} --out-dir DIR

Only `run` and `watch` touch the network (read-only). export/status/backup/stop are offline.
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
    watch = sub.add_parser('watch', help='записывать ВСЕ новые монеты Pump.fun и следить за ценой N минут (сеть, только чтение)')
    watch.add_argument('--out-dir', default='../coin_watch')
    watch.add_argument('--window-sec', type=float, default=None, help='сколько следить за монетой (по умолчанию 300)')
    watch.add_argument('--move-pct', type=float, default=None, help='порог движения цены для moves.csv (по умолчанию 20)')
    watch.add_argument('--duration-sec', type=float, default=None, help='остановиться сам через N секунд')
    watch.add_argument('--no-trades', action='store_true', help='не писать сделки (trades.txt/csv), экономия диска')
    watch.add_argument('--format', choices=('txt', 'csv'), default=None, help='txt (по умолчанию, читаемый текст) или csv')
    wstop = sub.add_parser('watch-stop', help='мягко остановить watch (создаёт OUT_DIR/STOP)')
    wstop.add_argument('--out-dir', default='../coin_watch')
    wrep = sub.add_parser('watch-report', help='офлайн-сводка по одному запуску watch')
    wrep.add_argument('run_dir', nargs='?', default=None, help='папка run_...; по умолчанию последняя')
    wrep.add_argument('--out-dir', default='../coin_watch')
    args = parser.parse_args(argv)

    if args.cmd == 'watch':
        from .watch import WatchConfig, run_watch
        cfg = WatchConfig.from_env(args.out_dir, window_sec=args.window_sec, move_pct=args.move_pct,
                                   duration_sec=args.duration_sec)
        if args.format:
            cfg.output_format = args.format
        if args.no_trades:
            cfg.record_trades = False
        return run_watch(cfg)
    if args.cmd == 'watch-stop':
        out = Path(args.out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / 'STOP').write_text('stop requested\n', encoding='utf-8')
        print(f'stop requested: {out / "STOP"} (watcher закроет окна и остановится в течение ~1 с)')
        return 0
    if args.cmd == 'watch-report':
        from .watch import report
        run_dir = args.run_dir
        if run_dir is None:
            runs = sorted(Path(args.out_dir).glob('run_*'))
            if not runs:
                print('нет запусков в', args.out_dir)
                return 1
            run_dir = runs[-1]
        if not (Path(run_dir) / 'summary.csv').exists():
            txt = Path(run_dir) / 'report.txt'
            print(txt.read_text(encoding='utf-8') if txt.exists() else f'нет report.txt в {run_dir}')
            return 0
        print(json.dumps({'run_dir': str(run_dir), **report(run_dir)}, ensure_ascii=False, indent=2))
        return 0

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
