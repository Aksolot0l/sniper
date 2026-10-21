"""Versioned SQLite schema and forward-only migrations.

All observation tables are append-only: the application never issues UPDATE or
DELETE against them (enforced by triggers from migration 2). Corrections are
new rows carrying `supersedes_id`.
"""

from __future__ import annotations

import sqlite3
from typing import Callable, Dict, List, Tuple

from .model import COMMON_FIELDS, KINDS, utc_iso

SCHEMA_VERSION = 2

_COMMON_SQL = {
    'event_id': 'TEXT NOT NULL UNIQUE',
    'candidate_id': 'TEXT',
    'chain_id': 'TEXT NOT NULL',
    'mint': 'TEXT',
    'source': 'TEXT',
    'detected_by': 'TEXT',
    'source_time_utc': 'TEXT',
    'received_at_utc': 'TEXT NOT NULL',
    'source_age_ms': 'REAL',
    'recorded_at_utc': 'TEXT NOT NULL',
    'parser_version': 'TEXT',
    'config_fingerprint': 'TEXT',
    'code_version': 'TEXT',
    'supersedes_id': 'TEXT',
}
assert tuple(_COMMON_SQL) == COMMON_FIELDS

# Typed, queryable columns per table. Anything not listed stays in payload_json.
# JSON-typed columns hold serialized JSON text.
TYPED_COLUMNS: Dict[str, List[Tuple[str, str]]] = {
    'detections': [
        ('detection_type', 'TEXT'),       # helius_creation_log | helius_mint_resolved | pumpportal_create | ...
        ('dedup_decision', 'TEXT'),       # first_seen | duplicate_signature | duplicate_mint | ...
        ('signature', 'TEXT'),
        ('slot', 'INTEGER'),
        ('related_event_id', 'TEXT'),
        ('symbol', 'TEXT'),
        ('name', 'TEXT'),
        ('creator', 'TEXT'),
        ('bonding_curve', 'TEXT'),
        ('pool', 'TEXT'),
        ('metadata_status', 'TEXT'),
        ('raw_json', 'TEXT'),             # redacted original message (JSON)
    ],
    'decisions': [
        ('stage', 'TEXT'),
        ('decision', 'TEXT CHECK (decision IN (\'PASS\',\'FAIL\',\'SKIP\',\'UNKNOWN\'))'),
        ('reason_code', 'TEXT'),
        ('reason_text', 'TEXT'),
        ('observed_value', 'TEXT'),       # JSON
        ('threshold', 'TEXT'),            # JSON
        ('unit', 'TEXT'),
        ('data_source', 'TEXT'),
        ('outcome', 'TEXT'),
        ('attempt', 'INTEGER'),
    ],
    'observations': [
        ('obs_type', 'TEXT'),             # market_dexscreener | curve_rpc | bot_spot_tick | filter_metric | sol_usd
        ('provider', 'TEXT'),
        ('status', 'TEXT'),               # ok | not_indexed | stale | http_429 | timeout | http_error | provider_error | ...
        ('status_reason', 'TEXT'),
        ('http_status', 'INTEGER'),
        ('latency_ms', 'REAL'),
        ('pair_address', 'TEXT'),
        ('dex_id', 'TEXT'),
        ('quote_asset', 'TEXT'),
        ('price_usd', 'REAL'),
        ('price_native', 'REAL'),
        ('price_sol', 'REAL'),
        ('liquidity_usd', 'REAL'),
        ('liquidity_base', 'REAL'),
        ('liquidity_quote', 'REAL'),
        ('market_cap_usd', 'REAL'),
        ('fdv_usd', 'REAL'),
        ('volume_json', 'TEXT'),          # JSON
        ('pool_created_at_utc', 'TEXT'),
        ('curve_complete', 'INTEGER'),
        ('curve_virtual_sol_raw', 'TEXT'),
        ('curve_virtual_token_raw', 'TEXT'),
        ('curve_real_sol_raw', 'TEXT'),
        ('curve_real_token_raw', 'TEXT'),
        ('metric', 'TEXT'),
        ('metric_value', 'TEXT'),         # JSON
        ('followup_id', 'TEXT'),
        ('horizon_label', 'TEXT'),
    ],
    'quotes': [
        ('purpose', 'TEXT'),              # shadow_buy | shadow_sell_exact | followup_sell_exact | bot_entry_buy | ...
        ('direction', 'TEXT'),            # buy | sell
        ('provider', 'TEXT'),
        ('status', 'TEXT'),               # ok | no_route | http_429 | timeout | http_error | ... | skipped_*
        ('status_reason', 'TEXT'),
        ('http_status', 'INTEGER'),
        ('latency_ms', 'REAL'),
        ('input_mint', 'TEXT'),
        ('output_mint', 'TEXT'),
        ('amount_raw', 'TEXT'),
        ('input_decimals', 'INTEGER'),
        ('output_decimals', 'INTEGER'),
        ('notional_rub', 'REAL'),
        ('notional_sol', 'REAL'),
        ('sol_usd_used', 'REAL'),
        ('usd_rub_used', 'REAL'),
        ('out_amount_raw', 'TEXT'),
        ('min_out_raw', 'TEXT'),
        ('slippage_bps', 'INTEGER'),
        ('price_impact_pct', 'REAL'),
        ('route_labels', 'TEXT'),         # JSON list
        ('context_slot', 'INTEGER'),
        ('quote_pair_id', 'TEXT'),
        ('attempt', 'INTEGER'),
        ('followup_id', 'TEXT'),
        ('horizon_label', 'TEXT'),
        ('quote_only', 'INTEGER NOT NULL CHECK (quote_only = 1)'),
        ('transaction_sent', 'INTEGER NOT NULL CHECK (transaction_sent = 0)'),
    ],
    'followups': [
        ('followup_id', 'TEXT'),
        ('horizon_label', 'TEXT'),
        ('horizon_sec', 'REAL'),
        ('anchor_time_utc', 'TEXT'),
        ('anchor_event_id', 'TEXT'),
        ('target_at_utc', 'TEXT'),
        ('actual_at_utc', 'TEXT'),
        ('delay_ms', 'REAL'),
        ('status', 'TEXT'),
        ('status_reason', 'TEXT'),
        ('observation_event_ids', 'TEXT'),  # JSON list
        ('quote_event_ids', 'TEXT'),        # JSON list
    ],
    'lifecycle': [
        ('lifecycle_type', 'TEXT'),
        ('position_key', 'TEXT'),
        ('reason', 'TEXT'),
        ('fraction', 'REAL'),
        ('amount_raw', 'TEXT'),
        ('tokens_raw', 'TEXT'),
        ('remaining_raw', 'TEXT'),
        ('paper_calculation', 'INTEGER'),
        ('is_real_transaction', 'INTEGER NOT NULL CHECK (is_real_transaction = 0)'),
    ],
    'health': [
        ('health_type', 'TEXT'),
        ('status', 'TEXT'),
    ],
}
assert set(TYPED_COLUMNS) == set(KINDS)

JSON_COLUMNS = frozenset({
    'raw_json', 'observed_value', 'threshold', 'volume_json', 'metric_value',
    'route_labels', 'observation_event_ids', 'quote_event_ids',
})


def table_columns(table: str) -> List[str]:
    return list(COMMON_FIELDS) + [name for name, _ in TYPED_COLUMNS[table]] + ['payload_json']


def _create_table_sql(table: str) -> str:
    cols = ['row_seq INTEGER PRIMARY KEY AUTOINCREMENT']
    cols += [f'{name} {sql}' for name, sql in _COMMON_SQL.items()]
    cols += [f'{name} {sql}' for name, sql in TYPED_COLUMNS[table]]
    cols.append('payload_json TEXT')
    return f'CREATE TABLE IF NOT EXISTS {table} (\n  ' + ',\n  '.join(cols) + '\n)'


def _migration_1(db: sqlite3.Connection) -> None:
    for table in KINDS:
        db.execute(_create_table_sql(table))


def _migration_2(db: sqlite3.Connection) -> None:
    for table in KINDS:
        db.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_candidate ON {table}(candidate_id, received_at_utc)')
        db.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_received ON {table}(received_at_utc)')
        # Append-only guarantee at the storage layer.
        db.execute(
            f'CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} '
            f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END"
        )
        db.execute(
            f'CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} '
            f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END"
        )
    db.execute('CREATE INDEX IF NOT EXISTS ix_detections_signature ON detections(signature)')
    db.execute('CREATE INDEX IF NOT EXISTS ix_followups_fid ON followups(followup_id)')
    # Bookkeeping (NOT historical data): how far each bot spool file was ingested.
    db.execute(
        'CREATE TABLE IF NOT EXISTS spool_offsets ('
        ' spool_file TEXT PRIMARY KEY, byte_offset INTEGER NOT NULL, updated_at_utc TEXT NOT NULL)'
    )


MIGRATIONS: Dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_1,
    2: _migration_2,
}


def current_version(db: sqlite3.Connection) -> int:
    db.execute(
        'CREATE TABLE IF NOT EXISTS schema_migrations ('
        ' version INTEGER PRIMARY KEY, applied_at_utc TEXT NOT NULL)'
    )
    row = db.execute('SELECT MAX(version) FROM schema_migrations').fetchone()
    return int(row[0] or 0)


def migrate(db: sqlite3.Connection, target: int = SCHEMA_VERSION) -> int:
    """Apply pending migrations in order, each in its own transaction."""
    version = current_version(db)
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f'database schema v{version} is newer than this code (v{SCHEMA_VERSION})'
        )
    for number in range(version + 1, target + 1):
        with db:
            MIGRATIONS[number](db)
            db.execute(
                'INSERT INTO schema_migrations(version, applied_at_utc) VALUES (?, ?)',
                (number, utc_iso()),
            )
    return current_version(db)
