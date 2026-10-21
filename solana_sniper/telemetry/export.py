"""Offline export: SQLite -> CSV + README (coverage) + data dictionary + manifest
-> optional ZIP. No network access anywhere in this module.

    cd solana_sniper && python -m telemetry export --data-dir ../telemetry_data --zip
"""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import time
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import dictionary, schema
from .collector import collector_running
from .config import parse_duration
from .labels import LABEL_COLUMNS, compute_labels
from .model import KINDS, parse_iso, utc_iso
from .store import DB_NAME, RAW_EVENTS, SPOOL_DIR, Store, ensure_data_dir, read_spool_chunk
from .version import code_version

CANDIDATE_COLUMNS = [
    'candidate_id', 'chain_id', 'mint', 'first_seen_at_utc', 'first_signal_at_utc',
    'first_seen_event_id', 'first_seen_source', 'first_seen_detected_by', 'first_signature',
    'symbol_at_first_detection', 'name_at_first_detection', 'creator_at_first_detection',
    'bonding_curve_at_first_detection', 'pool_at_first_detection',
    'metadata_status_at_first_detection', 'detection_events', 'duplicate_detection_events',
    'detected_by_set', 'sources_set', 'bot_saw', 'bot_filters_total_postdetection',
    'bot_entry_outcomes_postdetection', 'bot_paper_entered_postdetection', 'quote_sampled',
    'followups_recorded', 'code_versions', 'config_fingerprints',
]
FEATURE_COLUMNS = [
    'candidate_id', 'mint', 'asof_time_utc', 'first_seen_event_id', 'source', 'detected_by',
    'symbol', 'name', 'creator', 'bonding_curve', 'has_name', 'has_symbol', 'has_uri',
    'name_len', 'symbol_len', 'v_sol_in_curve', 'v_tokens_in_curve', 'market_cap_sol',
    'initial_buy', 'sol_amount', 'pool',
]


def ingest_all_spool(store: Store, root: Path) -> int:
    total = 0
    while True:
        records, offsets = read_spool_chunk(root / SPOOL_DIR, store.spool_offsets(), 5000)
        if not offsets:
            return total
        valid = [r for r in records if r.get('kind') in KINDS and r.get('event_id') and r.get('received_at_utc')]
        store.append(valid, offsets=offsets)
        total += len(valid)


def _snapshot(root: Path, target: Path) -> None:
    if target.exists():
        target.unlink()
    src = sqlite3.connect(f'file:{root / DB_NAME}?mode=ro', uri=True)
    dst = sqlite3.connect(str(target))
    src.backup(dst)
    dst.close()
    src.close()


def _cell(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _write_csv(path: Path, columns: List[str], rows: Iterable[Any]) -> int:
    count = 0
    with open(path, 'w', encoding='utf-8', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for row in rows:
            if isinstance(row, dict):
                writer.writerow([_cell(row.get(c)) for c in columns])
            else:
                writer.writerow([_cell(v) for v in row])
            count += 1
    return count


def _payload(row: sqlite3.Row) -> Dict[str, Any]:
    try:
        return json.loads(row['payload_json']) if row['payload_json'] else {}
    except (TypeError, ValueError):
        return {}


def build_candidates(con: sqlite3.Connection) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    con.row_factory = sqlite3.Row
    first_sig = dict(con.execute(
        'SELECT signature, MIN(received_at_utc) FROM detections WHERE signature IS NOT NULL GROUP BY signature'))
    cands: Dict[str, Dict[str, Any]] = {}
    features: Dict[str, Dict[str, Any]] = {}
    dets = defaultdict(list)
    for row in con.execute('SELECT * FROM detections WHERE candidate_id IS NOT NULL ORDER BY received_at_utc, row_seq'):
        dets[row['candidate_id']].append(row)
    extra_first = {}
    for row in con.execute('SELECT candidate_id, mint, MIN(received_at_utc) AS t, source, detected_by, event_id '
                           'FROM decisions WHERE candidate_id IS NOT NULL GROUP BY candidate_id'):
        extra_first[row['candidate_id']] = row
    outcomes = defaultdict(list)
    filters_total = defaultdict(list)
    for row in con.execute("SELECT candidate_id, stage, decision, reason_code FROM decisions "
                           "WHERE stage IN ('entry','paper_entry','filters_total') ORDER BY received_at_utc, row_seq"):
        if row['stage'] == 'filters_total':
            filters_total[row['candidate_id']].append(row['decision'])
        else:
            outcomes[row['candidate_id']].append(f"{row['stage']}:{row['reason_code']}")
    sampled = {}
    for row in con.execute("SELECT candidate_id, status FROM quotes WHERE purpose='shadow_buy' AND detected_by='collector'"):
        sampled[row['candidate_id']] = sampled.get(row['candidate_id'], False) or row['status'] != 'not_sampled'
    fu_count = dict(con.execute('SELECT candidate_id, COUNT(DISTINCT followup_id) FROM followups GROUP BY candidate_id'))
    versions = defaultdict(set)
    fps = defaultdict(set)
    for table in ('detections', 'decisions'):
        for cid, cv, fp in con.execute(f'SELECT candidate_id, code_version, config_fingerprint FROM {table} '
                                       'WHERE candidate_id IS NOT NULL GROUP BY candidate_id, code_version, config_fingerprint'):
            if cv:
                versions[cid].add(cv)
            if fp:
                fps[cid].add(fp)

    for cid in set(dets) | set(extra_first):
        rows = dets.get(cid, [])
        first = rows[0] if rows else None
        dec_first = extra_first.get(cid)
        first_time = first['received_at_utc'] if first else dec_first['t']
        if dec_first is not None and dec_first['t'] < first_time:
            first_time = dec_first['t']
        sig_times = [first_sig[r['signature']] for r in rows if r['signature'] in first_sig]
        first_signal = min([first_time] + sig_times)
        mint = first['mint'] if first else dec_first['mint']
        entry = outcomes.get(cid, [])
        cands[cid] = {
            'candidate_id': cid, 'chain_id': 'solana-mainnet', 'mint': mint,
            'first_seen_at_utc': first_time, 'first_signal_at_utc': first_signal,
            'first_seen_event_id': first['event_id'] if first else dec_first['event_id'],
            'first_seen_source': first['source'] if first else dec_first['source'],
            'first_seen_detected_by': first['detected_by'] if first else dec_first['detected_by'],
            'first_signature': next((r['signature'] for r in rows if r['signature']), None),
            'symbol_at_first_detection': first['symbol'] if first else None,
            'name_at_first_detection': first['name'] if first else None,
            'creator_at_first_detection': first['creator'] if first else None,
            'bonding_curve_at_first_detection': first['bonding_curve'] if first else None,
            'pool_at_first_detection': first['pool'] if first else None,
            'metadata_status_at_first_detection': first['metadata_status'] if first else None,
            'detection_events': len(rows),
            'duplicate_detection_events': sum(1 for r in rows if (r['dedup_decision'] or '').startswith('duplicate')),
            'detected_by_set': '|'.join(sorted({r['detected_by'] or '' for r in rows} | ({dec_first['detected_by']} if dec_first else set()))),
            'sources_set': '|'.join(sorted({r['source'] or '' for r in rows})),
            'bot_saw': int(any(r['detected_by'] == 'bot' for r in rows) or (dec_first is not None and dec_first['detected_by'] == 'bot')),
            'bot_filters_total_postdetection': '|'.join(filters_total.get(cid, [])) or None,
            'bot_entry_outcomes_postdetection': '|'.join(entry) or None,
            'bot_paper_entered_postdetection': int(any(o.endswith(':paper_entry_opened') for o in entry)),
            'quote_sampled': None if cid not in sampled else int(sampled[cid]),
            'followups_recorded': fu_count.get(cid, 0),
            'code_versions': '|'.join(sorted(versions.get(cid, set()))) or None,
            'config_fingerprints': '|'.join(sorted(fps.get(cid, set()))) or None,
        }
        raw = {}
        if first is not None and first['raw_json']:
            try:
                raw = json.loads(first['raw_json'])
            except ValueError:
                raw = {}
        features[cid] = {
            'candidate_id': cid, 'mint': mint, 'asof_time_utc': first_time,
            'first_seen_event_id': cands[cid]['first_seen_event_id'],
            'source': cands[cid]['first_seen_source'], 'detected_by': cands[cid]['first_seen_detected_by'],
            'symbol': first['symbol'] if first else None, 'name': first['name'] if first else None,
            'creator': first['creator'] if first else None,
            'bonding_curve': first['bonding_curve'] if first else None,
            'has_name': None if first is None else int(bool(first['name'])),
            'has_symbol': None if first is None else int(bool(first['symbol'])),
            'has_uri': None if not raw else int(bool(raw.get('uri'))),
            'name_len': len(first['name']) if first is not None and first['name'] else None,
            'symbol_len': len(first['symbol']) if first is not None and first['symbol'] else None,
            'v_sol_in_curve': raw.get('vSolInBondingCurve'), 'v_tokens_in_curve': raw.get('vTokensInBondingCurve'),
            'market_cap_sol': raw.get('marketCapSol'), 'initial_buy': raw.get('initialBuy'),
            'sol_amount': raw.get('solAmount'), 'pool': raw.get('pool'),
        }
    order = sorted(cands, key=lambda c: (cands[c]['first_seen_at_utc'] or '', c))
    return [cands[c] for c in order], [features[c] for c in order]


def coverage(con: sqlite3.Connection, candidates: List[Dict[str, Any]], labels: List[Dict[str, Any]],
             now_ts: float) -> Dict[str, Any]:
    con.row_factory = sqlite3.Row
    n = len(candidates)
    cov: Dict[str, Any] = {'candidates_total': n}
    cov['by_first_source'] = dict(Counter(c['first_seen_source'] for c in candidates))
    cov['by_detected_by_set'] = dict(Counter(c['detected_by_set'] for c in candidates))
    cov['metadata_at_first_detection'] = dict(Counter(c['metadata_status_at_first_detection'] for c in candidates))
    cov['detection_events_total'] = int(con.execute('SELECT COUNT(*) FROM detections').fetchone()[0])
    cov['detection_dedup_decisions'] = dict(con.execute(
        'SELECT dedup_decision, COUNT(*) FROM detections GROUP BY dedup_decision').fetchall())
    cov['bot_saw_candidates'] = sum(c['bot_saw'] for c in candidates)
    cov['bot_filters_total'] = dict(con.execute(
        "SELECT decision, COUNT(*) FROM decisions WHERE stage='filters_total' GROUP BY decision").fetchall())
    cov['bot_entry_reason_codes'] = dict(con.execute(
        "SELECT stage || ':' || reason_code, COUNT(*) FROM decisions WHERE stage IN ('entry','paper_entry') "
        'GROUP BY 1 ORDER BY 2 DESC').fetchall())
    cov['bot_filter_stage_reasons'] = dict(con.execute(
        "SELECT stage || ':' || decision || ':' || reason_code, COUNT(*) FROM decisions "
        "WHERE outcome IN ('filter_stage_result','not_evaluated') GROUP BY 1 ORDER BY 2 DESC").fetchall())
    cov['candidates_with_ok_market_observation'] = int(con.execute(
        "SELECT COUNT(DISTINCT candidate_id) FROM observations WHERE obs_type='market_dexscreener' AND status='ok'"
    ).fetchone()[0])
    cov['market_observation_status'] = dict(con.execute(
        "SELECT status, COUNT(*) FROM observations WHERE obs_type IN ('market_dexscreener','curve_rpc') GROUP BY status"
    ).fetchall())
    cov['quote_status_by_purpose'] = {
        f'{p}:{s}': c for p, s, c in con.execute(
            'SELECT purpose, status, COUNT(*) FROM quotes GROUP BY purpose, status ORDER BY purpose, 3 DESC')}
    cov['candidates_with_ok_shadow_buy'] = int(con.execute(
        "SELECT COUNT(DISTINCT candidate_id) FROM quotes WHERE purpose='shadow_buy' AND status='ok'").fetchone()[0])
    cov['candidates_with_ok_shadow_sell_exact'] = int(con.execute(
        "SELECT COUNT(DISTINCT candidate_id) FROM quotes WHERE purpose='shadow_sell_exact' AND status='ok'").fetchone()[0])
    per_h: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for label, status, count in con.execute(
            'SELECT horizon_label, status, COUNT(DISTINCT followup_id) FROM followups GROUP BY 1, 2'):
        per_h[label][status] += count
    matured: Dict[str, int] = {}
    for label, sec in con.execute('SELECT DISTINCT horizon_label, horizon_sec FROM followups'):
        matured[label] = sum(1 for c in candidates
                             if parse_iso(c['first_seen_at_utc']) and parse_iso(c['first_seen_at_utc']) + sec <= now_ts)
    cov['followups_by_horizon'] = {k: dict(v) for k, v in per_h.items()}
    cov['candidates_matured_by_horizon'] = matured
    lost = 0
    for row in con.execute("SELECT payload_json FROM health WHERE health_type='data_loss'"):
        try:
            lost += int(json.loads(row[0] or '{}').get('lost_events_total') or 0)
        except ValueError:
            pass
    cov['data_loss_events_reported'] = lost
    cov['discovery_disconnects'] = int(con.execute(
        "SELECT COUNT(*) FROM health WHERE health_type='discovery_disconnected'").fetchone()[0])
    cov['labels_total'] = len(labels)
    cov['labels_censored'] = sum(1 for l in labels if l.get('censored'))
    cov['time_range_utc'] = list(con.execute(
        'SELECT MIN(received_at_utc), MAX(received_at_utc) FROM detections').fetchone())
    return cov


def _md_table(mapping: Dict[Any, Any], key: str = 'key', value: str = 'count') -> str:
    if not mapping:
        return '_none_\n'
    rows = [f'| {key} | {value} |', '|---|---|']
    rows += [f'| {k if k is not None else "null"} | {v} |' for k, v in mapping.items()]
    return '\n'.join(rows) + '\n'


def _duration_key(label: Any) -> float:
    try:
        return parse_duration(str(label))
    except ValueError:
        return float('inf')


def render_readme(cov: Dict[str, Any], manifest_hint: Dict[str, Any]) -> str:
    fh = cov.get('followups_by_horizon', {})
    matured = cov.get('candidates_matured_by_horizon', {})
    horizon_rows = ['| horizon | matured candidates | records by status |', '|---|---|---|']
    for label in sorted(fh, key=_duration_key):
        horizon_rows.append(f'| {label} | {matured.get(label, 0)} | {json.dumps(fh[label], sort_keys=True)} |')
    return f"""# Sniper telemetry bundle

Generated: {manifest_hint['generated_at_utc']}  •  schema v{manifest_hint['schema_version']}  •  code {manifest_hint['code_version']}

Passive, read-only telemetry of a Solana **paper** bot. Every detected candidate is kept from its
earliest detection — including filter FAIL/SKIP, slot busy, no quote/no route, errors, and tokens
that later died or stopped updating. **Quotes are quote_only (no transaction was ever signed or sent);
lifecycle rows are paper calculations, not fills.**

## Files
* `collector.sqlite` – primary store (append-only tables, versioned schema)
* `raw_events.jsonl` (+ `raw_archive/*.jsonl.gz`) – redacted append-only mirror of every record
* `exports/*.csv` – one row per event/observation (UTF-8; JSON columns serialized; raw amounts are strings — import them as TEXT)
* `exports/candidates.csv`, `exports/features_at_detection.csv` – one row per candidate (T0 information only)
* `exports/labels.csv` – FUTURE labels, kept separate from features
* `data_dictionary.md`, `manifest.json` (sha256 + row counts)

## Coverage (do not evaluate a strategy only on rows with successful quotes or paper trades)
* candidates: **{cov['candidates_total']}**; detection events: {cov['detection_events_total']}; seen by bot: {cov['bot_saw_candidates']}
* time range (detections, UTC): {cov['time_range_utc']}
* candidates with ≥1 ok market observation: {cov['candidates_with_ok_market_observation']}
* candidates with ok shadow buy / exact sell quote: {cov['candidates_with_ok_shadow_buy']} / {cov['candidates_with_ok_shadow_sell_exact']}
* reported data loss (events dropped by bounded queues/sink errors): **{cov['data_loss_events_reported']}**
* discovery disconnects (gaps in collector's own stream): {cov['discovery_disconnects']}
* labels: {cov['labels_total']} (censored: {cov['labels_censored']})

### By first source
{_md_table(cov['by_first_source'], 'source')}
### By detecting process
{_md_table(cov['by_detected_by_set'], 'detected_by')}
### Metadata at first detection
{_md_table(cov['metadata_at_first_detection'], 'metadata_status')}
### Detection dedup decisions (duplicates are kept as rows)
{_md_table(cov['detection_dedup_decisions'], 'dedup_decision')}
### Bot filters total
{_md_table(cov['bot_filters_total'], 'decision')}
### Bot entry decisions (why skipped / entered)
{_md_table(cov['bot_entry_reason_codes'], 'stage:reason_code')}
### Bot filter stages
{_md_table(cov['bot_filter_stage_reasons'], 'stage:decision:reason_code')}
### Market observation status
{_md_table(cov['market_observation_status'], 'status')}
### Quote status by purpose
{_md_table(cov['quote_status_by_purpose'], 'purpose:status')}
### Follow-ups per horizon
{chr(10).join(horizon_rows)}

## Rules for analysis (look-ahead / survivorship)
1. A value is available at time T only if `received_at_utc <= T`. Detection-time features are in
   `features_at_detection.csv` (from the first detection message only). Never join later metadata into T0 features.
2. Everything after detection (returns, peaks, drawdowns, route availability, bot outcomes) is a **label**
   (`labels.csv`, `*_postdetection` columns). Censored labels have an empty value and `censored=1` — keep the rows;
   never treat censored as 0 and never drop dead/illiquid candidates.
3. **No random train/test split.** Split chronologically by `first_seen_at_utc` (e.g. walk-forward), and
   group related rows by `mint`, `creator` and `bonding_curve/pool` so near-duplicates do not leak across folds.
4. Quote labels are hypothetical (quote_only, no fees/priority, no fill). `no_route` at a horizon is informative
   (`route_available=0`), not a missing value to impute.
5. Coverage gaps are explicit: `followups.status` = missed / skipped_* / failed, `health.data_loss`,
   `health.discovery_disconnected`. Quote sampling is a deterministic hash sample of candidates (unbiased by outcome).

## How to verify completeness
* every `candidates.csv` row should have ~N follow-up records once matured (`followups_recorded` vs horizons);
* compare `detection_events` with the bot console ("Новая монета") for the same time window;
* `health` rows `data_loss` must be zero (or quantify the loss interval);
* `manifest.json` row counts must match the CSV line counts (minus header).
"""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def export_bundle(data_dir: str, *, make_zip: bool = False, ingest: bool = True,
                  now_ts: Optional[float] = None) -> Dict[str, Any]:
    now_ts = time.time() if now_ts is None else now_ts
    root = ensure_data_dir(data_dir)
    running = collector_running(root)
    ingested = 0
    if not running:
        store = Store(root)   # creates/migrates the schema if needed (single writer: collector is not running)
        if ingest:
            ingested = ingest_all_spool(store, root)
        store.close()
    exports = root / 'exports'
    exports.mkdir(exist_ok=True)
    snap = root / '.export_snapshot.sqlite'
    _snapshot(root, snap)
    con = sqlite3.connect(str(snap))
    con.row_factory = sqlite3.Row
    counts: Dict[str, int] = {}
    files: Dict[str, Path] = {}
    for table in KINDS:
        columns = ['row_seq'] + schema.table_columns(table)
        cur = con.execute(f'SELECT {", ".join(columns)} FROM {table} ORDER BY row_seq')
        path = exports / f'{table}.csv'
        counts[table] = _write_csv(path, columns, (tuple(r) for r in cur))
        files[f'exports/{table}.csv'] = path
    candidates, features = build_candidates(con)
    counts['candidates'] = _write_csv(exports / 'candidates.csv', CANDIDATE_COLUMNS, candidates)
    counts['features_at_detection'] = _write_csv(exports / 'features_at_detection.csv', FEATURE_COLUMNS, features)
    labels = compute_labels(con, now_ts)
    counts['labels'] = _write_csv(exports / 'labels.csv', LABEL_COLUMNS, labels)
    for name in ('candidates', 'features_at_detection', 'labels'):
        files[f'exports/{name}.csv'] = exports / f'{name}.csv'
    cov = coverage(con, candidates, labels, now_ts)
    schema_version = int(con.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0])
    con.close()
    hint = {'generated_at_utc': utc_iso(now_ts), 'schema_version': schema_version, 'code_version': code_version()}
    (root / 'README.md').write_text(render_readme(cov, hint), encoding='utf-8')
    (root / 'data_dictionary.md').write_text(dictionary.render(counts), encoding='utf-8')
    files['README.md'] = root / 'README.md'
    files['data_dictionary.md'] = root / 'data_dictionary.md'
    if (root / RAW_EVENTS).exists():
        files[RAW_EVENTS] = root / RAW_EVENTS
    for archived in sorted((root / 'raw_archive').glob('*.jsonl.gz')) if (root / 'raw_archive').exists() else []:
        files[f'raw_archive/{archived.name}'] = archived
    manifest = {
        **hint, 'collector_running_during_export': running, 'spool_records_ingested': ingested,
        'row_counts': counts, 'coverage': cov,
        'files': {name: {'bytes': p.stat().st_size, 'sha256': _sha256(p)} for name, p in files.items()},
        'collector.sqlite': {'note': 'consistent snapshot taken at export time',
                             'bytes': snap.stat().st_size, 'sha256': _sha256(snap)},
        'notes': ['quote rows are quote_only=1, transaction_sent=0', 'lifecycle rows are paper calculations',
                  'labels are future information and must not be used as features'],
    }
    (root / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
                                        encoding='utf-8')
    result: Dict[str, Any] = {'data_dir': str(root), 'row_counts': counts, 'coverage': cov,
                              'ingested_spool_records': ingested, 'zip': None}
    if make_zip:
        bundles = root / 'bundles'
        bundles.mkdir(exist_ok=True)
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(now_ts))
        zip_path = bundles / f'sniper_telemetry_{stamp}.zip'
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            zf.write(snap, DB_NAME)
            for name, path in files.items():
                zf.write(path, name)
            zf.write(root / 'manifest.json', 'manifest.json')
        result['zip'] = str(zip_path)
    snap.unlink()
    return result
