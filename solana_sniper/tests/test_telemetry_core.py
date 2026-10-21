"""Offline tests: model, redaction, storage, migrations, observer back-pressure."""

from __future__ import annotations

import csv
import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from telemetry import hooks, schema
from telemetry.model import (build_record, candidate_id, classify_filter_reason, dumps,
                             redact, refresh_secret_values)
from telemetry.observer import NullObserver, Observer, WriterThread
from telemetry.store import SpoolSink, Store, UnsafeDataDir, ensure_data_dir, read_spool_chunk

BIG = 123456789012345678901234567890   # > 2**64, must survive as an exact string
MINT = '7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU'


def rec(kind, **fields):
    r = build_record(kind, fields)
    r['kind'] = kind
    return r


# ----------------------------------------------------------------- redaction

def test_recursive_redaction_of_secrets(monkeypatch):
    monkeypatch.setenv('HELIUS_API_KEY', 'SUPERSECRETKEY-1234567')
    monkeypatch.setenv('SOME_PRIVATE_SEED', 'seed words here please')
    refresh_secret_values()
    payload = {
        'headers': {'Authorization': 'Bearer abc.def', 'X-API-KEY': 'k1'},
        'url': 'https://mainnet.helius-rpc.com/?api-key=SUPERSECRETKEY-1234567',
        'error': 'ClientError: 401 for https://rpc.example/path/SUPERSECRETKEY-1234567?x=1',
        'note': 'failed with api-key=zzzz9999 and token: qqq',
        'nested': [{'privateKey': [1, 2, 3]}, {'apiKey': 'abc'}, {'secret_value': 'x'}],
        'keypair_file': list(range(64)), 'blob': list(range(64)),
        'echo': 'value seed words here please end',
        'mintAuthority': None, 'traderPublicKey': 'Creator111', 'signature': 'Sig' * 29,
    }
    text = dumps(payload)
    for secret in ('SUPERSECRETKEY-1234567', 'abc.def', 'zzzz9999', 'qqq', 'k1',
                   'seed words here please', '"apiKey":"abc"'):
        assert secret not in text
    out = redact(payload)
    assert out['keypair_file'] == '[REDACTED]' and out['blob'] == '[REDACTED_KEYPAIR]'
    assert out['url'] == 'https://mainnet.helius-rpc.com/[REDACTED]'
    # Public addresses/signatures are data, not secrets.
    assert out['traderPublicKey'] == 'Creator111' and out['signature'] == 'Sig' * 29
    monkeypatch.delenv('HELIUS_API_KEY')
    monkeypatch.delenv('SOME_PRIVATE_SEED')
    refresh_secret_values()


def test_quote_flags_are_forced_and_amounts_are_strings():
    r = build_record('quotes', {'mint': MINT, 'quote_only': 0, 'transaction_sent': 1,
                                'amount_raw': BIG, 'out_amount_raw': 5.0})
    assert r['quote_only'] == 1 and r['transaction_sent'] == 0
    assert r['amount_raw'] == str(BIG) and r['out_amount_raw'] == '5'
    life = build_record('lifecycle', {'mint': MINT, 'is_real_transaction': 1})
    assert life['is_real_transaction'] == 0 and life['paper_calculation'] == 1


def test_candidate_id_is_stable_and_chain_scoped():
    assert candidate_id('solana-mainnet', MINT) == candidate_id('solana-mainnet', MINT)
    assert candidate_id('solana-mainnet', MINT) != candidate_id('solana-devnet', MINT)
    assert build_record('detections', {'mint': MINT})['candidate_id'] == candidate_id('solana-mainnet', MINT)


def test_reason_classifier_has_stable_codes_and_fallback():
    assert classify_filter_reason('liquidity', 'FAIL', 'ликвидность ниже порога $500') == 'liquidity_below_threshold'
    assert classify_filter_reason('honeypot_route', 'FAIL', 'Jupiter не дал маршрут продажи (HTTP 429)') == 'sell_route_http_error'
    assert classify_filter_reason('sol_pair', 'UNKNOWN', 'проверка упала: boom') == 'check_exception'
    assert classify_filter_reason('new_stage', 'FAIL', '???') == 'new_stage_fail_unclassified'


def test_times_are_utc_iso_and_source_age():
    r = build_record('detections', {'_received_ts': 1_700_000_010.0, '_source_ts': 1_700_000_000.0})
    assert r['received_at_utc'].endswith('Z') and r['source_time_utc'].endswith('Z')
    assert r['source_age_ms'] == pytest.approx(10_000.0)
    bad = build_record('detections', {'source_time_utc': 'not-a-time'})
    assert bad['source_age_ms'] is None and bad['source_time_status'] == 'unparseable'


# ------------------------------------------------------------------- storage

def test_store_idempotent_by_event_id_but_keeps_duplicate_raw_events(tmp_path):
    store = Store(tmp_path / 'tel')
    a = rec('detections', mint=MINT, dedup_decision='first_mint', raw_json={'n': 1})
    b = rec('detections', mint=MINT, dedup_decision='duplicate_mint', raw_json={'n': 2})
    assert store.append([a, b]) == (2, 0)
    assert store.append([a]) == (0, 1)        # re-delivery of the SAME event
    rows = store.db.execute('SELECT candidate_id, dedup_decision FROM detections ORDER BY row_seq').fetchall()
    assert [r[1] for r in rows] == ['first_mint', 'duplicate_mint']
    assert len({r[0] for r in rows}) == 1
    lines = (tmp_path / 'tel' / 'raw_events.jsonl').read_text(encoding='utf-8').splitlines()
    assert len(lines) == 2                    # JSONL mirrors inserted rows only
    store.close()


def test_tables_are_append_only_and_quote_checks_hold(tmp_path):
    store = Store(tmp_path / 'tel')
    store.append([rec('quotes', mint=MINT, purpose='shadow_buy', status='ok', amount_raw=BIG)])
    with pytest.raises(sqlite3.DatabaseError):
        store.db.execute("UPDATE quotes SET status='changed'")
    with pytest.raises(sqlite3.DatabaseError):
        store.db.execute('DELETE FROM quotes')
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute("INSERT INTO quotes (event_id, chain_id, received_at_utc, recorded_at_utc, "
                         "quote_only, transaction_sent) VALUES ('x','c','t','t',1,1)")
    value = store.db.execute('SELECT amount_raw, typeof(amount_raw) FROM quotes').fetchone()
    assert value == (str(BIG), 'text')
    store.close()


def test_migration_from_v1_preserves_rows(tmp_path):
    root = ensure_data_dir(tmp_path / 'tel')
    db = sqlite3.connect(str(root / 'collector.sqlite'), isolation_level=None)
    schema.current_version(db)
    with db:
        schema.MIGRATIONS[1](db)
        db.execute("INSERT INTO schema_migrations VALUES (1, '2026-01-01T00:00:00Z')")
    db.execute("INSERT INTO detections (event_id, chain_id, mint, received_at_utc, recorded_at_utc) "
               "VALUES ('old-1', 'solana-mainnet', ?, '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')", (MINT,))
    db.close()
    store = Store(root)
    assert store.schema_version == schema.SCHEMA_VERSION == 2
    assert store.count('detections') == 1
    assert store.db.execute("SELECT name FROM sqlite_master WHERE name='spool_offsets'").fetchone()
    with pytest.raises(sqlite3.DatabaseError):
        store.db.execute('DELETE FROM detections')
    store.close()
    assert Store(root).schema_version == 2      # idempotent re-open


def test_unsafe_data_dirs_are_refused(tmp_path):
    with pytest.raises(UnsafeDataDir):
        ensure_data_dir(tmp_path / 'solana_sniper' / 'runs' / 'new')
    legacy = tmp_path / 'legacy_run'
    legacy.mkdir()
    (legacy / 'wallet.json').write_text('{}')
    with pytest.raises(UnsafeDataDir):
        ensure_data_dir(legacy)
    assert (legacy / 'wallet.json').read_text() == '{}'


def test_spool_roundtrip_with_partial_line(tmp_path):
    sink = SpoolSink(tmp_path / 'tel')
    sink.append([rec('detections', mint=MINT, symbol='🚀Ракета')])
    sink.close()
    spool = tmp_path / 'tel' / 'spool'
    path = next(spool.glob('*.jsonl'))
    with open(path, 'a', encoding='utf-8') as fh:
        fh.write('{"kind": "detections", "event_id": "half')     # writer mid-line
    records, offsets = read_spool_chunk(spool, {})
    assert len(records) == 1 and records[0]['symbol'] == '🚀Ракета'
    again, offsets2 = read_spool_chunk(spool, offsets)
    assert again == [] and offsets2 == {}


# ------------------------------------------------------------------ observer

class ListSink:
    def __init__(self):
        self.records = []

    def append(self, records, offsets=None):
        self.records.extend(records)
        return len(records), 0


def test_queue_overflow_is_counted_and_reported():
    obs = Observer(capacity=2)
    results = [obs.emit('detections', {'mint': MINT}) for _ in range(5)]
    assert results == [True, True, False, False, False]
    sink = ListSink()
    obs.drain(sink)
    loss = [r for r in sink.records if r['kind'] == 'health']
    assert loss and loss[0]['health_type'] == 'data_loss'
    assert loss[0]['lost_events_total'] == 3 and loss[0]['loss_counts'] == {'queue_full': 3}
    assert loss[0]['loss_interval_start_utc'] and loss[0]['loss_interval_end_utc']


def test_slow_writer_never_blocks_emit():
    class SlowSink(ListSink):
        def append(self, records, offsets=None):
            time.sleep(0.5)
            return super().append(records)

    obs = Observer(capacity=100_000)
    writer = WriterThread(obs, SlowSink(), interval=0.01)
    writer.start()
    started = time.perf_counter()
    for i in range(5000):
        obs.emit('decisions', {'mint': MINT, 'stage': 'x', 'decision': 'PASS'})
    elapsed = time.perf_counter() - started
    writer.stop(5)
    assert elapsed < 0.25, elapsed


def test_broken_sink_is_contained_and_counted():
    class BrokenSink:
        def append(self, records, offsets=None):
            raise OSError('disk full')

    obs = Observer()
    obs.emit('detections', {'mint': MINT})
    assert obs.drain(BrokenSink()) == 0
    losses = obs.take_losses()
    assert losses['loss_counts'] == {'sink_error:OSError': 1}


def test_hooks_never_raise_even_with_broken_observer():
    class Exploding:
        enabled = True

        def emit(self, kind, fields):
            raise RuntimeError('collector down')

    hooks.install(Exploding())
    try:
        assert hooks.pumpportal_raw({'mint': MINT, 'txType': 'create'}, set()) is None
        assert hooks.filter_metric(MINT, 'liquidity', 'x', 1.0) is None
        assert hooks.jupiter_quote('a', 'b', 'not-int', 100, 'ok') is None
    finally:
        hooks.uninstall()
    assert isinstance(hooks._OBS, NullObserver)
    assert hooks.candidate_received(object()) is None     # disabled: no-op


def test_install_from_env_is_off_without_dir_and_never_raises(monkeypatch, tmp_path):
    monkeypatch.delenv('TELEMETRY_DIR', raising=False)
    assert hooks.install_from_env(None) is False
    bad = tmp_path / 'x' / 'runs'                 # refused data dir -> stays disabled
    monkeypatch.setenv('TELEMETRY_DIR', str(bad))
    assert hooks.install_from_env(None) is False
    assert not hooks.enabled()
    good = tmp_path / 'tel'
    monkeypatch.setenv('TELEMETRY_DIR', str(good))
    settings = {'api_key': 'SHOULD-NOT-LEAK-123', 'http_rpc_url': 'https://h/?api-key=SHOULD-NOT-LEAK-123',
                'min_liquidity_usd': 500.0}
    assert hooks.install_from_env(settings) is True
    hooks.shutdown()
    text = ''.join(p.read_text(encoding='utf-8') for p in (good / 'spool').glob('*.jsonl'))
    assert 'config_snapshot' in text and 'min_liquidity_usd' in text
    assert 'SHOULD-NOT-LEAK-123' not in text
