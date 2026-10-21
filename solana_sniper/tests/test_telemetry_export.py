"""Offline export round-trip: SQLite -> CSV/JSONL/README/manifest/ZIP, labels,
Unicode, big raw amounts, and secret redaction in every output file."""

from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path

from telemetry.export import export_bundle
from telemetry.model import WSOL, build_record, candidate_id, refresh_secret_values, stable_id, utc_iso
from telemetry.store import Store

T0 = 1_800_000_000.0
MINT = 'Ab1cD2eF3gH4iJ5kL6mN7oP8qR9sT0uV1wX2yZ3pump'
DEAD = 'De4dD2eF3gH4iJ5kL6mN7oP8qR9sT0uV1wX2yZ3pump'
BIG = '340282366920938463463374607431768211455'
SECRET = 'ENV-SECRET-VALUE-42xyz'


def rec(kind, ts, **fields):
    r = build_record(kind, dict(fields, _received_ts=ts))
    r['kind'] = kind
    return r


def fixture_store(root: Path) -> None:
    store = Store(root)
    cid = candidate_id('solana-mainnet', MINT)
    pair = stable_id('shadow', cid, 250.0)
    fu = lambda label: stable_id(cid, label)
    records = [
        rec('detections', T0, mint=MINT, source='Pump.fun', detected_by='collector',
            detection_type='pumpportal_create', dedup_decision='first_seen_by_collector',
            symbol='РКТ🚀', name='Ракета «Луна»', creator='Creator' + 'x' * 37, bonding_curve='Curve' + 'y' * 39,
            metadata_status='present', signature='S' * 88,
            raw_json={'mint': MINT, 'vSolInBondingCurve': 30.5, 'initialBuy': 1e7, 'uri': 'ipfs://x',
                      'headers': {'Authorization': f'Bearer {SECRET}'}}),
        rec('detections', T0 + 0.2, mint=MINT, source='Pump.fun', detected_by='bot',
            detection_type='pumpportal_create', dedup_decision='duplicate_mint'),
        rec('detections', T0 + 1, mint=DEAD, source='Raydium CPMM', detected_by='bot',
            detection_type='helius_mint_resolved', dedup_decision='first_mint', metadata_status='mint_only'),
        rec('decisions', T0 + 0.3, mint=MINT, detected_by='bot', stage='trader_intake', decision='PASS',
            reason_code='received_by_trader', outcome='filters_pending'),
        rec('decisions', T0 + 2, mint=MINT, detected_by='bot', stage='filters_total', decision='PASS',
            reason_code='all_filters_passed', outcome='would_enter_paper_if_slot_and_quote'),
        rec('decisions', T0 + 2.1, mint=MINT, detected_by='bot', stage='entry', decision='SKIP',
            reason_code='slot_busy', observed_value=3, threshold=3, unit='open_positions', outcome='slot_busy'),
        rec('decisions', T0 + 3, mint=DEAD, detected_by='bot', stage='liquidity', decision='FAIL',
            reason_text='ликвидность ниже порога $500', threshold=500.0, unit='USD', outcome='filter_stage_result'),
        rec('quotes', T0 + 1, mint=MINT, detected_by='collector', purpose='shadow_buy', status='ok',
            direction='buy', input_mint=WSOL, output_mint=MINT, amount_raw=1_000_000_000, out_amount_raw=BIG,
            min_out_raw=BIG[:-1], notional_rub=250.0, quote_pair_id=pair, route_labels=['Pump.fun'],
            error=f'https://api.example/?api-key={SECRET}'),
        rec('quotes', T0 + 1.5, mint=MINT, detected_by='collector', purpose='shadow_sell_exact', status='ok',
            direction='sell', input_mint=MINT, output_mint=WSOL, amount_raw=BIG, out_amount_raw='950000000',
            min_out_raw='940000000', quote_pair_id=pair),
        rec('quotes', T0 + 60, mint=MINT, detected_by='collector', purpose='followup_sell_exact', status='no_route',
            direction='sell', amount_raw=BIG, quote_pair_id=pair, horizon_label='60s', followup_id=fu('60s')),
        rec('quotes', T0 + 1, mint=DEAD, detected_by='collector', purpose='shadow_buy', status='not_sampled',
            quote_pair_id=stable_id('shadow', candidate_id('solana-mainnet', DEAD), 250.0), notional_rub=250.0),
        rec('observations', T0 + 1, mint=MINT, detected_by='collector', obs_type='market_dexscreener', status='ok',
            price_usd=0.0001, horizon_label='1s', followup_id=fu('1s')),
        rec('observations', T0 + 30, mint=MINT, detected_by='collector', obs_type='market_dexscreener', status='ok',
            price_usd=0.0003),
        rec('observations', T0 + 60, mint=MINT, detected_by='collector', obs_type='market_dexscreener', status='ok',
            price_usd=0.00005, horizon_label='60s', followup_id=fu('60s')),
        rec('lifecycle', T0 + 5, mint=MINT, detected_by='bot', lifecycle_type='paper_entry_open', tokens_raw=BIG),
        rec('health', T0 + 9, detected_by='bot', health_type='data_loss', status='lost', lost_events_total=7,
            loss_counts={'queue_full': 7}),
    ]
    for label, sec, status in (('1s', 1, 'complete'), ('60s', 60, 'partial'), ('1h', 3600, 'missed')):
        records.append(rec('followups', T0 + sec, mint=MINT, detected_by='collector', followup_id=fu(label),
                           event_id=stable_id('followup', cid, label), horizon_label=label, horizon_sec=sec,
                           anchor_time_utc=utc_iso(T0), anchor_event_id=records[0]['event_id'],
                           target_at_utc=utc_iso(T0 + sec), status=status))
    store.append(records)
    store.close()


def read_csv(path):
    with open(path, encoding='utf-8', newline='') as fh:
        return list(csv.DictReader(fh))


def test_export_roundtrip_zip_labels_and_redaction(tmp_path, monkeypatch):
    monkeypatch.setenv('PROVIDER_API_KEY', SECRET)
    refresh_secret_values()
    root = tmp_path / 'telemetry_data'
    fixture_store(root)
    result = export_bundle(str(root), make_zip=True, now_ts=T0 + 120)
    monkeypatch.delenv('PROVIDER_API_KEY')
    refresh_secret_values()

    for name in ('collector.sqlite', 'raw_events.jsonl', 'README.md', 'data_dictionary.md', 'manifest.json',
                 'exports/candidates.csv', 'exports/detections.csv', 'exports/decisions.csv',
                 'exports/observations.csv', 'exports/quotes.csv', 'exports/followups.csv',
                 'exports/labels.csv', 'exports/features_at_detection.csv'):
        assert (root / name).exists(), name

    quotes = read_csv(root / 'exports/quotes.csv')
    buy = [q for q in quotes if q['purpose'] == 'shadow_buy' and q['status'] == 'ok'][0]
    assert buy['out_amount_raw'] == BIG and buy['quote_only'] == '1' and buy['transaction_sent'] == '0'
    assert json.loads(buy['route_labels']) == ['Pump.fun']
    dets = read_csv(root / 'exports/detections.csv')
    assert dets[0]['symbol'] == 'РКТ🚀' and dets[0]['name'] == 'Ракета «Луна»'
    assert len(dets) == 3                                     # duplicate kept

    cands = {c['mint']: c for c in read_csv(root / 'exports/candidates.csv')}
    assert set(cands) == {MINT, DEAD}                         # failed/dead candidate kept
    assert cands[MINT]['detection_events'] == '2' and cands[MINT]['duplicate_detection_events'] == '1'
    assert cands[MINT]['detected_by_set'] == 'bot|collector'
    assert cands[MINT]['bot_entry_outcomes_postdetection'] == 'entry:slot_busy'
    assert cands[MINT]['symbol_at_first_detection'] == 'РКТ🚀'
    assert cands[DEAD]['quote_sampled'] == '0'

    feats = {f['mint']: f for f in read_csv(root / 'exports/features_at_detection.csv')}
    assert feats[MINT]['asof_time_utc'] == cands[MINT]['first_seen_at_utc'] and feats[MINT]['has_uri'] == '1'

    labels = read_csv(root / 'exports/labels.csv')
    get = lambda name, h: [l for l in labels if l['mint'] == MINT and l['label_name'] == name and l['horizon_label'] == h][0]
    pr = get('price_return_pct[market_dexscreener]', '60s')
    assert abs(float(pr['value']) + 50.0) < 1e-9 and pr['censored'] == '0'
    assert abs(float(get('max_upside_pct[market_dexscreener]', '60s')['value']) - 200.0) < 1e-9
    assert get('price_return_pct[market_dexscreener]', '1h')['censored'] == '1'
    assert get('price_return_pct[market_dexscreener]', '1h')['value'] == ''        # censored != 0
    qi = get('quote_roundtrip_return_pct', 'shadow_immediate')
    assert abs(float(qi['value']) + 5.0) < 1e-9
    q60 = get('quote_roundtrip_return_pct', '60s')
    assert q60['censored'] == '1' and q60['route_available'] == '0' and q60['value'] == ''
    dead = [l for l in labels if l['mint'] == DEAD]
    assert dead and all(l['censored'] == '1' for l in dead)

    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    for table in ('detections', 'quotes', 'labels', 'candidates'):
        assert manifest['row_counts'][table] == len(read_csv(root / f'exports/{table}.csv'))
    assert manifest['coverage']['data_loss_events_reported'] == 7
    readme = (root / 'README.md').read_text(encoding='utf-8')
    assert 'candidates: **2**' in readme and 'No random train/test split' in readme
    assert len((root / 'raw_events.jsonl').read_text(encoding='utf-8').splitlines()) == 19

    with zipfile.ZipFile(result['zip']) as zf:
        names = set(zf.namelist())
        assert {'collector.sqlite', 'raw_events.jsonl', 'README.md', 'data_dictionary.md', 'manifest.json',
                'exports/candidates.csv', 'exports/quotes.csv', 'exports/followups.csv'} <= names
        for name in names:
            assert SECRET.encode() not in zf.read(name), name
    for path in root.rglob('*'):
        if path.is_file():
            assert SECRET.encode() not in path.read_bytes(), path


def test_export_on_empty_dir_creates_valid_bundle(tmp_path):
    result = export_bundle(str(tmp_path / 'empty'), make_zip=True, now_ts=T0)
    assert result['row_counts']['candidates'] == 0
    assert zipfile.ZipFile(result['zip']).testzip() is None
