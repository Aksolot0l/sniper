"""Offline tests of the standalone collector with fake HTTP/WS (no network)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from telemetry import hooks, providers
from telemetry.collector import Collector, sampled_for_quotes
from telemetry.config import CollectorConfig, parse_horizons
from telemetry.model import WSOL, candidate_id, utc_iso
from telemetry.observer import Observer, WriterThread
from telemetry.ratelimit import ProviderLimiter
from telemetry.store import SpoolSink

MINT = 'Gq3nY8w2CJvNfRk4tZ6hUe9bLxA1sMpD7oVcW5iTpump'
MINT2 = 'Zz9nY8w2CJvNfRk4tZ6hUe9bLxA1sMpD7oVcW5iTpump'
BIG_OUT = '987654321987654321987'


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status, self.body, self.headers = status, body, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def text(self):
        return '' if self.body is None else (self.body if isinstance(self.body, str) else json.dumps(self.body))


class FakeHttp:
    """Routes GET/POST to a handler; records every call; never opens sockets."""

    def __init__(self, handler):
        self.handler, self.calls = handler, []

    def _do(self, method, url, kwargs):
        self.calls.append((method, url, kwargs.get('params'), kwargs.get('json')))
        out = self.handler(method, url, kwargs.get('params') or {}, kwargs.get('json'))
        if isinstance(out, BaseException):
            raise out
        return out

    def get(self, url, **kwargs):
        return self._do('GET', url, kwargs)

    def post(self, url, **kwargs):
        return self._do('POST', url, kwargs)


def jup_ok(params, out=BIG_OUT):
    return Resp(200, {'inputMint': params['inputMint'], 'outputMint': params['outputMint'],
                      'inAmount': params['amount'], 'outAmount': out, 'otherAmountThreshold': out[:-1] or '1',
                      'priceImpactPct': '0.02', 'contextSlot': 77, 'routePlan': [{'swapInfo': {'label': 'Pump.fun'}}]})


def config(tmp_path, **over):
    cfg = CollectorConfig(data_dir=str(tmp_path / 'tel'))
    cfg.horizons = parse_horizons(over.pop('horizons', '1s,5s,60s'))
    cfg.quote_horizons = over.pop('quote_horizons', ['5s'])
    cfg.quote_sample_rate = 1.0
    cfg.jupiter_rpm = cfg.dexscreener_rpm = cfg.rpc_rpm = 6000
    cfg.discovery = 'none'
    cfg.backup_interval_hours = 0
    cfg.shadow_quote_deadline_sec = 0.5
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def rows(col, table, where='1=1'):
    col.observer.drain(col.store)
    col.store.db.row_factory = None
    cur = col.store.db.execute(f'SELECT * FROM {table} WHERE {where} ORDER BY row_seq')
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


async def settle(col):
    for _ in range(50):
        if not col.tasks:
            return
        await asyncio.gather(*list(col.tasks), return_exceptions=True)


# --------------------------------------------------------------- discovery

def test_collector_keeps_duplicate_detections_and_one_candidate(tmp_path):
    clock = Clock()
    col = Collector(config(tmp_path, quote_sample_rate=0.0), session=FakeHttp(lambda *a: Resp(500)),
                    clock=clock, start_writer=False)
    msg = {'txType': 'create', 'mint': MINT, 'signature': 'A', 'name': '', 'symbol': 'X',
           'traderPublicKey': 'Cr', 'bondingCurveKey': 'Cv'}

    async def scenario():
        col.on_pumpportal_message(msg)
        clock.t += 0.3
        col.on_pumpportal_message(dict(msg, signature='B'))
    asyncio.run(scenario())
    dets = rows(col, 'detections')
    assert [d['dedup_decision'] for d in dets] == ['first_seen_by_collector', 'duplicate_mint_collector']
    assert {d['candidate_id'] for d in dets} == {candidate_id('solana-mainnet', MINT)}
    assert dets[0]['metadata_status'] == 'partial' and dets[0]['name'] is None
    assert len(col.candidates) == 1
    not_sampled = rows(col, 'quotes')
    assert [q['status'] for q in not_sampled] == ['not_sampled', 'not_sampled']   # explicit, not silent
    col.store.close()


# ----------------------------------------------------------- shadow quotes

def test_shadow_buy_then_exact_size_sell_quote_only(tmp_path):
    def handler(method, url, params, body):
        assert method == 'GET' and url.endswith('/quote')
        if params['inputMint'] == WSOL:
            return jup_ok(params)
        assert params['amount'] == BIG_OUT            # exact amount from the buy quote
        return jup_ok(params, out='1666666')

    clock = Clock()
    http = FakeHttp(handler)
    col = Collector(config(tmp_path), session=http, clock=clock, start_writer=False)
    col.sol_usd = (clock(), 150.0)

    async def scenario():
        col.register_candidate(MINT, 'Pump.fun', clock(), 'ev-1')
        await settle(col)
    asyncio.run(scenario())
    quotes = rows(col, 'quotes')
    buys = [q for q in quotes if q['purpose'] == 'shadow_buy']
    sells = [q for q in quotes if q['purpose'] == 'shadow_sell_exact']
    assert [q['status'] for q in buys] == ['ok', 'ok'] and [q['status'] for q in sells] == ['ok', 'ok']
    assert [q['notional_rub'] for q in buys] == [250.0, 500.0]
    assert buys[0]['amount_raw'] == str(int(250 / (150 * 90) * 1e9))
    assert sells[0]['amount_raw'] == BIG_OUT and buys[0]['out_amount_raw'] == BIG_OUT
    assert sells[0]['quote_pair_id'] == buys[0]['quote_pair_id']
    assert all(q['quote_only'] == 1 and q['transaction_sent'] == 0 for q in quotes)
    assert all(call[0] == 'GET' for call in http.calls)
    assert col.candidates[candidate_id('solana-mainnet', MINT)].shadow_tokens_raw == int(BIG_OUT)
    col.store.close()


def test_quote_errors_are_explicit_statuses(tmp_path):
    seq = iter([Resp(429, {'error': 'rate'}, {'Retry-After': '30'}),
                asyncio.TimeoutError(), asyncio.TimeoutError(), asyncio.TimeoutError(),
                Resp(400, {'error': 'no route', 'errorCode': 'COULD_NOT_FIND_ANY_ROUTE'})])
    clock = Clock()
    col = Collector(config(tmp_path, notionals_rub=[250.0]), session=FakeHttp(lambda *a: next(seq)),
                    clock=clock, start_writer=False)
    col.sol_usd = (clock(), 150.0)
    cand = col.register_candidate(MINT, 'Pump.fun', clock(), 'e', recovering=True)

    async def scenario():
        out = []
        out.append(await col._quote(cand, 'shadow_buy', WSOL, MINT, 1000, clock() + 0.3))
        out.append(await col._quote(cand, 'shadow_buy', WSOL, MINT, 1000, clock() + 0.3))  # cooldown -> no request
        col.lim_jup.cooldown_until = 0
        out.append(await col._quote(cand, 'shadow_buy', WSOL, MINT, 1000, clock() + 60))   # 3 timeouts
        out.append(await col._quote(cand, 'shadow_buy', WSOL, MINT, 1000, clock() + 60))   # no route
        return [o[0] for o in out]

    col.lim_jup = ProviderLimiter('jupiter', 6000, clock=clock, rng=lambda: 0.5)

    async def fake_sleep(seconds):
        clock.t += seconds                # virtual time: no real waiting
        await asyncio.sleep(0)
    col.sleep = fake_sleep
    statuses = asyncio.run(scenario())
    assert statuses == ['http_429', 'skipped_provider_cooldown', 'timeout', 'no_route']
    q = rows(col, 'quotes')
    assert [r['status'] for r in q] == ['http_429', 'skipped_provider_cooldown', 'timeout', 'timeout', 'timeout', 'no_route']
    assert q[0]['http_status'] == 429 and q[1]['http_status'] is None
    assert all(r['out_amount_raw'] is None for r in q)            # no fake zero
    col.store.close()


def test_stale_sol_usd_is_not_used(tmp_path):
    clock = Clock()
    col = Collector(config(tmp_path), session=FakeHttp(lambda *a: Resp(500)), clock=clock, start_writer=False)
    col.sol_usd = (clock() - 10_000, 150.0)

    async def scenario():
        col.register_candidate(MINT, 'Pump.fun', clock(), 'e')
        await settle(col)
    asyncio.run(scenario())
    statuses = {r['status'] for r in rows(col, 'quotes', "purpose='shadow_buy'")}
    assert statuses == {'skipped_stale_sol_usd'}
    col.store.close()


# --------------------------------------------------------------- followups

def test_followups_market_observations_and_not_indexed(tmp_path):
    def handler(method, url, params, body):
        assert 'dexscreener' in url and method == 'GET'
        return Resp(200, [{'baseToken': {'address': MINT, 'symbol': 'ABC', 'name': 'Альфа'},
                           'quoteToken': {'address': WSOL}, 'pairAddress': 'PairFull' + 'x' * 36,
                           'dexId': 'pumpswap', 'priceUsd': '0.0001', 'priceNative': '0.0000007',
                           'liquidity': {'usd': 9000, 'base': 1e8, 'quote': 30}, 'volume': {'m5': 10},
                           'fdv': 100000, 'marketCap': 90000, 'pairCreatedAt': 1_800_000_000_000}])

    clock = Clock()
    col = Collector(config(tmp_path, quote_sample_rate=0.0), session=FakeHttp(handler), clock=clock,
                    start_writer=False)

    async def scenario():
        col.register_candidate(MINT, 'Pump.fun', clock(), 'a1')
        col.register_candidate(MINT2, 'Raydium CPMM', clock(), 'a2')
        clock.t += 1.2
        col._start_due_jobs()
        col._flush_batches()              # coalescing window still open: nothing sent yet
        assert col.session.calls == []
        clock.t += 0.9
        col._flush_batches()
        await settle(col)
    asyncio.run(scenario())
    obs = rows(col, 'observations', "obs_type='market_dexscreener'")
    by_mint = {o['mint']: o for o in obs}
    assert by_mint[MINT]['status'] == 'ok' and by_mint[MINT]['price_usd'] == 0.0001
    assert by_mint[MINT]['price_sol'] == 0.0000007 and by_mint[MINT]['pair_address'].startswith('PairFull')
    assert by_mint[MINT2]['status'] == 'not_indexed' and by_mint[MINT2]['price_usd'] is None
    fus = rows(col, 'followups')
    assert {(f['mint'], f['horizon_label'], f['status']) for f in fus} == {(MINT, '1s', 'complete'), (MINT2, '1s', 'failed')}
    f = [x for x in fus if x['mint'] == MINT][0]
    assert f['delay_ms'] == pytest.approx(1100.0, abs=1) and f['target_at_utc'] < f['actual_at_utc']
    assert json.loads(f['observation_event_ids']) == [by_mint[MINT]['event_id']]
    col.store.close()


def test_rate_budget_exhaustion_is_recorded_not_hidden(tmp_path):
    clock = Clock()
    col = Collector(config(tmp_path, quote_sample_rate=0.0), session=FakeHttp(lambda *a: Resp(200, [])),
                    clock=clock, start_writer=False)
    col.lim_dex = ProviderLimiter('dexscreener', 1, burst=0.5, clock=clock)   # no budget at all

    async def scenario():
        col.register_candidate(MINT, 'Pump.fun', clock(), 'a1')
        clock.t += 1.0
        col._start_due_jobs()
        col._flush_batches()
        clock.t += 5.0                        # past the 1s horizon tolerance
        col._flush_batches()
        await settle(col)
    asyncio.run(scenario())
    obs = rows(col, 'observations')
    assert obs[0]['status'] == 'skipped_rate_budget_exhausted'
    fu = rows(col, 'followups')[0]
    assert fu['status'] == 'failed' and 'skipped_rate_budget_exhausted' in fu['status_reason']
    col.store.close()


def test_restart_recovery_marks_missed_and_reschedules(tmp_path):
    clock = Clock()
    cfg = config(tmp_path, horizons='1s,5s,1h', quote_horizons=[], quote_sample_rate=0.0)
    col = Collector(cfg, session=FakeHttp(lambda *a: Resp(500)), clock=clock, start_writer=False)
    col.emit('detections', mint=MINT, source='Pump.fun', detection_type='pumpportal_create',
             _received_ts=clock() - 600)
    col.observer.drain(col.store)
    col.store.close()

    col2 = Collector(cfg, session=FakeHttp(lambda *a: Resp(500)), clock=clock, start_writer=False)
    assert col2.recover() == 1
    fus = rows(col2, 'followups')
    assert {(f['horizon_label'], f['status'], f['status_reason']) for f in fus} == {
        ('1s', 'missed', 'collector_not_running_at_target'), ('5s', 'missed', 'collector_not_running_at_target')}
    assert [h[3] for h in col2.heap] == ['1h']
    col2.store.close()


def test_curve_rpc_readonly_and_stale_and_provider_error(tmp_path):
    import base64
    import struct
    raw = b'\x00' * 8 + struct.pack('<QQQQQ', 1_073_000_191_279_242, 30_000_000_000, 793_100_000_000_000,
                                    0, 1_000_000_000_000_000) + b'\x00'
    data = base64.b64encode(raw).decode()
    slots = iter([1000, 500])

    def handler(method, url, params, body):
        assert method == 'POST' and body['method'] == 'getMultipleAccounts'
        return Resp(200, {'result': {'context': {'slot': next(slots)}, 'value': [{'data': [data, 'base64']}]}})

    clock = Clock()
    cfg = config(tmp_path, horizons='1s,5s', quote_horizons=[], quote_sample_rate=0.0,
                 rpc_url='https://rpc.example/?api-key=SECRET-KEY-XYZ-123')
    col = Collector(cfg, session=FakeHttp(handler), clock=clock, start_writer=False)
    col.lim_dex = ProviderLimiter('dexscreener', 1, burst=0.5, clock=clock)

    async def scenario():
        col.register_candidate(MINT, 'Pump.fun', clock(), 'a1', curve='CurveFull111')
        for step in (1.0, 2.9):
            clock.t += step
            col._start_due_jobs()
            clock.t += 1.1
            col._flush_batches()
            await settle(col)
    asyncio.run(scenario())
    curve = rows(col, 'observations', "obs_type='curve_rpc'")
    assert [c['status'] for c in curve] == ['ok', 'stale']
    assert curve[0]['curve_virtual_sol_raw'] == '30000000000' and curve[0]['curve_complete'] == 0
    assert curve[0]['price_sol'] == pytest.approx(30 / 1_073_000_191.279242)
    col.observer.drain(col.store)
    everything = (Path(cfg.data_dir) / 'raw_events.jsonl').read_text(encoding='utf-8')
    assert 'SECRET-KEY-XYZ-123' not in everything
    col.store.close()

    async def forbidden():
        rpc = providers.ReadOnlyRpc('https://rpc.example')
        with pytest.raises(providers.ForbiddenRpcMethod):
            await rpc.call(FakeHttp(lambda *a: pytest.fail('must not do IO')), 'sendTransaction', [])
        err = await rpc.call(FakeHttp(lambda *a: Resp(200, {'error': {'code': -32005, 'message': 'busy'}})),
                             'getSlot', [])
        assert err.status == 'provider_error'
    asyncio.run(forbidden())


def test_jupiter_swap_endpoint_is_refused():
    with pytest.raises(providers.ForbiddenEndpoint):
        providers.validate_quote_url('https://lite-api.jup.ag/swap/v1/swap')
    assert providers.validate_quote_url('https://lite-api.jup.ag/swap/v1/quote')


def test_sampling_is_deterministic_and_outcome_independent():
    cid = candidate_id('solana-mainnet', MINT)
    assert sampled_for_quotes(cid, 0.5) == sampled_for_quotes(cid, 0.5)
    share = sum(sampled_for_quotes(candidate_id('solana-mainnet', f'm{i}'), 0.1) for i in range(5000)) / 5000
    assert 0.08 < share < 0.12


def test_limiter_respects_retry_after_and_backoff():
    clock = Clock(100.0)
    lim = ProviderLimiter('x', 600, burst=5, clock=clock, rng=lambda: 0.5)
    assert lim.try_acquire() is None
    lim.release('http_429', retry_after=12)
    assert lim.try_acquire() == 'provider_cooldown'
    clock.t += 12.1
    assert lim.try_acquire() is None
    lim.release('http_429')                       # 2nd consecutive 429: 2*2^1 = 4s (jitter x1.0)
    assert lim.cooldown_until == pytest.approx(clock.t + 4.0)


# ---------------------------------------------------- bot spool -> SQLite

def test_bot_spool_ingested_once_and_registers_candidates(tmp_path):
    data_dir = tmp_path / 'tel'
    observer = Observer()
    sink = SpoolSink(data_dir)
    writer = WriterThread(observer, sink, interval=0.01)
    writer.start()
    hooks.install(observer, writer, sink)
    raw_id = hooks.pumpportal_raw({'txType': 'create', 'mint': MINT, 'name': 'N', 'symbol': 'S',
                                   'vTokensInBondingCurve': 1, 'vSolInBondingCurve': 1}, set())
    hooks.pumpportal_raw({'txType': 'create', 'mint': MINT, 'vTokensInBondingCurve': 1,
                          'vSolInBondingCurve': 1}, {MINT})
    hooks.shutdown()
    assert raw_id

    clock = Clock()
    col = Collector(config(tmp_path, quote_sample_rate=0.0), session=FakeHttp(lambda *a: Resp(500)),
                    clock=clock, start_writer=False)

    async def scenario():
        col.loop = asyncio.get_running_loop()
        first = col.ingest_spool_once()
        await asyncio.sleep(0)
        second = col.ingest_spool_once()
        return first, second
    first, second = asyncio.run(scenario())
    assert first >= 3 and second == 0          # 2 detections + health config snapshot / shutdown
    dets = rows(col, 'detections')
    assert [d['dedup_decision'] for d in dets] == ['first_mint', 'duplicate_mint']
    assert all(d['detected_by'] == 'bot' for d in dets)
    assert candidate_id('solana-mainnet', MINT) in col.candidates
    col.store.close()


# ------------------------------------------------- full run loop (offline)

class FakeStream:
    def __init__(self, messages):
        self.messages = messages
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def send(self, data):
        self.sent.append(data)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for message in self.messages:
            yield message
        await asyncio.sleep(3600)          # idle stream until cancelled


def test_run_loop_end_to_end_offline_with_stop_file(tmp_path):
    def handler(method, url, params, body):
        assert method == 'GET'
        if 'latest/dex/tokens' in url:
            return Resp(200, {'pairs': [{'baseToken': {'address': WSOL}, 'priceUsd': '150', 'liquidity': {'usd': 1e7}}]})
        if 'tokens/v1/solana' in url:
            return Resp(200, [])
        return jup_ok(params) if params['inputMint'] == WSOL else jup_ok(params, out='1000')

    cfg = config(tmp_path, horizons='1s', quote_horizons=[], notionals_rub=[250.0], discovery='pumpportal',
                 health_interval_sec=0.2)
    stream = FakeStream([json.dumps({'message': 'Successfully subscribed'}),
                         json.dumps({'txType': 'create', 'mint': MINT, 'name': 'E2E', 'symbol': 'E'})])
    col = Collector(cfg, session=FakeHttp(handler), ws_connect=lambda *a, **k: stream)

    async def main():
        task = asyncio.create_task(col.run())
        await asyncio.sleep(2.5)
        (Path(cfg.data_dir) / 'STOP').write_text('stop')
        await asyncio.wait_for(task, 10)
    asyncio.run(main())

    import sqlite3
    db = sqlite3.connect(str(Path(cfg.data_dir) / 'collector.sqlite'))
    q = lambda sql: db.execute(sql).fetchall()
    assert q("SELECT dedup_decision FROM detections") == [('first_seen_by_collector',)]
    assert {r[0] for r in q("SELECT status FROM quotes WHERE purpose IN ('shadow_buy','shadow_sell_exact')")} == {'ok'}
    assert q("SELECT horizon_label, status FROM followups") == [('1s', 'failed')]          # not indexed yet
    assert q("SELECT status FROM observations WHERE obs_type='market_dexscreener'") == [('not_indexed',)]
    types = [r[0] for r in q('SELECT health_type FROM health ORDER BY row_seq')]
    assert types[0] == 'collector_started' and 'discovery_connected' in types and types[-1] == 'collector_stopped'
    assert json.loads(stream.sent[0]) == {'method': 'subscribeNewToken'}
    health_log = (Path(cfg.data_dir) / 'health.log').read_text(encoding='utf-8')
    assert 'collector_heartbeat' in health_log
