"""Offline integration tests of the observation hooks inside the real bot
pipeline (detector -> filters -> trader -> paper lifecycle). No network, no
main.py, synthetic fixtures only."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import detector
import filters
import price
import trader
from models import FilterResult, NewCoin, Position, SwapQuote
from price import WSOL
from telemetry import hooks
from telemetry.observer import NullObserver, Observer
from trader import TraderState, _pos_to_dict
from wallet import Wallet

MINT = 'Hx7vZp9TQd8Lh1WkSRnqmG5u2pYVjE3cBfNa6t4KXpump'


class ListSink:
    def __init__(self):
        self.records = []

    def append(self, records, offsets=None):
        self.records.extend(records)
        return len(records), 0


class Exploding:
    enabled = True

    def emit(self, kind, fields):
        raise RuntimeError('collector is down')


@pytest.fixture
def obs():
    observer = Observer(capacity=100_000)
    hooks.install(observer)
    yield observer
    hooks.uninstall()


def drained(observer, kind=None):
    sink = ListSink()
    while observer.drain(sink):
        pass
    return [r for r in sink.records if kind is None or r['kind'] == kind]


def queued_kinds(observer):
    return [(kind, fields.get('stage')) for kind, fields in list(observer.queue.queue)]


def settings(**over):
    base = dict(http_rpc_url='http://rpc.invalid', min_liquidity_usd=500.0, honeypot_check=True,
                only_sol_pairs=True, indexer_retries=0, indexer_retry_delay_sec=0.0, mode='portfolio',
                max_positions=3, usd_rub_rate=90.0, entry_fee_sol=0.0005, max_trade_rub=1500.0,
                take_profit_pct=35.0, be_offset_pct=6.0, trail_pct=18.0, stop_loss_pct=10.0,
                max_hold_sec=120.0, max_entry_price_impact_pct=5.0, exit_fee_sol=0.0001)
    base.update(over)
    return SimpleNamespace(**base)


def coin(**over):
    data = dict(mint=MINT, source='Pump.fun', signature='5' * 88, curve='Curve1111', creator='Creator1111',
                packet={'v_sol': 30.0, 'v_tokens': 1.07e9, 'symbol': 'TST', 'name': 'Тест 🚀'}, decimals=6)
    data.update(over)
    return NewCoin(**data)


def state(slots=3):
    return TraderState(slots=asyncio.Semaphore(slots), filters=asyncio.Semaphore(6))


BUY = SwapQuote(WSOL, MINT, 55_555_555, 2_000_000_000_000, 1_980_000_000_000, 2.0, ('Pump.fun',), 1)
SELL = SwapQuote(MINT, WSOL, 1_980_000_000_000, 54_000_000, 53_460_000, 1.5, ('Pump.fun',), 2)


async def handle(c, st, wallet, verdict, quotes=None, cfg=None):
    async def fake_checks(*args, **kwargs):
        return verdict
    with patch('trader.run_all_checks', new=fake_checks), \
            patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)), \
            patch('trader.get_jupiter_quote', new=AsyncMock(side_effect=quotes or [None, None])), \
            patch('trader._spawn', side_effect=lambda coro: coro.close()):
        await trader._handle_coin(c, None, None, cfg or settings(), st, wallet,
                                  asyncio.Event(), asyncio.Event())


def decisions(records):
    return [(r['stage'], r['decision'], r['reason_code']) for r in records if r['kind'] == 'decisions']


# ------------------------------------------------- candidate before filters

def test_candidate_recorded_before_filters_and_kept_on_fail(obs, tmp_path):
    seen_at_filter_time = []

    async def checks(*args, **kwargs):
        seen_at_filter_time.extend(queued_kinds(obs))
        return FilterResult(False, ['ликвидность ниже порога $500 (DexScreener max $0; GeckoTerminal max $0)'])

    wallet = Wallet(str(tmp_path / 'w.json'), initial=1500)
    with patch('trader.run_all_checks', new=checks), patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
        asyncio.run(trader._handle_coin(coin(), None, None, settings(), state(), wallet,
                                        asyncio.Event(), asyncio.Event()))
    assert ('decisions', 'trader_intake') in seen_at_filter_time        # written BEFORE filters ran
    got = decisions(drained(obs))
    assert got[0] == ('trader_intake', 'PASS', 'received_by_trader')
    assert ('filters_total', 'FAIL', 'blocked_by_filters') in got
    assert ('entry', 'FAIL', 'blocked_by_filters') in got


def test_pass_but_slot_busy_is_recorded(obs, tmp_path):
    wallet = Wallet(str(tmp_path / 'w.json'), initial=1500)
    asyncio.run(handle(coin(), state(slots=0), wallet, FilterResult(True, ['ok'])))
    recs = drained(obs, 'decisions')
    busy = [r for r in recs if r['reason_code'] == 'slot_busy'][0]
    assert busy['decision'] == 'SKIP' and busy['outcome'] == 'slot_busy'
    assert json.loads(json.dumps(busy['threshold'])) == 3 and busy['unit'] == 'open_positions'
    assert ('filters_total', 'PASS', 'all_filters_passed') in decisions(recs)


def test_pass_but_no_quote_keeps_candidate_and_wallet(obs, tmp_path):
    wallet = Wallet(str(tmp_path / 'w.json'), initial=1500)
    st = state()
    asyncio.run(handle(coin(), st, wallet, FilterResult(True, ['ok']), quotes=[None]))
    got = decisions(drained(obs))
    assert ('entry', 'PASS', 'slot_acquired') in got
    assert ('paper_entry', 'SKIP', 'no_buy_quote') in got
    assert ('entry', 'FAIL', 'paper_entry_aborted') in got
    assert wallet.balance_rub == 1500 and wallet.open_positions == [] and st.open_count == 0


def test_mode_one_target_taken_and_small_balance_skips(obs, tmp_path):
    wallet = Wallet(str(tmp_path / 'w.json'), initial=1500)
    st = state()
    st.first_taken = True
    asyncio.run(handle(coin(), st, wallet, FilterResult(True, ['ok']), cfg=settings(mode='one')))
    poor = Wallet(str(tmp_path / 'p.json'), initial=1.5)
    asyncio.run(handle(coin(), state(), poor, FilterResult(True, ['ok'])))
    got = decisions(drained(obs))
    assert ('entry', 'SKIP', 'target_already_taken_mode_one') in got
    assert ('entry', 'SKIP', 'position_size_below_minimum') in got


def test_trading_outcome_identical_with_disabled_ok_or_broken_telemetry(tmp_path):
    outcomes = []
    for name, observer in (('off', NullObserver()), ('on', Observer()), ('broken', Exploding())):
        hooks.install(observer)
        try:
            wallet = Wallet(str(tmp_path / f'{name}.json'), initial=1500)
            st = state()
            asyncio.run(handle(coin(), st, wallet, FilterResult(True, ['ok']), quotes=[BUY, SELL]))
            outcomes.append((round(wallet.balance_rub, 6), [p['mint'] for p in wallet.open_positions],
                             wallet.open_positions[0]['tokens_raw'], st.open_count, st.first_taken))
            if name == 'on':
                life = drained(observer, 'lifecycle')
                assert life[0]['lifecycle_type'] == 'paper_entry_open'
                assert life[0]['is_real_transaction'] == 0 and life[0]['tokens_raw'] == '1980000000000'
        finally:
            hooks.uninstall()
    assert outcomes[0] == outcomes[1] == outcomes[2]


# ------------------------------------------------------------ filter stages

def test_each_filter_stage_recorded_with_value_threshold_and_unknown(obs):
    meta = {'mintAuthority': None, 'freezeAuthority': 'Frz1111', 'decimals': 6, 'supply': '1000000000000000'}
    c = coin(source='Raydium CPMM', curve=None, packet={})
    with patch('filters.fetch_mint_meta', new=AsyncMock(return_value=meta)), \
            patch('filters.check_sol_pair', new=AsyncMock(return_value=FilterResult(True, ['пара с SOL (DexScreener)']))), \
            patch('filters.check_honeypot_route', new=AsyncMock(side_effect=RuntimeError('boom'))), \
            patch('filters.check_liquidity', new=AsyncMock(return_value=FilterResult(False, ['ликвидность ниже порога $500 (DexScreener max $12; GeckoTerminal max $0)']))):
        verdict = asyncio.run(filters.run_all_checks(None, 'http://rpc.invalid', c, 500.0, True, True))
    # bot verdict unchanged by hooks
    assert verdict.passed is False
    assert 'проверка упала: boom' in verdict.reasons
    assert any('freeze authority НЕ отозван' in r for r in verdict.reasons)
    got = decisions(drained(obs))
    for expected in [('mint_meta', 'PASS', 'mint_meta_read'),
                     ('mint_authority', 'PASS', 'mint_authority_revoked'),
                     ('freeze_authority', 'FAIL', 'freeze_authority_present'),
                     ('supply', 'PASS', 'supply_ok'),
                     ('sol_pair', 'PASS', 'sol_pair_dexscreener'),
                     ('honeypot_route', 'UNKNOWN', 'check_exception'),
                     ('liquidity', 'FAIL', 'liquidity_below_threshold')]:
        assert expected in got, expected


def test_liquidity_threshold_and_observed_value_are_recorded(obs):
    pairs = [{'liquidity': {'usd': 12.5}}]
    with patch('filters.fetch_dexscreener_pairs', new=AsyncMock(return_value=pairs)), \
            patch('filters.fetch_geckoterminal_pools', new=AsyncMock(return_value=[])):
        result = asyncio.run(filters.check_liquidity(None, MINT, 500.0))
    assert result.passed is False
    metrics = {r['metric']: r for r in drained(obs, 'observations')}
    dex = metrics['liquidity.dexscreener_max_liquidity_usd']
    assert dex['metric_value'] == 12.5 and dex['threshold'] == 500.0 and dex['status'] == 'ok'
    gecko = metrics['liquidity.geckoterminal_max_reserve_usd']
    assert gecko['metric_value'] is None and gecko['status'] == 'not_indexed_or_error'   # no fake zero


def test_missing_metadata_returns_same_verdict_and_marks_not_evaluated(obs):
    c = coin(source='Raydium CPMM')
    with patch('filters.fetch_mint_meta', new=AsyncMock(return_value=None)), \
            patch('filters.check_sol_pair', new=AsyncMock(return_value=FilterResult(True, []))), \
            patch('filters.check_honeypot_route', new=AsyncMock(return_value=FilterResult(True, []))), \
            patch('filters.check_liquidity', new=AsyncMock(return_value=FilterResult(True, []))):
        verdict = asyncio.run(filters.run_all_checks(None, 'http://rpc.invalid', c, 500.0, True, True))
    assert verdict.passed is False and verdict.reasons == ['не прочитали mint-аккаунт — пропускаем']
    got = decisions(drained(obs))
    assert ('mint_meta', 'FAIL', 'mint_meta_unavailable') in got
    assert ('liquidity', 'UNKNOWN', 'not_evaluated_mint_meta_missing') in got


# ------------------------------------------------------------------- quotes

class FakeResp:
    def __init__(self, status, data=None):
        self.status, self.data = status, data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def json(self):
        return self.data


class FakeSession:
    def __init__(self, items):
        self.items = list(items)

    def get(self, url, **kwargs):
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


GOOD = {'inputMint': WSOL, 'outputMint': MINT, 'inAmount': '1000', 'outAmount': str(10 ** 25),
        'otherAmountThreshold': str(10 ** 25 - 1), 'priceImpactPct': '0.01', 'contextSlot': 9,
        'routePlan': [{'swapInfo': {'label': 'Pump.fun'}}]}


def test_jupiter_quote_statuses_are_explicit_and_quote_only(obs):
    hooks.quote_context('bot_entry_buy', notional_rub=500.0)
    cases = [FakeResp(429), asyncio.TimeoutError(), FakeResp(200, dict(GOOD, routePlan=[])),
             FakeResp(500), FakeResp(200, GOOD)]
    session = FakeSession(cases)
    results = [asyncio.run(price.get_jupiter_quote(session, WSOL, MINT, 1000)) for _ in cases]
    assert results[:4] == [None, None, None, None] and results[4].out_amount_raw == 10 ** 25
    quotes = drained(obs, 'quotes')
    assert [q['status'] for q in quotes] == ['http_429', 'timeout', 'no_route_or_mint_mismatch', 'http_error', 'ok']
    assert all(q['quote_only'] == 1 and q['transaction_sent'] == 0 for q in quotes)
    assert quotes[4]['out_amount_raw'] == str(10 ** 25)          # big raw amount exact, as string
    assert quotes[4]['purpose'] == 'bot_entry_buy' and quotes[4]['direction'] == 'buy'
    assert quotes[1].get('out_amount_raw') is None                   # failure is not a zero quote


def test_honeypot_probe_attempts_are_recorded(obs):
    session = FakeSession([FakeResp(429), FakeResp(400), FakeResp(200, {'outAmount': None})])
    with patch('filters.asyncio.sleep', new=AsyncMock()):
        result = asyncio.run(filters.check_honeypot_route(session, MINT))
    assert result.passed is False
    probes = drained(obs, 'quotes')
    assert [p['status'] for p in probes] == ['http_429', 'http_error', 'no_route']
    assert [p['attempt'] for p in probes] == [0, 1, 2]
    assert all(p['purpose'] == 'bot_honeypot_probe' and p['quote_only'] == 1 for p in probes)


# ---------------------------------------------------------------- detection

def test_helius_duplicates_kept_as_separate_detection_events(obs):
    detector._seen_signatures.clear()
    detector._seen_mints.clear()
    event = {'signature': 'SigA' * 22, 'err': None, 'logs': ['Program log: Instruction: Initialize']}

    async def scenario():
        events, coins = asyncio.Queue(), asyncio.Queue()
        for _ in range(2):
            await events.put(('Raydium CPMM', dict(event)))
        with patch('detector.fetch_mint_from_signature', new=AsyncMock(return_value=(MINT, None))):
            worker = asyncio.create_task(detector.detector_worker(events, coins, None, 'http://rpc.invalid', asyncio.Event()))
            await events.join()
            await asyncio.sleep(0.05)
            worker.cancel()
        return coins.qsize()

    assert asyncio.run(scenario()) == 1                  # bot behaviour unchanged
    dets = drained(obs, 'detections')
    logs = [d for d in dets if d['detection_type'] == 'helius_creation_log']
    assert [d['dedup_decision'] for d in logs] == ['first_signature', 'duplicate_signature']
    assert len({d['event_id'] for d in logs}) == 2
    resolved = [d for d in dets if d['detection_type'] == 'helius_mint_resolved']
    assert resolved[0]['mint'] == MINT and resolved[0]['related_event_id'] == logs[0]['event_id']


class FakeWS:
    def __init__(self, messages, watch_stop):
        self.messages, self.watch_stop = messages, watch_stop

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def send(self, data):
        return None

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for message in self.messages:
            yield message
        self.watch_stop.set()
        yield '{}'


def test_pumpportal_duplicates_and_empty_metadata(obs):
    detector._seen_mints.clear()
    create = {'txType': 'create', 'mint': MINT, 'signature': 'S1', 'name': 'Ракета 🚀', 'symbol': 'RKT',
              'vTokensInBondingCurve': 1.07e9, 'vSolInBondingCurve': 30.0, 'bondingCurveKey': 'CurveA',
              'traderPublicKey': 'CreatorA', 'marketCapSol': 28.0}
    empty = dict(create, mint='Other' + MINT[5:], name='', symbol='   ', signature='S2')
    stop = asyncio.Event()
    msgs = [json.dumps(create), json.dumps(dict(create, signature='S1-dup')), json.dumps(empty)]

    async def scenario():
        coins = asyncio.Queue()
        with patch('detector.websockets.connect', side_effect=lambda *a, **k: FakeWS(msgs, stop)):
            await detector.pumpportal_listener(coins, stop)
        return coins.qsize()

    assert asyncio.run(scenario()) == 2
    recs = drained(obs)
    dets = [r for r in recs if r['kind'] == 'detections']
    assert [d['dedup_decision'] for d in dets] == ['first_mint', 'duplicate_mint', 'first_mint']
    assert dets[0]['candidate_id'] == dets[1]['candidate_id'] and dets[0]['event_id'] != dets[1]['event_id']
    assert dets[0]['name'] == 'Ракета 🚀' and dets[0]['raw_json']['bondingCurveKey'] == 'CurveA'
    assert dets[2]['name'] is None and dets[2]['symbol'] is None and dets[2]['metadata_status'] == 'missing'
    parse = [d for d in decisions(recs) if d[0] == 'detector_pumpportal_parse']
    assert [p[1] for p in parse] == ['PASS', 'SKIP', 'PASS']


# --------------------------------------------------------- paper lifecycle

def test_paper_exit_lifecycle_pending_then_full_fill(obs, tmp_path):
    wallet = Wallet(str(tmp_path / 'w.json'), initial=1500)
    pos = Position(mint=MINT, source='Pump.fun', entry_price=0.000003, simulated=False, opened_at=1.0,
                   deposit_rub=500.0, tokens=1_980_000.0, decimals=6, tokens_raw=1_980_000_000_000,
                   invested_rub=504.5, fees_rub=4.5)
    wallet.open_position(504.5, _pos_to_dict(pos))
    quote = SwapQuote(MINT, WSOL, pos.tokens_raw, 100_000_000, 99_000_000, 1.5, ('Pump.fun',), 456)
    cfg = settings(trade_log_path=str(tmp_path / 'trades.log'))

    async def scenario():
        trades = asyncio.Queue()

        async def consumer():
            while True:
                await trades.get()
                trades.task_done()
        task = asyncio.create_task(consumer())
        st = TraderState(open_count=1)
        with patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
            with patch('trader.get_jupiter_quote', new=AsyncMock(return_value=None)):
                first = await trader.sell_part(pos, 1.0, 0.000003, 'Timeout', None, cfg, wallet, trades, st, asyncio.Event())
            with patch('trader.get_jupiter_quote', new=AsyncMock(return_value=quote)):
                second = await trader.sell_part(pos, 1.0, 0.000003, 'Timeout', None, cfg, wallet, trades, st, asyncio.Event())
        task.cancel()
        return first, second

    assert asyncio.run(scenario()) == (False, True)
    types = [r['lifecycle_type'] for r in drained(obs, 'lifecycle')]
    assert types == ['exit_trigger', 'exit_pending_no_quote', 'exit_trigger', 'paper_fill_full', 'paper_position_closed']
    assert not any(Path(tmp_path).glob('runs'))
