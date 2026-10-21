"""Offline tests for telemetry.watch (synthetic PumpPortal messages, fake websocket)."""

from __future__ import annotations

import asyncio
import csv
import json

from telemetry.watch import CoinWatcher, CsvSink, TxtSink, WatchConfig, new_run_dir, report, watch_loop

CREATOR = 'Creator1111111111111111111111111111111111'
SNIPER = 'Sniper22222222222222222222222222222222222'
WHALE = 'Whale333333333333333333333333333333333333'


def create(mint='MintAAAA1111111111111111111111111111pump', vsol=31.0, vtok=1_040_000_000.0):
    return {'txType': 'create', 'mint': mint, 'symbol': 'TST', 'name': 'Test', 'traderPublicKey': CREATOR,
            'signature': 'sigC', 'initialBuy': 30_000_000.0, 'solAmount': 1.0, 'pool': 'pump',
            'vSolInBondingCurve': vsol, 'vTokensInBondingCurve': vtok, 'marketCapSol': vsol / vtok * 1e9}


def trade(side, trader, sol, tokens, vsol, vtok, mint='MintAAAA1111111111111111111111111111pump'):
    return {'txType': side, 'mint': mint, 'traderPublicKey': trader, 'solAmount': sol, 'tokenAmount': tokens,
            'vSolInBondingCurve': vsol, 'vTokensInBondingCurve': vtok, 'marketCapSol': vsol / vtok * 1e9,
            'signature': f'sig-{side}-{vsol}'}


def rows(run_dir, name):
    with open(run_dir / f'{name}.csv', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


def make(tmp_path, **kw):
    cfg = WatchConfig(out_dir=str(tmp_path), **kw)
    run_dir = new_run_dir(str(tmp_path))
    sink = CsvSink(run_dir)
    return CoinWatcher(cfg, sink, clock=lambda: 0.0), sink, run_dir


def test_every_created_coin_is_written_and_subscribed(tmp_path):
    w, sink, run_dir = make(tmp_path)
    assert w.on_message(create('M1' + 'x' * 30), now=100.0) == ['M1' + 'x' * 30]
    assert w.on_message(create('M2' + 'x' * 30), now=101.0) == ['M2' + 'x' * 30]
    assert w.on_message({'message': 'Successfully subscribed'}, now=101.0) == []
    sink.close()
    coins = rows(run_dir, 'coins')
    assert [c['mint'][:2] for c in coins] == ['M1', 'M2']
    assert float(coins[0]['price0_sol']) > 0 and coins[0]['creator'] == CREATOR


def test_rug_by_dev_detected_with_reasons_and_unsubscribe_after_window(tmp_path):
    w, sink, run_dir = make(tmp_path, window_sec=300, move_pct=20)
    mint = 'MintAAAA1111111111111111111111111111pump'
    w.on_message(create(), now=0.0)
    # sniper buys early -> price up >20%
    w.on_message(trade('buy', SNIPER, 3.0, 80_000_000, 38.0, 850_000_000), now=2.0)
    w.on_message(trade('buy', WHALE, 2.0, 40_000_000, 40.0, 810_000_000), now=20.0)
    # dev dumps all initial tokens -> crash
    w.on_message(trade('sell', CREATOR, 2.5, 30_000_000, 33.0, 980_000_000), now=60.0)
    w.on_message(trade('sell', SNIPER, 1.5, 80_000_000, 31.0, 1_050_000_000), now=61.0)
    assert w.tick(now=299.0) == []
    assert w.tick(now=300.0) == [mint]
    assert mint not in w.active
    sink.close()
    moves = rows(run_dir, 'moves')
    assert moves[0]['direction'] == 'up' and 'early_snipers' in moves[0]['reason_codes']
    down = [m for m in moves if m['direction'] == 'down']
    assert down and 'dev_sell' in down[0]['reason_codes'] and 'создатель продал' in down[0]['why']
    (summary,) = rows(run_dir, 'summary')
    assert summary['outcome'] == 'rug_dev_dump' and summary['complete'] == '1'
    assert float(summary['dev_sold_pct']) == 100.0 and summary['early_buyers_sold'] == '1'
    assert float(summary['max_up_pct']) > 20 and summary['buys'] == '2' and summary['sells'] == '2'
    assert 'слив' in summary['ideas']
    assert len(rows(run_dir, 'trades')) == 4


def test_dead_coin_and_unfinished_at_stop(tmp_path):
    w, sink, run_dir = make(tmp_path)
    w.on_message(create('Dead' + 'x' * 30), now=0.0)
    w.tick(now=300.0)
    w.on_message(create('Live' + 'x' * 30), now=10.0)
    assert w.finalize_all(now=20.0) == 1
    sink.close()
    summary = {r['mint'][:4]: r for r in rows(run_dir, 'summary')}
    assert summary['Dead']['outcome'] == 'dead' and summary['Dead']['complete'] == '1'
    assert summary['Live']['complete'] == '0'   # окно не досмотрено — помечено


def test_trades_for_unknown_coins_ignored_and_bad_messages_safe(tmp_path):
    w, sink, run_dir = make(tmp_path)
    w.on_message(trade('buy', WHALE, 1, 1, 31, 1e9, mint='Unknown' + 'x' * 30), now=1.0)
    for bad in (None, [], 'x', {'mint': None}, {'mint': 'Z' * 32, 'txType': 'migrate'}):
        assert w.on_message(bad, now=1.0) == []
    w.on_message(create(), now=2.0)
    w.on_message({'txType': 'buy', 'mint': create()['mint'], 'solAmount': 'nan?'}, now=3.0)
    sink.close()
    assert w.stats['trades_for_unwatched'] == 1
    assert len(rows(run_dir, 'trades')) == 1


def test_previous_runs_are_never_overwritten(tmp_path):
    a = new_run_dir(str(tmp_path))
    b = new_run_dir(str(tmp_path))
    assert a != b and a.exists() and b.exists()


class FakeWS:
    def __init__(self, messages, stop):
        self.inbox = [json.dumps(m) for m in messages]
        self.sent = []
        self.stop = stop

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def recv(self):
        if self.inbox:
            return self.inbox.pop(0)
        self.stop.set()
        await asyncio.sleep(10)


def test_watch_loop_subscribes_unsubscribes_and_writes_report(tmp_path):
    clock = {'t': 0.0}

    def tick_clock():
        clock['t'] += 1.0
        return clock['t']

    msgs = [create(), trade('buy', WHALE, 2.0, 50_000_000, 40.0, 800_000_000)]
    stop = asyncio.Event()
    holder = {}

    def connect(url, **kw):
        assert url.startswith('wss://pumpportal.fun')
        holder['ws'] = FakeWS(msgs, stop)
        return holder['ws']

    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=3.0, status_every_sec=1e9, output_format='csv')
    run_dir = new_run_dir(str(tmp_path))
    result = asyncio.run(watch_loop(cfg, run_dir, ws_connect=connect, clock=tick_clock, stop_event=stop))
    sent = holder['ws'].sent
    assert sent[0] == {'method': 'subscribeNewToken'}
    assert {'method': 'subscribeTokenTrade', 'keys': [create()['mint']]} in sent
    assert {'method': 'unsubscribeTokenTrade', 'keys': [create()['mint']]} in sent
    assert all(m['method'] in ('subscribeNewToken', 'subscribeTokenTrade', 'unsubscribeTokenTrade') for m in sent)
    assert result['files']['coins'] == 1 and result['files']['summary'] == 1
    rep = report(run_dir)
    assert rep['coins_created'] == 1 and rep['coins_finished'] == 1
    assert rep['move_reasons_up'].get('whale_buy') == 1


def test_txt_format_is_default_and_readable(tmp_path):
    assert WatchConfig().output_format == 'txt'
    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=300, move_pct=20)
    run_dir = new_run_dir(str(tmp_path))
    sink = TxtSink(run_dir)
    w = CoinWatcher(cfg, sink, clock=lambda: 0.0)
    w.on_message(create(), now=0.0)
    w.on_message(trade('buy', SNIPER, 3.0, 80_000_000, 38.0, 850_000_000), now=2.0)
    w.on_message(trade('buy', WHALE, 2.0, 40_000_000, 40.0, 810_000_000), now=20.0)
    w.on_message(trade('sell', CREATOR, 2.5, 30_000_000, 31.0, 1_050_000_000), now=60.0)
    w.tick(now=300.0)
    (run_dir / 'report.txt').write_text(w.report_text(), encoding='utf-8')
    sink.close()
    assert not list(run_dir.glob('*.csv'))
    read = lambda n: (run_dir / f'{n}.txt').read_text(encoding='utf-8')
    assert 'НОВАЯ МОНЕТА' in read('coins') and create()['mint'] in read('coins')
    assert 'ПОКУПКА 3.0 SOL' in read('trades') and '(СОЗДАТЕЛЬ)' in read('trades')
    assert '▲ ВВЕРХ' in read('moves') and '▼ ВНИЗ' in read('moves') and 'ПОЧЕМУ:' in read('moves')
    assert 'ИСХОД: СЛИВ СОЗДАТЕЛЕМ' in read('summary') and 'ИДЕИ ПОЧЕМУ' in read('summary')
    everything = read('all')
    assert everything.index('НОВАЯ МОНЕТА') < everything.index('▲ ВВЕРХ') < everything.index('===== ИТОГ')
    assert 'ПОКУПКА' not in everything        # сделки только в trades.txt
    rep = read('report')
    assert 'Новых монет записано: 1' in rep and 'СЛИВ СОЗДАТЕЛЕМ: 1' in rep and 'создатель продаёт (dev_sell)' in rep
