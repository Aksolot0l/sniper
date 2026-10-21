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
    kw.setdefault('price_source', 'trades')
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
        self.empty = getattr(self, 'empty', 0) + 1
        if self.empty >= self.stop_after_empty:
            self.stop.set()
        await asyncio.sleep(0.2)
        raise asyncio.TimeoutError

    stop_after_empty = 4


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
        holder['ws'].stop_after_empty = 12
        return holder['ws']

    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=3.0, status_every_sec=1e9, output_format='csv',
                      price_source='trades')
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
    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=300, move_pct=20, price_source='trades')
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


# ----------------------------------------------------------------- rpc mode
import base64
import struct

from telemetry.solana_addr import (TOKEN_2022_PROGRAM, TOKEN_PROGRAM, associated_token_address,
                                   b58decode, b58encode, creator_ata_candidates)

K0 = 1073000191279242 * 30 * 10 ** 9
REAL_CREATOR = '2SssDAZBovyY7bnTYH2wFLDL2tpAvQBpAcEywUfbE8GN'
REAL_MINT = '2spSXsdnUPDst1xSGeNprfniqcncF6XiAvDKDoC2pump'
CURVE = 'Curve' + '1' * 39


def curve_account(vsol_sol, complete=False):
    vsol = int(vsol_sol * 1e9)
    vtok = K0 // vsol
    rsol = vsol - 30 * 10 ** 9
    raw = bytes(8) + struct.pack('<QQQQQ', vtok, vsol, 0, max(rsol, 0), 10 ** 15) + bytes([int(complete)]) + bytes(32)
    return {'data': [base64.b64encode(raw).decode(), 'base64']}


def token_account(tokens_ui):
    raw = bytes(64) + struct.pack('<Q', int(tokens_ui * 1e6)) + bytes(93)
    return {'data': [base64.b64encode(raw).decode(), 'base64']}


def rpc_create(vsol=31.0):
    msg = create(REAL_MINT, vsol=vsol, vtok=K0 / 1e15 * 1e9 / vsol * 1e-0)
    msg['vTokensInBondingCurve'] = (K0 // int(vsol * 1e9)) / 1e6
    msg['traderPublicKey'] = REAL_CREATOR
    msg['bondingCurveKey'] = CURVE
    msg['initialBuy'] = 30_000_000.0
    return msg


def test_ata_derivation_matches_reference_vectors():
    # эталон получен библиотекой solders (Pubkey.find_program_address), 600 случайных проверок совпали
    assert associated_token_address(REAL_CREATOR, REAL_MINT, TOKEN_PROGRAM) == 'GSGCZrRwsxmd7nmUsorasM1gTpd3bXA715YHVfWBqhim'
    assert associated_token_address(REAL_CREATOR, REAL_MINT, TOKEN_2022_PROGRAM) == 'BEYZs9dmwDuM2pRcGG5iCZs3voiHcyGzPajeRs47jPnN'
    assert b58encode(b58decode(REAL_MINT)) == REAL_MINT


class FakeRpc:
    """Отвечает getMultipleAccounts по сценарию: state[account] -> dict|None."""

    def __init__(self, state):
        self.state = state
        self.calls = []

    async def call(self, session, method, params):
        from telemetry.providers import HttpResult
        assert method == 'getMultipleAccounts'
        keys = params[0]
        self.calls.append(list(keys))
        return HttpResult('ok', 200, {'result': {'context': {'slot': 1}, 'value': [self.state.get(k) for k in keys]}})


def test_rpc_mode_tracks_price_dev_sell_and_writes_txt(tmp_path):
    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=300, move_pct=20, price_source='rpc')
    run_dir = new_run_dir(str(tmp_path))
    sink = TxtSink(run_dir)
    w = CoinWatcher(cfg, sink, clock=lambda: 0.0)
    w.on_message(rpc_create(), now=0.0)
    coin = w.active[REAL_MINT]
    spl_ata, t22_ata = creator_ata_candidates(REAL_CREATOR, REAL_MINT)
    assert coin.dev_atas == [spl_ata, t22_ata]
    batch = w.poll_accounts()
    assert [k for _, _, k in batch] == ['dev?', 'dev?', 'curve']   # счёт создателя раньше кривой
    state = {t22_ata: token_account(30_000_000), CURVE: curve_account(31.0)}
    w.on_accounts(batch, [state.get(a) for a, _, _ in batch], now=2.0)
    assert coin.dev_ata == t22_ata
    # покупки: 31 -> 40 SOL (рост цены ~66%)
    batch = w.poll_accounts()
    assert [k for _, _, k in batch] == ['dev', 'curve']
    state[CURVE] = curve_account(40.0)
    w.on_accounts(batch, [state.get(a) for a, _, _ in batch], now=4.0)
    # создатель продаёт всё, кривая падает
    state[t22_ata] = token_account(0)
    state[CURVE] = curve_account(31.5)
    w.on_accounts(batch, [state.get(a) for a, _, _ in batch], now=60.0)
    w.tick(now=300.0)
    sink.close()
    read = lambda n: (run_dir / f'{n}.txt').read_text(encoding='utf-8')
    assert '▲ ВВЕРХ' in read('moves') and 'зашло 9.00 SOL' in read('moves')
    assert '⚠ СОЗДАТЕЛЬ ПРОДАЛ 30000000 токенов (100.0% от стартовых)' in read('moves')
    down = [l for l in read('moves').splitlines() if '▼ ВНИЗ' in l][0]
    assert 'создатель продал 100% своих стартовых токенов' in down
    assert 'ИСХОД: СЛИВ СОЗДАТЕЛЕМ' in read('summary') and 'деньги в кривой: зашло' in read('summary')
    assert 'приток +9.000 SOL' in read('prices')


def test_rpc_mode_unsupported_curve_and_migration(tmp_path):
    cfg = WatchConfig(out_dir=str(tmp_path), price_source='rpc')
    run_dir = new_run_dir(str(tmp_path))
    sink = TxtSink(run_dir)
    w = CoinWatcher(cfg, sink, clock=lambda: 0.0)
    other = dict(rpc_create(), mint='Bonk' + 'x' * 40, bondingCurveKey='Pool' + '1' * 40, initialBuy=0)
    w.on_message(other, now=0.0)
    w.on_message(rpc_create(), now=0.0)
    garbage = {'data': [base64.b64encode(bytes(200)).decode(), 'base64']}
    for t in (1.0, 2.0, 3.0):
        batch = w.poll_accounts()
        vals = []
        for acct, mint, kind in batch:
            vals.append(garbage if mint.startswith('Bonk') else (curve_account(115.0, complete=(t == 3.0)) if kind == 'curve' else None))
        w.on_accounts(batch, vals, now=t)
    assert w.active['Bonk' + 'x' * 40].curve_ok is False
    assert w.active[REAL_MINT].migrated and w.poll_accounts() == []
    w.tick(now=400.0)
    sink.close()
    summary = (run_dir / 'summary.txt').read_text(encoding='utf-8')
    assert 'ЦЕНА НЕДОСТУПНА' in summary and 'УШЛА НА DEX' in summary
    assert '★ КРИВАЯ ЗАПОЛНЕНА' in (run_dir / 'all.txt').read_text(encoding='utf-8')


def test_watch_loop_rpc_mode_polls_and_never_subscribes_to_paid_trades(tmp_path):
    clock = {'t': 0.0}

    def fake_clock():
        clock['t'] += 0.05
        return clock['t']

    stop = asyncio.Event()
    holder = {}
    rpc = FakeRpc({CURVE: curve_account(45.0)})

    def connect(url, **kw):
        assert 'api-key' not in url
        ws = FakeWS([rpc_create()], stop)
        ws.stop_after_empty = 8
        holder['ws'] = ws
        return ws

    cfg = WatchConfig(out_dir=str(tmp_path), window_sec=1000, poll_sec=0.1, rpc_min_interval_sec=0.0,
                      status_every_sec=1e9, price_source='rpc')
    run_dir = new_run_dir(str(tmp_path))
    result = asyncio.run(watch_loop(cfg, run_dir, ws_connect=connect, clock=fake_clock, stop_event=stop,
                                    session=object(), rpc=rpc))
    assert [m['method'] for m in holder['ws'].sent] == ['subscribeNewToken']
    assert rpc.calls and CURVE in rpc.calls[0]
    assert result['stats']['price_ticks'] >= 1 and result['unfinished_at_stop'] == 1
    assert 'Solana RPC' in (run_dir / 'report.txt').read_text(encoding='utf-8')


def test_config_never_leaks_keys(monkeypatch, tmp_path):
    monkeypatch.setenv('PUMPPORTAL_API_KEY', 'secret-pp-key')
    monkeypatch.setenv('WATCH_RPC_URL', 'https://mainnet.example-rpc.com/?api-key=secret-rpc')
    cfg = WatchConfig.from_env(str(tmp_path))
    assert cfg.price_source == 'trades' and 'secret-pp-key' in cfg.ws_url()
    dumped = json.dumps(cfg.safe_dict())
    assert 'secret' not in dumped and 'secret' not in repr(cfg)
    monkeypatch.delenv('PUMPPORTAL_API_KEY')
    cfg = WatchConfig.from_env(str(tmp_path))
    assert cfg.price_source == 'rpc' and 'api-key' not in cfg.ws_url()
