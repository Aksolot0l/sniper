"""Coin watcher: записывает ВСЕ новые монеты Pump.fun и следит за их ценой N минут.

Только чтение: одно WebSocket-подключение к PumpPortal (subscribeNewToken +
subscribeTokenTrade по каждой новой монете). Никаких ключей, подписей,
транзакций и запросов к кошельку. Цена берётся прямо из сделок
(vSolInBondingCurve / vTokensInBondingCurve), поэтому нет лимитов HTTP API.

Файлы одного запуска (OUT_DIR/run_YYYYmmdd_HHMMSSZ/):
  coins.csv    — каждая созданная монета (строка пишется сразу при создании)
  trades.csv   — каждая сделка по монете в окне наблюдения (можно отключить)
  moves.csv    — движения цены >= MOVE_PCT от предыдущей точки: вверх/вниз + гипотезы «почему»
  summary.csv  — итог по монете после окна (по умолчанию 5 минут): макс/мин/итог, исход, идеи
  watch.log    — статус раз в N секунд, переподключения, ошибки
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import json
import os
import random
import signal
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .model import utc_iso

PUMP_TOTAL_SUPPLY = 1_000_000_000.0
CURVE_START_VSOL = 30.0          # виртуальные SOL в кривой Pump.fun при создании
CURVE_COMPLETE_REAL_SOL = 85.0   # ~столько реальных SOL нужно до миграции (приближённо)

COINS_FIELDS = [
    'created_utc', 'mint', 'symbol', 'name', 'creator', 'signature', 'pool', 'uri',
    'dev_initial_buy_sol', 'dev_initial_buy_tokens', 'price0_sol', 'mcap0_sol',
]
TRADES_FIELDS = [
    'recv_utc', 'age_sec', 'mint', 'symbol', 'side', 'sol', 'tokens', 'trader',
    'is_creator', 'is_early_buyer', 'price_sol', 'change_from_start_pct', 'mcap_sol',
    'curve_progress_pct', 'signature',
]
MOVES_FIELDS = [
    'recv_utc', 'age_sec', 'mint', 'symbol', 'direction', 'from_price_sol', 'to_price_sol',
    'move_pct', 'change_from_start_pct', 'move_duration_sec', 'buys', 'sells',
    'sol_in', 'sol_out', 'unique_buyers', 'unique_sellers', 'reason_codes', 'why',
]
SUMMARY_FIELDS = [
    'mint', 'symbol', 'name', 'creator', 'created_utc', 'watched_sec', 'complete',
    'trades', 'buys', 'sells', 'unique_buyers', 'unique_sellers', 'sol_in', 'sol_out', 'net_sol',
    'price0_sol', 'price_final_sol', 'price_max_sol', 'price_min_sol',
    'change_final_pct', 'max_up_pct', 'max_down_pct', 'drawdown_from_peak_pct',
    't_max_sec', 't_min_sec', 'moves_up', 'moves_down',
    'dev_initial_buy_sol', 'dev_sold_pct', 'dev_first_sell_sec', 'dev_bought_more_sol',
    'early_buyers', 'early_buyers_sold',
    'largest_buy_sol', 'largest_sell_sol', 'mcap0_sol', 'mcap_final_sol',
    'curve_progress_final_pct', 'last_trade_sec', 'outcome', 'reason_codes', 'ideas',
]


def _f(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in (float('inf'), float('-inf')) else None


def _pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or not old:
        return None
    return (new / old - 1.0) * 100.0


def _r(value: Optional[float], nd: int = 2) -> Any:
    return '' if value is None else round(value, nd)


def _price(msg: Dict[str, Any]) -> Optional[float]:
    vsol, vtok = _f(msg.get('vSolInBondingCurve')), _f(msg.get('vTokensInBondingCurve'))
    if vsol and vtok:
        return vsol / vtok
    mcap = _f(msg.get('marketCapSol'))
    return mcap / PUMP_TOTAL_SUPPLY if mcap else None


def _progress(msg: Dict[str, Any]) -> Optional[float]:
    vsol = _f(msg.get('vSolInBondingCurve'))
    if vsol is None:
        return None
    return max(0.0, min(100.0, (vsol - CURVE_START_VSOL) / CURVE_COMPLETE_REAL_SOL * 100.0))


def _short(addr: Optional[str]) -> str:
    return f'{addr[:4]}…{addr[-4:]}' if addr and len(addr) > 10 else (addr or '?')


@dataclass
class WatchConfig:
    out_dir: str = '../coin_watch'
    window_sec: float = 300.0          # сколько следить за каждой монетой
    move_pct: float = 20.0             # порог движения для moves.csv
    big_trade_sol: float = 1.0         # «крупная» сделка
    early_sec: float = 10.0            # покупатели в первые N секунд = «снайперы/ранние»
    record_trades: bool = True
    duration_sec: Optional[float] = None   # None = до Ctrl+C / stop
    status_every_sec: float = 30.0
    max_active: int = 5000             # предохранитель от неограниченного роста подписок
    url: str = 'wss://pumpportal.fun/api/data'

    @classmethod
    def from_env(cls, out_dir: str, **overrides: Any) -> 'WatchConfig':
        env = os.environ
        cfg = cls(
            out_dir=out_dir,
            window_sec=float(env.get('WATCH_WINDOW_SEC', 300)),
            move_pct=float(env.get('WATCH_MOVE_PCT', 20)),
            big_trade_sol=float(env.get('WATCH_BIG_TRADE_SOL', 1.0)),
            early_sec=float(env.get('WATCH_EARLY_SEC', 10)),
            record_trades=env.get('WATCH_RECORD_TRADES', 'on').lower() not in ('0', 'off', 'false', 'no'),
            duration_sec=_f(env.get('WATCH_DURATION_SEC')) or None,
            url=env.get('WATCH_PUMPPORTAL_URL', cls.url),
        )
        for key, value in overrides.items():
            if value is not None:
                setattr(cfg, key, value)
        return cfg


@dataclass
class Trade:
    ts: float
    side: str
    sol: float
    tokens: float
    trader: str
    price: Optional[float]


@dataclass
class Coin:
    mint: str
    symbol: str
    name: str
    creator: str
    created_ts: float
    created_utc: str
    price0: Optional[float]
    mcap0: Optional[float]
    dev_sol: float
    dev_tokens: float
    last_price: Optional[float] = None
    max_price: Optional[float] = None
    min_price: Optional[float] = None
    t_max: float = 0.0
    t_min: float = 0.0
    anchor_price: Optional[float] = None
    anchor_ts: float = 0.0
    window: List[Trade] = field(default_factory=list)
    buys: int = 0
    sells: int = 0
    sol_in: float = 0.0
    sol_out: float = 0.0
    buyers: Set[str] = field(default_factory=set)
    sellers: Set[str] = field(default_factory=set)
    early_buyers: Set[str] = field(default_factory=set)
    early_sellers: Set[str] = field(default_factory=set)
    dev_sold_tokens: float = 0.0
    dev_first_sell_ts: Optional[float] = None
    dev_bought_more_sol: float = 0.0
    largest_buy: float = 0.0
    largest_sell: float = 0.0
    last_trade_ts: Optional[float] = None
    last_mcap: Optional[float] = None
    last_progress: Optional[float] = None
    moves_up: int = 0
    moves_down: int = 0
    move_codes: Counter = field(default_factory=Counter)


class CsvSink:
    """Append-only CSV файлы; каждая строка сразу сбрасывается на диск."""

    def __init__(self, run_dir: Path, record_trades: bool = True) -> None:
        self.run_dir = run_dir
        self._files: Dict[str, Any] = {}
        self._writers: Dict[str, csv.DictWriter] = {}
        tables = {'coins': COINS_FIELDS, 'moves': MOVES_FIELDS, 'summary': SUMMARY_FIELDS}
        if record_trades:
            tables['trades'] = TRADES_FIELDS
        for name, fields in tables.items():
            path = run_dir / f'{name}.csv'
            new = not path.exists()
            fh = open(path, 'a', newline='', encoding='utf-8')
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
            if new:
                writer.writeheader()
            self._files[name], self._writers[name] = fh, writer
        self.counts: Counter = Counter()

    def write(self, table: str, row: Dict[str, Any]) -> None:
        writer = self._writers.get(table)
        if writer is None:
            return
        writer.writerow(row)
        self._files[table].flush()
        self.counts[table] += 1

    def close(self) -> None:
        for fh in self._files.values():
            try:
                fh.close()
            except OSError:
                pass


class CoinWatcher:
    """Чистая логика без сети: сообщения PumpPortal -> строки CSV + (от)подписки."""

    def __init__(self, cfg: WatchConfig, sink: Any, clock: Callable[[], float] = time.time) -> None:
        self.cfg = cfg
        self.sink = sink
        self.clock = clock
        self.active: Dict[str, Coin] = {}
        self.stats: Counter = Counter()

    # ---------------------------------------------------------------- input
    def on_message(self, msg: Any, now: Optional[float] = None) -> List[str]:
        """Возвращает список mint, на сделки которых нужно подписаться."""
        if not isinstance(msg, dict) or not msg.get('mint'):
            self.stats['non_coin_messages'] += 1
            return []
        now = self.clock() if now is None else now
        tx = str(msg.get('txType') or '').lower()
        if tx == 'create':
            return self._on_create(msg, now)
        if tx in ('buy', 'sell'):
            self._on_trade(msg, tx, now)
        else:
            self.stats[f'other_tx_{tx or "none"}'] += 1
        return []

    def _on_create(self, msg: Dict[str, Any], now: float) -> List[str]:
        mint = str(msg['mint'])
        self.stats['coins_created'] += 1
        if mint in self.active:
            self.stats['duplicate_create'] += 1
            return []
        price0 = _price(msg)
        coin = Coin(
            mint=mint, symbol=str(msg.get('symbol') or ''), name=str(msg.get('name') or ''),
            creator=str(msg.get('traderPublicKey') or ''), created_ts=now, created_utc=utc_iso(now),
            price0=price0, mcap0=_f(msg.get('marketCapSol')),
            dev_sol=_f(msg.get('solAmount')) or 0.0, dev_tokens=_f(msg.get('initialBuy')) or 0.0,
            last_price=price0, max_price=price0, min_price=price0, anchor_price=price0, anchor_ts=now,
            last_mcap=_f(msg.get('marketCapSol')), last_progress=_progress(msg),
        )
        self.sink.write('coins', {
            'created_utc': coin.created_utc, 'mint': mint, 'symbol': coin.symbol, 'name': coin.name,
            'creator': coin.creator, 'signature': msg.get('signature') or '', 'pool': msg.get('pool') or '',
            'uri': msg.get('uri') or '', 'dev_initial_buy_sol': _r(coin.dev_sol, 4),
            'dev_initial_buy_tokens': _r(coin.dev_tokens, 0), 'price0_sol': _r(price0, 12),
            'mcap0_sol': _r(coin.mcap0, 3),
        })
        if len(self.active) >= self.cfg.max_active:
            self.stats['not_watched_max_active'] += 1
            return []
        self.active[mint] = coin
        return [mint]

    def _on_trade(self, msg: Dict[str, Any], side: str, now: float) -> None:
        coin = self.active.get(str(msg['mint']))
        if coin is None:
            self.stats['trades_for_unwatched'] += 1
            return
        self.stats['trades'] += 1
        age = now - coin.created_ts
        sol = _f(msg.get('solAmount')) or 0.0
        tokens = _f(msg.get('tokenAmount')) or 0.0
        trader = str(msg.get('traderPublicKey') or '')
        price = _price(msg)
        is_creator = bool(trader) and trader == coin.creator
        if side == 'buy':
            coin.buys += 1
            coin.sol_in += sol
            coin.buyers.add(trader)
            coin.largest_buy = max(coin.largest_buy, sol)
            if age <= self.cfg.early_sec and not is_creator:
                coin.early_buyers.add(trader)
            if is_creator:
                coin.dev_bought_more_sol += sol
        else:
            coin.sells += 1
            coin.sol_out += sol
            coin.sellers.add(trader)
            coin.largest_sell = max(coin.largest_sell, sol)
            if trader in coin.early_buyers:
                coin.early_sellers.add(trader)
            if is_creator:
                coin.dev_sold_tokens += tokens
                if coin.dev_first_sell_ts is None:
                    coin.dev_first_sell_ts = now
        coin.last_trade_ts = now
        coin.last_mcap = _f(msg.get('marketCapSol')) or coin.last_mcap
        coin.last_progress = _progress(msg) if _progress(msg) is not None else coin.last_progress
        trade = Trade(now, side, sol, tokens, trader, price)
        coin.window.append(trade)
        if price is not None:
            coin.last_price = price
            if coin.price0 is None:
                coin.price0 = coin.anchor_price = coin.max_price = coin.min_price = price
            if coin.max_price is None or price > coin.max_price:
                coin.max_price, coin.t_max = price, age
            if coin.min_price is None or price < coin.min_price:
                coin.min_price, coin.t_min = price, age
        self.sink.write('trades', {
            'recv_utc': utc_iso(now), 'age_sec': _r(age, 3), 'mint': coin.mint, 'symbol': coin.symbol,
            'side': side, 'sol': _r(sol, 6), 'tokens': _r(tokens, 2), 'trader': trader,
            'is_creator': int(is_creator), 'is_early_buyer': int(trader in coin.early_buyers),
            'price_sol': _r(price, 12), 'change_from_start_pct': _r(_pct(price, coin.price0)),
            'mcap_sol': _r(coin.last_mcap, 3), 'curve_progress_pct': _r(coin.last_progress, 1),
            'signature': msg.get('signature') or '',
        })
        if price is not None:
            self._check_move(coin, price, now)

    # ---------------------------------------------------------------- moves
    def _check_move(self, coin: Coin, price: float, now: float) -> None:
        change = _pct(price, coin.anchor_price)
        if change is None or abs(change) < self.cfg.move_pct:
            return
        direction = 'up' if change > 0 else 'down'
        codes, why = self.explain(coin, direction, coin.window, now)
        trades = coin.window
        buys = [t for t in trades if t.side == 'buy']
        sells = [t for t in trades if t.side == 'sell']
        self.sink.write('moves', {
            'recv_utc': utc_iso(now), 'age_sec': _r(now - coin.created_ts, 3), 'mint': coin.mint,
            'symbol': coin.symbol, 'direction': direction, 'from_price_sol': _r(coin.anchor_price, 12),
            'to_price_sol': _r(price, 12), 'move_pct': _r(change),
            'change_from_start_pct': _r(_pct(price, coin.price0)),
            'move_duration_sec': _r(now - coin.anchor_ts, 3), 'buys': len(buys), 'sells': len(sells),
            'sol_in': _r(sum(t.sol for t in buys), 4), 'sol_out': _r(sum(t.sol for t in sells), 4),
            'unique_buyers': len({t.trader for t in buys}), 'unique_sellers': len({t.trader for t in sells}),
            'reason_codes': ','.join(codes), 'why': '; '.join(why),
        })
        if direction == 'up':
            coin.moves_up += 1
        else:
            coin.moves_down += 1
        coin.move_codes.update(codes)
        self.stats[f'moves_{direction}'] += 1
        coin.anchor_price, coin.anchor_ts, coin.window = price, now, []

    def explain(self, coin: Coin, direction: str, trades: List[Trade], now: float) -> Tuple[List[str], List[str]]:
        """Эвристические гипотезы «почему» — это идеи, а не доказанные причины."""
        codes: List[str] = []
        why: List[str] = []
        buys = [t for t in trades if t.side == 'buy']
        sells = [t for t in trades if t.side == 'sell']
        sol_in, sol_out = sum(t.sol for t in buys), sum(t.sol for t in sells)
        span = max(0.0, now - (trades[0].ts if trades else now))
        age = now - coin.created_ts
        if direction == 'down':
            dev_sells = [t for t in sells if t.trader == coin.creator]
            if dev_sells:
                tok = sum(t.tokens for t in dev_sells)
                share = f' ({tok / coin.dev_tokens * 100:.0f}% его стартовых токенов)' if coin.dev_tokens else ''
                codes.append('dev_sell')
                why.append(f'создатель продал на {sum(t.sol for t in dev_sells):.2f} SOL{share} — похоже на слив разработчиком')
            if sells:
                big = max(sells, key=lambda t: t.sol)
                if big.sol >= self.cfg.big_trade_sol or (len(sells) > 1 and sol_out and big.sol / sol_out >= 0.5):
                    if big.trader != coin.creator:
                        codes.append('whale_sell')
                        why.append(f'крупная продажа {big.sol:.2f} SOL кошельком {_short(big.trader)}'
                                   f' ({big.sol / sol_out * 100:.0f}% всех продаж за движение)')
            early = {t.trader for t in sells if t.trader in coin.early_buyers}
            if early:
                codes.append('early_buyers_exit')
                why.append(f'ранние покупатели/снайперы (первые {self.cfg.early_sec:.0f} с) фиксируют прибыль: {len(early)} кошельк.')
            if len(sells) > len(buys) and buys:
                codes.append('sell_pressure')
                why.append(f'продаж больше, чем покупок ({len(sells)} против {len(buys)})')
            if not buys:
                codes.append('no_buyers')
                why.append('покупок за это время не было — спрос иссяк')
            if span <= 3 and len(sells) >= 1:
                codes.append('fast_crash')
                why.append(f'резкое падение за {span:.1f} с')
        else:
            dev_buys = [t for t in buys if t.trader == coin.creator]
            if dev_buys:
                codes.append('dev_buy')
                why.append(f'создатель докупает ({sum(t.sol for t in dev_buys):.2f} SOL) — поддерживает цену или заманивает')
            if buys:
                big = max(buys, key=lambda t: t.sol)
                if big.sol >= self.cfg.big_trade_sol or (len(buys) > 1 and sol_in and big.sol / sol_in >= 0.5):
                    codes.append('whale_buy')
                    why.append(f'крупная покупка {big.sol:.2f} SOL кошельком {_short(big.trader)}'
                               f' ({big.sol / sol_in * 100:.0f}% всех покупок за движение)')
            uniq = {t.trader for t in buys}
            if len(uniq) >= 5:
                codes.append('buy_wave')
                why.append(f'волна покупок: {len(uniq)} разных покупателей за {span:.1f} с (FOMO/боты)')
            repeat = [tr for tr, n in Counter(t.trader for t in buys).items() if n >= 3]
            if repeat:
                codes.append('repeat_buyer')
                why.append(f'кошелёк {_short(repeat[0])} купил 3+ раза подряд — возможна накрутка/бот')
            if age <= self.cfg.early_sec:
                codes.append('early_snipers')
                why.append(f'первые {age:.1f} с после создания — снайпер-боты заходят сразу')
            if coin.last_progress is not None and coin.last_progress >= 50:
                codes.append('curve_hype')
                why.append(f'кривая заполнена на ~{coin.last_progress:.0f}% — ожидание миграции на DEX')
        if not codes:
            codes.append('small_flow')
            why.append('явной причины нет — серия мелких сделок'
                       + (f' (покупки {sol_in:.2f} SOL / продажи {sol_out:.2f} SOL)' if trades else ''))
        return codes, why

    # -------------------------------------------------------------- expiry
    def tick(self, now: Optional[float] = None) -> List[str]:
        """Закрыть монеты, чьё окно истекло. Возвращает mint для отписки."""
        now = self.clock() if now is None else now
        done = [m for m, c in self.active.items() if now - c.created_ts >= self.cfg.window_sec]
        for mint in done:
            self._finalize(self.active.pop(mint), now, complete=True)
        return done

    def finalize_all(self, now: Optional[float] = None) -> int:
        now = self.clock() if now is None else now
        coins = list(self.active.values())
        self.active.clear()
        for coin in coins:
            self._finalize(coin, now, complete=False)
        return len(coins)

    def _finalize(self, coin: Coin, now: float, complete: bool) -> None:
        trades = coin.buys + coin.sells
        final = coin.last_price
        change_final = _pct(final, coin.price0)
        max_up = _pct(coin.max_price, coin.price0)
        max_down = _pct(coin.min_price, coin.price0)
        from_peak = _pct(final, coin.max_price)
        dev_sold_pct = (coin.dev_sold_tokens / coin.dev_tokens * 100.0) if coin.dev_tokens else None
        outcome, codes, ideas = self.classify(coin, change_final, max_up, from_peak, dev_sold_pct, now)
        self.stats[f'outcome_{outcome}'] += 1
        self.sink.write('summary', {
            'mint': coin.mint, 'symbol': coin.symbol, 'name': coin.name, 'creator': coin.creator,
            'created_utc': coin.created_utc, 'watched_sec': _r(now - coin.created_ts, 1),
            'complete': int(complete), 'trades': trades, 'buys': coin.buys, 'sells': coin.sells,
            'unique_buyers': len(coin.buyers), 'unique_sellers': len(coin.sellers),
            'sol_in': _r(coin.sol_in, 4), 'sol_out': _r(coin.sol_out, 4),
            'net_sol': _r(coin.sol_in - coin.sol_out, 4),
            'price0_sol': _r(coin.price0, 12), 'price_final_sol': _r(final, 12),
            'price_max_sol': _r(coin.max_price, 12), 'price_min_sol': _r(coin.min_price, 12),
            'change_final_pct': _r(change_final), 'max_up_pct': _r(max_up), 'max_down_pct': _r(max_down),
            'drawdown_from_peak_pct': _r(from_peak), 't_max_sec': _r(coin.t_max, 2),
            't_min_sec': _r(coin.t_min, 2), 'moves_up': coin.moves_up, 'moves_down': coin.moves_down,
            'dev_initial_buy_sol': _r(coin.dev_sol, 4), 'dev_sold_pct': _r(dev_sold_pct, 1),
            'dev_first_sell_sec': _r(coin.dev_first_sell_ts - coin.created_ts if coin.dev_first_sell_ts else None, 2),
            'dev_bought_more_sol': _r(coin.dev_bought_more_sol, 4),
            'early_buyers': len(coin.early_buyers), 'early_buyers_sold': len(coin.early_sellers),
            'largest_buy_sol': _r(coin.largest_buy, 4), 'largest_sell_sol': _r(coin.largest_sell, 4),
            'mcap0_sol': _r(coin.mcap0, 3), 'mcap_final_sol': _r(coin.last_mcap, 3),
            'curve_progress_final_pct': _r(coin.last_progress, 1),
            'last_trade_sec': _r(coin.last_trade_ts - coin.created_ts if coin.last_trade_ts else None, 2),
            'outcome': outcome, 'reason_codes': ','.join(codes), 'ideas': '; '.join(ideas),
        })

    def classify(self, coin: Coin, change_final: Optional[float], max_up: Optional[float],
                 from_peak: Optional[float], dev_sold_pct: Optional[float], now: float
                 ) -> Tuple[str, List[str], List[str]]:
        codes: List[str] = []
        ideas: List[str] = []
        trades = coin.buys + coin.sells
        cf = change_final or 0.0
        if trades == 0:
            return 'dead', ['no_trades'], ['после создания сделок не было — монету никто не заметил (или купил только создатель)']
        if dev_sold_pct is not None and dev_sold_pct >= 50 and (cf <= -30 or (from_peak or 0) <= -40):
            outcome = 'rug_dev_dump'
            ideas.append(f'создатель продал {dev_sold_pct:.0f}% токенов'
                         f' на {coin.dev_first_sell_ts - coin.created_ts:.0f}-й секунде — классический слив')
        elif (max_up or 0) >= 100 and (from_peak or 0) <= -50:
            outcome = 'pump_and_dump'
            ideas.append(f'взлетела на {max_up:.0f}% к {coin.t_max:.0f}-й секунде, потом упала на {abs(from_peak):.0f}% от пика')
        elif cf >= 50:
            outcome = 'pump'
            ideas.append(f'держит рост: +{cf:.0f}% за окно, покупатели продолжают заходить')
        elif cf <= -30:
            outcome = 'dump'
            ideas.append(f'упала на {abs(cf):.0f}% за окно')
        else:
            outcome = 'flat'
            ideas.append(f'без сильного движения: итог {cf:+.0f}%')
        codes.append(outcome)
        if dev_sold_pct and outcome != 'rug_dev_dump':
            codes.append('dev_sold')
            ideas.append(f'создатель продал {dev_sold_pct:.0f}% своих токенов')
        if coin.dev_bought_more_sol > 0:
            codes.append('dev_bought_more')
            ideas.append(f'создатель докупал на {coin.dev_bought_more_sol:.2f} SOL')
        if coin.early_buyers:
            share = len(coin.early_sellers) / len(coin.early_buyers) * 100
            if coin.early_sellers:
                codes.append('early_buyers_sold')
            ideas.append(f'ранних покупателей {len(coin.early_buyers)}, из них продали {share:.0f}%')
        if coin.largest_sell >= self.cfg.big_trade_sol:
            codes.append('whale_sell')
            ideas.append(f'самая крупная продажа {coin.largest_sell:.2f} SOL')
        if coin.largest_buy >= self.cfg.big_trade_sol:
            codes.append('whale_buy')
            ideas.append(f'самая крупная покупка {coin.largest_buy:.2f} SOL')
        if len(coin.buyers) <= 3:
            codes.append('few_buyers')
            ideas.append(f'всего {len(coin.buyers)} покупателей — почти нет интереса')
        elif len(coin.buyers) >= 30:
            codes.append('many_buyers')
            ideas.append(f'{len(coin.buyers)} разных покупателей — заметный интерес')
        if coin.last_trade_ts is not None:
            quiet = now - coin.last_trade_ts
            if quiet >= 60:
                codes.append('went_quiet')
                ideas.append(f'последняя сделка на {coin.last_trade_ts - coin.created_ts:.0f}-й секунде, дальше тишина {quiet:.0f} с')
        if coin.last_progress is not None and coin.last_progress >= 50:
            codes.append('curve_half_full')
            ideas.append(f'кривая заполнена на ~{coin.last_progress:.0f}%')
        common = [c for c, _ in coin.move_codes.most_common(3)]
        if common:
            ideas.append('частые причины движений: ' + ', '.join(common))
        return outcome, codes, ideas


# ======================================================================= runtime

def _log(run_dir: Path, text: str) -> None:
    line = f'{utc_iso()} {text}'
    print(line, flush=True)
    try:
        with open(run_dir / 'watch.log', 'a', encoding='utf-8') as fh:
            fh.write(line + '\n')
    except OSError:
        pass


async def watch_loop(cfg: WatchConfig, run_dir: Path, *, ws_connect: Any = None,
                     clock: Callable[[], float] = time.time,
                     stop_event: Optional[asyncio.Event] = None) -> Dict[str, Any]:
    stop_event = stop_event or asyncio.Event()
    if ws_connect is None:  # pragma: no cover - real network only
        import websockets
        ws_connect = websockets.connect
    sink = CsvSink(run_dir, cfg.record_trades)
    watcher = CoinWatcher(cfg, sink, clock)
    started = clock()
    last_status = started
    backoff = 1.0
    stop_file = Path(cfg.out_dir) / 'STOP'

    def should_stop() -> bool:
        if stop_event.is_set():
            return True
        if stop_file.exists():
            stop_event.set()
        if cfg.duration_sec and clock() - started >= cfg.duration_sec:
            stop_event.set()
        return stop_event.is_set()

    try:
        while not should_stop():
            try:
                async with ws_connect(cfg.url, open_timeout=15, ping_interval=20, ping_timeout=20,
                                      max_size=2 ** 23) as ws:
                    await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                    keys = list(watcher.active)
                    for i in range(0, len(keys), 200):   # после переподключения
                        await ws.send(json.dumps({'method': 'subscribeTokenTrade', 'keys': keys[i:i + 200]}))
                    _log(run_dir, f'connected; resubscribed {len(keys)} active coins')
                    backoff = 1.0
                    while not should_stop():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            raw = None
                        if raw is not None:
                            try:
                                msg = json.loads(raw)
                            except (TypeError, ValueError):
                                watcher.stats['invalid_json'] += 1
                                msg = None
                            subs = watcher.on_message(msg) if msg is not None else []
                            if subs:
                                await ws.send(json.dumps({'method': 'subscribeTokenTrade', 'keys': subs}))
                        expired = watcher.tick()
                        if expired:
                            await ws.send(json.dumps({'method': 'unsubscribeTokenTrade', 'keys': expired}))
                        if clock() - last_status >= cfg.status_every_sec:
                            last_status = clock()
                            _log(run_dir, 'status ' + json.dumps({
                                'coins': watcher.stats['coins_created'], 'watching': len(watcher.active),
                                'trades': watcher.stats['trades'], 'moves_up': watcher.stats['moves_up'],
                                'moves_down': watcher.stats['moves_down'],
                                'finished': sink.counts['summary']}, ensure_ascii=False))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                watcher.stats['disconnects'] += 1
                _log(run_dir, f'disconnected: {type(exc).__name__}: {str(exc)[:200]}; retry in {backoff:.0f}s '
                              f'(сделки за время разрыва пропущены)')
                watcher.tick()
                try:
                    await asyncio.wait_for(stop_event.wait(), backoff * (0.5 + random.random()))
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60.0)
    finally:
        unfinished = watcher.finalize_all()
        sink.close()
        result = {'run_dir': str(run_dir), 'files': dict(sink.counts), 'stats': dict(watcher.stats),
                  'unfinished_at_stop': unfinished}
        _log(run_dir, 'stopped ' + json.dumps(result, ensure_ascii=False))
    return result


def new_run_dir(out_dir: str) -> Path:
    from .store import ensure_data_dir
    root = ensure_data_dir(out_dir)
    stamp = dt.datetime.now(dt.timezone.utc).strftime('run_%Y%m%d_%H%M%SZ')
    run_dir = root / stamp
    n = 1
    while run_dir.exists():          # никогда не перезаписываем прошлые запуски
        run_dir = root / f'{stamp}_{n}'
        n += 1
    run_dir.mkdir()
    return run_dir


def run_watch(cfg: WatchConfig) -> int:  # pragma: no cover - real network only
    from .collector import SingleInstanceLock
    from .store import ensure_data_dir
    root = ensure_data_dir(cfg.out_dir)
    lock = SingleInstanceLock(root / 'watch.lock')
    if not lock.acquire():
        print('coin watcher already running for this dir (watch.lock is held)')
        return 2
    if (root / 'STOP').exists():
        (root / 'STOP').unlink()
    run_dir = new_run_dir(str(root))
    (root / 'watch.pid').write_text(str(os.getpid()), encoding='utf-8')
    (run_dir / 'config.json').write_text(json.dumps(cfg.__dict__, indent=2), encoding='utf-8')

    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        await watch_loop(cfg, run_dir, stop_event=stop)

    try:
        asyncio.run(main())
    finally:
        try:
            (root / 'watch.pid').unlink()
        except FileNotFoundError:
            pass
        lock.release()
    return 0


def report(run_dir: os.PathLike | str) -> Dict[str, Any]:
    """Офлайн-сводка по summary.csv и moves.csv одного запуска."""
    run_dir = Path(run_dir)
    rows = list(csv.DictReader(open(run_dir / 'summary.csv', encoding='utf-8')))
    moves = list(csv.DictReader(open(run_dir / 'moves.csv', encoding='utf-8'))) if (run_dir / 'moves.csv').exists() else []
    coins = sum(1 for _ in open(run_dir / 'coins.csv', encoding='utf-8')) - 1
    outcomes = Counter(r['outcome'] for r in rows)
    reasons = {d: Counter() for d in ('up', 'down')}
    for m in moves:
        for code in filter(None, m['reason_codes'].split(',')):
            reasons[m['direction']][code] += 1
    ups = sorted((_f(r['max_up_pct']) or 0.0 for r in rows), reverse=True)
    return {
        'coins_created': coins, 'coins_finished': len(rows),
        'outcomes': dict(outcomes.most_common()),
        'share_pct': {k: round(v / len(rows) * 100, 1) for k, v in outcomes.items()} if rows else {},
        'max_up_pct_median': ups[len(ups) // 2] if ups else None,
        'coins_with_max_up_ge_100pct': sum(1 for u in ups if u >= 100),
        'move_reasons_up': dict(reasons['up'].most_common(8)),
        'move_reasons_down': dict(reasons['down'].most_common(8)),
    }
