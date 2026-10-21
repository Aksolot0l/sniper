"""Coin watcher: записывает ВСЕ новые монеты Pump.fun и следит за их ценой N минут.

Только чтение: одно WebSocket-подключение к PumpPortal (subscribeNewToken +
subscribeTokenTrade по каждой новой монете). Никаких ключей, подписей,
транзакций и запросов к кошельку. Цена берётся прямо из сделок
(vSolInBondingCurve / vTokensInBondingCurve), поэтому нет лимитов HTTP API.

Файлы одного запуска (OUT_DIR/run_YYYYmmdd_HHMMSSZ/), по умолчанию обычный текст:
  all.txt      — всё подряд по времени: новые монеты, движения цены, итоги (без отдельных сделок)
  coins.txt    — каждая созданная монета (строка пишется сразу при создании)
  trades.txt   — каждая сделка по монете в окне наблюдения (можно отключить)
  moves.txt    — движения цены >= MOVE_PCT от предыдущей точки: вверх/вниз + гипотезы «почему»
  summary.txt  — итог по монете после окна (по умолчанию 5 минут): макс/мин/итог, исход, идеи
  report.txt   — общая статистика запуска (обновляется по ходу работы)
  watch.log    — статус раз в N секунд, переподключения, ошибки
С --format csv вместо *.txt пишутся coins/trades/moves/summary.csv (для таблиц/анализа).
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

import base64

from .model import utc_iso
from .providers import ReadOnlyRpc, parse_pump_curve
from .solana_addr import creator_ata_candidates, token_account_amount

PUMP_TOTAL_SUPPLY = 1_000_000_000.0
PUMP_DECIMALS = 6
DEFAULT_RPC_URL = 'https://api.mainnet-beta.solana.com'
PUMP_CURVE_DISCRIMINATOR = bytes.fromhex('17b7f83760d8ac60')   # sha256('account:BondingCurve')[:8]
CURVE_NOT_FOUND_GIVE_UP_SEC = 45.0   # публичный RPC может видеть новый счёт с задержкой
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
    'price_source', 'recv_utc', 'age_sec', 'mint', 'symbol', 'direction', 'from_price_sol', 'to_price_sol',
    'move_pct', 'change_from_start_pct', 'move_duration_sec', 'buys', 'sells',
    'sol_in', 'sol_out', 'unique_buyers', 'unique_sellers', 'reason_codes', 'why',
]
PRICES_FIELDS = [
    'recv_utc', 'age_sec', 'mint', 'symbol', 'price_sol', 'change_from_start_pct', 'mcap_sol',
    'curve_real_sol', 'flow_sol', 'curve_progress_pct',
]
DEV_FIELDS = ['recv_utc', 'age_sec', 'mint', 'symbol', 'creator', 'action', 'tokens', 'pct_of_initial',
              'dev_balance_tokens', 'price_sol', 'change_from_start_pct']
SUMMARY_FIELDS = [
    'price_source', 'migrated', 'price_ticks', 'mint', 'symbol', 'name', 'creator', 'created_utc', 'watched_sec', 'complete',
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
    output_format: str = 'txt'         # 'txt' (читаемый текст) или 'csv'
    # Откуда брать цену после создания:
    #   'rpc'    — бесплатно: опрос кривой Pump.fun и баланса создателя через Solana RPC getMultipleAccounts;
    #   'trades' — сделки из PumpPortal subscribeTokenTrade (ПЛАТНО у PumpPortal, нужен PUMPPORTAL_API_KEY).
    price_source: str = 'rpc'
    rpc_url: str = field(default=DEFAULT_RPC_URL, repr=False)
    poll_sec: float = 2.0              # как часто опрашивать цены всех монет под наблюдением
    rpc_min_interval_sec: float = 0.35  # не чаще ~3 запросов/с (лимит публичного RPC — 40/10 с на метод)
    pumpportal_api_key: Optional[str] = field(default=None, repr=False)

    def safe_dict(self) -> Dict[str, Any]:
        out = {k: v for k, v in self.__dict__.items() if k != 'pumpportal_api_key'}
        out['pumpportal_api_key'] = 'set' if self.pumpportal_api_key else None
        from urllib.parse import urlsplit
        parts = urlsplit(self.rpc_url)
        out['rpc_url'] = f'{parts.scheme}://{parts.hostname}' + ('/<скрыто>' if (parts.path.strip('/') or parts.query) else '')
        return out

    def ws_url(self) -> str:
        if self.pumpportal_api_key and self.price_source == 'trades':
            return f"{self.url}{'&' if '?' in self.url else '?'}api-key={self.pumpportal_api_key}"
        return self.url

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
            output_format=env.get('WATCH_FORMAT', 'txt').lower(),
            rpc_url=env.get('WATCH_RPC_URL') or DEFAULT_RPC_URL,
            poll_sec=float(env.get('WATCH_POLL_SEC', 2.0)),
            pumpportal_api_key=env.get('PUMPPORTAL_API_KEY') or None,
        )
        cfg.price_source = (env.get('WATCH_PRICE_SOURCE') or ('trades' if cfg.pumpportal_api_key else 'rpc')).lower()
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
    bonding_curve: str = ''
    pool: str = ''
    last_rsol: Optional[float] = None     # реальные SOL в кривой (rpc-режим)
    price_ticks: int = 0
    curve_ok: Optional[bool] = None
    curve_fail: int = 0
    curve_missing: int = 0
    curve_reason: str = ''
    migrated: bool = False
    dev_atas: List[str] = field(default_factory=list)
    dev_ata: Optional[str] = None
    dev_ata_misses: int = 0
    dev_balance: Optional[float] = None
    dev_bought_more_tokens: float = 0.0
    prev_tick_price: Optional[float] = None


class CsvSink:
    """Append-only CSV файлы; каждая строка сразу сбрасывается на диск."""

    def __init__(self, run_dir: Path, record_trades: bool = True) -> None:
        self.run_dir = run_dir
        self._files: Dict[str, Any] = {}
        self._writers: Dict[str, csv.DictWriter] = {}
        tables = {'coins': COINS_FIELDS, 'moves': MOVES_FIELDS, 'summary': SUMMARY_FIELDS, 'dev': DEV_FIELDS}
        if record_trades:
            tables['trades'] = TRADES_FIELDS
            tables['prices'] = PRICES_FIELDS
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


def _local(iso: Any) -> str:
    try:
        moment = dt.datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
    except ValueError:
        return str(iso)
    return moment.astimezone().strftime('%Y-%m-%d %H:%M:%S')


def _cap(price: Any) -> str:
    """Цена токена -> капитализация в SOL (у Pump.fun 1 млрд токенов) — так читать проще."""
    value = _f(price)
    return f'{value * PUMP_TOTAL_SUPPLY:.1f} SOL' if value else '?'


def _sgn(value: Any, suffix: str = '%') -> str:
    v = _f(value)
    return '?' if v is None else f'{v:+.1f}{suffix}'


def _tag(row: Dict[str, Any]) -> str:
    sym = row.get('symbol') or '?'
    return f"{sym} [{row.get('mint', '')}]"


OUTCOME_RU = {
    'dead': 'МЁРТВАЯ (сделок нет)', 'rug_dev_dump': 'СЛИВ СОЗДАТЕЛЕМ', 'pump_and_dump': 'ПАМП И СЛИВ',
    'pump': 'РОСТ', 'dump': 'ПАДЕНИЕ', 'flat': 'БЕЗ ДВИЖЕНИЯ',
    'migrated': 'УШЛА НА DEX (кривая заполнена)', 'no_price': 'ЦЕНА НЕДОСТУПНА',
}


REASON_RU = {
    'whale_buy': 'крупная покупка', 'buy_wave': 'волна покупок от многих кошельков',
    'repeat_buyer': 'один кошелёк покупает много раз', 'dev_buy': 'создатель докупает',
    'early_snipers': 'снайперы в первые секунды', 'curve_hype': 'кривая заполнена, ждут миграции',
    'dev_sell': 'создатель продаёт', 'whale_sell': 'крупная продажа',
    'early_buyers_exit': 'ранние покупатели фиксируют прибыль', 'sell_pressure': 'продаж больше покупок',
    'no_buyers': 'покупки прекратились', 'fast_crash': 'резкий обвал за секунды',
    'small_flow': 'без явной причины (мелкие сделки)', 'big_outflow': 'крупный отток вместе с продажей создателя',
    'fast_pump': 'резкий скачок за один интервал',
}


class TxtSink:
    """Читаемые .txt файлы (append-only, сброс на диск после каждой строки) + общий all.txt."""

    # таблица -> в какие файлы писать (сделки/тики цены — только в свой файл, они очень частые)
    ROUTES = {'coins': ('coins', 'all'), 'moves': ('moves', 'all'), 'summary': ('summary', 'all'),
              'dev': ('moves', 'all'), 'trades': ('trades',), 'prices': ('prices',)}

    def __init__(self, run_dir: Path, record_trades: bool = True) -> None:
        self.run_dir = run_dir
        self.record_trades = record_trades
        self._files: Dict[str, Any] = {}
        for name in ('all', 'coins', 'moves', 'summary'):
            self._open(name)
        self.counts: Counter = Counter()

    def _open(self, name: str) -> Any:
        if name not in self._files:
            self._files[name] = open(self.run_dir / f'{name}.txt', 'a', encoding='utf-8')
        return self._files[name]

    def _put(self, name: str, text: str) -> None:
        fh = self._open(name)
        fh.write(text + '\n')
        fh.flush()

    def write(self, table: str, row: Dict[str, Any]) -> None:
        if table in ('trades', 'prices') and not self.record_trades:
            return
        text = getattr(self, f'_fmt_{table}')(row) + ('\n' if table == 'summary' else '')
        for name in self.ROUTES.get(table, (table,)):
            self._put(name, text)
        self.counts[table] += 1

    @staticmethod
    def _fmt_prices(r: Dict[str, Any]) -> str:
        flow = _f(r.get('flow_sol'))
        flow_txt = '' if flow is None else (f' | приток +{flow:.3f} SOL' if flow > 0 else f' | отток {flow:.3f} SOL')
        return (f"{_local(r['recv_utc'])} | +{r['age_sec']}с | {_tag(r)} | капа {_cap(r.get('price_sol'))}"
                f" ({_sgn(r.get('change_from_start_pct'))} от старта){flow_txt}"
                f" | в кривой {r.get('curve_real_sol')} SOL | кривая {r.get('curve_progress_pct') or '?'}%")

    @staticmethod
    def _fmt_dev(r: Dict[str, Any]) -> str:
        if r['action'] == 'migrated':
            return (f"{_local(r['recv_utc'])} | +{r['age_sec']}с | {_tag(r)} | ★ КРИВАЯ ЗАПОЛНЕНА — монета ушла на DEX"
                    f" (капа {_cap(r.get('price_sol'))}, {_sgn(r.get('change_from_start_pct'))} от старта)")
        what = 'ПРОДАЛ' if r['action'] == 'sell' else 'ДОКУПИЛ'
        pct = f" ({r['pct_of_initial']}% от стартовых)" if r.get('pct_of_initial') not in ('', None) else ''
        return (f"{_local(r['recv_utc'])} | +{r['age_sec']}с | {_tag(r)} | ⚠ СОЗДАТЕЛЬ {what} {r['tokens']} токенов{pct}"
                f" | у него осталось {r.get('dev_balance_tokens')} | капа {_cap(r.get('price_sol'))}"
                f" ({_sgn(r.get('change_from_start_pct'))} от старта)")

    @staticmethod
    def _fmt_coins(r: Dict[str, Any]) -> str:
        name = f" ({r['name']})" if r.get('name') else ''
        return (f"{_local(r['created_utc'])} | НОВАЯ МОНЕТА | {r.get('symbol') or '?'}{name} | mint {r['mint']}"
                f" | создатель {r.get('creator') or '?'} | создатель сразу купил на {r.get('dev_initial_buy_sol') or 0} SOL"
                f" | стартовая капа {_cap(r.get('price0_sol'))} | площадка {r.get('pool') or '?'}")

    @staticmethod
    def _fmt_trades(r: Dict[str, Any]) -> str:
        who = ' (СОЗДАТЕЛЬ)' if str(r.get('is_creator')) == '1' else (
            ' (ранний покупатель)' if str(r.get('is_early_buyer')) == '1' else '')
        side = 'ПОКУПКА' if r['side'] == 'buy' else 'ПРОДАЖА'
        return (f"{_local(r['recv_utc'])} | +{r['age_sec']}с | {_tag(r)} | {side} {r['sol']} SOL"
                f" | кошелёк {r['trader']}{who} | капа {_cap(r.get('price_sol'))}"
                f" ({_sgn(r.get('change_from_start_pct'))} от старта) | кривая {r.get('curve_progress_pct') or '?'}%")

    @staticmethod
    def _fmt_moves(r: Dict[str, Any]) -> str:
        arrow = '▲ ВВЕРХ' if r['direction'] == 'up' else '▼ ВНИЗ'
        return (f"{_local(r['recv_utc'])} | +{r['age_sec']}с | {_tag(r)} | {arrow} {_sgn(r['move_pct'])}"
                f" за {r['move_duration_sec']}с | капа {_cap(r['from_price_sol'])} -> {_cap(r['to_price_sol'])}"
                f" ({_sgn(r.get('change_from_start_pct'))} от старта) | "
                + (f"покупок {r['buys']} на {r['sol_in']} SOL, продаж {r['sells']} на {r['sol_out']} SOL"
                   if r.get('price_source') == 'trades' else
                   f"зашло {r['sol_in']} SOL, вышло {r['sol_out']} SOL")
                + f" | ПОЧЕМУ: {r['why']}")

    @staticmethod
    def _fmt_summary(r: Dict[str, Any]) -> str:
        done = '' if str(r.get('complete')) == '1' else ' (НЕ ДОСМОТРЕНА — запуск остановлен раньше)'
        lines = [
            f"===== ИТОГ {r.get('symbol') or '?'} [{r['mint']}] через {r['watched_sec']}с{done} =====",
            f"  создана: {_local(r['created_utc'])}, создатель {r.get('creator') or '?'}",
            f"  ИСХОД: {OUTCOME_RU.get(r['outcome'], r['outcome'])}",
            f"  капа: старт {_cap(r['price0_sol'])} | максимум {_cap(r['price_max_sol'])} ({_sgn(r['max_up_pct'])}"
            f" на {r['t_max_sec'] or '?'}с) | минимум {_cap(r['price_min_sol'])} ({_sgn(r['max_down_pct'])})"
            f" | в конце {_cap(r['price_final_sol'])} ({_sgn(r['change_final_pct'])}, от пика {_sgn(r['drawdown_from_peak_pct'])})",
            (f"  сделки: {r['trades']} (покупок {r['buys']} на {r['sol_in']} SOL, продаж {r['sells']} на {r['sol_out']} SOL),"
             f" покупателей {r['unique_buyers']}, продавцов {r['unique_sellers']}"
             if r.get('price_source') == 'trades' else
             f"  деньги в кривой: зашло {r['sol_in']} SOL ({r['buys']} интервалов притока), вышло {r['sol_out']} SOL"
             f" ({r['sells']} интервалов оттока), чистый поток {r['net_sol']} SOL; изменений цены {r.get('price_ticks')}"),
            f"  движений: вверх {r['moves_up']}, вниз {r['moves_down']} | крупнейш. {'покупка' if r.get('price_source') == 'trades' else 'приток за интервал'}"
            f" {r['largest_buy_sol']} SOL, {'продажа' if r.get('price_source') == 'trades' else 'отток за интервал'}"
            f" {r['largest_sell_sol']} SOL" + (' | ★ ушла на DEX' if str(r.get('migrated')) == '1' else ''),
            f"  создатель: купил при создании {r['dev_initial_buy_sol']} SOL, продал {r['dev_sold_pct'] or 0}% токенов"
            + (f" (первая продажа на {r['dev_first_sell_sec']}с)" if r.get('dev_first_sell_sec') not in ('', None) else '')
            + (f", ранних покупателей {r['early_buyers']}, из них продали {r['early_buyers_sold']}"
               if r.get('price_source') == 'trades' else ''),
            f"  ИДЕИ ПОЧЕМУ: {r['ideas']}",
        ]
        return '\n'.join(lines)

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
        self.move_reasons: Dict[str, Counter] = {'up': Counter(), 'down': Counter()}
        self.max_ups: List[float] = []
        self.outcomes: Counter = Counter()

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
        coin.bonding_curve = str(msg.get('bondingCurveKey') or '')
        coin.pool = str(msg.get('pool') or '')
        vsol0 = _f(msg.get('vSolInBondingCurve'))
        coin.last_rsol = max(0.0, vsol0 - CURVE_START_VSOL) if vsol0 is not None else None
        if self.cfg.price_source == 'rpc' and coin.creator and coin.dev_tokens > 0:
            try:
                coin.dev_atas = creator_ata_candidates(coin.creator, mint)
            except (ValueError, KeyError):
                coin.dev_atas = []
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

    # ------------------------------------------------------- rpc price mode
    def poll_accounts(self) -> List[Tuple[str, str, str]]:
        """(account, mint, kind) для одного цикла опроса; счёт создателя идёт ПЕРЕД кривой,
        чтобы продажа создателя попала в окно до проверки движения цены."""
        out: List[Tuple[str, str, str]] = []
        for mint, coin in self.active.items():
            if coin.migrated or coin.curve_ok is False or not coin.bonding_curve:
                continue
            if coin.dev_ata:
                out.append((coin.dev_ata, mint, 'dev'))
            elif coin.dev_atas:
                out.extend((ata, mint, 'dev?') for ata in coin.dev_atas)
            out.append((coin.bonding_curve, mint, 'curve'))
        return out

    def on_accounts(self, batch: List[Tuple[str, str, str]], values: List[Any], now: Optional[float] = None) -> None:
        now = self.clock() if now is None else now
        found_dev: Dict[str, bool] = {}
        for (account, mint, kind), item in zip(batch, values):
            coin = self.active.get(mint)
            if coin is None:
                continue
            data = (item or {}).get('data') if isinstance(item, dict) else None
            b64 = data[0] if isinstance(data, list) and data else None
            if kind == 'curve':
                self._on_curve(coin, b64, now)
                continue
            raw = None
            if b64:
                try:
                    raw = base64.b64decode(b64)
                except (ValueError, TypeError):
                    raw = None
            amount = token_account_amount(raw)
            if amount is None:
                found_dev.setdefault(mint, False)
                continue
            found_dev[mint] = True
            coin.dev_ata = account
            self._on_dev_balance(coin, amount / 10 ** PUMP_DECIMALS, now)
        for mint, found in found_dev.items():
            coin = self.active.get(mint)
            if coin is not None and not found:
                coin.dev_ata_misses += 1
                if coin.dev_ata is None and coin.dev_ata_misses >= 3:
                    coin.dev_atas = []          # не нашли счёт создателя — больше не опрашиваем
                elif coin.dev_ata is not None:  # счёт закрыт = продал/перевёл всё
                    self._on_dev_balance(coin, 0.0, now)

    def _on_curve(self, coin: Coin, b64: Optional[str], now: float) -> None:
        if not b64:
            # счёт ещё не виден RPC-ноде (задержка) — ждём, а не сдаёмся сразу
            coin.curve_missing += 1
            if coin.curve_ok is None and now - coin.created_ts >= CURVE_NOT_FOUND_GIVE_UP_SEC:
                coin.curve_ok = False
                coin.curve_reason = 'not_found'
                self.stats['curve_not_found'] += 1
            return
        try:
            head = base64.b64decode(b64)[:8]
        except (ValueError, TypeError):
            head = b''
        if head != PUMP_CURVE_DISCRIMINATOR:
            coin.curve_fail += 1
            if coin.curve_ok is None and coin.curve_fail >= 2:
                coin.curve_ok = False      # счёт есть, но это не кривая Pump.fun (другой launchpad)
                coin.curve_reason = 'other_launchpad'
                self.stats['unsupported_curve'] += 1
            return
        info = parse_pump_curve(b64)
        if info.get('curve_complete'):
            if not coin.migrated:
                coin.migrated = True
                self.stats['migrated'] += 1
                coin.last_progress = 100.0
                self.sink.write('dev', self._dev_row(coin, now, 'migrated', 0.0))
            return
        price = info.get('price_sol')
        if info.get('status') != 'ok' or price is None:
            self.stats['curve_parse_failed'] += 1
            return
        if not info.get('curve_invariant_ok'):
            self.stats['curve_nonstandard_k'] += 1     # другие стартовые резервы — цена всё равно верна
        coin.curve_ok = True
        rsol = int(info['curve_real_sol_raw']) / 1e9
        vsol = int(info['curve_virtual_sol_raw']) / 1e9
        self.on_price(coin, float(price), rsol, now,
                      progress=max(0.0, min(100.0, (vsol - CURVE_START_VSOL) / CURVE_COMPLETE_REAL_SOL * 100.0)))

    def on_price(self, coin: Coin, price: float, rsol: float, now: float, progress: Optional[float] = None) -> None:
        flow = None if coin.last_rsol is None else rsol - coin.last_rsol
        coin.last_rsol = rsol
        if progress is not None:
            coin.last_progress = progress
        coin.last_mcap = price * PUMP_TOTAL_SUPPLY
        if coin.last_price is not None and abs(price - coin.last_price) <= coin.last_price * 1e-9:
            return
        age = now - coin.created_ts
        coin.price_ticks += 1
        self.stats['price_ticks'] += 1
        if flow is not None and abs(flow) > 1e-9:
            side = 'buy' if flow > 0 else 'sell'
            if side == 'buy':
                coin.buys += 1
                coin.sol_in += flow
                coin.largest_buy = max(coin.largest_buy, flow)
            else:
                coin.sells += 1
                coin.sol_out += -flow
                coin.largest_sell = max(coin.largest_sell, -flow)
            coin.window.append(Trade(now, side, abs(flow), 0.0, '', price))
        coin.last_trade_ts = now
        coin.prev_tick_price = coin.last_price
        coin.last_price = price
        if coin.price0 is None:
            coin.price0 = coin.anchor_price = coin.max_price = coin.min_price = price
        if coin.max_price is None or price > coin.max_price:
            coin.max_price, coin.t_max = price, age
        if coin.min_price is None or price < coin.min_price:
            coin.min_price, coin.t_min = price, age
        self.sink.write('prices', {
            'recv_utc': utc_iso(now), 'age_sec': _r(age, 2), 'mint': coin.mint, 'symbol': coin.symbol,
            'price_sol': _r(price, 12), 'change_from_start_pct': _r(_pct(price, coin.price0)),
            'mcap_sol': _r(coin.last_mcap, 3), 'curve_real_sol': _r(rsol, 4),
            'flow_sol': _r(flow, 4), 'curve_progress_pct': _r(coin.last_progress, 1),
        })
        self._check_move(coin, price, now)

    def _dev_row(self, coin: Coin, now: float, action: str, tokens: float) -> Dict[str, Any]:
        return {
            'recv_utc': utc_iso(now), 'age_sec': _r(now - coin.created_ts, 2), 'mint': coin.mint,
            'symbol': coin.symbol, 'creator': coin.creator, 'action': action, 'tokens': int(round(tokens)),
            'pct_of_initial': _r(tokens / coin.dev_tokens * 100 if coin.dev_tokens else None, 1),
            'dev_balance_tokens': '' if coin.dev_balance is None else int(round(coin.dev_balance)), 'price_sol': _r(coin.last_price, 12),
            'change_from_start_pct': _r(_pct(coin.last_price, coin.price0)),
        }

    def _on_dev_balance(self, coin: Coin, balance: float, now: float) -> None:
        prev = coin.dev_balance if coin.dev_balance is not None else coin.dev_tokens
        coin.dev_balance = balance
        delta = balance - prev
        if abs(delta) < 1.0:
            return
        if delta < 0:
            coin.dev_sold_tokens += -delta
            if coin.dev_first_sell_ts is None:
                coin.dev_first_sell_ts = now
            coin.window.append(Trade(now, 'sell', 0.0, -delta, coin.creator, coin.last_price))
            self.stats['dev_sells'] += 1
            self.sink.write('dev', self._dev_row(coin, now, 'sell', -delta))
        else:
            coin.dev_bought_more_tokens += delta
            coin.window.append(Trade(now, 'buy', 0.0, delta, coin.creator, coin.last_price))
            self.sink.write('dev', self._dev_row(coin, now, 'buy', delta))

    # ---------------------------------------------------------------- moves
    def _check_move(self, coin: Coin, price: float, now: float) -> None:
        change = _pct(price, coin.anchor_price)
        if change is None or abs(change) < self.cfg.move_pct:
            return
        direction = 'up' if change > 0 else 'down'
        codes, why = self.explain(coin, direction, coin.window, now)
        trades = coin.window if self.cfg.price_source == 'trades' else [t for t in coin.window if not t.trader]
        buys = [t for t in trades if t.side == 'buy']
        sells = [t for t in trades if t.side == 'sell']
        self.sink.write('moves', {
            'price_source': self.cfg.price_source, 'recv_utc': utc_iso(now), 'age_sec': _r(now - coin.created_ts, 3), 'mint': coin.mint,
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
        self.move_reasons[direction].update(codes)
        self.stats[f'moves_{direction}'] += 1
        coin.anchor_price, coin.anchor_ts, coin.window = price, now, []

    def explain(self, coin: Coin, direction: str, trades: List[Trade], now: float) -> Tuple[List[str], List[str]]:
        """Эвристические гипотезы «почему» — это идеи, а не доказанные причины."""
        if self.cfg.price_source == 'rpc':
            return self.explain_flow(coin, direction, trades, now)
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

    def explain_flow(self, coin: Coin, direction: str, trades: List[Trade], now: float) -> Tuple[List[str], List[str]]:
        """Гипотезы для rpc-режима: видим приток/отток SOL в кривую за интервал опроса
        и изменения баланса создателя, но не отдельные сделки и кошельки."""
        codes: List[str] = []
        why: List[str] = []
        flows = [t for t in trades if not t.trader]
        dev = [t for t in trades if t.trader == coin.creator]
        inflow = [t for t in flows if t.side == 'buy']
        outflow = [t for t in flows if t.side == 'sell']
        sol_in, sol_out = sum(t.sol for t in inflow), sum(t.sol for t in outflow)
        span = max(0.0, now - coin.anchor_ts)
        age = now - coin.created_ts
        step = f'~{self.cfg.poll_sec:.0f} с'
        last_step = _pct(coin.last_price, coin.prev_tick_price)
        if direction == 'down':
            dev_sells = [t for t in dev if t.side == 'sell']
            if dev_sells:
                tok = sum(t.tokens for t in dev_sells)
                share = f'{tok / coin.dev_tokens * 100:.0f}% своих стартовых токенов' if coin.dev_tokens else f'{tok:,.0f} токенов'
                codes.append('dev_sell')
                why.append(f'создатель продал {share} — похоже на слив разработчиком')
            if outflow:
                big = max(t.sol for t in outflow)
                if big >= self.cfg.big_trade_sol:
                    codes.append('whale_sell' if not dev_sells else 'big_outflow')
                    why.append(f'за один интервал ({step}) из кривой вышло {big:.2f} SOL — крупная продажа')
            if len(outflow) > len(inflow) and inflow:
                codes.append('sell_pressure')
                why.append(f'интервалов с оттоком больше, чем с притоком ({len(outflow)} против {len(inflow)})')
            if not inflow:
                codes.append('no_buyers')
                why.append('притока денег не было — покупатели не заходят, только выходят')
            if last_step is not None and last_step <= -self.cfg.move_pct:
                codes.append('fast_crash')
                why.append(f'резкое падение {last_step:.0f}% за один интервал ({step}) — разовый крупный выход')
            if age <= 30 and outflow and not dev_sells:
                codes.append('early_buyers_exit')
                why.append('падение в первые 30 с — снайперы/ранние покупатели быстро фиксируют прибыль')
        else:
            dev_buys = [t for t in dev if t.side == 'buy']
            if dev_buys:
                codes.append('dev_buy')
                why.append('создатель докупает — поддерживает цену или заманивает')
            if inflow:
                big = max(t.sol for t in inflow)
                if big >= self.cfg.big_trade_sol:
                    codes.append('whale_buy')
                    why.append(f'за один интервал ({step}) в кривую зашло {big:.2f} SOL — крупная покупка')
            if last_step is not None and last_step >= self.cfg.move_pct and 'whale_buy' not in codes:
                codes.append('fast_pump')
                why.append(f'резкий скачок +{last_step:.0f}% за один интервал ({step})')
            if len(inflow) >= 4:
                codes.append('buy_wave')
                why.append(f'рост волнами: {len(inflow)} интервалов притока за {span:.0f} с — устойчивый спрос/FOMO')
            if age <= self.cfg.early_sec:
                codes.append('early_snipers')
                why.append(f'первые {age:.0f} с после создания — снайпер-боты заходят сразу')
            if coin.last_progress is not None and coin.last_progress >= 50:
                codes.append('curve_hype')
                why.append(f'кривая заполнена на ~{coin.last_progress:.0f}% — ожидание миграции на DEX')
        if not codes:
            codes.append('small_flow')
            why.append('явной причины нет — серия мелких сделок')
        if flows:
            why.append(f'чистый поток {sol_in - sol_out:+.2f} SOL (приток {sol_in:.2f}, отток {sol_out:.2f})')
        return codes, why

    def report_text(self) -> str:
        done = sum(self.outcomes.values())
        ups = sorted(self.max_ups)
        lines = [
            f'Отчёт на {dt.datetime.now().astimezone():%Y-%m-%d %H:%M:%S}',
            f"Новых монет записано: {self.stats['coins_created']}, сейчас под наблюдением: {len(self.active)},"
            f" досмотрено: {done}" + (f", сделок: {self.stats['trades']}" if self.cfg.price_source == 'trades' else ''),
            f"Источник цены: {'PumpPortal сделки' if self.cfg.price_source == 'trades' else f'Solana RPC, опрос кривой каждые ~{self.cfg.poll_sec:.0f} с'}"
            f" | изменений цены {self.stats['price_ticks']}, продаж создателем {self.stats['dev_sells']},"
            f" ушли на DEX {self.stats['migrated']}, не Pump.fun кривая {self.stats['unsupported_curve']},"
            f" кривая не найдена RPC {self.stats['curve_not_found']}",
            f"Движений цены: вверх {self.stats['moves_up']}, вниз {self.stats['moves_down']}"
            f" (порог {self.cfg.move_pct:.0f}%, окно {self.cfg.window_sec:.0f} с)",
            '', 'Исходы монет:',
        ]
        for code, n in self.outcomes.most_common():
            lines.append(f'  {OUTCOME_RU.get(code, code)}: {n} ({n / done * 100:.1f}%)')
        if ups:
            lines += ['', f'Максимальный рост от старта: медиана {ups[len(ups) // 2]:+.0f}%,'
                          f' выросли на 100%+ : {sum(1 for u in ups if u >= 100)} монет,'
                          f' на 500%+ : {sum(1 for u in ups if u >= 500)} монет']
        for direction, title in (('up', 'Частые причины роста'), ('down', 'Частые причины падения')):
            if self.move_reasons[direction]:
                lines += ['', f'{title}:'] + [f'  {REASON_RU.get(c, c)} ({c}): {n}'
                                                  for c, n in self.move_reasons[direction].most_common(10)]
        if self.stats['disconnects']:
            lines += ['', f"Обрывов соединения: {self.stats['disconnects']} (сделки в это время пропущены)"]
        return '\n'.join(lines) + '\n'

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
        self.outcomes[outcome] += 1
        if max_up is not None and trades:
            self.max_ups.append(max_up)
        self.sink.write('summary', {
            'price_source': self.cfg.price_source, 'migrated': int(coin.migrated),
            'price_ticks': coin.price_ticks, 'mint': coin.mint, 'symbol': coin.symbol, 'name': coin.name, 'creator': coin.creator,
            'created_utc': coin.created_utc, 'watched_sec': _r(now - coin.created_ts, 1),
            'complete': int(complete), 'trades': trades, 'buys': coin.buys, 'sells': coin.sells,
            'unique_buyers': len(coin.buyers) if self.cfg.price_source == 'trades' else '',
            'unique_sellers': len(coin.sellers) if self.cfg.price_source == 'trades' else '',
            'sol_in': _r(coin.sol_in, 4), 'sol_out': _r(coin.sol_out, 4),
            'net_sol': _r(coin.sol_in - coin.sol_out, 4),
            'price0_sol': _r(coin.price0, 12), 'price_final_sol': _r(final, 12),
            'price_max_sol': _r(coin.max_price, 12), 'price_min_sol': _r(coin.min_price, 12),
            'change_final_pct': _r(change_final), 'max_up_pct': _r(max_up), 'max_down_pct': _r(max_down),
            'drawdown_from_peak_pct': _r(from_peak), 't_max_sec': _r(coin.t_max, 2),
            't_min_sec': _r(coin.t_min, 2), 'moves_up': coin.moves_up, 'moves_down': coin.moves_down,
            'dev_initial_buy_sol': _r(coin.dev_sol, 4), 'dev_sold_pct': _r(dev_sold_pct, 1),
            'dev_first_sell_sec': _r(coin.dev_first_sell_ts - coin.created_ts if coin.dev_first_sell_ts else None, 2),
            'dev_bought_more_sol': _r(coin.dev_bought_more_sol, 4) if self.cfg.price_source == 'trades'
            else ('' if not coin.dev_bought_more_tokens else f'{coin.dev_bought_more_tokens:.0f} tokens'),
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
        rpc = self.cfg.price_source == 'rpc'
        if rpc and coin.curve_ok is False:
            if coin.curve_reason == 'not_found':
                return 'no_price', ['curve_not_found'], [f'цена недоступна: RPC не нашёл счёт кривой за {CURVE_NOT_FOUND_GIVE_UP_SEC:.0f} с']
            return 'no_price', ['unsupported_curve'], [f'цена недоступна: это не кривая Pump.fun (площадка {coin.pool or "?"})']
        if trades == 0 and not coin.migrated and not coin.dev_sold_tokens:
            return 'dead', ['no_trades'], ['после создания сделок не было — монету никто не заметил (или купил только создатель)']
        if coin.migrated:
            outcome = 'migrated'
            ideas.append('кривая заполнилась — монета ушла на PumpSwap/DEX (редкий успех), дальше цена не отслеживалась')
        elif dev_sold_pct is not None and dev_sold_pct >= 50 and (cf <= -30 or (from_peak or 0) <= -30):
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
        if rpc:
            if coin.dev_tokens and not coin.dev_ata and not coin.dev_sold_tokens:
                ideas.append('счёт создателя не найден — его продажи не отслеживались')
            ideas.append(f'всего зашло {coin.sol_in:.2f} SOL, вышло {coin.sol_out:.2f} SOL')
            if coin.sol_in < 0.5:
                codes.append('few_buyers')
                ideas.append('денег почти не зашло — почти нет интереса')
            elif coin.sol_in >= 20:
                codes.append('many_buyers')
                ideas.append('зашло много денег — заметный интерес')
        elif coin.early_buyers:
            share = len(coin.early_sellers) / len(coin.early_buyers) * 100
            if coin.early_sellers:
                codes.append('early_buyers_sold')
            ideas.append(f'ранних покупателей {len(coin.early_buyers)}, из них продали {share:.0f}%')
        if coin.largest_sell >= self.cfg.big_trade_sol:
            codes.append('whale_sell')
            ideas.append(f'самая крупная продажа {coin.largest_sell:.2f} SOL' if not rpc
                         else f'крупнейший отток за интервал {coin.largest_sell:.2f} SOL')
        if coin.largest_buy >= self.cfg.big_trade_sol:
            codes.append('whale_buy')
            ideas.append(f'самая крупная покупка {coin.largest_buy:.2f} SOL' if not rpc
                         else f'крупнейший приток за интервал {coin.largest_buy:.2f} SOL')
        if rpc:
            pass
        elif len(coin.buyers) <= 3:
            codes.append('few_buyers')
            ideas.append(f'всего {len(coin.buyers)} покупателей — почти нет интереса')
        elif len(coin.buyers) >= 30:
            codes.append('many_buyers')
            ideas.append(f'{len(coin.buyers)} разных покупателей — заметный интерес')
        if coin.last_trade_ts is not None:
            quiet = now - coin.last_trade_ts
            if quiet >= 60:
                codes.append('went_quiet')
                ideas.append(f'последнее изменение цены на {coin.last_trade_ts - coin.created_ts:.0f}-й секунде, дальше тишина {quiet:.0f} с')
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
                     stop_event: Optional[asyncio.Event] = None,
                     session: Any = None, rpc: Any = None) -> Dict[str, Any]:
    """Три задачи: discovery (PumpPortal WS), poller (Solana RPC, только rpc-режим), housekeeping."""
    stop_event = stop_event or asyncio.Event()
    if ws_connect is None:  # pragma: no cover - real network only
        import websockets
        ws_connect = websockets.connect
    trades_mode = cfg.price_source == 'trades'
    own_session = False
    if not trades_mode:
        rpc = rpc or ReadOnlyRpc(cfg.rpc_url, timeout=10.0)
        if session is None:  # pragma: no cover - real network only
            import aiohttp
            session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=4))
            own_session = True
    sink = (CsvSink if cfg.output_format == 'csv' else TxtSink)(run_dir, cfg.record_trades)
    watcher = CoinWatcher(cfg, sink, clock)
    started = clock()
    stop_file = Path(cfg.out_dir) / 'STOP'
    pending_unsub: List[str] = []

    def write_report() -> None:
        try:
            (run_dir / 'report.txt').write_text(watcher.report_text(), encoding='utf-8')
        except OSError:
            pass

    def should_stop() -> bool:
        if stop_file.exists():
            stop_event.set()
        if cfg.duration_sec and clock() - started >= cfg.duration_sec:
            stop_event.set()
        return stop_event.is_set()

    async def pause(sec: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), max(0.01, sec))
        except asyncio.TimeoutError:
            pass

    async def discovery() -> None:
        backoff = 1.0
        while not stop_event.is_set():
            try:
                async with ws_connect(cfg.ws_url(), open_timeout=15, ping_interval=20, ping_timeout=20,
                                      max_size=2 ** 23) as ws:
                    await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                    keys = list(watcher.active) if trades_mode else []
                    for i in range(0, len(keys), 200):   # после переподключения
                        await ws.send(json.dumps({'method': 'subscribeTokenTrade', 'keys': keys[i:i + 200]}))
                    _log(run_dir, f'connected to PumpPortal; price source = {cfg.price_source}')
                    backoff = 1.0
                    while not stop_event.is_set():
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
                            if subs and trades_mode:
                                await ws.send(json.dumps({'method': 'subscribeTokenTrade', 'keys': subs}))
                            if (not isinstance(msg, dict) or not msg.get('mint')) and watcher.stats['non_coin_messages'] <= 5:
                                _log(run_dir, 'service message: ' + str(raw)[:300])
                        if trades_mode and pending_unsub:
                            keys, pending_unsub[:] = list(pending_unsub), []
                            await ws.send(json.dumps({'method': 'unsubscribeTokenTrade', 'keys': keys}))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                watcher.stats['disconnects'] += 1
                _log(run_dir, f'PumpPortal disconnected: {type(exc).__name__}: {str(exc)[:200]};'
                              f' retry in {backoff:.0f}s (новые монеты за время разрыва пропущены)')
                await pause(backoff * (0.5 + random.random()))
                backoff = min(backoff * 2, 60.0)

    async def poller() -> None:
        last_call = -1e9
        errors_logged = 0
        while not stop_event.is_set():
            cycle = clock()
            accounts = watcher.poll_accounts()
            for i in range(0, len(accounts), 100):
                if stop_event.is_set():
                    return
                batch = accounts[i:i + 100]
                wait = cfg.rpc_min_interval_sec - (clock() - last_call)
                if wait > 0:
                    await pause(wait)
                last_call = clock()
                try:
                    res = await rpc.call(session, 'getMultipleAccounts',
                                         [[a for a, _, _ in batch], {'encoding': 'base64', 'commitment': 'confirmed'}])
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    watcher.stats['rpc_exception'] += 1
                    if errors_logged < 10:
                        errors_logged += 1
                        _log(run_dir, f'RPC error: {type(exc).__name__}: {str(exc)[:200]}')
                    await pause(2.0)
                    continue
                if res.status != 'ok':
                    watcher.stats[f'rpc_{res.status}'] += 1
                    if errors_logged < 10:
                        errors_logged += 1
                        _log(run_dir, f'RPC {res.status}: {str(res.error)[:200]}')
                    if res.status == 'http_429':
                        await pause(res.retry_after or 5.0)
                        break
                    continue
                watcher.stats['rpc_ok'] += 1
                values = (((res.data or {}).get('result') or {}).get('value')) or []
                watcher.on_accounts(batch, values)
            await pause(cfg.poll_sec - (clock() - cycle))

    async def housekeeping() -> None:
        last_status = clock()
        while not should_stop():
            expired = watcher.tick()
            if expired and trades_mode:
                pending_unsub.extend(expired)
            if clock() - last_status >= cfg.status_every_sec:
                last_status = clock()
                write_report()
                _log(run_dir, 'status ' + json.dumps({
                    'coins': watcher.stats['coins_created'], 'watching': len(watcher.active),
                    'price_updates': watcher.stats['price_ticks'] or watcher.stats['trades'],
                    'moves_up': watcher.stats['moves_up'], 'moves_down': watcher.stats['moves_down'],
                    'dev_sells': watcher.stats['dev_sells'], 'finished': sink.counts['summary'],
                    'rpc_ok': watcher.stats['rpc_ok'], 'rpc_429': watcher.stats['rpc_http_429']}, ensure_ascii=False))
            await pause(0.5)

    tasks = [asyncio.ensure_future(discovery()), asyncio.ensure_future(housekeeping())]
    if not trades_mode:
        tasks.append(asyncio.ensure_future(poller()))
    try:
        await stop_event.wait()
    finally:
        stop_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if own_session:
            await session.close()
        unfinished = watcher.finalize_all()
        sink.close()
        write_report()
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
    print(f'Пишу файлы в: {run_dir}', flush=True)
    (root / 'watch.pid').write_text(str(os.getpid()), encoding='utf-8')
    (run_dir / 'config.json').write_text(json.dumps(cfg.safe_dict(), indent=2, ensure_ascii=False), encoding='utf-8')

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
