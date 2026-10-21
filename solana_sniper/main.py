"""
main.py — точка входа: собираем конвейер из модулей.

Конвейер (как в плане обучения, День 13):

  ws_client → [очередь событий] → detector → [очередь монет]
      → trader (фильтры + paper trading) → [очередь сделок] → storage (SQLite)
"""

from __future__ import annotations

import asyncio
import sys
from typing import Dict

import aiohttp

from config import Settings, load_settings
from console import Color, log
from detector import (
    PUMP_PROGRAM_ID,
    detector_worker,
    probe_pumpportal,
    pumpportal_listener,
    select_programs,
)
from storage import get_stats, storage_worker
from trader import trader_worker
from wallet import Wallet
from ws_client import ws_listener
from telemetry import hooks as _tel  # пассивная телеметрия: не влияет на решения

_summary_printed = False


# ПЛАН:
# 1. print_banner()  — красивая шапка с настройками
# 2. print_summary() — итоги из SQLite (один раз, не дублировать)
# 3. main()          — очереди, задачи, остановка, итоги

def print_banner(
    settings: Settings, programs: Dict[str, str],
    wallet: Wallet, pump_fast: bool,
) -> None:
    # ПЛАН:
    # 1. Показать режим работы и ключевые настройки
    # 2. Показать, какие площадки слушаем (выбор из настройки SOURCES)
    # 3. Показать путь быстрого входа и параметры Сейфа
    # 4. Напомнить, что торговля ВИРТУАЛЬНАЯ
    entry_path = (
        'Pumportal WSS — mint из пакета, 0 HTTP' if pump_fast
        else 'Helius logs + getTransaction (медленно)'
    )
    print(
        f'''
{Color.CYAN}{Color.BOLD}╔══════════════════════════════════════════════════════════════╗
║   🎯  SOLANA SNIPER — PAPER TRADING (без реальных сделок)     ║
╚══════════════════════════════════════════════════════════════╝{Color.RESET}
  {Color.DIM}Программы: {', '.join(programs.values())}{Color.RESET}
  {Color.DIM}Цена: он-чейн кривая Pump.fun → DexScreener → GeckoTerminal{Color.RESET}

  🎯 Режим:             {Color.BOLD}{settings.mode}{Color.RESET}
  💳 Баланс:            {Color.BOLD}{wallet.balance_rub:.0f} ₽{Color.RESET} (файл {settings.wallet_path})
  💵 Размер позиции:    min(1/3 баланса, {settings.max_trade_rub:.0f} ₽) ≈ {Color.BOLD}{wallet.position_size():.0f} ₽{Color.RESET}
  🎯 Лестница выхода:   TP1 +{settings.take_profit_pct:.0f}% (50%) → BE +{settings.be_offset_pct:.1f}% → трейлинг −{settings.trail_pct:.0f}% от ATH | SL −{settings.stop_loss_pct:.0f}%
  🧩 Слотов входа:      {settings.max_positions} (Semaphore) | фильтры: ×{settings.filter_concurrency} параллельно
  🏦 Сейф:              {settings.vault_start_rub:.0f}₽ → каждые +{settings.vault_step_rub:.0f}₽, {settings.vault_share:.0%} чистой прибыли
  🚀 Вход Pump.fun:     {entry_path}
  ⏱  Проверка цены:     каждые {settings.check_min_sec:.0f}–{settings.check_max_sec:.0f} с
  🛡  Фильтры:           authority, supply, liquidity ≥${settings.min_liquidity_usd:.0f}{', honeypot' if settings.honeypot_check else ''}, SOL-пары{Color.RESET}
  💸 Исполнение:       Jupiter exact-size buy+sell quote; max impact {settings.max_entry_price_impact_pct:.1f}%, slippage 1% | сеть: вход {settings.entry_fee_sol} SOL, выход {settings.exit_fee_sol} SOL
  💾 Журнал сделок:     {settings.db_path} + {settings.trade_log_path} + {settings.wallet_path}
  {Color.DIM}Ctrl+C — остановить в любой момент{Color.RESET}
'''
    )


async def print_summary(db_path: str, wallet: Wallet, settings: Settings) -> None:
    # ПЛАН:
    # 1. Проверить, не печатали ли уже итоги (защита от дублей)
    # 2. Получить статистику из SQLite
    # 3. Напечатать итоговый блок с цветом по знаку PnL
    global _summary_printed
    if _summary_printed:
        return
    _summary_printed = True

    stats = await get_stats(db_path)
    if stats['total'] == 0:
        log('Закрытых quote-fill пока нет.', Color.YELLOW)

    color = Color.GREEN if stats['total_pnl'] >= 0 else Color.RED
    print(f'\n{Color.BOLD}═════════ ИТОГИ ФЕЙК-ТОРГОВЛИ (из SQLite) ═════════{Color.RESET}')
    print(f'  Сделок:            {stats['total']}')
    print(f'  Прибыльных:        {stats['wins']}')
    print(f'  Убыточных:         {stats['total'] - stats['wins']}')
    print(f'  Лучшая:            {stats['best_pct']:+.1f}%')
    print(f'  Худшая:            {stats['worst_pct']:+.1f}%')
    print(f'  Общий фейк-PnL:    {color}{stats['total_pnl']:+.2f} руб{Color.RESET}')
    print(f'  💳 Баланс на конец: {wallet.balance_rub:.2f} ₽ (файл {settings.wallet_path})')
    pending_exits = sum(1 for pos in wallet.open_positions if pos.get('exit_pending'))
    print(
        f'  📌 Открытые позиции: {len(wallet.open_positions)} '
        f'(выход без подтверждённого quote: {pending_exits})'
    )
    print(f'  🏦 Сейф: {wallet.vault_rub:.2f} ₽ (унесено всего: {wallet.total_swept_rub:.2f} ₽)')
    print(f'  💸 Сетевые комиссии: {wallet.fees_paid_rub:.2f} ₽ | сделок: {len(wallet.trades)}')
    print(f'  {Color.DIM}Симуляция. Реальных денег не задействовано.{Color.RESET}\n')


async def main() -> None:
    # ПЛАН:
    # 1. Загрузить настройки и напечатать баннер
    # 2. Создать три очереди: события, монеты, сделки
    # 3. Запустить воркеры и WebSocket-слушатель фоновыми задачами
    # 4. Режим 'one' — ждать stop_event; 'portfolio' — работать до Ctrl+C
    # 5. В finally — отменить задачи и напечатать итоги из SQLite
    settings = load_settings()

    # «Челлендж 1500»: кошелёк (с Сейфом) живёт в wallet.json
    # и переживает перезапуски
    wallet = Wallet(
        settings.wallet_path,
        initial=settings.deposit_rub,
        max_trade_rub=settings.max_trade_rub,
        vault_start=settings.vault_start_rub,
        vault_step=settings.vault_step_rub,
        vault_share=settings.vault_share,
    )
    # Пассивная телеметрия: включается только при заданном TELEMETRY_DIR.
    # Ошибка установки не останавливает бота и не меняет его решения.
    _tel.install_from_env(settings)

    # Быстрый вход: Pump.fun через Pumportal WSS (mint и резервы
    # кривой — прямо из пакета, без getTransaction). Если Pumportal
    # недоступен — откатываемся на Helius logs (медленнее).
    pump_fast = settings.pump_source == 'pumpportal'
    if pump_fast:
        pump_fast = await probe_pumpportal()
        if not pump_fast:
            log(
                '⚠️ Pumportal недоступен — Pump.fun пойдёт через '
                'Helius logs (медленнее, с getTransaction)',
                Color.YELLOW,
            )

    programs = select_programs(settings.sources)  # какие площадки слушаем
    if pump_fast:
        # Pump.fun слушает Pumportal — из Helius-подписки убираем
        programs = {
            pid: name for pid, name in programs.items()
            if pid != PUMP_PROGRAM_ID
        }
    print_banner(settings, programs, wallet, pump_fast)

    events_queue: asyncio.Queue = asyncio.Queue()
    coins_queue: asyncio.Queue = asyncio.Queue()
    trades_queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()    # всё завершить (сделка закрыта / Ctrl+C)
    watch_stop = asyncio.Event()    # больше не слушать новые монеты (экономия кредитов)

    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        storage_task = asyncio.create_task(
            storage_worker(trades_queue, settings.db_path)
        )
        producer_tasks = [
            asyncio.create_task(
                detector_worker(
                    events_queue, coins_queue, session,
                    settings.http_rpc_url, watch_stop
                )
            ),
            asyncio.create_task(
                trader_worker(
                    coins_queue, trades_queue, session, settings,
                    wallet, stop_event, watch_stop
                )
            ),
            asyncio.create_task(ws_listener(events_queue, settings, programs, watch_stop)),
        ]
        if pump_fast:
            producer_tasks.append(
                asyncio.create_task(
                    pumpportal_listener(coins_queue, watch_stop)
                )
            )
        # storage_task гасим ПОСЛЕДНИМ (журнал не теряет сделки)
        try:
            if settings.mode == 'one':
                await stop_event.wait()
            else:
                await asyncio.Event().wait()  # висим, пока пользователь не остановит
        finally:
            log('⏹ Останавливаем задачи…', Color.DIM)
            # 1. Гасим производителей (WS, детектор, трейдер)
            for task in producer_tasks:
                task.cancel()
            await asyncio.gather(*producer_tasks, return_exceptions=True)
            # 2. Ждём, пока хранилище допишет все сделки из очереди
            try:
                await asyncio.wait_for(trades_queue.join(), timeout=5.0)
            except asyncio.TimeoutError:
                log('⚠️ Не все сделки успели записаться в журнал', Color.YELLOW)
            # 3. Гасим хранилище и печатаем итоги
            storage_task.cancel()
            await asyncio.gather(storage_task, return_exceptions=True)
            await print_summary(settings.db_path, wallet, settings)
            _tel.positions_at_shutdown(wallet.open_positions)
            _tel.shutdown()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n')
        sys.exit(0)
