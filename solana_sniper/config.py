"""
config.py — настройки проекта.

Правило: секреты (API-ключи) — через переменные окружения.
Код может быть публичным, ключ — нет. Встроенного фоллбэк-ключа нет:
без HELIUS_API_KEY бот завершается с понятной ошибкой.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Встроенный ключ удалён: ранее засвеченный ключ нужно отозвать/перевыпустить
# в дашборде Helius и передавать новый только через HELIUS_API_KEY.
FALLBACK_API_KEY = ''  # встроенный ключ удалён: только переменная окружения HELIUS_API_KEY


# ПЛАН:
# 1. Описать все настройки проекта как неизменяемый dataclass
# 2. Прочитать переменные окружения с разумными умолчаниями
# 3. Проверить, что API-ключ Helius задан (без него бот не работает)
# 4. Проверить режим работы и вернуть готовые настройки

@dataclass(frozen=True)
class Settings:
    api_key: str
    wss_url: str
    http_rpc_url: str
    deposit_rub: float          # стартовый баланс челленджа, ₽
    take_profit_pct: float      # TP1: рост, при котором продаём 50%, %
    stop_loss_pct: float        # Hard Stop-Loss, %
    check_min_sec: float        # минимум секунд между проверками цены
    check_max_sec: float        # максимум секунд между проверками цены
    max_hold_sec: float         # принудительная продажа через N секунд
    mode: str                   # 'one' — одна монета и стоп | 'portfolio' — портфель
    sources: str                # какие площадки слушать: 'all' | 'pumpfun,raydium-cpmm,...'
    max_positions: int          # максимум одновременных позиций (слотов)
    min_liquidity_usd: float    # минимальная ликвидность для фильтра, $
    honeypot_check: bool        # включить предварительную route-проверку фиксированного размера
    max_entry_price_impact_pct: float  # предел impact для точной round-trip quote перед входом
    entry_quote_retries: int    # повторы точных Jupiter quote для свежего пула
    entry_quote_retry_delay_sec: float  # базовая пауза линейного backoff, сек
    indexer_retries: int        # короткие повторы при пустых ответах индексаторов
    indexer_retry_delay_sec: float  # пауза между запросами DexScreener/Gecko, сек
    only_sol_pairs: bool        # искать только пулы с SOL (не USDC/USDT)
    # --- честный PnL: комиссии ---
    protocol_fee_pct: float     # legacy: не вычитаем повторно, quote Jupiter уже учитывает DEX fee
    entry_fee_sol: float        # priority fee / Jito tip на вход, SOL
    exit_fee_sol: float         # priority fee / Jito tip на выход, SOL
    usd_rub_rate: float         # курс USD→RUB (для перевода SOL-комиссий)
    # --- лестница выхода ---
    be_offset_pct: float        # истинный безубыток после TP1, %
    trail_pct: float            # откат от ATH для трейлинг-стопа, %
    stagnation_sec: float       # таймаут застоя, сек
    stagnation_range_pct: float # допустимый размах цены при застое, %
    wallet_path: str            # wallet.json (кошелёк челленджа)
    trade_log_path: str         # текстовый журнал сделок
    # --- быстрый вход и Сейф ---
    max_trade_rub: float        # кап размера сделки, ₽
    vault_start_rub: float      # первая веха Сейфа, ₽
    vault_step_rub: float       # шаг вех Сейфа, ₽
    vault_share: float          # доля чистой прибыли в Сейф
    pump_source: str            # 'pumpportal' | 'helius-logs'
    filter_concurrency: int     # сколько фильтр-наборов одновременно
    db_path: str                # путь к файлу SQLite


def load_settings() -> Settings:
    # ПЛАН:
    # 1. Достать API-ключ из окружения, пустой — это ошибка
    # 2. Собрать URL для WSS и HTTP из ключа
    # 3. Прочитать числовые настройки с умолчаниями
    # 4. Проверить режим ('one' / 'portfolio')
    # 5. Вернуть заполненный dataclass Settings

    api_key = os.getenv('HELIUS_API_KEY', '').strip()
    if not api_key:
        api_key = FALLBACK_API_KEY
    if not api_key:
        # Секреты — только из окружения: встроенный ключ из кода удалён.
        print('❌ HELIUS_API_KEY не задан. Задай ключ переменной окружения HELIUS_API_KEY.')
        raise SystemExit(1)

    mode = os.getenv('MODE', 'one').strip().lower()
    if mode not in ('one', 'portfolio'):
        print(f'⚠️ Неизвестный MODE={mode!r} — используем "one"')
        mode = 'one'

    return Settings(
        api_key=api_key,
        wss_url=f'wss://mainnet.helius-rpc.com/?api-key={api_key}',
        http_rpc_url=f'https://mainnet.helius-rpc.com/?api-key={api_key}',
        deposit_rub=float(os.getenv('DEPOSIT_RUB', '1500')),
        take_profit_pct=float(os.getenv('TAKE_PROFIT_PCT', '30')),
        stop_loss_pct=float(os.getenv('STOP_LOSS_PCT', '10')),
        check_min_sec=float(os.getenv('CHECK_MIN_SEC', '0.5')),
        check_max_sec=float(os.getenv('CHECK_MAX_SEC', '0.5')),
        max_hold_sec=float(os.getenv('MAX_HOLD_SEC', '120')),
        mode=mode,
        sources=os.getenv('SOURCES', 'all'),
        max_positions=int(os.getenv('MAX_POSITIONS', '3')),
        min_liquidity_usd=float(os.getenv('MIN_LIQUIDITY_USD', '500')),
        honeypot_check=os.getenv('HONEYPOT_CHECK', 'on').strip().lower() != 'off',
        max_entry_price_impact_pct=float(
            os.getenv('MAX_ENTRY_PRICE_IMPACT_PCT', '5')
        ),
        entry_quote_retries=min(
            max(int(os.getenv('ENTRY_QUOTE_RETRIES', '0')), 0), 5
        ),
        entry_quote_retry_delay_sec=max(
            float(os.getenv('ENTRY_QUOTE_RETRY_DELAY_SEC', '1.0')), 0.0
        ),
        indexer_retries=min(
            max(int(os.getenv('INDEXER_RETRIES', '0')), 0), 3
        ),
        indexer_retry_delay_sec=max(
            float(os.getenv('INDEXER_RETRY_DELAY_SEC', '1.0')), 0.0
        ),
        only_sol_pairs=os.getenv('ONLY_SOL_PAIRS', 'on').strip().lower() != 'off',
        protocol_fee_pct=float(os.getenv('PROTOCOL_FEE_PCT', '1.25')),
        # 0.001 SOL ≈ обычный своп-тip (Jito-флор 0.0001–0.001);
        # 0.002 — гонка за горячим митом, бумаге не нужен:
        # на позиции 500₽ экономит ~1.9% кругового хода
        entry_fee_sol=float(os.getenv('ENTRY_FEE_SOL', '0.001')),
        exit_fee_sol=float(os.getenv('EXIT_FEE_SOL', '0.0001')),
        usd_rub_rate=float(os.getenv('USD_RUB_RATE', '90')),
        be_offset_pct=float(os.getenv('BE_OFFSET_PCT', '3.5')),
        trail_pct=float(os.getenv('TRAIL_PCT', '15')),
        # 180с вместо 120: ракеты часто консолидируются
        # 2–4 мин перед взлётом; 120с резало их раньше SL
        stagnation_sec=float(os.getenv('STAGNATION_SEC', '180')),
        stagnation_range_pct=float(os.getenv('STAGNATION_RANGE_PCT', '3')),
        wallet_path=os.getenv('WALLET_PATH', 'wallet.json'),
        trade_log_path=os.getenv('TRADE_LOG', 'trades.log'),
        max_trade_rub=float(os.getenv('MAX_TRADE_RUB', '1500')),
        vault_start_rub=float(os.getenv('VAULT_START_RUB', '5000')),
        vault_step_rub=float(os.getenv('VAULT_STEP_RUB', '2500')),
        vault_share=float(os.getenv('VAULT_SHARE', '0.5')),
        pump_source=os.getenv('PUMP_SOURCE', 'pumpportal').strip().lower(),
        # 6 вместо 10: меньше параллельных honeypot-проверок
        # → меньше HTTP 429 от Jupiter на горячих монетах
        filter_concurrency=int(os.getenv('FILTER_CONCURRENCY', '6')),
        db_path=os.getenv('DB_PATH', 'sniper.db'),
    )
