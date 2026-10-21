"""
price.py — поиск реальной цены монеты.

Цепочка источников:
  1. Pump.fun — НАПРЯМУЮ из он-чейн бондинг-кривой (сразу при создании!)
  2. DexScreener — индексатор DEX-пар
  3. GeckoTerminal — запасной индексатор
  4. None — «цены нет» (вызывающий код решает, что делать)

Бондинг-кривая Pump.fun — аккаунт с фиксированной раскладкой байтов:
    offset  8: virtual_token_reserves (u64, little-endian)
    offset 16: virtual_sol_reserves   (u64, little-endian)
    offset 48: complete (bool) — True = монета мигрировала на DEX
Цена в SOL = (virtual_sol / 1e9) / (virtual_token / 1e6).
"""

from __future__ import annotations

import base64
import struct
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from console import log
from models import SwapQuote
from telemetry import hooks as _tel  # пассивная телеметрия: не влияет на решения

DEXSCREENER_URL = 'https://api.dexscreener.com/latest/dex/tokens/{mint}'
JUPITER_QUOTE_URL = 'https://lite-api.jup.ag/swap/v1/quote'
GECKOTERMINAL_URL = 'https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/pools'
COINGECKO_SOL_URL = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
WSOL = 'So11111111111111111111111111111111111111112'  # обёрнутый SOL — его mint в пулах

# Кэш курса SOL/USD: (когда получили, цена). Не долбим API на каждый тик.
_sol_usd_cache: Tuple[float, Optional[float]] = (0.0, None)


# ПЛАН:
# 1. get_sol_usd()          — курс SOL→USD с кэшем (DexScreener → CoinGecko)
# 2. rpc_get_account_raw()  — сырые байты аккаунта из RPC
# 3. find_curve_owner()     — найти бондинг-кривую монеты Pump.fun
# 4. pump_curve_price_usd() — цена из резервов кривой
# 5. fetch_dexscreener_price_usd()   — цена с DexScreener
# 6. fetch_geckoterminal_price_usd() — цена с GeckoTerminal
# 7. resolve_real_price()   — полная цепочка: (цена, имя_источника)

async def get_sol_usd(session: aiohttp.ClientSession) -> Optional[float]:
    # ПЛАН:
    # 1. Если свежий курс есть в кэше — вернуть его
    # 2. Попросить DexScreener цену WSOL (пара с максимальной ликвидностью)
    # 3. Если не вышло — попросить CoinGecko
    # 4. Удачный курс запомнить в кэш и вернуть
    global _sol_usd_cache
    cached_at, cached = _sol_usd_cache
    if cached and time.time() - cached_at < 60.0:
        return cached

    price: Optional[float] = None
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint=WSOL),
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status == 200:
                pairs = ((await resp.json()).get('pairs')) or []

                def score(pair: Dict[str, Any]) -> Tuple[float, float]:
                    # ПЛАН:
                    # 1. Достать ликвидность и цену из пары (с защитой от мусора)
                    # 2. Вернуть кортеж (ликвидность, цена) — для max()
                    try:
                        liq = float(((pair.get('liquidity') or {}).get('usd')) or 0.0)
                        px = float(pair.get('priceUsd') or 0.0)
                    except (TypeError, ValueError):
                        return (0.0, 0.0)
                    return (liq, px)

                best = max(pairs, key=score, default={})
                px = score(best)[1]
                if px > 0:
                    price = px
    except Exception:
        pass  # сеть барахлит — пробуем запасной источник ниже

    if not price:
        try:
            async with session.get(
                COINGECKO_SOL_URL,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status == 200:
                    price = float((await resp.json())['solana']['usd'])
        except Exception:
            pass

    if price:
        _sol_usd_cache = (time.time(), price)
    return price


async def rpc_get_account_raw(
    session: aiohttp.ClientSession, http_rpc_url: str, address: str
) -> Optional[bytes]:
    # ПЛАН:
    # 1. Отправить getAccountInfo с кодировкой base64
    # 2. Достать первый элемент data и раскодировать из base64
    # 3. При любой ошибке вернуть None (вызывающий код решает, что делать)
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getAccountInfo',
        'params': [address, {'encoding': 'base64', 'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
        value = ((data.get('result') or {}).get('value')) or {}
        raw = value.get('data') or []
        if not raw:
            return None
        return base64.b64decode(raw[0])
    except Exception:
        return None


async def get_jupiter_quote(
    session: aiohttp.ClientSession,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int = 100,
) -> Optional[SwapQuote]:
    # ПЛАН:
    # 1. Запросить у Jupiter котировку ExactIn для точного размера в raw-единицах
    # 2. Проверить mint, сумму, наличие маршрута и положительную выручку
    # 3. Вернуть ожидаемый output и нижний порог с учётом slippage
    t0 = _tel.now()
    if amount_raw <= 0 or slippage_bps < 0:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps, 'invalid_request', t0=t0)
        return None

    params = {
        'inputMint': input_mint,
        'outputMint': output_mint,
        'amount': str(amount_raw),
        'slippageBps': str(slippage_bps),
        'swapMode': 'ExactIn',
    }
    try:
        async with session.get(
            JUPITER_QUOTE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                                   'http_error', t0=t0, http_status=resp.status)
                return None
            data = await resp.json()
    except Exception as exc:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'exception', t0=t0, error=exc)
        return None

    if not isinstance(data, dict):
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'invalid_payload', t0=t0, http_status=200, data=data)
        return None
    route_plan = data.get('routePlan')
    if (data.get('inputMint') != input_mint
            or data.get('outputMint') != output_mint
            or not isinstance(route_plan, list)
            or not route_plan):
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'no_route_or_mint_mismatch', t0=t0, http_status=200, data=data)
        return None

    try:
        in_amount = int(data.get('inAmount') or 0)
        out_amount = int(data.get('outAmount') or 0)
        min_out = int(data.get('otherAmountThreshold') or out_amount)
        raw_impact = data.get('priceImpactPct')
        price_impact = (
            float(raw_impact) * 100.0 if raw_impact is not None else None
        )
        raw_slot = data.get('contextSlot')
        context_slot = int(raw_slot) if raw_slot is not None else None
    except (TypeError, ValueError) as exc:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'parse_error', t0=t0, http_status=200, data=data, error=exc)
        return None

    if in_amount != amount_raw or out_amount <= 0 or min_out <= 0:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'rejected_amounts', t0=t0, http_status=200, data=data)
        return None
    if min_out > out_amount:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'rejected_min_out_above_out', t0=t0, http_status=200, data=data)
        return None

    route_labels: List[str] = []
    for step in route_plan:
        if not isinstance(step, dict):
            continue
        swap_info = step.get('swapInfo')
        if isinstance(swap_info, dict):
            route_labels.append(str(swap_info.get('label') or 'DEX'))
    if not route_labels:
        _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                           'no_route_labels', t0=t0, http_status=200, data=data)
        return None
    labels = tuple(route_labels)
    _tel.jupiter_quote(input_mint, output_mint, amount_raw, slippage_bps,
                       'ok', t0=t0, http_status=200, data=data)

    return SwapQuote(
        input_mint=input_mint,
        output_mint=output_mint,
        in_amount_raw=in_amount,
        out_amount_raw=out_amount,
        min_out_raw=min_out,
        price_impact_pct=price_impact,
        route_labels=labels,
        context_slot=context_slot,
    )


async def find_curve_owner(
    session: aiohttp.ClientSession, http_rpc_url: str, mint: str
) -> Optional[str]:
    # ПЛАН:
    # 1. Узнать крупнейший токен-аккаунт монеты (getTokenLargestAccounts)
    # 2. Узнать владельца этого токен-аккаунта (getAccountInfo jsonParsed)
    # 3. Для Pump.fun владелец = бондинг-кривая — вернуть её адрес
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getTokenLargestAccounts',
        'params': [mint, {'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
        value = (data.get('result') or {}).get('value') or []
        if not value:
            return None
        token_account = value[0].get('address')
    except Exception:
        return None

    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getAccountInfo',
        'params': [token_account, {'encoding': 'jsonParsed', 'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
        value = ((data.get('result') or {}).get('value')) or {}
        info = (((value.get('data') or {}).get('parsed')) or {}).get('info') or {}
        return info.get('owner')
    except Exception:
        return None


async def pump_curve_price_usd(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    curve: Optional[str],
) -> Optional[float]:
    # ПЛАН:
    # 1. Если адрес кривой неизвестен — найти его
    # 2. Загрузить байты аккаунта кривой
    # 3. Распаковать резервы (u64 по смещениям 8 и 16) и флаг complete
    # 4. Проверить инвариант CPMM: vTok × vSol = const
    #    (спам/тестовые аккаунты нарушают его — цену не вернём)
    # 5. Если монета мигрировала или резервы пустые — вернуть None
    # 6. Посчитать цену в SOL и перевести в USD через курс SOL
    if not curve:
        curve = await find_curve_owner(session, http_rpc_url, mint)
    if not curve:
        return None

    raw = await rpc_get_account_raw(session, http_rpc_url, curve)
    if not raw or len(raw) < 48:
        return None

    vtok, vsol = struct.unpack_from('<QQ', raw, 8)
    complete = bool(raw[48]) if len(raw) > 48 else False
    if complete or vtok == 0:
        return None  # мигрировала с кривой — дальше по индексаторам

    # Инвариант бондинг-кривой: виртуальные резервы всегда
    # лежат на одной гиперболе vTok × vSol = k0. Начальные
    # резервы Pump.fun: 1.073e15 токенов × 30 SOL.
    # Если произведение отличается — аккаунт мусорный
    # (такие «ракеты +2395%» мы уже видели живьём).
    k = vtok * vsol
    k0 = int(1073000191279242 * 30 * 1e9)
    if k <= 0 or abs(k - k0) / k0 > 0.01:
        return None

    price_sol = (vsol / 1e9) / (vtok / 1e6)
    sol_usd = await get_sol_usd(session)
    if not sol_usd:
        return None
    return price_sol * sol_usd


async def fetch_dexscreener_pairs(
    session: aiohttp.ClientSession, mint: str
) -> List[Dict[str, Any]]:
    # ПЛАН:
    # 1. Запросить все пары монеты у DexScreener
    # 2. Вернуть список пар (пустой — данных нет)
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint=mint),
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return []
            return (await resp.json()).get('pairs') or []
    except Exception:
        return []


async def fetch_dexscreener_price_usd(
    session: aiohttp.ClientSession, mint: str
) -> Optional[float]:
    # ПЛАН:
    # 1. Загрузить все пары монеты
    # 2. Выбрать пару с максимальной ликвидностью
    # 3. Вернуть её цену в USD (или None, если данных нет)
    pairs = await fetch_dexscreener_pairs(session, mint)
    if not pairs:
        return None

    def liquidity(pair: Dict[str, Any]) -> float:
        # ПЛАН:
        # 1. Достать из пары ликвидность в USD
        # 2. При мусорных данных вернуть 0.0
        try:
            return float(((pair.get('liquidity') or {}).get('usd')) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    best = max(pairs, key=liquidity)
    try:
        price = float(best.get('priceUsd') or 0.0)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


async def fetch_geckoterminal_pools(
    session: aiohttp.ClientSession, mint: str
) -> List[Dict[str, Any]]:
    # ПЛАН:
    # 1. Запросить пулы токена у GeckoTerminal
    # 2. Вернуть список пулов (пустой — данных нет)
    try:
        async with session.get(
            GECKOTERMINAL_URL.format(mint=mint),
            headers={'Accept': 'application/json'},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return []
            return (await resp.json()).get('data') or []
    except Exception:
        return []


async def fetch_geckoterminal_price_usd(
    session: aiohttp.ClientSession, mint: str
) -> Optional[float]:
    # ПЛАН:
    # 1. Загрузить пулы токена
    # 2. Выбрать пул с максимальным reserve_in_usd
    # 3. Определить, наш токен base или quote, и вернуть его цену
    pools = await fetch_geckoterminal_pools(session, mint)
    if not pools:
        return None

    def reserve(pool: Dict[str, Any]) -> float:
        # ПЛАН:
        # 1. Достать из пула объём ликвидности в USD
        # 2. При мусорных данных вернуть 0.0
        try:
            return float(((pool.get('attributes') or {}).get('reserve_in_usd')) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    attrs = (max(pools, key=reserve).get('attributes')) or {}
    try:
        if attrs.get('base_token_address') == mint:
            price = float(attrs.get('base_token_price_usd') or 0.0)
        elif attrs.get('quote_token_address') == mint:
            price = float(attrs.get('quote_token_price_usd') or 0.0)
        else:
            price = float(attrs.get('base_token_price_usd') or 0.0)
    except (TypeError, ValueError):
        return None
    return price if price and price > 0 else None


async def resolve_real_price(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    source: str,
    curve: Optional[str],
) -> Tuple[Optional[float], str]:
    # ПЛАН:
    # 1. Для Pump.fun попробовать он-чейн кривую (работает сразу!)
    # 2. Потом DexScreener
    # 3. Потом GeckoTerminal
    # 4. Вернуть (цена, имя_источника) или (None, '')
    if source == 'Pump.fun':
        price = await pump_curve_price_usd(session, http_rpc_url, mint, curve)
        if price:
            return price, 'он-чейн кривая'

    price = await fetch_dexscreener_price_usd(session, mint)
    if price:
        return price, 'DexScreener'

    price = await fetch_geckoterminal_price_usd(session, mint)
    if price:
        return price, 'GeckoTerminal'

    return None, ''
