"""
filters.py — фильтры безопасности (анти-скам).

Принцип: каждый фильтр — ЧИСТАЯ проверка, которая возвращает FilterResult
с честной причиной. Проверки, которым нужен RPC/сеть — async-обёртки.

Что проверяем:
  ✅ mint authority отозван   — эмитент не может допечатать монету
  ✅ freeze authority отозван — никто не может заморозить твои токены
  ✅ разумный supply          — ноль или безумие = плохо
  ✅ ликвидность              — кривая Pump.fun или $-объём у индексаторов
  ✅ honeypot-маршрут         — есть ли вообще ПУТЬ ПРОДАЖИ (lite-проверка)
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any, Dict, Optional

import aiohttp

from console import log, short
from models import FilterResult, NewCoin
from telemetry import hooks as _tel  # пассивная телеметрия: не влияет на решения
from price import (
    WSOL,
    fetch_dexscreener_pairs,
    fetch_geckoterminal_pools,
    find_curve_owner,
    rpc_get_account_raw,
)

JUPITER_QUOTE_URL = (
    'https://lite-api.jup.ag/swap/v1/quote?inputMint={mint}'
    '&outputMint=So11111111111111111111111111111111111111112'
    '&amount=1000000&slippageBps=500'
)


# ПЛАН:
# 1. check_mint_authority()     — authority выпуска токенов отозван?
# 2. check_freeze_authority()   — никто не сможет заморозить токены?
# 3. check_supply()             — supply и decimals выглядят разумно?
# 4. check_pump_curve()         — подтверждена ли Pump.fun кривая?
# 5. check_sol_pair()           — пара торгуется именно с SOL?
# 6. check_liquidity()          — ликвидность подтверждена индексаторами?
# 7. check_honeypot_route()     — есть ли маршрут продажи через Jupiter?
# 8. fetch_mint_meta()          — получить метаданные mint через RPC
# 9. run_all_checks()            — запустить независимые проверки параллельно


def check_mint_authority(authority: Optional[str]) -> FilterResult:
    # ПЛАН:
    # 1. None означает, что mint authority отозван
    # 2. Любой адрес authority означает риск новой эмиссии
    if authority is None:
        return FilterResult(True, ['mint authority отозван — допечатать нельзя'])
    return FilterResult(
        False,
        [f'mint authority НЕ отозван ({short(authority)}) — риск допечатки'],
    )


def check_freeze_authority(authority: Optional[str]) -> FilterResult:
    # ПЛАН:
    # 1. None означает, что freeze authority отозван
    # 2. Любой адрес authority означает риск заморозки токенов
    if authority is None:
        return FilterResult(True, ['freeze authority отозван — заморозить нельзя'])
    return FilterResult(
        False,
        [f'freeze authority НЕ отозван ({short(authority)}) — могут заморозить'],
    )


def check_supply(raw_amount: int, decimals: int) -> FilterResult:
    # ПЛАН:
    # 1. Отклонить нулевой или отрицательный raw supply
    # 2. Отклонить decimals вне безопасного диапазона
    # 3. Остальное считать разумным для дальнейшей проверки
    if raw_amount <= 0:
        return FilterResult(False, ['supply = 0 — у монеты нет токенов'])
    if not 0 <= decimals <= 18:
        return FilterResult(False, [f'подозрительные decimals={decimals}'])
    return FilterResult(True, [f'supply выглядит разумно (decimals={decimals})'])


async def check_pump_curve(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    curve: Optional[str],
    packet: Optional[Dict[str, Any]],
) -> FilterResult:
    # ПЛАН:
    # 1. Если WSS-пакет содержит виртуальные резервы — довериться свежим данным
    # 2. Иначе найти кривую через RPC и прочитать её raw-аккаунт
    # 3. Отклонить отсутствующий аккаунт, мигрировавшую или пустую кривую
    # 4. Разрешить только активную кривую с ненулевыми резервами
    if packet and packet.get('v_sol') and packet.get('v_tokens'):
        _tel.filter_metric(mint, 'pump_curve', 'packet_reserves',
                           {'v_sol': packet.get('v_sol'), 'v_tokens': packet.get('v_tokens')},
                           data_source='pumpportal_wss_packet')
        return FilterResult(
            True,
            [f'кривая из WSS-пакета (вирт. SOL: {packet["v_sol"]:.2f})'],
        )

    if not curve:
        curve = await find_curve_owner(session, http_rpc_url, mint)
    if not curve:
        return FilterResult(False, ['не нашли бондинг-кривую Pump.fun'])

    raw = await rpc_get_account_raw(session, http_rpc_url, curve)
    if not raw or len(raw) < 48:
        return FilterResult(False, ['не прочитали аккаунт кривой'])

    vtok, vsol = struct.unpack_from('<QQ', raw, 8)
    complete = bool(raw[48]) if len(raw) > 48 else False
    _tel.filter_metric(mint, 'pump_curve', 'curve_account',
                       {'virtual_token_raw': str(vtok), 'virtual_sol_raw': str(vsol),
                        'complete': complete, 'curve': curve}, data_source='rpc:getAccountInfo')
    if complete:
        return FilterResult(False, ['монета мигрировала с кривой — торгуем не по ней'])
    if vtok == 0 or vsol == 0:
        return FilterResult(False, ['пустые резервы кривой'])
    return FilterResult(
        True,
        [f'кривая жива (вирт. SOL-резерв: {vsol / 1_000_000_000:.2f})'],
    )


async def check_sol_pair(
    session: aiohttp.ClientSession, coin: NewCoin
) -> FilterResult:
    # ПЛАН:
    # 1. Pump.fun bonding curve считается SOL-парой
    # 2. Для индексированных DEX-пар найти WSOL среди base/quote mint
    # 3. Если DexScreener пуст — так же проверить GeckoTerminal
    # 4. Не принимать неизвестную пару как SOL-пару
    if coin.source == 'Pump.fun':
        return FilterResult(True, ['Pump.fun: кривая торгуется только за SOL'])

    pairs = await fetch_dexscreener_pairs(session, coin.mint)
    _tel.filter_metric(coin.mint, 'sol_pair', 'dexscreener_pairs_count', len(pairs),
                       data_source='dexscreener', status='ok' if pairs else 'not_indexed_or_error')
    for pair in pairs:
        base = (pair.get('baseToken') or {}).get('address', '')
        quote = (pair.get('quoteToken') or {}).get('address', '')
        if base == WSOL or quote == WSOL:
            return FilterResult(True, ['пара с SOL (DexScreener)'])
    if pairs:
        return FilterResult(False, ['пара без SOL (USDC/USDT?) — не ищем'])

    pools = await fetch_geckoterminal_pools(session, coin.mint)
    _tel.filter_metric(coin.mint, 'sol_pair', 'geckoterminal_pools_count', len(pools),
                       data_source='geckoterminal', status='ok' if pools else 'not_indexed_or_error')
    for pool in pools:
        attrs = pool.get('attributes') or {}
        if (
            attrs.get('base_token_address') == WSOL
            or attrs.get('quote_token_address') == WSOL
        ):
            return FilterResult(True, ['пара с SOL (GeckoTerminal)'])
    if pools:
        return FilterResult(False, ['пара без SOL (GeckoTerminal) — не ищем'])
    return FilterResult(False, ['пары не найдены у индексаторов'])


async def check_liquidity(
    session: aiohttp.ClientSession,
    mint: str,
    min_usd: float,
    retries: int = 0,
    retry_delay_sec: float = 1.0,
) -> FilterResult:
    # ПЛАН:
    # 1. Сравнить DexScreener liquidity.usd и GeckoTerminal reserve_in_usd
    # 2. Если найденная ликвидность ниже порога — блокировать без послаблений
    # 3. Повторить только случай, когда оба индексатора пока не вернули данных
    # 4. После попыток без подтверждения применить fail-closed блокировку
    retry_count = min(max(int(retries), 0), 3)
    base_delay = max(float(retry_delay_sec), 0.0)

    for attempt in range(retry_count + 1):
        dex_pairs = await fetch_dexscreener_pairs(session, mint)
        dex_max_usd = 0.0
        for pair in dex_pairs:
            try:
                liquidity_usd = float((pair.get('liquidity') or {}).get('usd') or 0)
            except (TypeError, ValueError):
                continue
            dex_max_usd = max(dex_max_usd, liquidity_usd)

        _tel.filter_metric(mint, 'liquidity', 'dexscreener_max_liquidity_usd',
                           dex_max_usd if dex_pairs else None, unit='USD', threshold=min_usd,
                           data_source='dexscreener', attempt=attempt,
                           status='ok' if dex_pairs else 'not_indexed_or_error')
        if dex_max_usd >= min_usd:
            return FilterResult(
                True,
                [f'DexScreener liquidity ${dex_max_usd:,.0f} >= порога ${min_usd:,.0f}'],
            )

        gecko_pools = await fetch_geckoterminal_pools(session, mint)
        gecko_max_usd = 0.0
        for pool in gecko_pools:
            attrs = pool.get('attributes') or {}
            try:
                reserve_usd = float(attrs.get('reserve_in_usd') or 0)
            except (TypeError, ValueError):
                continue
            gecko_max_usd = max(gecko_max_usd, reserve_usd)

        _tel.filter_metric(mint, 'liquidity', 'geckoterminal_max_reserve_usd',
                           gecko_max_usd if gecko_pools else None, unit='USD', threshold=min_usd,
                           data_source='geckoterminal', attempt=attempt,
                           status='ok' if gecko_pools else 'not_indexed_or_error')
        if gecko_max_usd >= min_usd:
            return FilterResult(
                True,
                [f'GeckoTerminal reserve ${gecko_max_usd:,.0f} >= порога ${min_usd:,.0f}'],
            )

        if not dex_pairs and not gecko_pools and attempt < retry_count:
            delay = base_delay * (attempt + 1)
            if delay > 0:
                await asyncio.sleep(delay)
            continue

        if dex_pairs or gecko_pools:
            return FilterResult(
                False,
                [
                    'ликвидность ниже порога '
                    f'${min_usd:,.0f} '
                    f'(DexScreener max ${dex_max_usd:,.0f}; '
                    f'GeckoTerminal max ${gecko_max_usd:,.0f})'
                ],
            )
        return FilterResult(
            False,
            ['ликвидность не найдена у индексаторов — консервативно блокируем'],
        )

    return FilterResult(
        False,
        ['ликвидность не найдена у индексаторов — консервативно блокируем'],
    )


async def check_honeypot_route(
    session: aiohttp.ClientSession,
    mint: str,
    attempts: int = 3,
) -> FilterResult:
    # ПЛАН:
    # 1. Попросить Jupiter посчитать маршрут ПРОДАЖИ монеты за SOL
    # 2. Если маршрут есть — продажа технически возможна
    # 3. HTTP 400 на свежей монете трактовать как возможную задержку индексации
    # 4. После коротких повторов без результата блокировать вход
    # ВНИМАНИЕ: это lite-проверка, а не симуляция swap-транзакции
    for attempt in range(attempts):
        t0 = _tel.now()
        try:
            async with session.get(
                JUPITER_QUOTE_URL.format(mint=mint),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    quote = await resp.json()
                    _tel.honeypot_probe(mint, attempt, t0=t0, http_status=200, data=quote)
                    if quote.get('outAmount'):
                        return FilterResult(
                            True,
                            ['маршрут продажи через Jupiter существует'],
                        )
                    return FilterResult(
                        False,
                        ['Jupiter не нашёл маршрут продажи — возможен honeypot'],
                    )
                _tel.honeypot_probe(mint, attempt, t0=t0, http_status=resp.status)
                if resp.status in (400, 429) and attempt < attempts - 1:
                    await asyncio.sleep(1.5 if resp.status == 400 else 3.0)
                    continue
                return FilterResult(
                    False,
                    [f'Jupiter не дал маршрут продажи (HTTP {resp.status})'],
                )
        except Exception as exc:
            _tel.honeypot_probe(mint, attempt, t0=t0, error=exc)
            if attempt < attempts - 1:
                await asyncio.sleep(1.5)
                continue
            return FilterResult(
                False,
                ['Jupiter недоступен — маршрут продажи не подтверждён'],
            )
    return FilterResult(False, ['Jupiter не подтвердил маршрут продажи'])


async def fetch_mint_meta(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
) -> Optional[Dict[str, Any]]:
    # ПЛАН:
    # 1. Запросить mint-аккаунт в формате jsonParsed
    # 2. Достать authority, supply и decimals
    # 3. При любой ошибке вернуть None, чтобы вход был заблокирован
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getAccountInfo',
        'params': [mint, {'encoding': 'jsonParsed', 'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()
        value = ((data.get('result') or {}).get('value')) or {}
        return (((value.get('data') or {}).get('parsed')) or {}).get('info')
    except Exception:
        return None


async def run_all_checks(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    coin: NewCoin,
    min_liquidity_usd: float,
    honeypot_check: bool,
    only_sol_pairs: bool,
    indexer_retries: int = 0,
    indexer_retry_delay_sec: float = 1.0,
) -> FilterResult:
    # ПЛАН:
    # 1. Запустить независимые проверки одновременно через gather
    # 2. Проверить mint-мету параллельно с сетевыми фильтрами
    # 3. Применить короткий повтор индексатора только при пустых ответах
    # 4. Любая ошибка/неподтверждённый сигнал остаётся fail-closed
    meta_task = asyncio.create_task(
        fetch_mint_meta(session, http_rpc_url, coin.mint)
    )
    tasks = [asyncio.create_task(check_sol_pair(session, coin))]
    stage_names = ['sol_pair']  # телеметрия: имя этапа для каждой задачи
    if honeypot_check:
        tasks.append(
            asyncio.create_task(check_honeypot_route(session, coin.mint))
        )
        stage_names.append('honeypot_route')
    stage_names.append('pump_curve' if coin.source == 'Pump.fun' else 'liquidity')
    if coin.source == 'Pump.fun':
        tasks.append(
            asyncio.create_task(
                check_pump_curve(
                    session,
                    http_rpc_url,
                    coin.mint,
                    coin.curve,
                    coin.packet,
                )
            )
        )
    else:
        tasks.append(
            asyncio.create_task(
                check_liquidity(
                    session,
                    coin.mint,
                    min_liquidity_usd,
                    retries=indexer_retries,
                    retry_delay_sec=indexer_retry_delay_sec,
                )
            )
        )

    result = FilterResult(True, [])
    meta = await meta_task
    if meta is None:
        blocked = FilterResult(False, ['не прочитали mint-аккаунт — пропускаем'])
        _tel.filter_stage(coin, 'mint_meta', blocked, data_source='rpc:getAccountInfo(jsonParsed)')
        _tel.filters_not_evaluated(
            coin, ['mint_authority', 'freeze_authority', 'supply', *stage_names],
            'not_evaluated_mint_meta_missing')
        return blocked

    _tel.filter_stage(coin, 'mint_meta', FilterResult(True, ['mint-аккаунт прочитан']),
                      observed=meta, data_source='rpc:getAccountInfo(jsonParsed)')
    stage_result = check_mint_authority(meta.get('mintAuthority'))
    _tel.filter_stage(coin, 'mint_authority', stage_result, observed=meta.get('mintAuthority'),
                      threshold='null (revoked)', data_source='rpc:getAccountInfo(jsonParsed)')
    result = result.merge(stage_result)
    stage_result = check_freeze_authority(meta.get('freezeAuthority'))
    _tel.filter_stage(coin, 'freeze_authority', stage_result, observed=meta.get('freezeAuthority'),
                      threshold='null (revoked)', data_source='rpc:getAccountInfo(jsonParsed)')
    result = result.merge(stage_result)
    try:
        coin.decimals = int(meta.get('decimals'))
    except (TypeError, ValueError):
        coin.decimals = None
    stage_result = check_supply(
        int(meta.get('supply') or 0),
        coin.decimals if coin.decimals is not None else -1,
    )
    _tel.filter_stage(coin, 'supply', stage_result,
                      observed={'supply_raw': meta.get('supply'), 'decimals': meta.get('decimals')},
                      threshold={'supply_raw': '> 0', 'decimals': '0..18'},
                      data_source='rpc:getAccountInfo(jsonParsed)')
    result = result.merge(stage_result)

    others = await asyncio.gather(*tasks, return_exceptions=True)
    for index, verdict in enumerate(others):
        stage_name = stage_names[index] if index < len(stage_names) else f'stage_{index}'
        if isinstance(verdict, Exception):
            _tel.filter_stage(coin, stage_name, None, error=verdict)
            result = result.merge(FilterResult(False, [f'проверка упала: {verdict}']))
        else:
            _tel.filter_stage(
                coin, stage_name, verdict,
                threshold=min_liquidity_usd if stage_name == 'liquidity' else None,
                unit='USD' if stage_name == 'liquidity' else None)
            result = result.merge(verdict)
    return result
