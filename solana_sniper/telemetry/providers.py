"""Read-only data providers for the collector.

* DexScreener / Jupiter: HTTP GET only.
* Solana JSON-RPC: POST is required by the protocol, but only for an explicit
  allowlist of read methods; anything else (sendTransaction, simulate*, ...)
  raises ForbiddenRpcMethod before any IO.
* Jupiter is used ONLY for /quote. There is no code path to /swap or to any
  signer; results are tagged quote_only=1 / transaction_sent=0 by the model.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

from .model import WSOL, redact_text, source_time_from_epoch

DEXSCREENER_TOKENS_URL = 'https://api.dexscreener.com/tokens/v1/solana/{addresses}'
DEXSCREENER_LATEST_URL = 'https://api.dexscreener.com/latest/dex/tokens/{mint}'
JUPITER_QUOTE_URL = 'https://lite-api.jup.ag/swap/v1/quote'

READ_ONLY_RPC_METHODS = frozenset({
    'getAccountInfo', 'getMultipleAccounts', 'getSlot', 'getTokenSupply',
    'getTransaction', 'getSignatureStatuses', 'getTokenLargestAccounts',
})


class ForbiddenRpcMethod(RuntimeError):
    pass


class ForbiddenEndpoint(RuntimeError):
    pass


@dataclass
class HttpResult:
    status: str                       # ok | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json
    http_status: Optional[int] = None
    data: Any = None
    error: Optional[str] = None
    latency_ms: Optional[float] = None
    retry_after: Optional[float] = None
    received_ts: float = field(default_factory=time.time)


def _retry_after(headers: Any) -> Optional[float]:
    try:
        value = headers.get('Retry-After') if headers is not None else None
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _request(session: Any, method: str, url: str, *, params: Optional[Dict[str, str]] = None,
                   json_body: Any = None, timeout: float = 8.0) -> HttpResult:
    started = time.monotonic()
    try:
        kwargs: Dict[str, Any] = {'timeout': _client_timeout(timeout)}
        if params is not None:
            kwargs['params'] = params
        if json_body is not None:
            kwargs['json'] = json_body
        requester = session.get if method == 'GET' else session.post
        async with requester(url, **kwargs) as resp:
            status_code = int(resp.status)
            text = await resp.text()
            retry_after = _retry_after(getattr(resp, 'headers', None))
    except asyncio.TimeoutError:
        return HttpResult('timeout', error='timeout', latency_ms=_ms(started))
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # aiohttp.ClientError and friends
        name = type(exc).__name__
        status = 'timeout' if 'Timeout' in name else 'request_error'
        return HttpResult(status, error=redact_text(f'{name}: {exc}')[:500], latency_ms=_ms(started))
    latency = _ms(started)
    data: Any = None
    if text:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = None
    if status_code == 429:
        return HttpResult('http_429', status_code, data, 'rate limited', latency, retry_after)
    if status_code >= 500:
        return HttpResult('http_5xx', status_code, data, f'HTTP {status_code}', latency, retry_after)
    if status_code >= 400:
        return HttpResult('http_4xx', status_code, data, f'HTTP {status_code}', latency, retry_after)
    if data is None:
        return HttpResult('invalid_json', status_code, None, 'response is not JSON', latency)
    return HttpResult('ok', status_code, data, None, latency)


def _client_timeout(total: float) -> Any:
    try:
        import aiohttp  # local import: telemetry core stays stdlib-only
        return aiohttp.ClientTimeout(total=total)
    except Exception:  # pragma: no cover - tests use fake sessions
        return total


def _ms(started: float) -> float:
    return round((time.monotonic() - started) * 1000.0, 3)


def _float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float('inf'), float('-inf')) else None


# ----------------------------------------------------------- DexScreener ----

def summarize_dexscreener(mint: str, pairs: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Pick the highest-liquidity pair where `mint` is base or quote token.

    Missing values stay None (never 0). Returns status not_indexed when the
    provider has no pair for this mint at this moment.
    """
    matched = []
    for pair in pairs or []:
        if not isinstance(pair, dict):
            continue
        base = (pair.get('baseToken') or {}).get('address')
        quote = (pair.get('quoteToken') or {}).get('address')
        if mint in (base, quote):
            matched.append(pair)
    if not matched:
        return {'status': 'not_indexed', 'status_reason': 'no pair for mint in provider response',
                'pairs_count': 0}

    def liquidity(pair: Dict[str, Any]) -> float:
        return _float((pair.get('liquidity') or {}).get('usd')) or -1.0

    best = max(matched, key=liquidity)
    base = best.get('baseToken') or {}
    quote = best.get('quoteToken') or {}
    is_base = base.get('address') == mint
    price_usd = _float(best.get('priceUsd'))
    price_native = _float(best.get('priceNative'))
    if not is_base:
        # DexScreener prices refer to the base token; invert only when sane.
        price_native = (1.0 / price_native) if price_native else None
        price_usd = None  # cannot derive the quote-side USD price safely
    liq = best.get('liquidity') or {}
    return {
        'status': 'ok',
        'pairs_count': len(matched),
        'pair_address': best.get('pairAddress'),
        'dex_id': best.get('dexId'),
        'quote_asset': (quote if is_base else base).get('address'),
        'token_side': 'base' if is_base else 'quote',
        'price_usd': price_usd,
        'price_native': price_native,
        'price_sol': price_native if (quote if is_base else base).get('address') == WSOL else None,
        'liquidity_usd': _float(liq.get('usd')),
        'liquidity_base': _float(liq.get('base')),
        'liquidity_quote': _float(liq.get('quote')),
        'market_cap_usd': _float(best.get('marketCap')),
        'fdv_usd': _float(best.get('fdv')),
        'volume_json': best.get('volume'),
        'txns': best.get('txns'),
        'price_change': best.get('priceChange'),
        'pool_created_at_utc': source_time_from_epoch(best.get('pairCreatedAt'), 'ms'),
        'symbol': base.get('symbol') if is_base else quote.get('symbol'),
        'name': base.get('name') if is_base else quote.get('name'),
        'pairs_compact': [
            {'pair_address': p.get('pairAddress'), 'dex_id': p.get('dexId'),
             'liquidity_usd': _float((p.get('liquidity') or {}).get('usd')),
             'price_usd': _float(p.get('priceUsd'))}
            for p in matched[:20]
        ],
    }


async def dexscreener_tokens(session: Any, mints: List[str], timeout: float = 8.0) -> HttpResult:
    if not 1 <= len(mints) <= 30:
        raise ValueError('DexScreener batch must contain 1..30 addresses')
    return await _request(session, 'GET', DEXSCREENER_TOKENS_URL.format(addresses=','.join(mints)),
                          timeout=timeout)


async def sol_usd_price(session: Any, timeout: float = 8.0) -> Tuple[HttpResult, Optional[float]]:
    result = await _request(session, 'GET', DEXSCREENER_LATEST_URL.format(mint=WSOL), timeout=timeout)
    if result.status != 'ok' or not isinstance(result.data, dict):
        return result, None
    best_liq, price = -1.0, None
    for pair in result.data.get('pairs') or []:
        if (pair.get('baseToken') or {}).get('address') != WSOL:
            continue
        liq = _float((pair.get('liquidity') or {}).get('usd')) or 0.0
        px = _float(pair.get('priceUsd'))
        if px and liq > best_liq:
            best_liq, price = liq, px
    return result, price


# --------------------------------------------------------------- Jupiter ----

def validate_quote_url(url: str) -> str:
    path = urlsplit(url).path.rstrip('/')
    if not path.endswith('/quote'):
        raise ForbiddenEndpoint('only the Jupiter /quote endpoint is allowed')
    return url


async def jupiter_quote(session: Any, input_mint: str, output_mint: str, amount_raw: int,
                        slippage_bps: int, *, url: str = JUPITER_QUOTE_URL,
                        timeout: float = 8.0) -> Tuple[HttpResult, Dict[str, Any]]:
    """Read-only ExactIn quote. Returns (http result, normalized quote fields)."""
    validate_quote_url(url)
    params = {
        'inputMint': input_mint, 'outputMint': output_mint, 'amount': str(int(amount_raw)),
        'slippageBps': str(int(slippage_bps)), 'swapMode': 'ExactIn',
    }
    result = await _request(session, 'GET', url, params=params, timeout=timeout)
    fields: Dict[str, Any] = {}
    data = result.data if isinstance(result.data, dict) else None
    if result.status == 'http_4xx' and data:
        code = str(data.get('errorCode') or data.get('error') or '')
        if 'ROUTE' in code.upper():
            result.status = 'no_route'
        elif 'TRADABLE' in code.upper():
            result.status = 'not_tradable'
        fields['provider_error_code'] = data.get('errorCode')
        fields['provider_error'] = redact_text(str(data.get('error') or ''))[:300] or None
    if result.status != 'ok' or data is None:
        return result, fields
    plan = data.get('routePlan')
    labels = [str((s.get('swapInfo') or {}).get('label') or 'DEX')
              for s in plan or [] if isinstance(s, dict)]
    impact = _float(data.get('priceImpactPct'))
    fields.update(
        out_amount_raw=data.get('outAmount'), min_out_raw=data.get('otherAmountThreshold'),
        price_impact_pct=impact * 100.0 if impact is not None else None,
        route_labels=labels or None, context_slot=data.get('contextSlot'),
        provider_time_taken=data.get('timeTaken'),
    )
    try:
        in_ok = int(data.get('inAmount') or -1) == int(amount_raw)
        out_ok = int(data.get('outAmount') or 0) > 0
    except (TypeError, ValueError):
        in_ok = out_ok = False
    if data.get('inputMint') != input_mint or data.get('outputMint') != output_mint:
        result.status, fields['validation'] = 'invalid_quote', 'mint_mismatch'
    elif not isinstance(plan, list) or not plan:
        result.status, fields['validation'] = 'no_route', 'empty_route_plan'
    elif not in_ok or not out_ok:
        result.status, fields['validation'] = 'invalid_quote', 'amount_mismatch_or_zero_out'
    return result, fields


# ------------------------------------------------------------ Solana RPC ----

class ReadOnlyRpc:
    def __init__(self, url: str, timeout: float = 10.0) -> None:
        self._url = url          # never logged; may contain a key from env
        self.timeout = timeout

    async def call(self, session: Any, method: str, params: List[Any]) -> HttpResult:
        if method not in READ_ONLY_RPC_METHODS:
            raise ForbiddenRpcMethod(f'RPC method {method!r} is not in the read-only allowlist')
        body = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
        result = await _request(session, 'POST', self._url, json_body=body, timeout=self.timeout)
        if result.status == 'ok' and isinstance(result.data, dict) and result.data.get('error'):
            result.status = 'provider_error'
            result.error = redact_text(json.dumps(result.data.get('error'))[:300])
        return result


PUMP_CURVE_K0 = 1073000191279242 * 30 * 10 ** 9


def parse_pump_curve(b64: Optional[str]) -> Dict[str, Any]:
    """Pump.fun bonding-curve account -> raw reserves (strings) + flags."""
    if not b64:
        return {'status': 'account_missing'}
    try:
        raw = base64.b64decode(b64)
    except (ValueError, TypeError):
        return {'status': 'invalid_account_data'}
    if len(raw) < 49:
        return {'status': 'invalid_account_data', 'status_reason': f'len={len(raw)}'}
    vtok, vsol, rtok, rsol, supply = struct.unpack_from('<QQQQQ', raw, 8)
    complete = bool(raw[48])
    out: Dict[str, Any] = {
        'status': 'ok', 'curve_complete': int(complete),
        'curve_virtual_token_raw': str(vtok), 'curve_virtual_sol_raw': str(vsol),
        'curve_real_token_raw': str(rtok), 'curve_real_sol_raw': str(rsol),
        'curve_token_total_supply_raw': str(supply),
    }
    if complete:
        out['status_reason'] = 'curve complete (migrated)'
    elif vtok > 0 and vsol > 0:
        # Price in SOL per token assumes Pump.fun's fixed 6 decimals (flagged).
        out['price_sol'] = (vsol / 1e9) / (vtok / 1e6)
        out['price_assumes_decimals'] = 6
        out['curve_invariant_ok'] = abs(vtok * vsol - PUMP_CURVE_K0) / PUMP_CURVE_K0 <= 0.01
    return out


async def pump_curves(rpc: ReadOnlyRpc, session: Any, curves: List[str]) -> Tuple[HttpResult, Dict[str, Dict[str, Any]]]:
    if not 1 <= len(curves) <= 100:
        raise ValueError('getMultipleAccounts batch must contain 1..100 accounts')
    result = await rpc.call(session, 'getMultipleAccounts',
                            [curves, {'encoding': 'base64', 'commitment': 'confirmed'}])
    parsed: Dict[str, Dict[str, Any]] = {}
    if result.status != 'ok':
        return result, parsed
    value = ((result.data or {}).get('result') or {})
    context_slot = (value.get('context') or {}).get('slot')
    for curve, item in zip(curves, value.get('value') or []):
        data = (item or {}).get('data') if isinstance(item, dict) else None
        info = parse_pump_curve(data[0] if isinstance(data, list) and data else None)
        info['context_slot'] = context_slot
        parsed[curve] = info
    for curve in curves:
        parsed.setdefault(curve, {'status': 'account_missing', 'context_slot': context_slot})
    return result, parsed
