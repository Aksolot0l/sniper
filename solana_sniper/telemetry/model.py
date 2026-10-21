"""Record model, time helpers, stable ids and recursive secret redaction.

Pure standard library. Importing this module performs no IO, starts no thread,
process or network connection.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

CHAIN_ID = 'solana-mainnet'
PARSER_VERSION = 'telemetry-parser/1'
WSOL = 'So11111111111111111111111111111111111111112'

# Record kinds -> SQLite table names (one append-only table per kind).
KINDS = (
    'detections', 'decisions', 'observations', 'quotes',
    'followups', 'lifecycle', 'health',
)

DECISIONS = ('PASS', 'FAIL', 'SKIP', 'UNKNOWN')

# ---------------------------------------------------------------- time ----

def utc_iso(ts: Optional[float] = None) -> str:
    """UTC ISO-8601 with microseconds and a trailing Z."""
    moment = (
        dt.datetime.now(dt.timezone.utc) if ts is None
        else dt.datetime.fromtimestamp(ts, dt.timezone.utc)
    )
    return moment.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def parse_iso(value: Optional[str]) -> Optional[float]:
    """ISO-8601 -> epoch seconds; None for missing or malformed values."""
    if not value or not isinstance(value, str):
        return None
    try:
        moment = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None  # naive timestamps are ambiguous: never guess the zone
    return moment.timestamp()


def source_time_from_epoch(value: Any, unit: str = 's') -> Optional[str]:
    """Provider epoch (s or ms) -> UTC ISO; None when absent/invalid."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    if unit == 'ms':
        number /= 1000.0
    try:
        return utc_iso(number)
    except (OverflowError, OSError, ValueError):
        return None


# ------------------------------------------------------------ identity ----

def candidate_id(chain_id: str, mint: str) -> str:
    """Stable id for (chain_id, mint); identical across runs and machines."""
    key = json.dumps([chain_id, mint], separators=(',', ':'))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'sniper-candidate:' + key))


def stable_id(*parts: Any) -> str:
    key = json.dumps([str(p) for p in parts], separators=(',', ':'))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'sniper-telemetry:' + key))


def new_event_id() -> str:
    return str(uuid.uuid4())


# ----------------------------------------------------------- redaction ----

_SECRET_KEY = re.compile(
    r'authori[sz]ation|cookie|api[\s_\-]?key|apikey|secret|password|passwd|'
    r'private|priv_?key|seed|mnemonic|signer|keypair|credential|bearer|'
    r'access[_\-]?token|refresh[_\-]?token|auth[_\-]?token|session[_\-]?token|'
    r'wallet[_\-]?(path|file|key)|x-api',
    re.I,
)
_SECRET_TEXT = re.compile(
    r'(?i)(bearer\s+[^\s,;"\']+'
    r'|(?:api[_\-]?key|apikey|token|secret|password|authorization|access_token)'
    r'\s*[=:]\s*[^\s,;&"\']+)'
)
_URL = re.compile(r'(?:https?|wss?)://[^\s"\'<>]+', re.I)
_SECRET_ENV_NAME = re.compile(
    r'KEY|SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE|AUTH|RPC_URL|WSS_URL|SEED|MNEMONIC',
    re.I,
)
_secret_values: Optional[List[str]] = None


def refresh_secret_values() -> None:
    """Snapshot env values that look like secrets (names with KEY/SECRET/...)."""
    global _secret_values
    values = set()
    for name, value in os.environ.items():
        value = (value or '').strip()
        if _SECRET_ENV_NAME.search(name) and len(value) >= 8:
            values.add(value)
    # Longest first so that a secret containing another one is fully removed.
    _secret_values = sorted(values, key=len, reverse=True)


def _secret_list() -> List[str]:
    if _secret_values is None:
        refresh_secret_values()
    return _secret_values or []


def _origin_only(match: 're.Match[str]') -> str:
    try:
        parts = urlsplit(match.group())
        host = parts.hostname or 'unknown-host'
    except ValueError:
        return '[REDACTED_URL]'
    # Path/query/userinfo can carry provider keys: keep only scheme + host.
    return f'{parts.scheme}://{host}/[REDACTED]'


def redact_text(value: str) -> str:
    for secret in _secret_list():
        if secret in value:
            value = value.replace(secret, '[REDACTED]')
    value = _URL.sub(_origin_only, value)
    return _SECRET_TEXT.sub('[REDACTED]', value)


def _looks_like_key_bytes(items: Any) -> bool:
    # Solana keypair files are JSON arrays of 64 byte values.
    return (
        isinstance(items, (list, tuple)) and len(items) == 64
        and all(isinstance(x, int) and not isinstance(x, bool) and 0 <= x <= 255 for x in items)
    )


def redact(value: Any, _depth: int = 0) -> Any:
    """Recursive redaction to a JSON-safe structure.

    * dict keys that look like credentials -> '[REDACTED]'
    * URLs -> scheme://host only (keys often live in path/query)
    * env secret values, 'Bearer ...', 'api-key=...' -> '[REDACTED]'
    * 64-byte integer arrays (keypair files) -> '[REDACTED_KEYPAIR]'
    * non-finite floats -> None; unknown objects -> type name only
    """
    if _depth > 40:
        return '[TRUNCATED_DEPTH]'
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            name = str(key)
            out[redact_text(name)] = (
                '[REDACTED]' if _SECRET_KEY.search(name) else redact(item, _depth + 1)
            )
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        if _looks_like_key_bytes(value):
            return '[REDACTED_KEYPAIR]'
        return [redact(item, _depth + 1) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return f'[BYTES len={len(value)}]'
    return f'[{type(value).__name__}]'


def dumps(value: Any) -> str:
    """Canonical redacted JSON (UTF-8 friendly, sorted keys, no NaN)."""
    return json.dumps(
        redact(value), ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode('utf-8')).hexdigest()


# ----------------------------------------------------- reason classifier ----

# (stage, decision, substring in bot's human text) -> stable reason_code.
_FILTER_REASONS: Tuple[Tuple[str, str, str, str], ...] = (
    ('mint_authority', 'PASS', 'mint authority отозван', 'mint_authority_revoked'),
    ('mint_authority', 'FAIL', 'mint authority НЕ отозван', 'mint_authority_present'),
    ('freeze_authority', 'PASS', 'freeze authority отозван', 'freeze_authority_revoked'),
    ('freeze_authority', 'FAIL', 'freeze authority НЕ отозван', 'freeze_authority_present'),
    ('supply', 'PASS', 'supply выглядит разумно', 'supply_ok'),
    ('supply', 'FAIL', 'supply = 0', 'supply_zero'),
    ('supply', 'FAIL', 'подозрительные decimals', 'decimals_out_of_range'),
    ('mint_meta', 'PASS', 'mint-аккаунт прочитан', 'mint_meta_read'),
    ('mint_meta', 'FAIL', 'не прочитали mint-аккаунт', 'mint_meta_unavailable'),
    ('sol_pair', 'PASS', 'Pump.fun: кривая', 'pumpfun_curve_is_sol_pair'),
    ('sol_pair', 'PASS', 'пара с SOL (DexScreener)', 'sol_pair_dexscreener'),
    ('sol_pair', 'PASS', 'пара с SOL (GeckoTerminal)', 'sol_pair_geckoterminal'),
    ('sol_pair', 'FAIL', 'пара без SOL (GeckoTerminal)', 'non_sol_pair_geckoterminal'),
    ('sol_pair', 'FAIL', 'пара без SOL', 'non_sol_pair_dexscreener'),
    ('sol_pair', 'FAIL', 'пары не найдены', 'pairs_not_indexed'),
    ('pump_curve', 'PASS', 'кривая из WSS-пакета', 'curve_from_wss_packet'),
    ('pump_curve', 'PASS', 'кривая жива', 'curve_alive_rpc'),
    ('pump_curve', 'FAIL', 'не нашли бондинг-кривую', 'curve_not_found'),
    ('pump_curve', 'FAIL', 'не прочитали аккаунт кривой', 'curve_account_unreadable'),
    ('pump_curve', 'FAIL', 'мигрировала', 'curve_migrated'),
    ('pump_curve', 'FAIL', 'пустые резервы', 'curve_empty_reserves'),
    ('liquidity', 'PASS', 'DexScreener liquidity', 'liquidity_ok_dexscreener'),
    ('liquidity', 'PASS', 'GeckoTerminal reserve', 'liquidity_ok_geckoterminal'),
    ('liquidity', 'FAIL', 'ликвидность ниже порога', 'liquidity_below_threshold'),
    ('liquidity', 'FAIL', 'ликвидность не найдена', 'liquidity_not_indexed'),
    ('honeypot_route', 'PASS', 'маршрут продажи через Jupiter существует', 'sell_route_exists'),
    ('honeypot_route', 'FAIL', 'не нашёл маршрут продажи', 'no_sell_route'),
    ('honeypot_route', 'FAIL', 'не дал маршрут продажи (HTTP', 'sell_route_http_error'),
    ('honeypot_route', 'FAIL', 'Jupiter недоступен', 'jupiter_unavailable'),
    ('honeypot_route', 'FAIL', 'не подтвердил маршрут продажи', 'sell_route_unconfirmed'),
    ('*', 'UNKNOWN', 'проверка упала', 'check_exception'),
)


def classify_filter_reason(stage: str, decision: str, text: Optional[str]) -> str:
    text = text or ''
    for st, dec, needle, code in _FILTER_REASONS:
        if (st == stage or st == '*') and dec == decision and needle in text:
            return code
    return f'{stage}_{decision.lower()}_unclassified'


# ------------------------------------------------------------- records ----

# Fields shared by every table. Everything else lands in kind-specific typed
# columns (see schema.TYPED_COLUMNS) or in payload_json.
COMMON_FIELDS = (
    'event_id', 'candidate_id', 'chain_id', 'mint', 'source', 'detected_by',
    'source_time_utc', 'received_at_utc', 'source_age_ms', 'recorded_at_utc',
    'parser_version', 'config_fingerprint', 'code_version', 'supersedes_id',
)

# Raw integer token/lamport amounts: always strings (no float rounding).
RAW_AMOUNT_FIELDS = frozenset({
    'amount_raw', 'out_amount_raw', 'min_out_raw', 'tokens_raw',
    'remaining_raw', 'curve_virtual_sol_raw', 'curve_virtual_token_raw',
    'curve_real_sol_raw', 'curve_real_token_raw', 'supply_raw',
    'notional_lamports',
})


def _as_raw_string(value: Any) -> Optional[str]:
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return str(int(value)) if value.is_integer() else repr(value)
    text = str(value).strip()
    return text or None


def build_record(
    kind: str,
    fields: Dict[str, Any],
    *,
    config_fingerprint: Optional[str] = None,
    code_version: Optional[str] = None,
    detected_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Turn hook/collector fields into one immutable, redacted record.

    `fields` may carry `_received_ts` (epoch float captured at hook time) and
    `_source_ts` (epoch float of the provider event). Received time is when
    *this process* got the data, never when the writer flushed it.
    """
    if kind not in KINDS:
        raise ValueError(f'unsupported record kind: {kind!r}')
    data = dict(fields)
    received_ts = data.pop('_received_ts', None)
    source_ts = data.pop('_source_ts', None)
    record: Dict[str, Any] = {name: None for name in COMMON_FIELDS}
    for name in COMMON_FIELDS:
        if name in data:
            record[name] = data.pop(name)

    record['event_id'] = record['event_id'] or new_event_id()
    record['chain_id'] = record['chain_id'] or CHAIN_ID
    if record['mint'] and not record['candidate_id']:
        record['candidate_id'] = candidate_id(record['chain_id'], record['mint'])
    record['detected_by'] = record['detected_by'] or detected_by
    record['parser_version'] = record['parser_version'] or PARSER_VERSION
    record['config_fingerprint'] = record['config_fingerprint'] or config_fingerprint
    record['code_version'] = record['code_version'] or code_version
    now = utc_iso()
    record['recorded_at_utc'] = now
    if not record['received_at_utc']:
        record['received_at_utc'] = utc_iso(received_ts) if received_ts else now
    if not record['source_time_utc'] and source_ts:
        record['source_time_utc'] = utc_iso(source_ts)
    if record['source_time_utc'] and record['source_age_ms'] is None:
        src = parse_iso(record['source_time_utc'])
        rcv = parse_iso(record['received_at_utc'])
        if src is not None and rcv is not None:
            record['source_age_ms'] = round((rcv - src) * 1000.0, 3)
        else:
            data.setdefault('source_time_status', 'unparseable')

    if kind == 'quotes':
        # Quotes are never executions: hard-coded, not caller-controlled.
        data['quote_only'] = 1
        data['transaction_sent'] = 0
    if kind == 'lifecycle':
        data['is_real_transaction'] = 0
        data.setdefault('paper_calculation', 1)
    if kind == 'decisions':
        decision = str(data.get('decision') or 'UNKNOWN').upper()
        data['decision'] = decision if decision in DECISIONS else 'UNKNOWN'
        if not data.get('reason_code'):
            data['reason_code'] = classify_filter_reason(
                str(data.get('stage') or 'unknown'), data['decision'],
                data.get('reason_text'),
            )

    for name in RAW_AMOUNT_FIELDS:
        if name in data:
            data[name] = _as_raw_string(data[name])

    record.update(data)
    # Redact everything once, at the boundary to persistent storage.
    return redact(record)


def iter_json_lines(lines: Iterable[str]) -> Iterable[Dict[str, Any]]:
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            yield item
