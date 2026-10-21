"""Observation hooks called from the bot pipeline.

Contract (enforced by tests):
* every public hook is wrapped by `_guard`: it never raises into the bot;
* hooks never await, never do IO, never touch the network: they build a small
  dict and `put_nowait` it into a bounded queue (or do nothing when disabled);
* hooks never return values that the bot uses for decisions (only event ids /
  None, ignored by callers except for linking telemetry records);
* hooks never receive wallet/keys; quotes are always quote_only.

Telemetry is OFF unless the bot process has TELEMETRY_DIR set; then a daemon
thread writes redacted JSONL into TELEMETRY_DIR/spool/ (no SQLite in the bot).
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import os
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, Optional

from .model import WSOL, new_event_id
from .observer import NullObserver, Observer, WriterThread

_OBS: Any = NullObserver()
_WRITER: Optional[WriterThread] = None
_SINK: Any = None
_quote_ctx: 'contextvars.ContextVar[Optional[Dict[str, Any]]]' = contextvars.ContextVar(
    'telemetry_quote_ctx', default=None)
_sig_events: 'OrderedDict[str, str]' = OrderedDict()   # signature -> detection event_id
_SIG_MAX = 50_000


def _guard(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _OBS.enabled:
            return None
        try:
            return fn(*args, **kwargs)
        except Exception:
            # Do not hide the loss: it becomes a health/data_loss record.
            try:
                _OBS._loss(f'hook_error:{fn.__name__}')
            except Exception:
                pass
            return None
    return wrapper


def _emit(kind: str, **fields: Any) -> Optional[str]:
    fields.setdefault('_received_ts', time.time())
    event_id = fields.setdefault('event_id', new_event_id())
    _OBS.emit(kind, fields)
    return event_id


def _value(observed: Any) -> Any:
    return observed() if callable(observed) else observed


def _blank_to_none(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value


# ------------------------------------------------------------ lifecycle ----

def enabled() -> bool:
    return bool(_OBS.enabled)


def install(observer: Any, writer: Optional[WriterThread] = None, sink: Any = None) -> None:
    """Testing/advanced: install a specific observer (e.g. a broken one)."""
    global _OBS, _WRITER, _SINK
    _OBS, _WRITER, _SINK = observer, writer, sink


def uninstall() -> None:
    install(NullObserver())


def install_from_env(settings: Any = None) -> bool:
    """Enable telemetry iff TELEMETRY_DIR is set. Never raises."""
    data_dir = os.getenv('TELEMETRY_DIR', '').strip()
    if not data_dir:
        return False
    try:
        from .store import SpoolSink
        from .version import code_version, config_fingerprint, safe_settings

        capacity = int(os.getenv('TELEMETRY_QUEUE_MAX', '50000'))
        observer = Observer(capacity, config_fingerprint=config_fingerprint(settings),
                            code_version=code_version(), detected_by='bot')
        sink = SpoolSink(data_dir)
        writer = WriterThread(observer, sink)
        writer.start()
        install(observer, writer, sink)
        _emit('health', health_type='config_snapshot', status='bot_telemetry_enabled',
              settings=safe_settings(settings), pid=os.getpid())
        return True
    except Exception:
        uninstall()
        return False


def shutdown(timeout: float = 3.0) -> None:
    """Flush and stop the writer (called after trading has finished)."""
    try:
        if _OBS.enabled:
            _emit('health', health_type='bot_shutdown', status='stopping')
        writer, sink = _WRITER, _SINK
        if writer is not None:
            writer.stop(timeout)
        if sink is not None and hasattr(sink, 'close'):
            sink.close()
    except Exception:
        pass
    finally:
        uninstall()


def now() -> float:
    """Monotonic timestamp for latency measurement (0.0 when disabled)."""
    return time.monotonic() if _OBS.enabled else 0.0


# ----------------------------------------------------------- detection ----

def _remember_signature(signature: str, event_id: str) -> None:
    if not signature:
        return
    _sig_events[signature] = event_id
    if len(_sig_events) > _SIG_MAX:
        _sig_events.popitem(last=False)


@_guard
def helius_creation_log(source: str, result: Dict[str, Any], duplicate_signature: bool) -> Optional[str]:
    """Creation log from Helius logsSubscribe, BEFORE signature dedup."""
    signature = result.get('signature') or None
    dedup = 'duplicate_signature' if duplicate_signature else 'first_signature'
    event_id = _emit('detections', source=source, detection_type='helius_creation_log',
                     dedup_decision=dedup, signature=signature, slot=result.get('slot'),
                     raw_json=dict(result), metadata_status='mint_unknown_until_getTransaction')
    if not duplicate_signature:
        _remember_signature(signature or '', event_id)
    _emit('decisions', source=source, stage='detector_signature_dedup',
          decision='SKIP' if duplicate_signature else 'PASS',
          reason_code=dedup, reason_text='bot detector signature dedup',
          observed_value={'signature': signature, 'detection_event_id': event_id},
          outcome='dropped_duplicate' if duplicate_signature else 'spawned_mint_resolution')
    return event_id


@_guard
def helius_ignored_after_watch_stop(source: str, result: Dict[str, Any],
                                    is_creation: Callable[[str, Any], bool]) -> None:
    if not is_creation(source, result.get('logs') or []):
        return
    event_id = _emit('detections', source=source, detection_type='helius_creation_log',
                     dedup_decision='ignored_after_watch_stop', signature=result.get('signature'),
                     raw_json=dict(result))
    _emit('decisions', source=source, stage='detector_watch_stop', decision='SKIP',
          reason_code='watch_stop_set', reason_text='mode=one target already taken; bot ignores new coins',
          observed_value={'detection_event_id': event_id}, outcome='not_forwarded')


@_guard
def helius_tx_fetched(signature: str, tx: Dict[str, Any]) -> None:
    block_time = tx.get('blockTime')
    _emit('detections', detection_type='helius_transaction_fetched', signature=signature,
          slot=tx.get('slot'), _source_ts=float(block_time) if block_time else None,
          related_event_id=_sig_events.get(signature),
          dedup_decision='not_applicable', metadata_status='tx_only')


@_guard
def helius_mint_resolved(source: str, signature: str, mint: str, owner: Optional[str],
                         seen_mints: Iterable[str]) -> None:
    duplicate = mint in seen_mints
    related = _sig_events.get(signature)
    _emit('detections', source=source, mint=mint, detection_type='helius_mint_resolved',
          dedup_decision='duplicate_mint' if duplicate else 'first_mint', signature=signature,
          related_event_id=related, bonding_curve=owner if source == 'Pump.fun' else None,
          metadata_status='mint_only')
    _emit('decisions', source=source, mint=mint, stage='detector_mint_dedup',
          decision='SKIP' if duplicate else 'PASS',
          reason_code='duplicate_mint' if duplicate else 'forwarded_to_trader',
          reason_text='bot detector mint dedup', outcome='not_forwarded' if duplicate else 'queued_for_trader',
          observed_value={'signature': signature, 'detection_event_id': related})


@_guard
def helius_mint_unresolved(source: str, signature: str) -> None:
    _emit('decisions', source=source, stage='detector_mint_extraction', decision='UNKNOWN',
          reason_code='mint_not_extracted', reason_text='getTransaction/account scan gave no mint',
          observed_value={'signature': signature, 'detection_event_id': _sig_events.get(signature)},
          outcome='not_forwarded')


def _pumpportal_dedup(msg: Dict[str, Any], seen_mints: Iterable[str]) -> str:
    # Mirrors (read-only) the bot's on_transaction_signal acceptance order.
    if msg.get('txType') != 'create':
        return 'not_create_tx'
    mint = msg.get('mint') or ''
    if not mint:
        return 'missing_mint'
    if mint in seen_mints:
        return 'duplicate_mint'
    try:
        if float(msg.get('vTokensInBondingCurve') or 0) <= 0 or float(msg.get('vSolInBondingCurve') or 0) <= 0:
            return 'invalid_reserves'
    except (TypeError, ValueError):
        return 'invalid_reserves'
    return 'first_mint'


def pumpportal_detection_fields(msg: Dict[str, Any], dedup: str) -> Dict[str, Any]:
    """Shared by bot hook and collector: PumpPortal create message -> detection."""
    name = _blank_to_none(msg.get('name'))
    symbol = _blank_to_none(msg.get('symbol'))
    return dict(
        source='Pump.fun', mint=_blank_to_none(msg.get('mint')),
        detection_type='pumpportal_create', dedup_decision=dedup,
        signature=_blank_to_none(msg.get('signature')),
        symbol=symbol, name=name, creator=_blank_to_none(msg.get('traderPublicKey')),
        bonding_curve=_blank_to_none(msg.get('bondingCurveKey')),
        pool=_blank_to_none(msg.get('pool')),
        metadata_status='present' if (name and symbol) else ('partial' if (name or symbol) else 'missing'),
        raw_json=dict(msg),
    )


@_guard
def pumpportal_raw(msg: Dict[str, Any], seen_mints: Iterable[str]) -> Optional[str]:
    """Every PumpPortal message with a mint, BEFORE the bot's dedup/parse."""
    dedup = _pumpportal_dedup(msg, seen_mints)
    return _emit('detections', **pumpportal_detection_fields(msg, dedup))


@_guard
def pumpportal_forwarded(raw_event_id: Optional[str], msg: Dict[str, Any], coin: Any) -> None:
    forwarded = coin is not None
    _emit('decisions', source='Pump.fun', mint=_blank_to_none(msg.get('mint')),
          stage='detector_pumpportal_parse', decision='PASS' if forwarded else 'SKIP',
          reason_code='forwarded_to_trader' if forwarded else 'ignored_by_bot_parser',
          reason_text='on_transaction_signal result',
          observed_value={'detection_event_id': raw_event_id},
          outcome='queued_for_trader' if forwarded else 'not_forwarded')


# ------------------------------------------------------- decisions/filters ----

def _coin_fields(coin: Any) -> Dict[str, Any]:
    return dict(mint=getattr(coin, 'mint', None), source=getattr(coin, 'source', None))


@_guard
def candidate_received(coin: Any) -> None:
    """Trader got the coin: BEFORE filters, slot check and entry decision."""
    packet = dict(getattr(coin, 'packet', None) or {})
    found_at = getattr(coin, 'found_at', None)
    _emit('decisions', **_coin_fields(coin), stage='trader_intake', decision='PASS',
          reason_code='received_by_trader', reason_text='coin dequeued by trader before filters',
          observed_value={
              'signature': getattr(coin, 'signature', None) or None,
              'bonding_curve': getattr(coin, 'curve', None),
              'creator': getattr(coin, 'creator', None),
              'packet': packet,
              'coin_found_at_epoch': found_at,
          }, outcome='filters_pending')


@_guard
def filter_stage(coin: Any, stage: str, result: Any = None, *, error: Optional[BaseException] = None,
                 observed: Any = None, threshold: Any = None, unit: Optional[str] = None,
                 data_source: Optional[str] = None) -> None:
    if error is not None or result is None:
        decision, reasons = 'UNKNOWN', [f'проверка упала: {type(error).__name__}: {error}' if error else 'no result']
    else:
        decision = 'PASS' if result.passed else 'FAIL'
        reasons = list(result.reasons)
    for reason in reasons or ['']:
        _emit('decisions', **_coin_fields(coin), stage=stage, decision=decision,
              reason_text=reason, observed_value=_value(observed), threshold=threshold,
              unit=unit, data_source=data_source, outcome='filter_stage_result')


@_guard
def filters_not_evaluated(coin: Any, stages: Iterable[str], reason_code: str) -> None:
    for stage in stages:
        _emit('decisions', **_coin_fields(coin), stage=stage, decision='UNKNOWN',
              reason_code=reason_code, reason_text='bot returned early; stage result not used',
              outcome='not_evaluated')


@_guard
def filter_metric(mint: str, stage: str, metric: str, value: Any, *, unit: Optional[str] = None,
                  threshold: Any = None, data_source: Optional[str] = None,
                  attempt: Optional[int] = None, status: str = 'ok') -> None:
    _emit('observations', mint=mint, obs_type='filter_metric', provider=data_source,
          status=status, metric=f'{stage}.{metric}', metric_value=value, unit=unit,
          threshold=threshold, attempt=attempt)


@_guard
def filters_verdict(coin: Any, verdict: Any) -> None:
    passed = bool(verdict.passed)
    _emit('decisions', **_coin_fields(coin), stage='filters_total',
          decision='PASS' if passed else 'FAIL',
          reason_code='all_filters_passed' if passed else 'blocked_by_filters',
          reason_text=' | '.join(verdict.reasons), observed_value=list(verdict.reasons),
          outcome='would_enter_paper_if_slot_and_quote' if passed else 'blocked')


@_guard
def entry_decision(coin: Any, decision: str, reason_code: str, outcome: str, *,
                   stage: str = 'entry', reason_text: Optional[str] = None,
                   observed: Any = None, threshold: Any = None, unit: Optional[str] = None) -> None:
    _emit('decisions', **_coin_fields(coin), stage=stage, decision=decision,
          reason_code=reason_code, reason_text=reason_text or reason_code,
          observed_value=_value(observed), threshold=threshold, unit=unit, outcome=outcome)


# --------------------------------------------------------------- quotes ----

@_guard
def quote_context(purpose: str, **ctx: Any) -> None:
    ctx['purpose'] = purpose
    ctx.setdefault('attempt', 0)
    _quote_ctx.set(ctx)


@_guard
def quote_attempt(attempt: int) -> None:
    ctx = dict(_quote_ctx.get() or {})
    ctx['attempt'] = attempt
    _quote_ctx.set(ctx)


def _classify_exception(error: BaseException) -> str:
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return 'timeout'
    name = type(error).__name__
    if 'Timeout' in name:
        return 'timeout'
    if 'JSON' in name or 'ContentType' in name or isinstance(error, ValueError):
        return 'invalid_json'
    return 'request_error'


def quote_fields_from_jupiter(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    labels = []
    for step in data.get('routePlan') or []:
        if isinstance(step, dict) and isinstance(step.get('swapInfo'), dict):
            labels.append(str(step['swapInfo'].get('label') or 'DEX'))
    impact = data.get('priceImpactPct')
    try:
        impact_pct = float(impact) * 100.0 if impact is not None else None
    except (TypeError, ValueError):
        impact_pct = None
    return dict(out_amount_raw=data.get('outAmount'), min_out_raw=data.get('otherAmountThreshold'),
                price_impact_pct=impact_pct, route_labels=labels or None,
                context_slot=data.get('contextSlot'), provider_time_taken=data.get('timeTaken'))


@_guard
def jupiter_quote(input_mint: str, output_mint: str, amount_raw: Any, slippage_bps: Any,
                  status: str, *, t0: float = 0.0, http_status: Optional[int] = None,
                  data: Any = None, error: Optional[BaseException] = None) -> None:
    """Every bot get_jupiter_quote outcome (success AND failure)."""
    ctx = dict(_quote_ctx.get() or {})
    purpose = ctx.pop('purpose', 'bot_unlabeled')
    if error is not None and status == 'exception':
        status = _classify_exception(error)
    if http_status == 429:
        status = 'http_429'
    mint = output_mint if input_mint == WSOL else input_mint
    _emit('quotes', mint=mint, source='bot', provider='jupiter', purpose=purpose,
          direction='buy' if input_mint == WSOL else 'sell', status=status,
          status_reason=f'{type(error).__name__}: {error}' if error else None,
          http_status=http_status,
          latency_ms=round((time.monotonic() - t0) * 1000.0, 3) if t0 else None,
          input_mint=input_mint, output_mint=output_mint, amount_raw=amount_raw,
          slippage_bps=slippage_bps, attempt=ctx.pop('attempt', None),
          input_decimals=9 if input_mint == WSOL else ctx.pop('input_decimals', None),
          output_decimals=9 if output_mint == WSOL else ctx.pop('output_decimals', None),
          notional_rub=ctx.pop('notional_rub', None),
          notional_sol=(int(amount_raw) / 1e9) if input_mint == WSOL else None,
          sol_usd_used=ctx.pop('sol_usd', None), usd_rub_used=ctx.pop('usd_rub', None),
          context=ctx, raw_response=data if isinstance(data, (dict, list)) else None,
          **quote_fields_from_jupiter(data))


@_guard
def honeypot_probe(mint: str, attempt: int, *, t0: float = 0.0, http_status: Optional[int] = None,
                   data: Any = None, error: Optional[BaseException] = None) -> None:
    if error is not None:
        status = _classify_exception(error)
    elif http_status == 200:
        status = 'ok' if isinstance(data, dict) and data.get('outAmount') else 'no_route'
    elif http_status == 429:
        status = 'http_429'
    else:
        status = 'http_error'
    _emit('quotes', mint=mint, source='bot', provider='jupiter', purpose='bot_honeypot_probe',
          direction='sell', status=status,
          status_reason=f'{type(error).__name__}: {error}' if error else None,
          http_status=http_status,
          latency_ms=round((time.monotonic() - t0) * 1000.0, 3) if t0 else None,
          input_mint=mint, output_mint=WSOL, amount_raw='1000000', slippage_bps=500,
          output_decimals=9, attempt=attempt,
          raw_response=data if isinstance(data, (dict, list)) else None,
          **quote_fields_from_jupiter(data))


# ------------------------------------------------------- paper lifecycle ----

def position_key(pos: Any) -> str:
    return f"{getattr(pos, 'mint', '')}:{getattr(pos, 'opened_at', '')}"


def _pos_fields(pos: Any) -> Dict[str, Any]:
    tokens_raw = getattr(pos, 'tokens_raw', None)
    sold = getattr(pos, 'sold_tokens_raw', None)
    remaining = (tokens_raw - sold) if isinstance(tokens_raw, int) and isinstance(sold, int) else None
    return dict(
        mint=getattr(pos, 'mint', None), source=getattr(pos, 'source', None),
        position_key=position_key(pos), tokens_raw=tokens_raw, remaining_raw=remaining,
        sold_tokens_raw=str(sold) if sold is not None else None,
        sold_frac=getattr(pos, 'sold_frac', None), decimals=getattr(pos, 'decimals', None),
        entry_price_usd=getattr(pos, 'entry_price', None),
        invested_rub=getattr(pos, 'invested_rub', None),
        realized_rub=getattr(pos, 'realized_rub', None), fees_rub=getattr(pos, 'fees_rub', None),
        tp1_done=getattr(pos, 'tp1_done', None), exit_pending=getattr(pos, 'exit_pending', None),
        opened_at_epoch=getattr(pos, 'opened_at', None), ath_price_usd=getattr(pos, 'ath_price', None),
    )


@_guard
def lifecycle(lifecycle_type: str, pos: Any, **values: Any) -> None:
    fields = _pos_fields(pos)
    fields.update(values)
    _emit('lifecycle', lifecycle_type=lifecycle_type, **fields)


@_guard
def spot_tick(pos: Any, price: Optional[float], origin: str) -> None:
    ok = price is not None
    _emit('observations', mint=getattr(pos, 'mint', None), source='bot', obs_type='bot_spot_tick',
          provider=origin or None, status='ok' if ok else 'missing',
          status_reason=None if ok else 'resolve_real_price returned no price',
          price_usd=price if ok else None, position_key=position_key(pos),
          entry_price_usd=getattr(pos, 'entry_price', None))


@_guard
def positions_at_shutdown(open_positions: Iterable[Dict[str, Any]]) -> None:
    for saved in list(open_positions or []):
        tokens_raw = saved.get('tokens_raw')
        sold = saved.get('sold_tokens_raw')
        _emit('lifecycle', lifecycle_type='paper_position_open_at_shutdown',
              mint=saved.get('mint'), source=saved.get('source'),
              position_key=f"{saved.get('mint', '')}:{saved.get('opened_at', '')}",
              tokens_raw=tokens_raw,
              remaining_raw=(tokens_raw - sold) if isinstance(tokens_raw, int) and isinstance(sold, int) else None,
              sold_frac=saved.get('sold_frac'), exit_pending=saved.get('exit_pending'),
              pending_exit_reason=saved.get('pending_exit_reason'))
