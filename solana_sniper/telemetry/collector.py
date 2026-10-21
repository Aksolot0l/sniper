"""Standalone passive collector (read-only). Run explicitly; never imported by the bot.

    cd solana_sniper && python -m telemetry run --data-dir ../telemetry_data

What it does
* discovers Pump.fun creations on its own PumpPortal subscription (independent
  of the bot's watch_stop), and ingests the bot's hook spool (all sources,
  filters, decisions, quotes, paper lifecycle) into one SQLite writer;
* for every candidate schedules follow-ups at fixed horizons from first
  detection: batched DexScreener market snapshot, optional batched Pump.fun
  curve read (RPC allowlist), and quote-only Jupiter samples;
* runs rate-limited shadow quotes (buy, then exact-size sell for the amount
  returned by the buy quote) for a deterministic sample of candidates.

Nothing here can sign or send a transaction.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import itertools
import json
import os
import random
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from . import providers
from .config import CollectorConfig
from .hooks import pumpportal_detection_fields
from .model import WSOL, candidate_id, new_event_id, parse_iso, redact_text, stable_id, utc_iso
from .observer import Observer, WriterThread
from .ratelimit import ProviderLimiter
from .store import SPOOL_DIR, Store, read_spool_chunk
from .version import code_version


@dataclass
class Candidate:
    cid: str
    mint: str
    source: Optional[str]
    anchor_ts: float
    anchor_event_id: Optional[str]
    curve: Optional[str] = None
    sampled: bool = False
    shadow_tokens_raw: Optional[int] = None     # from primary-notional shadow buy
    shadow_lamports: Optional[int] = None
    scheduled: Set[str] = field(default_factory=set)
    finished: Set[str] = field(default_factory=set)


@dataclass
class FollowupJob:
    cand: Candidate
    label: str
    horizon_sec: float
    target_ts: float
    deadline_ts: float
    parts: Dict[str, Optional[Tuple[str, List[str], Optional[str]]]] = field(default_factory=dict)
    started_ts: Optional[float] = None
    refusal: Dict[str, str] = field(default_factory=dict)
    resolved_ts: Dict[str, float] = field(default_factory=dict)


def sampled_for_quotes(cid: str, rate: float) -> bool:
    """Deterministic, reproducible sample (independent of any outcome)."""
    if rate >= 1.0:
        return True
    if rate <= 0.0:
        return False
    bucket = int(hashlib.sha256(cid.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return bucket < rate


class SingleInstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = None

    def acquire(self) -> bool:
        self._fh = open(self.path, 'a+')
        try:
            import fcntl
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:  # pragma: no cover - non-POSIX fallback
            return True
        except OSError:
            self._fh.close()
            self._fh = None
            return False
        return True

    def release(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


def collector_running(data_dir: Path) -> bool:
    path = data_dir / 'collector.lock'
    if not path.exists():
        return False
    try:
        import fcntl
    except ImportError:  # pragma: no cover
        return (data_dir / 'collector.pid').exists()
    with open(path, 'a+') as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    return False


class Collector:
    def __init__(self, cfg: CollectorConfig, *, session: Any = None,
                 ws_connect: Optional[Callable[..., Any]] = None,
                 clock: Callable[[], float] = time.time,
                 start_writer: bool = True) -> None:
        self.cfg = cfg
        self.clock = clock
        self.session = session
        self.ws_connect = ws_connect
        self.store = Store(cfg.data_dir, raw_max_bytes=int(cfg.raw_max_mb * 1024 * 1024))
        self.root = self.store.root
        self.code_version = code_version()
        self.observer = Observer(cfg.queue_capacity, config_fingerprint=cfg.fingerprint(),
                                 code_version=self.code_version, detected_by='collector')
        self.writer = WriterThread(self.observer, self.store, interval=0.25,
                                   periodic=[self._ingest_spool_periodic, self._maintenance_periodic])
        self._start_writer = start_writer
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.lim_jup = ProviderLimiter('jupiter', cfg.jupiter_rpm, concurrency=2)
        self.lim_dex = ProviderLimiter('dexscreener', cfg.dexscreener_rpm, concurrency=2)
        self.lim_rpc = ProviderLimiter('rpc', cfg.rpc_rpm, concurrency=2)
        self.rpc = providers.ReadOnlyRpc(cfg.rpc_url) if cfg.rpc_url else None
        self.candidates: Dict[str, Candidate] = {}
        self.heap: List[Tuple[float, int, str, str]] = []
        self._seq = itertools.count()
        self.dex_queue: List[FollowupJob] = []
        self.curve_queue: List[FollowupJob] = []
        self.sol_usd: Optional[Tuple[float, float]] = None
        self.stop_event: Optional[asyncio.Event] = None
        self.tasks: Set[asyncio.Task] = set()
        self.pending_shadow = 0
        self.storage_limited = False
        self.counters: Dict[str, int] = {}
        self._last_health = 0.0
        self._last_backup = time.time()
        self.max_horizon = max(sec for _, sec in cfg.horizons)
        self.max_rpc_slot = 0
        self.sleep = asyncio.sleep     # injectable for offline tests
        self.stale_slot_lag = 150      # ~60 s of slots

    # ------------------------------------------------------------ emit ----
    def emit(self, kind: str, **fields: Any) -> str:
        fields.setdefault('_received_ts', self.clock())
        event_id = fields.setdefault('event_id', new_event_id())
        self.observer.emit(kind, fields)
        return event_id

    def _count(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n

    # ------------------------------------------------------- candidates ----
    def register_candidate(self, mint: str, source: Optional[str], anchor_ts: float,
                           anchor_event_id: Optional[str], curve: Optional[str] = None,
                           *, recovering: bool = False) -> Optional[Candidate]:
        if not mint:
            return None
        cid = candidate_id('solana-mainnet', mint)
        cand = self.candidates.get(cid)
        if cand is not None:
            if curve and not cand.curve:
                cand.curve = curve
            return cand
        cand = Candidate(cid, mint, source, anchor_ts, anchor_event_id, curve,
                         sampled=sampled_for_quotes(cid, self.cfg.quote_sample_rate))
        self.candidates[cid] = cand
        self._count('candidates_registered')
        now = self.clock()
        for label, sec in self.cfg.horizons:
            target = anchor_ts + sec
            if target + self.cfg.tolerance(sec) < now:
                self._emit_missed(cand, label, sec, target,
                                  'collector_not_running_at_target' if recovering
                                  else 'candidate_registered_after_horizon')
                continue
            cand.scheduled.add(label)
            heapq.heappush(self.heap, (target, next(self._seq), cid, label))
        if not recovering:
            self._schedule_shadow(cand)
        return cand

    def _emit_missed(self, cand: Candidate, label: str, sec: float, target: float, reason: str) -> None:
        cand.finished.add(label)
        self.emit('followups', event_id=stable_id('followup', cand.cid, label), mint=cand.mint,
                  source=cand.source, followup_id=stable_id(cand.cid, label), horizon_label=label,
                  horizon_sec=sec, anchor_time_utc=utc_iso(cand.anchor_ts),
                  anchor_event_id=cand.anchor_event_id, target_at_utc=utc_iso(target),
                  status='missed', status_reason=reason)
        self._count(f'followup_missed_{reason}')

    def on_pumpportal_message(self, msg: Dict[str, Any]) -> None:
        mint = msg.get('mint') if isinstance(msg, dict) else None
        if not mint:
            return
        received = self.clock()
        known = candidate_id('solana-mainnet', mint) in self.candidates
        dedup = 'duplicate_mint_collector' if known else 'first_seen_by_collector'
        if msg.get('txType') not in (None, 'create'):
            dedup = 'not_create_tx'
        fields = pumpportal_detection_fields(msg, dedup)
        event_id = self.emit('detections', _received_ts=received, **fields)
        self._count('collector_detections')
        if dedup != 'not_create_tx':
            self.register_candidate(mint, 'Pump.fun', received, event_id, fields.get('bonding_curve'))

    def on_spool_record(self, record: Dict[str, Any]) -> None:
        mint = record.get('mint')
        if not mint or record.get('kind') not in ('detections', 'decisions'):
            return
        anchor = parse_iso(record.get('received_at_utc')) or self.clock()
        self.register_candidate(mint, record.get('source'), anchor, record.get('event_id'),
                                record.get('bonding_curve'))

    # ------------------------------------------------------ spool ingest ----
    def _ingest_spool_periodic(self) -> None:
        if not self.cfg.ingest_bot_spool:
            return
        self.ingest_spool_once()

    def ingest_spool_once(self, max_records: int = 5000) -> int:
        """Writer thread: bot spool -> SQLite (idempotent by event_id)."""
        offsets = self.store.spool_offsets()
        records, new_offsets = read_spool_chunk(self.root / SPOOL_DIR, offsets, max_records)
        if not new_offsets:
            return 0
        valid = [r for r in records if r.get('kind') in ('detections', 'decisions', 'observations',
                                                          'quotes', 'followups', 'lifecycle', 'health')
                 and r.get('event_id') and r.get('received_at_utc')]
        self.store.append(valid, offsets=new_offsets)
        if self.loop is not None:
            for record in valid:
                if record.get('mint'):
                    self.loop.call_soon_threadsafe(self.on_spool_record, record)
        return len(valid)

    def _maintenance_periodic(self) -> None:
        now = time.time()
        if self.cfg.backup_interval_hours > 0 and now - self._last_backup >= self.cfg.backup_interval_hours * 3600:
            self._last_backup = now
            self.store.backup(self.cfg.backups_keep)

    # ------------------------------------------------------------ shadow ----
    def _schedule_shadow(self, cand: Candidate) -> None:
        if not cand.sampled:
            for notional in self.cfg.notionals_rub:
                self.emit('quotes', mint=cand.mint, source=cand.source, provider='jupiter',
                          purpose='shadow_buy', direction='buy', status='not_sampled',
                          status_reason=f'deterministic sample rate {self.cfg.quote_sample_rate}',
                          input_mint=WSOL, output_mint=cand.mint, notional_rub=notional,
                          slippage_bps=self.cfg.slippage_bps,
                          quote_pair_id=stable_id('shadow', cand.cid, notional))
            return
        if self.pending_shadow >= self.cfg.max_pending_shadow_jobs:
            for notional in self.cfg.notionals_rub:
                self.emit('quotes', mint=cand.mint, source=cand.source, provider='jupiter',
                          purpose='shadow_buy', direction='buy', status='skipped_queue_overflow',
                          status_reason=f'pending shadow jobs >= {self.cfg.max_pending_shadow_jobs}',
                          input_mint=WSOL, output_mint=cand.mint, notional_rub=notional,
                          quote_pair_id=stable_id('shadow', cand.cid, notional))
            return
        self._spawn(self._shadow_quotes(cand))

    async def _acquire(self, limiter: ProviderLimiter, deadline: float) -> Optional[str]:
        reason = None
        while True:
            reason = limiter.try_acquire()
            if reason is None:
                return None
            if self.clock() >= deadline or (self.stop_event and self.stop_event.is_set()):
                return reason
            await self.sleep(0.2 + random.random() * 0.1)

    async def _quote(self, cand: Candidate, purpose: str, input_mint: str, output_mint: str,
                     amount_raw: int, deadline: float, **extra: Any) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """One quote-only request with budget/retry policy; always records a row."""
        base = dict(mint=cand.mint, source=cand.source, provider='jupiter', purpose=purpose,
                    direction='buy' if input_mint == WSOL else 'sell', input_mint=input_mint,
                    output_mint=output_mint, amount_raw=amount_raw,
                    slippage_bps=self.cfg.slippage_bps,
                    input_decimals=9 if input_mint == WSOL else None,
                    output_decimals=9 if output_mint == WSOL else None, **extra)
        if self.storage_limited:
            eid = self.emit('quotes', status='skipped_storage_limit', **base)
            return 'skipped_storage_limit', None, eid
        for attempt in range(3):
            refusal = await self._acquire(self.lim_jup, deadline)
            if refusal is not None:
                eid = self.emit('quotes', status=f'skipped_{refusal}', attempt=attempt,
                                status_reason='no request sent: provider budget/cooldown', **base)
                return f'skipped_{refusal}', None, eid
            try:
                result, fields = await providers.jupiter_quote(
                    self.session, input_mint, output_mint, amount_raw, self.cfg.slippage_bps,
                    url=self.cfg.jupiter_quote_url)
            except asyncio.CancelledError:
                self.lim_jup.release('cancelled')
                raise
            except Exception as exc:
                self.lim_jup.release('collector_error')
                eid = self.emit('quotes', status='collector_error', attempt=attempt,
                                status_reason=type(exc).__name__, **base)
                return 'collector_error', None, eid
            self.lim_jup.release(result.status, result.retry_after)
            eid = self.emit('quotes', _received_ts=result.received_ts, status=result.status,
                            status_reason=result.error, http_status=result.http_status,
                            latency_ms=result.latency_ms, attempt=attempt,
                            raw_response=result.data, **fields, **base)
            # Retry only transient failures, with backoff + jitter, bounded.
            if result.status in ('timeout', 'request_error', 'http_5xx') and attempt < 2:
                delay = self.lim_jup.jitter(1.0 * 2 ** attempt)
                if self.clock() + delay < deadline:
                    await self.sleep(delay)
                    continue
            return result.status, (fields if result.status == 'ok' else None), eid
        return 'retries_exhausted', None, ''

    async def _shadow_quotes(self, cand: Candidate) -> None:
        self.pending_shadow += 1
        try:
            deadline = self.clock() + self.cfg.shadow_quote_deadline_sec
            sol, sol_reason = self._sol_usd_state()
            for index, notional in enumerate(self.cfg.notionals_rub):
                pair_id = stable_id('shadow', cand.cid, notional)
                if sol is None:
                    self.emit('quotes', mint=cand.mint, source=cand.source, provider='jupiter',
                              purpose='shadow_buy', direction='buy', status=f'skipped_{sol_reason}',
                              status_reason='no fresh SOL/USD observation (stale values are not used)',
                              input_mint=WSOL,
                              output_mint=cand.mint, notional_rub=notional, quote_pair_id=pair_id)
                    continue
                lamports = int(notional / (sol * self.cfg.usd_rub_rate) * 1e9)
                ctx = dict(notional_rub=notional, notional_sol=lamports / 1e9, sol_usd_used=sol,
                           usd_rub_used=self.cfg.usd_rub_rate, quote_pair_id=pair_id)
                status, fields, _ = await self._quote(cand, 'shadow_buy', WSOL, cand.mint, lamports,
                                                      deadline, **ctx)
                if status != 'ok' or not fields:
                    self.emit('quotes', mint=cand.mint, source=cand.source, provider='jupiter',
                              purpose='shadow_sell_exact', direction='sell', status='skipped_no_buy_quote',
                              status_reason=f'buy quote status={status}', input_mint=cand.mint,
                              output_mint=WSOL, quote_pair_id=pair_id, notional_rub=notional)
                    continue
                tokens = int(fields['out_amount_raw'])
                if index == 0:
                    cand.shadow_tokens_raw, cand.shadow_lamports = tokens, lamports
                await self._quote(cand, 'shadow_sell_exact', cand.mint, WSOL, tokens, deadline,
                                  exact_size_from='shadow_buy.out_amount_raw', **ctx)
        finally:
            self.pending_shadow -= 1

    def _fresh_sol_usd(self) -> Optional[float]:
        price, _ = self._sol_usd_state()
        return price

    def _sol_usd_state(self) -> Tuple[Optional[float], str]:
        """Stale prices are never used: (price, '') or (None, reason)."""
        if not self.sol_usd:
            return None, 'no_sol_usd'
        if self.clock() - self.sol_usd[0] > 3 * self.cfg.sol_usd_refresh_sec:
            return None, 'stale_sol_usd'
        return self.sol_usd[1], ''

    async def refresh_sol_usd_once(self) -> None:
        refusal = self.lim_dex.try_acquire()
        if refusal is not None:
            self.emit('observations', obs_type='sol_usd', provider='dexscreener',
                      status=f'skipped_{refusal}')
            return
        try:
            result, price = await providers.sol_usd_price(self.session)
        except BaseException:
            self.lim_dex.release('collector_error')
            raise
        self.lim_dex.release(result.status, result.retry_after)
        status = result.status if result.status != 'ok' else ('ok' if price else 'missing')
        self.emit('observations', _received_ts=result.received_ts, obs_type='sol_usd', mint=None,
                  provider='dexscreener', status=status, status_reason=result.error,
                  http_status=result.http_status, latency_ms=result.latency_ms, price_usd=price,
                  quote_asset='USD', metric='WSOL/USD')
        if price:
            self.sol_usd = (result.received_ts, price)

    # --------------------------------------------------------- followups ----
    def _start_due_jobs(self) -> None:
        now = self.clock()
        while self.heap and self.heap[0][0] <= now:
            target, _, cid, label = heapq.heappop(self.heap)
            cand = self.candidates.get(cid)
            if cand is None or label in cand.finished:
                continue
            sec = dict(self.cfg.horizons)[label]
            job = FollowupJob(cand, label, sec, target, target + self.cfg.tolerance(sec), started_ts=now)
            if self.storage_limited:
                job.parts['market'] = ('skipped_storage_limit', [], 'storage limit reached')
                self._finish(job)
                continue
            job.parts['market'] = None
            self.dex_queue.append(job)
            if self.rpc and cand.curve:
                job.parts['curve'] = None
                self.curve_queue.append(job)
            if cand.sampled and label in self.cfg.quote_horizons:
                job.parts['quote'] = None
                self._spawn(self._followup_quote(job))

    def _resolve(self, job: FollowupJob, part: str, status: str, ids: List[str],
                 reason: Optional[str] = None) -> None:
        if job.parts.get(part) is not None:
            return
        job.parts[part] = (status, ids, reason)
        job.resolved_ts[part] = self.clock()
        if all(v is not None for v in job.parts.values()):
            self._finish(job)

    def _finish(self, job: FollowupJob) -> None:
        cand = job.cand
        if job.label in cand.finished:
            return
        cand.finished.add(job.label)
        statuses = {k: v[0] for k, v in job.parts.items() if v}
        ok = [k for k, s in statuses.items() if s == 'ok']
        status = 'complete' if len(ok) == len(statuses) else ('partial' if ok else 'failed')
        obs_ids = [i for k, v in job.parts.items() if v and k != 'quote' for i in v[1]]
        quote_ids = list(job.parts['quote'][1]) if job.parts.get('quote') else []
        now = self.clock()
        # actual = when the horizon's primary data point (market snapshot) arrived
        actual = job.resolved_ts.get('market') or min(job.resolved_ts.values(), default=now)
        self.emit('followups', event_id=stable_id('followup', cand.cid, job.label), mint=cand.mint,
                  source=cand.source, followup_id=stable_id(cand.cid, job.label),
                  horizon_label=job.label, horizon_sec=job.horizon_sec,
                  anchor_time_utc=utc_iso(cand.anchor_ts), anchor_event_id=cand.anchor_event_id,
                  target_at_utc=utc_iso(job.target_ts), actual_at_utc=utc_iso(actual),
                  delay_ms=round((actual - job.target_ts) * 1000.0, 3),
                  started_at_utc=utc_iso(job.started_ts or now),
                  status=status, status_reason=json.dumps(statuses, sort_keys=True),
                  part_reasons={k: v[2] for k, v in job.parts.items() if v and v[2]},
                  curve_part='scheduled' if 'curve' in job.parts else ('rpc_not_configured' if not self.rpc else 'no_curve_address'),
                  quote_part='scheduled' if 'quote' in job.parts else ('not_sampled' if not cand.sampled else 'horizon_not_in_quote_horizons'),
                  observation_event_ids=obs_ids, quote_event_ids=quote_ids)
        self._count(f'followup_{status}')
        if len(cand.finished) >= len(self.cfg.horizons):
            self.candidates.pop(cand.cid, None)   # memory only; data stays in SQLite

    def _flush_batches(self) -> None:
        self._flush_queue(self.dex_queue, 30, self.lim_dex, self._dex_batch, 'market')
        if self.rpc:
            self._flush_queue(self.curve_queue, 100, self.lim_rpc, self._curve_batch, 'curve')

    def _flush_queue(self, queue: List[FollowupJob], size: int, limiter: ProviderLimiter,
                     runner: Callable[[List[FollowupJob]], Any], part: str) -> None:
        now = self.clock()
        keep: List[FollowupJob] = []
        for job in queue:
            if now > job.deadline_ts:
                reason = job.refusal.get(part, 'rate_budget_exhausted')
                ids = [self.emit('observations', mint=job.cand.mint, source=job.cand.source,
                                 obs_type='market_dexscreener' if part == 'market' else 'curve_rpc',
                                 provider='dexscreener' if part == 'market' else 'rpc',
                                 status=f'skipped_{reason}', status_reason='no request sent before horizon deadline',
                                 followup_id=stable_id(job.cand.cid, job.label), horizon_label=job.label)]
                self._resolve(job, part, f'skipped_{reason}', ids, reason)
            else:
                keep.append(job)
        queue[:] = keep
        # Coalesce: wait (within each job's tolerance) until a batch fills up or
        # some job must go now. Cuts provider load ~Nx without losing horizons.
        if queue and len(queue) < size and all(
                now < job.target_ts + min(0.5 * (job.deadline_ts - job.target_ts), 30.0) for job in queue):
            return
        while queue:
            refusal = limiter.try_acquire()
            if refusal is not None:
                for job in queue:
                    job.refusal[part] = refusal
                return
            batch: List[FollowupJob] = []
            seen: Set[str] = set()
            for job in list(queue):
                key = job.cand.mint if part == 'market' else (job.cand.curve or '')
                if key in seen or len(seen) < size:
                    seen.add(key)
                    batch.append(job)
                    queue.remove(job)
            self._spawn(self._guarded_batch(runner, batch, limiter, part))

    async def _guarded_batch(self, runner: Callable[[List[FollowupJob]], Any], jobs: List[FollowupJob],
                             limiter: ProviderLimiter, part: str) -> None:
        """A failing batch must still resolve every job (explicit status) and free the slot."""
        try:
            await runner(jobs)
        except asyncio.CancelledError:
            limiter.release('cancelled')
            raise
        except Exception as exc:
            limiter.release('collector_error')
            for job in jobs:
                eid = self.emit('observations', mint=job.cand.mint, source=job.cand.source,
                                obs_type='market_dexscreener' if part == 'market' else 'curve_rpc',
                                status='collector_error', status_reason=type(exc).__name__,
                                followup_id=stable_id(job.cand.cid, job.label), horizon_label=job.label)
                self._resolve(job, part, 'collector_error', [eid], type(exc).__name__)

    async def _dex_batch(self, jobs: List[FollowupJob]) -> None:
        mints = sorted({j.cand.mint for j in jobs})
        result = await providers.dexscreener_tokens(self.session, mints)
        self.lim_dex.release(result.status, result.retry_after)
        pairs = result.data if isinstance(result.data, list) else (
            (result.data or {}).get('pairs') if isinstance(result.data, dict) else None)
        for job in jobs:
            common = dict(mint=job.cand.mint, source=job.cand.source, obs_type='market_dexscreener',
                          provider='dexscreener', http_status=result.http_status,
                          latency_ms=result.latency_ms, followup_id=stable_id(job.cand.cid, job.label),
                          horizon_label=job.label, batch_size=len(mints), _received_ts=result.received_ts)
            if result.status != 'ok' or pairs is None:
                status = result.status if result.status != 'ok' else 'invalid_payload'
                eid = self.emit('observations', status=status, status_reason=result.error, **common)
                self._resolve(job, 'market', status, [eid], result.error)
                continue
            summary = providers.summarize_dexscreener(job.cand.mint, pairs)
            eid = self.emit('observations', **summary, **common)
            self._resolve(job, 'market', summary['status'], [eid])

    async def _curve_batch(self, jobs: List[FollowupJob]) -> None:
        curves = sorted({j.cand.curve for j in jobs if j.cand.curve})
        result, parsed = await providers.pump_curves(self.rpc, self.session, curves)
        self.lim_rpc.release(result.status, result.retry_after)
        for job in jobs:
            common = dict(mint=job.cand.mint, source=job.cand.source, obs_type='curve_rpc',
                          provider='rpc:getMultipleAccounts', http_status=result.http_status,
                          latency_ms=result.latency_ms, followup_id=stable_id(job.cand.cid, job.label),
                          horizon_label=job.label, bonding_curve=job.cand.curve,
                          _received_ts=result.received_ts)
            if result.status != 'ok':
                eid = self.emit('observations', status=result.status, status_reason=result.error, **common)
                self._resolve(job, 'curve', result.status, [eid], result.error)
                continue
            info = dict(parsed.get(job.cand.curve or '', {'status': 'account_missing'}))
            slot = info.get('context_slot')
            if isinstance(slot, int):
                if self.max_rpc_slot and slot < self.max_rpc_slot - self.stale_slot_lag:
                    # Lagging RPC node: keep the data but flag it; labels use only status=ok.
                    info['status'] = 'stale'
                    info['status_reason'] = f'context slot {slot} < latest seen {self.max_rpc_slot} - {self.stale_slot_lag}'
                self.max_rpc_slot = max(self.max_rpc_slot, slot)
            eid = self.emit('observations', **info, **common)
            self._resolve(job, 'curve', info.get('status', 'unknown'), [eid])

    async def _followup_quote(self, job: FollowupJob) -> None:
        cand = job.cand
        fid = stable_id(cand.cid, job.label)
        try:
            if cand.shadow_tokens_raw:
                status, _, eid = await self._quote(
                    cand, 'followup_sell_exact', cand.mint, WSOL, cand.shadow_tokens_raw,
                    job.deadline_ts, followup_id=fid, horizon_label=job.label,
                    quote_pair_id=stable_id('shadow', cand.cid, self.cfg.notionals_rub[0]),
                    notional_rub=self.cfg.notionals_rub[0],
                    exact_size_from='shadow_buy.out_amount_raw')
            else:
                sol, sol_reason = self._sol_usd_state()
                if sol is None:
                    status = f'skipped_{sol_reason}'
                    eid = self.emit('quotes', mint=cand.mint, source=cand.source, provider='jupiter',
                                    purpose='followup_buy', direction='buy', status=status,
                                    input_mint=WSOL, output_mint=cand.mint, followup_id=fid,
                                    horizon_label=job.label)
                else:
                    notional = self.cfg.notionals_rub[0]
                    lamports = int(notional / (sol * self.cfg.usd_rub_rate) * 1e9)
                    status, _, eid = await self._quote(
                        cand, 'followup_buy', WSOL, cand.mint, lamports, job.deadline_ts,
                        followup_id=fid, horizon_label=job.label, notional_rub=notional,
                        notional_sol=lamports / 1e9, sol_usd_used=sol,
                        usd_rub_used=self.cfg.usd_rub_rate,
                        status_note='no shadow buy tokens: measuring buy-route availability')
            self._resolve(job, 'quote', status, [eid] if eid else [])
        except Exception as exc:
            self._resolve(job, 'quote', 'collector_error', [], type(exc).__name__)

    # ----------------------------------------------------------- recovery ----
    def recover(self) -> int:
        """Restart: re-schedule pending horizons; past ones become explicit misses."""
        cutoff = utc_iso(self.clock() - self.max_horizon - self.cfg.tolerance(self.max_horizon))
        db = self.store.db
        rows = db.execute(
            'SELECT d.candidate_id, d.mint, d.source, d.received_at_utc, d.event_id FROM detections d '
            'JOIN (SELECT candidate_id, MIN(received_at_utc) AS first FROM detections '
            '      WHERE mint IS NOT NULL GROUP BY candidate_id) f '
            'ON d.candidate_id = f.candidate_id AND d.received_at_utc = f.first '
            'WHERE f.first >= ? GROUP BY d.candidate_id', (cutoff,)).fetchall()
        done = {r[0] for r in db.execute('SELECT DISTINCT followup_id FROM followups')}
        curves = dict(db.execute(
            'SELECT candidate_id, MIN(bonding_curve) FROM detections WHERE bonding_curve IS NOT NULL '
            'GROUP BY candidate_id').fetchall())
        shadow = {}
        for cid, amount, out, notional in db.execute(
                "SELECT candidate_id, amount_raw, out_amount_raw, notional_rub FROM quotes "
                "WHERE purpose='shadow_buy' AND status='ok' AND detected_by='collector'"):
            if notional == self.cfg.notionals_rub[0] and out:
                shadow[cid] = (int(amount), int(out))
        restored = 0
        for cid, mint, source, received, event_id in rows:
            anchor = parse_iso(received)
            if anchor is None:
                continue
            labels_done = {label for label, _ in self.cfg.horizons if stable_id(cid, label) in done}
            if len(labels_done) == len(self.cfg.horizons):
                continue
            cand = Candidate(cid, mint, source, anchor, event_id, curves.get(cid),
                             sampled=sampled_for_quotes(cid, self.cfg.quote_sample_rate))
            cand.finished |= labels_done
            if cid in shadow:
                cand.shadow_lamports, cand.shadow_tokens_raw = shadow[cid]
            self.candidates[cid] = cand
            now = self.clock()
            for label, sec in self.cfg.horizons:
                if label in labels_done:
                    continue
                target = anchor + sec
                if target + self.cfg.tolerance(sec) < now:
                    self._emit_missed(cand, label, sec, target, 'collector_not_running_at_target')
                else:
                    heapq.heappush(self.heap, (target, next(self._seq), cid, label))
            restored += 1
        return restored

    # ------------------------------------------------------------- health ----
    def health_snapshot(self) -> Dict[str, Any]:
        size = 0
        for path in (self.store.db_path, self.store.raw_path, Path(str(self.store.db_path) + '-wal')):
            try:
                size += path.stat().st_size
            except FileNotFoundError:
                pass
        self.storage_limited = size > self.cfg.max_storage_mb * 1024 * 1024
        return {
            'candidates_tracked': len(self.candidates), 'scheduled_horizons': len(self.heap),
            'dex_queue': len(self.dex_queue), 'curve_queue': len(self.curve_queue),
            'pending_shadow_jobs': self.pending_shadow, 'observer_queue': self.observer.queue.qsize(),
            'storage_bytes': size, 'storage_limited': self.storage_limited,
            'store_inserted': self.store.inserted,
            'ignored_duplicate_event_ids': self.store.ignored_duplicate_event_ids,
            'writer_errors': self.writer.errors, 'counters': dict(self.counters),
            'limiters': [l.snapshot() for l in (self.lim_jup, self.lim_dex, self.lim_rpc)],
            'sol_usd_age_sec': round(self.clock() - self.sol_usd[0], 1) if self.sol_usd else None,
        }

    def write_health(self, health_type: str = 'collector_heartbeat', status: str = 'ok', **extra: Any) -> None:
        snap = self.health_snapshot()
        snap.update(extra)
        self.emit('health', health_type=health_type, status=status, **snap)
        line = json.dumps({'at_utc': utc_iso(), 'type': health_type, 'status': status, **snap},
                          ensure_ascii=False, sort_keys=True, default=str)
        path = self.root / 'health.log'
        try:
            if path.exists() and path.stat().st_size > 10 * 1024 * 1024:
                for i in (2, 1):
                    older = self.root / f'health.log.{i}'
                    if older.exists():
                        older.replace(self.root / f'health.log.{i + 1}')
                path.replace(self.root / 'health.log.1')
            with open(path, 'a', encoding='utf-8') as fh:
                fh.write(redact_text(line) + '\n')
        except OSError:
            pass

    # ---------------------------------------------------------- runtime ----
    def _spawn(self, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self.tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: 'asyncio.Task') -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self._count(f'task_error_{type(task.exception()).__name__}')

    async def _scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._start_due_jobs()
                self._flush_batches()
                if self.clock() - self._last_health >= self.cfg.health_interval_sec:
                    self._last_health = self.clock()
                    self.write_health()
                if (self.root / 'STOP').exists():
                    self.stop_event.set()
            except Exception as exc:
                self._count(f'scheduler_error_{type(exc).__name__}')
            await asyncio.sleep(0.25)

    async def _sol_usd_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.refresh_sol_usd_once()
            except Exception as exc:
                self._count(f'sol_usd_error_{type(exc).__name__}')
            try:
                await asyncio.wait_for(self.stop_event.wait(), self.cfg.sol_usd_refresh_sec)
            except asyncio.TimeoutError:
                pass

    async def _discovery_loop(self) -> None:
        backoff = 1.0
        gap_started: Optional[float] = None
        while not self.stop_event.is_set():
            try:
                async with self.ws_connect(self.cfg.pumpportal_url, open_timeout=15, ping_interval=20,
                                           ping_timeout=20, max_size=2 ** 23) as ws:
                    await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                    self.write_health('discovery_connected', 'ok',
                                      gap_started_utc=utc_iso(gap_started) if gap_started else None)
                    gap_started, backoff = None, 1.0
                    async for raw in ws:
                        if self.stop_event.is_set():
                            return
                        try:
                            msg = json.loads(raw)
                        except (TypeError, ValueError):
                            self._count('discovery_invalid_json')
                            continue
                        if isinstance(msg, dict) and msg.get('mint'):
                            self.on_pumpportal_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                gap_started = gap_started or self.clock()
                self.write_health('discovery_disconnected', 'error',
                                  error=f'{type(exc).__name__}: {exc}'[:300],
                                  gap_started_utc=utc_iso(gap_started), retry_in_sec=backoff)
            if self.stop_event.is_set():
                return
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, 60.0)

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self.loop.add_signal_handler(sig, self.stop_event.set)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                pass
        restored = self.recover()
        if self._start_writer:
            self.writer.start()
        self.write_health('collector_started', 'ok', restored_candidates=restored,
                          config=self.cfg.to_safe_dict(), pid=os.getpid())
        own_session = self.session is None
        if own_session:
            import aiohttp
            self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20))
        if self.ws_connect is None and self.cfg.discovery == 'pumpportal':
            import websockets
            self.ws_connect = websockets.connect
        try:
            loops = [self._scheduler_loop(), self._sol_usd_loop()]
            if self.cfg.discovery == 'pumpportal':
                loops.append(self._discovery_loop())
            runners = [asyncio.ensure_future(c) for c in loops]
            await self.stop_event.wait()
            for task in runners + list(self.tasks):
                task.cancel()
            await asyncio.gather(*runners, *self.tasks, return_exceptions=True)
        finally:
            self.write_health('collector_stopped', 'ok',
                              pending_horizons_left_for_recovery=len(self.heap))
            if own_session and self.session is not None:
                await self.session.close()
            if self._start_writer:
                self.writer.stop(10.0)
            else:
                self.observer.drain(self.store)
            self.store.close()


def run_collector(cfg: CollectorConfig) -> int:
    root = Path(cfg.data_dir).expanduser().resolve()
    from .store import ensure_data_dir
    root = ensure_data_dir(root)
    lock = SingleInstanceLock(root / 'collector.lock')
    if not lock.acquire():
        print('collector already running for this data dir (collector.lock is held)')
        return 2
    stop_file = root / 'STOP'
    if stop_file.exists():
        stop_file.unlink()   # our own control file from a previous stop
    pid_file = root / 'collector.pid'
    pid_file.write_text(str(os.getpid()), encoding='utf-8')
    try:
        asyncio.run(Collector(cfg).run())
    finally:
        try:
            pid_file.unlink()
        except FileNotFoundError:
            pass
        lock.release()
    return 0
