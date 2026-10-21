"""Non-blocking hand-off between producers (bot hooks/collector) and one writer.

`Observer.emit` never blocks and never raises: it is `queue.put_nowait` on a
bounded queue. On overflow the event is dropped *and counted*; the writer turns
the counters into explicit `health/data_loss` records with the loss interval.
Serialization and redaction happen in the writer thread, not in the caller.
"""

from __future__ import annotations

import queue
import threading
import time
from collections import Counter
from typing import Any, Callable, Dict, List, Optional

from .model import build_record, utc_iso


class NullObserver:
    """Telemetry disabled: every emit is a cheap no-op."""

    enabled = False

    def emit(self, kind: str, fields: Dict[str, Any]) -> bool:
        return False


class Observer:
    enabled = True

    def __init__(self, capacity: int = 50_000, *, config_fingerprint: Optional[str] = None,
                 code_version: Optional[str] = None, detected_by: str = 'bot') -> None:
        self.queue: 'queue.Queue[tuple]' = queue.Queue(maxsize=capacity)
        self.capacity = capacity
        self.config_fingerprint = config_fingerprint
        self.code_version = code_version
        self.detected_by = detected_by
        self._losses: Counter = Counter()
        self._first_loss: Optional[float] = None
        self._last_loss: Optional[float] = None
        self._lock = threading.Lock()
        self.emitted = 0

    def _loss(self, reason: str, n: int = 1) -> None:
        with self._lock:
            now = time.time()
            if self._first_loss is None:
                self._first_loss = now
            self._last_loss = now
            self._losses[reason] += n

    def emit(self, kind: str, fields: Dict[str, Any]) -> bool:
        try:
            self.queue.put_nowait((kind, fields))
            self.emitted += 1
            return True
        except queue.Full:
            self._loss('queue_full')
        except Exception:  # pragma: no cover - defensive
            self._loss('observer_error')
        return False

    def take_losses(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._losses:
                return None
            result = {
                'loss_counts': dict(self._losses),
                'lost_events_total': sum(self._losses.values()),
                'loss_interval_start_utc': utc_iso(self._first_loss),
                'loss_interval_end_utc': utc_iso(self._last_loss),
                'queue_capacity': self.capacity,
            }
            self._losses.clear()
            self._first_loss = self._last_loss = None
            return result

    def build(self, kind: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        record = build_record(kind, fields, config_fingerprint=self.config_fingerprint,
                              code_version=self.code_version, detected_by=self.detected_by)
        record['kind'] = kind
        return record

    def drain(self, sink: Any, limit: int = 2000,
              extra: Optional[Callable[[], List[Dict[str, Any]]]] = None) -> int:
        """Writer-thread only: move up to `limit` events into `sink`."""
        records: List[Dict[str, Any]] = []
        for _ in range(limit):
            try:
                kind, fields = self.queue.get_nowait()
            except queue.Empty:
                break
            try:
                records.append(self.build(kind, fields))
            except Exception as exc:
                self._loss(f'invalid_event:{type(exc).__name__}')
        losses = self.take_losses()
        if losses:
            records.append(self.build('health', dict(health_type='data_loss', status='lost', **losses)))
        if not records:
            return 0
        try:
            sink.append(records)
        except Exception as exc:
            # Never propagate; account for the loss so it is visible later.
            self._loss(f'sink_error:{type(exc).__name__}', len(records))
            return 0
        return len(records)


class WriterThread(threading.Thread):
    """Daemon thread draining an Observer into a sink; optional periodic tasks."""

    def __init__(self, observer: Observer, sink: Any, *, interval: float = 0.2,
                 periodic: Optional[List[Callable[[], None]]] = None) -> None:
        super().__init__(name='telemetry-writer', daemon=True)
        self.observer = observer
        self.sink = sink
        self.interval = interval
        self.periodic = periodic or []
        self._stop_event = threading.Event()
        self.errors = 0

    def run(self) -> None:
        while not self._stop_event.is_set():
            self._cycle()
            self._stop_event.wait(self.interval)
        # final flush
        for _ in range(100):
            if self.observer.drain(self.sink) == 0:
                break

    def _cycle(self) -> None:
        try:
            while self.observer.drain(self.sink) >= 2000:
                pass
        except Exception:
            self.errors += 1
        for task in self.periodic:
            try:
                task()
            except Exception:
                self.errors += 1

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self.join(timeout)
