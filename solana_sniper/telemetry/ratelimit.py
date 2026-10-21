"""Per-provider budget: token bucket + concurrency + 429 cooldown with jitter.

Never waits on behalf of the caller: `try_acquire` answers immediately with
None (granted) or a concrete refusal reason that is persisted as a status.
"""

from __future__ import annotations

import random
import time
from typing import Callable, Dict, Optional


class ProviderLimiter:
    def __init__(self, name: str, rpm: float, *, burst: Optional[float] = None,
                 concurrency: int = 2, base_backoff: float = 2.0, max_backoff: float = 300.0,
                 clock: Callable[[], float] = time.monotonic,
                 rng: Callable[[], float] = random.random) -> None:
        if rpm <= 0:
            raise ValueError('rpm must be positive (use enabled=False to disable a provider)')
        self.name = name
        self.rate_per_sec = rpm / 60.0
        self.capacity = burst if burst is not None else max(1.0, min(rpm / 10.0, 10.0))
        self.tokens = self.capacity
        self.concurrency = concurrency
        self.in_flight = 0
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.clock = clock
        self.rng = rng
        self.updated = clock()
        self.cooldown_until = 0.0
        self.consecutive_429 = 0
        self.consecutive_errors = 0
        self.stats: Dict[str, int] = {}

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate_per_sec)
        self.updated = now

    def _count(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    def try_acquire(self) -> Optional[str]:
        now = self.clock()
        if now < self.cooldown_until:
            self._count('refused_provider_cooldown')
            return 'provider_cooldown'
        if self.in_flight >= self.concurrency:
            self._count('refused_concurrency')
            return 'concurrency_limit'
        self._refill()
        if self.tokens < 1.0:
            self._count('refused_rate_budget')
            return 'rate_budget_exhausted'
        self.tokens -= 1.0
        self.in_flight += 1
        self._count('granted')
        return None

    def jitter(self, seconds: float) -> float:
        return seconds * (0.5 + self.rng())  # 50%..150%

    def release(self, status: str, retry_after: Optional[float] = None) -> None:
        """Report the outcome; 429 and repeated errors open a cooldown window."""
        self.in_flight = max(0, self.in_flight - 1)
        self._count(f'result_{status}')
        if status == 'http_429':
            self.consecutive_429 += 1
            if retry_after is not None and retry_after > 0:
                delay = min(self.max_backoff, float(retry_after))  # provider rule first
            else:
                delay = self.jitter(min(self.max_backoff, self.base_backoff * 2 ** (self.consecutive_429 - 1)))
            self.cooldown_until = max(self.cooldown_until, self.clock() + delay)
            return
        if status in ('timeout', 'request_error') or status.startswith('http_5'):
            self.consecutive_errors += 1
            if self.consecutive_errors >= 3:
                delay = self.jitter(min(self.max_backoff, self.base_backoff * 2 ** (self.consecutive_errors - 3)))
                self.cooldown_until = max(self.cooldown_until, self.clock() + delay)
            return
        self.consecutive_429 = 0
        self.consecutive_errors = 0

    def snapshot(self) -> Dict[str, object]:
        self._refill()
        return {
            'provider': self.name, 'tokens': round(self.tokens, 3), 'in_flight': self.in_flight,
            'cooldown_remaining_sec': round(max(0.0, self.cooldown_until - self.clock()), 3),
            'consecutive_429': self.consecutive_429, 'stats': dict(self.stats),
        }
