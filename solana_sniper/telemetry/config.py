"""Collector configuration (env/CLI). Secrets are read from env only and never
serialized: `to_safe_dict()` is what gets fingerprinted and stored."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple

from .model import fingerprint

DEFAULT_HORIZONS = '1s,5s,15s,30s,60s,120s,5m,15m,1h,6h,24h'
DEFAULT_QUOTE_HORIZONS = '5s,30s,60s,120s,5m,15m,1h'


def parse_duration(label: str) -> float:
    label = label.strip().lower()
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    if not label or label[-1] not in units:
        raise ValueError(f'bad horizon {label!r}: use e.g. 5s, 15m, 1h')
    value = float(label[:-1])
    if value <= 0:
        raise ValueError(f'horizon must be positive: {label!r}')
    return value * units[label[-1]]


def parse_horizons(spec: str) -> List[Tuple[str, float]]:
    items = [(part.strip(), parse_duration(part)) for part in spec.split(',') if part.strip()]
    labels = [label for label, _ in items]
    if len(set(labels)) != len(labels):
        raise ValueError('duplicate horizon labels')
    return sorted(items, key=lambda item: item[1])


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


@dataclass
class CollectorConfig:
    data_dir: str = '../telemetry_data'
    horizons: List[Tuple[str, float]] = field(default_factory=lambda: parse_horizons(DEFAULT_HORIZONS))
    quote_horizons: List[str] = field(default_factory=lambda: DEFAULT_QUOTE_HORIZONS.split(','))
    notionals_rub: List[float] = field(default_factory=lambda: [250.0, 500.0])
    usd_rub_rate: float = 90.0
    slippage_bps: int = 100
    quote_sample_rate: float = 0.05
    shadow_quote_deadline_sec: float = 60.0
    jupiter_rpm: float = 30.0
    dexscreener_rpm: float = 60.0
    rpc_rpm: float = 60.0
    sol_usd_refresh_sec: float = 300.0
    discovery: str = 'pumpportal'          # pumpportal | none
    pumpportal_url: str = 'wss://pumpportal.fun/ws'
    jupiter_quote_url: str = 'https://lite-api.jup.ag/swap/v1/quote'
    ingest_bot_spool: bool = True
    max_storage_mb: float = 20_000.0
    raw_max_mb: float = 512.0
    queue_capacity: int = 100_000
    health_interval_sec: float = 60.0
    backup_interval_hours: float = 6.0
    backups_keep: int = 4
    max_pending_shadow_jobs: int = 2_000
    # Never serialized; resolved from env at runtime only.
    rpc_url: Optional[str] = field(default=None, repr=False)

    @classmethod
    def from_env(cls, data_dir: Optional[str] = None) -> 'CollectorConfig':
        cfg = cls()
        cfg.data_dir = data_dir or _env('TELEMETRY_DIR', cfg.data_dir)
        cfg.horizons = parse_horizons(_env('TELEMETRY_HORIZONS', DEFAULT_HORIZONS))
        cfg.quote_horizons = [h.strip() for h in _env('TELEMETRY_QUOTE_HORIZONS', DEFAULT_QUOTE_HORIZONS).split(',') if h.strip()]
        cfg.notionals_rub = [float(x) for x in _env('TELEMETRY_NOTIONALS_RUB', '250,500').split(',') if x.strip()]
        cfg.usd_rub_rate = float(_env('USD_RUB_RATE', '90'))
        cfg.slippage_bps = int(_env('TELEMETRY_SLIPPAGE_BPS', '100'))
        cfg.quote_sample_rate = min(max(float(_env('TELEMETRY_QUOTE_SAMPLE_RATE', '0.05')), 0.0), 1.0)
        cfg.jupiter_rpm = float(_env('TELEMETRY_JUPITER_RPM', '30'))
        cfg.dexscreener_rpm = float(_env('TELEMETRY_DEXSCREENER_RPM', '60'))
        cfg.rpc_rpm = float(_env('TELEMETRY_RPC_RPM', '60'))
        cfg.discovery = _env('TELEMETRY_DISCOVERY', 'pumpportal').lower()
        cfg.pumpportal_url = _env('TELEMETRY_PUMPPORTAL_WS', cfg.pumpportal_url)
        cfg.jupiter_quote_url = _env('TELEMETRY_JUPITER_QUOTE_URL', cfg.jupiter_quote_url)
        cfg.max_storage_mb = float(_env('TELEMETRY_MAX_STORAGE_MB', '20000'))
        cfg.ingest_bot_spool = _env('TELEMETRY_INGEST_BOT_SPOOL', 'on').lower() != 'off'
        rpc = os.getenv('TELEMETRY_RPC_URL', '').strip()
        key = os.getenv('HELIUS_API_KEY', '').strip()
        if not rpc and key and _env('TELEMETRY_USE_HELIUS_RPC', 'off').lower() == 'on':
            rpc = f'https://mainnet.helius-rpc.com/?api-key={key}'
        cfg.rpc_url = rpc or None
        cfg.validate()
        return cfg

    def validate(self) -> None:
        labels = {label for label, _ in self.horizons}
        unknown = [h for h in self.quote_horizons if h not in labels]
        if unknown:
            raise ValueError(f'quote horizons not in horizons: {unknown}')
        if any(n <= 0 for n in self.notionals_rub):
            raise ValueError('notionals must be positive')
        if self.discovery not in ('pumpportal', 'none'):
            raise ValueError('TELEMETRY_DISCOVERY must be pumpportal or none')

    def tolerance(self, horizon_sec: float) -> float:
        """How late a horizon may still be observed before it counts as missed."""
        return min(600.0, max(2.0, 0.2 * horizon_sec))

    def to_safe_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data.pop('rpc_url', None)
        data['rpc_configured'] = bool(self.rpc_url)
        data['horizons'] = [label for label, _ in self.horizons]
        return data

    def fingerprint(self) -> str:
        return fingerprint(self.to_safe_dict())
