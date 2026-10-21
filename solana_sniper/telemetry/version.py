"""Code version and safe configuration fingerprints (no subprocess, no network)."""

from __future__ import annotations

import dataclasses
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

from .model import fingerprint, redact

# Settings fields that are secrets, contain secrets, or are local paths.
_UNSAFE_SETTING_FIELDS = frozenset({
    'api_key', 'wss_url', 'http_rpc_url', 'wallet_path', 'trade_log_path', 'db_path',
})


def _git_commit(start: Path) -> Optional[str]:
    for folder in [start, *start.parents]:
        git = folder / '.git'
        if not git.is_dir():
            continue
        try:
            head = (git / 'HEAD').read_text(encoding='utf-8').strip()
            if not head.startswith('ref:'):
                return head
            ref = head.split(' ', 1)[1].strip()
            ref_file = git / ref
            if ref_file.exists():
                return ref_file.read_text(encoding='utf-8').strip()
            packed = git / 'packed-refs'
            if packed.exists():
                for line in packed.read_text(encoding='utf-8').splitlines():
                    if line.endswith(' ' + ref):
                        return line.split(' ', 1)[0]
        except OSError:
            return None
        return None
    return None


def source_digest(package_root: Path) -> str:
    """sha256 over bot + telemetry *.py sources (detects uncommitted edits)."""
    digest = hashlib.sha256()
    for path in sorted(package_root.rglob('*.py')):
        if 'tests' in path.parts or '__pycache__' in path.parts:
            continue
        digest.update(str(path.relative_to(package_root)).encode())
        try:
            digest.update(path.read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:16]


def code_version(package_root: Optional[Path] = None) -> str:
    root = package_root or Path(__file__).resolve().parents[1]
    commit = _git_commit(root) or 'unknown-commit'
    return f'git:{commit[:12]}+src:{source_digest(root)}'


def safe_settings(settings: Any) -> Dict[str, Any]:
    """Bot Settings -> dict without secrets/URLs/paths (thresholds stay)."""
    if settings is None:
        return {}
    if dataclasses.is_dataclass(settings):
        raw = dataclasses.asdict(settings)
    elif isinstance(settings, dict):
        raw = dict(settings)
    else:
        raw = {k: v for k, v in vars(settings).items() if not k.startswith('_')}
    clean = {k: v for k, v in raw.items() if k not in _UNSAFE_SETTING_FIELDS}
    return redact(clean)


def config_fingerprint(settings: Any) -> str:
    return fingerprint(safe_settings(settings))
