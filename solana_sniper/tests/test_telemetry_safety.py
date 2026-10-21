"""Safety invariants: no signing/sending code, no bot-module imports in the
telemetry package, inert imports, and old archives/wallet files untouched."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TELEMETRY = ROOT / 'telemetry'
BOT_MODULES = {'main', 'trader', 'wallet', 'detector', 'filters', 'price', 'ws_client', 'config',
               'console', 'storage', 'trade_logger', 'models'}
FORBIDDEN_IDENT = re.compile(
    r'(?i)(send_?(raw_?)?transaction|sign_?(all_?)?transactions?|partial_?sign|keypair|signer|'
    r'swap_?instructions|execute_?swap|send_?and_?confirm|simulate_?transaction)')
FORBIDDEN_IMPORTS = ('solders', 'solana', 'nacl', 'base58', 'anchorpy', 'spl', 'jupiter_python_sdk')


def _identifiers(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            yield node.id
        elif isinstance(node, ast.Attribute):
            yield node.attr
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield node.name
        elif isinstance(node, ast.keyword) and node.arg:
            yield node.arg


def _imports(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            yield node.module


def _py_files():
    return sorted(TELEMETRY.glob('*.py'))


def test_no_signing_or_sending_code_in_telemetry_or_hooked_bot_modules():
    files = _py_files() + [ROOT / f'{m}.py' for m in ('trader', 'price', 'filters', 'detector', 'main', 'ws_client')]
    for path in files:
        try:
            tree = ast.parse(path.read_text(encoding='utf-8'), str(path))
        except SyntaxError:
            tree = None   # main.py/ws_client.py use 3.12-only f-strings; fall back to a text scan
        if tree is None:
            text = path.read_text(encoding='utf-8')
            assert not FORBIDDEN_IDENT.search(text), path
            continue
        bad = [i for i in _identifiers(tree) if FORBIDDEN_IDENT.search(i)]
        assert not bad, (path.name, bad)
        for module in _imports(tree):
            assert not module.split('.')[0] in FORBIDDEN_IMPORTS, (path.name, module)


def test_telemetry_never_imports_bot_modules():
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for module in _imports(tree):
            assert module.split('.')[0] not in BOT_MODULES, (path.name, module)


def test_only_providers_performs_http_and_only_get_or_allowlisted_rpc():
    for path in _py_files():
        text = path.read_text(encoding='utf-8')
        if path.name == 'providers.py':
            assert text.count('session.post') == 1 and "method == 'GET'" in text
            continue
        assert 'session.get(' not in text and 'session.post(' not in text, path.name
        assert '.post(' not in text, path.name


def test_rpc_allowlist_is_read_only():
    from telemetry.providers import READ_ONLY_RPC_METHODS
    assert READ_ONLY_RPC_METHODS <= {'getAccountInfo', 'getMultipleAccounts', 'getSlot', 'getTokenSupply',
                                     'getTransaction', 'getSignatureStatuses', 'getTokenLargestAccounts'}
    assert not any(m.lower().startswith(('send', 'simulate', 'request')) for m in READ_ONLY_RPC_METHODS)


def test_import_is_inert_no_threads_sockets_or_network_libs():
    code = (
        'import sys, threading; sys.path.insert(0, %r)\n'
        'import telemetry, telemetry.hooks, telemetry.collector, telemetry.export, telemetry.providers\n'
        'print(threading.active_count(), "aiohttp" in sys.modules, "websockets" in sys.modules,'
        ' telemetry.hooks.enabled())\n' % str(ROOT))
    env = {k: v for k, v in os.environ.items() if k != 'TELEMETRY_DIR'}
    out = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ['1', 'False', 'False', 'False']


def _digest_tree(root: Path):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}


def test_full_offline_flow_leaves_wallets_and_old_archives_untouched(tmp_path, monkeypatch):
    project = tmp_path / 'project'
    runs = project / 'solana_sniper' / 'runs' / 'config6-ideal60'
    runs.mkdir(parents=True)
    (runs / 'wallet.json').write_text('{"balance_rub": 1500}', encoding='utf-8')
    (runs / 'sniper.db').write_bytes(b'SQLite format 3\x00' + b'\x00' * 64)
    (runs / 'console.log').write_text('old run', encoding='utf-8')
    (project / 'solana_sniper' / 'wallet.json').write_text('{"balance_rub": 999}', encoding='utf-8')
    before = _digest_tree(project / 'solana_sniper')

    data_dir = project / 'telemetry_data'
    monkeypatch.setenv('TELEMETRY_DIR', str(data_dir))
    from telemetry import hooks
    from telemetry.collector import Collector
    from telemetry.config import CollectorConfig, parse_horizons
    from telemetry.export import export_bundle
    assert hooks.install_from_env({'min_liquidity_usd': 500.0})
    hooks.pumpportal_raw({'txType': 'create', 'mint': 'M' * 44, 'vTokensInBondingCurve': 1,
                          'vSolInBondingCurve': 1}, set())
    hooks.shutdown()

    cfg = CollectorConfig(data_dir=str(data_dir))
    cfg.horizons = parse_horizons('1s')
    cfg.quote_horizons = []
    cfg.quote_sample_rate = 0.0
    cfg.discovery = 'none'
    collector = Collector(cfg, session=object(), start_writer=False)
    collector.ingest_spool_once()
    collector.store.close()
    result = export_bundle(str(data_dir), make_zip=True)
    assert result['row_counts']['detections'] == 1

    assert _digest_tree(project / 'solana_sniper') == before        # nothing changed/added in bot dirs
    assert not any(p.name == 'wallet.json' for p in data_dir.rglob('*'))
