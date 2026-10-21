"""Offline guard for every test: any real network connection fails loudly."""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class NetworkBlocked(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _block_network(monkeypatch):
    def guard(*args, **kwargs):
        raise NetworkBlocked('network access is forbidden in tests')

    monkeypatch.setattr(socket.socket, 'connect', guard)
    monkeypatch.setattr(socket.socket, 'connect_ex', guard)
    monkeypatch.setattr(socket, 'create_connection', guard)
    monkeypatch.setattr(socket, 'getaddrinfo', guard)
    yield
