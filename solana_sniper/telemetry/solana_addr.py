"""Минимальные чистые функции адресов Solana (без зависимостей, только вычисления).

base58, проверка «точка на кривой ed25519», find_program_address и адрес
associated token account (ATA). Нужны, чтобы читать баланс токенов создателя
монеты через getMultipleAccounts — без подписей и без ключей.
"""

from __future__ import annotations

import hashlib
import struct
from typing import List, Optional, Sequence, Tuple

B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
_B58_INDEX = {c: i for i, c in enumerate(B58)}

TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
ATA_PROGRAM = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'

_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P


def b58decode(text: str) -> bytes:
    num = 0
    for ch in text:
        num = num * 58 + _B58_INDEX[ch]
    body = num.to_bytes((num.bit_length() + 7) // 8, 'big') if num else b''
    pad = len(text) - len(text.lstrip('1'))
    return b'\0' * pad + body


def b58encode(data: bytes) -> str:
    num = int.from_bytes(data, 'big')
    out = ''
    while num:
        num, rem = divmod(num, 58)
        out = B58[rem] + out
    pad = len(data) - len(data.lstrip(b'\0'))
    return '1' * pad + out


def pubkey_bytes(address: str) -> bytes:
    raw = b58decode(address)
    if len(raw) != 32:
        raise ValueError(f'not a 32-byte address: {address!r}')
    return raw


def is_on_curve(point: bytes) -> bool:
    """True, если 32 байта — валидная сжатая точка ed25519 (тогда это не PDA)."""
    y = int.from_bytes(point, 'little') & ((1 << 255) - 1)
    sign = point[31] >> 7
    if y >= _P:
        return False
    y2 = y * y % _P
    x2 = (y2 - 1) * pow((_D * y2 + 1) % _P, _P - 2, _P) % _P
    if x2 == 0:
        return sign == 0
    return pow(x2, (_P - 1) // 2, _P) == 1


def find_program_address(seeds: Sequence[bytes], program_id: str) -> Tuple[str, int]:
    program = pubkey_bytes(program_id)
    for bump in range(255, -1, -1):
        digest = hashlib.sha256(b''.join(seeds) + bytes([bump]) + program + b'ProgramDerivedAddress').digest()
        if not is_on_curve(digest):
            return b58encode(digest), bump
    raise ValueError('no viable bump seed')


def associated_token_address(owner: str, mint: str, token_program: str = TOKEN_PROGRAM) -> str:
    seeds = [pubkey_bytes(owner), pubkey_bytes(token_program), pubkey_bytes(mint)]
    return find_program_address(seeds, ATA_PROGRAM)[0]


def creator_ata_candidates(owner: str, mint: str) -> List[str]:
    """ATA создателя для классического SPL Token и для Token-2022 (Pump.fun использует оба)."""
    return [associated_token_address(owner, mint, TOKEN_PROGRAM),
            associated_token_address(owner, mint, TOKEN_2022_PROGRAM)]


def token_account_amount(raw: Optional[bytes]) -> Optional[int]:
    """Сырой баланс из данных token account (mint 32 | owner 32 | amount u64)."""
    if not raw or len(raw) < 72:
        return None
    return struct.unpack_from('<Q', raw, 64)[0]
