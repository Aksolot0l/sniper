"""Passive telemetry for the Solana paper bot.

Importing this package starts no thread, process, socket or file IO.
* telemetry.hooks     – guarded, non-blocking observation hooks for the bot
* telemetry.collector – standalone read-only collector (run explicitly)
* telemetry.export    – offline export / ZIP bundle
"""

__all__ = ['hooks']
