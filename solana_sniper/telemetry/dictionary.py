"""Column descriptions -> data_dictionary.md (generated from the live schema)."""

from __future__ import annotations

from typing import Dict, List

from . import schema
from .labels import LABEL_COLUMNS

COMMON = {
    'row_seq': 'Monotonic insertion sequence inside this SQLite file (ordering only).',
    'event_id': 'Unique id of this record. Idempotency key: re-delivery of the same event is ignored; different raw events always get different ids.',
    'candidate_id': 'Stable UUIDv5 of (chain_id, mint). Same mint => same id across runs/machines. Null while the mint is not yet known (Helius log before getTransaction).',
    'chain_id': 'Always solana-mainnet.',
    'mint': 'Full token mint address (never shortened).',
    'source': 'Discovery/venue source as reported (Pump.fun, Raydium CPMM, ...), or bot/collector for derived records.',
    'detected_by': 'Which process produced the record: bot (hooks inside the paper bot) or collector (standalone passive collector).',
    'source_time_utc': 'Timestamp given by the provider/chain (e.g. blockTime), UTC ISO-8601. Null if the provider gives none (PumpPortal, DexScreener snapshot).',
    'received_at_utc': 'When THIS process received the data (UTC ISO-8601, microseconds). Use this for point-in-time availability: a value is known at T only if received_at_utc <= T.',
    'source_age_ms': 'received_at - source_time in ms, when both exist.',
    'recorded_at_utc': 'When the writer serialized the record (>= received_at). Not a feature.',
    'parser_version': 'Version of the telemetry parser that produced the record.',
    'config_fingerprint': 'sha256 of the secret-free configuration (bot Settings or collector config).',
    'code_version': 'git commit + hash of *.py sources at process start.',
    'supersedes_id': 'If set, this record corrects the record with that event_id. Old records are never updated or deleted.',
    'payload_json': 'All remaining fields of the record as redacted JSON (provider payloads, context, notes).',
}

TYPED = {
    'detection_type': 'helius_creation_log | helius_transaction_fetched | helius_mint_resolved | pumpportal_create.',
    'dedup_decision': 'Dedup verdict at that moment: first_signature/duplicate_signature, first_mint/duplicate_mint, first_seen_by_collector/duplicate_mint_collector, ignored_after_watch_stop, not_create_tx, missing_mint, invalid_reserves. Duplicates are kept as rows.',
    'signature': 'Full transaction signature (when available).',
    'slot': 'Solana slot (when available).',
    'related_event_id': 'Links a resolved mint / fetched transaction to the earlier signature-only detection.',
    'symbol': 'Token symbol exactly as provided; null when missing/blank (never guessed).',
    'name': 'Token name exactly as provided; null when missing/blank.',
    'creator': 'Creator wallet from the creation message (public address).',
    'bonding_curve': 'Pump.fun bonding-curve account (when available).',
    'pool': 'Pool/venue field from the provider (when available).',
    'metadata_status': 'present | partial | missing | mint_only | mint_unknown_until_getTransaction | tx_only.',
    'raw_json': 'Redacted original message (JSON).',
    'stage': 'Pipeline stage: detector_*, trader_intake, mint_meta, mint_authority, freeze_authority, supply, sol_pair, honeypot_route, pump_curve, liquidity, filters_total, entry, paper_entry.',
    'decision': 'PASS | FAIL | SKIP | UNKNOWN.',
    'reason_code': 'Stable machine reason (e.g. slot_busy, liquidity_below_threshold, no_buy_quote, buy_impact_above_limit, check_exception, not_evaluated_mint_meta_missing).',
    'reason_text': 'Human text as produced by the bot (Russian).',
    'observed_value': 'Actual value the decision looked at (JSON), when exposed by the pipeline.',
    'threshold': 'Applied threshold (JSON), when the stage has one.',
    'unit': 'Unit of observed_value/threshold.',
    'data_source': 'Where the decision data came from (rpc:getAccountInfo, dexscreener, ...).',
    'outcome': 'Pipeline consequence: filters_pending, filter_stage_result, blocked, skipped, slot_busy, would_enter_paper, entry_aborted, paper_entered, queued_for_trader, not_forwarded, not_evaluated ...',
    'attempt': 'Retry attempt index (0-based) when applicable.',
    'obs_type': 'market_dexscreener | curve_rpc | bot_spot_tick | filter_metric | sol_usd.',
    'provider': 'Data provider / endpoint family.',
    'status': 'ok | not_indexed | missing | account_missing | invalid_account_data | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json | invalid_payload | provider_error | no_route | not_tradable | invalid_quote | skipped_<reason> | not_sampled | collector_error ... Missing data is a status, never a zero.',
    'status_reason': 'Detail for status (redacted).',
    'http_status': 'HTTP status code when a request was sent.',
    'latency_ms': 'Request latency in ms.',
    'pair_address': 'Full pair/pool address of the selected (highest-liquidity) pair.',
    'dex_id': 'DEX id reported by the provider.',
    'quote_asset': 'Mint of the other side of the selected pair.',
    'price_usd': 'Token price in USD (provider value; null if unavailable).',
    'price_native': 'Token price in the quote asset of the pair.',
    'price_sol': 'Token price in SOL (pair quote is WSOL, or curve reserves).',
    'liquidity_usd': 'Pair liquidity in USD as reported.',
    'liquidity_base': 'Base-side liquidity (token units) as reported.',
    'liquidity_quote': 'Quote-side liquidity as reported.',
    'market_cap_usd': 'Market cap as reported.',
    'fdv_usd': 'FDV as reported.',
    'volume_json': 'Volume by interval as reported (JSON, e.g. m5/h1/h6/h24).',
    'pool_created_at_utc': 'Pair creation time reported by provider.',
    'curve_complete': '1 if the Pump.fun curve is complete (migrated).',
    'curve_virtual_sol_raw': 'Virtual SOL reserves, lamports, string.',
    'curve_virtual_token_raw': 'Virtual token reserves, raw units, string.',
    'curve_real_sol_raw': 'Real SOL reserves, lamports, string.',
    'curve_real_token_raw': 'Real token reserves, raw units, string.',
    'metric': 'Name of a filter metric (stage.metric).',
    'metric_value': 'Value of the filter metric (JSON). Null = not available (never fake zero).',
    'followup_id': 'Stable id of (candidate, horizon).',
    'horizon_label': 'Follow-up horizon (1s, 5s, ..., 24h) the record belongs to.',
    'purpose': 'shadow_buy | shadow_sell_exact | followup_sell_exact | followup_buy | bot_entry_buy | bot_entry_sell_precheck | bot_exit | bot_honeypot_probe | bot_unlabeled.',
    'direction': 'buy (SOL->token) | sell (token->SOL).',
    'input_mint': 'Quote input mint.',
    'output_mint': 'Quote output mint.',
    'amount_raw': 'Exact input amount in raw base units (string).',
    'input_decimals': 'Decimals of input mint when known (9 for WSOL).',
    'output_decimals': 'Decimals of output mint when known.',
    'notional_rub': 'Hypothetical notional in RUB that defined the buy size.',
    'notional_sol': 'Same notional in SOL.',
    'sol_usd_used': 'SOL/USD used to convert the notional.',
    'usd_rub_used': 'USD/RUB used to convert the notional.',
    'out_amount_raw': 'Quoted output amount, raw units (string).',
    'min_out_raw': 'otherAmountThreshold (slippage floor), raw units (string).',
    'slippage_bps': 'Requested slippage in bps.',
    'price_impact_pct': 'Quoted price impact in percent.',
    'route_labels': 'Route venue labels (JSON list).',
    'context_slot': 'Quote context slot.',
    'quote_pair_id': 'Links a shadow buy with its exact-size sell and follow-up sells.',
    'quote_only': 'Always 1: this is a quote, not an execution (DB CHECK constraint).',
    'transaction_sent': 'Always 0: no transaction was signed or sent (DB CHECK constraint).',
    'horizon_sec': 'Horizon in seconds from anchor.',
    'anchor_time_utc': 'Reference time of follow-ups: first detection known to the collector.',
    'anchor_event_id': 'Event id of that first detection.',
    'target_at_utc': 'Planned observation time = anchor + horizon.',
    'actual_at_utc': 'When the follow-up was actually started.',
    'delay_ms': 'actual - target in ms.',
    'observation_event_ids': 'Observation records produced for this follow-up (JSON list).',
    'quote_event_ids': 'Quote records produced for this follow-up (JSON list).',
    'lifecycle_type': 'paper_entry_open | exit_trigger | exit_pending_no_quote | exit_pending_no_exact_amount | exit_pending_zero_amount | exit_trigger_not_confirmed_by_quote | paper_fill_partial | paper_fill_full | paper_position_closed | paper_position_restored | paper_position_open_at_shutdown.',
    'position_key': 'mint:opened_at of the paper position.',
    'reason': 'Exit reason as produced by the bot.',
    'fraction': 'Fraction of the position addressed by the event.',
    'tokens_raw': 'Paper position size in raw units (string).',
    'remaining_raw': 'Open remainder in raw units (string).',
    'paper_calculation': '1: values are paper-bot calculations, not real balances.',
    'is_real_transaction': 'Always 0 (DB CHECK constraint).',
    'health_type': 'config_snapshot | collector_started | collector_heartbeat | discovery_connected | discovery_disconnected | data_loss | collector_stopped | bot_shutdown ...',
}

DERIVED = {
    'candidates.csv': {
        'candidate_id': 'Stable id of (chain_id, mint).',
        'mint': 'Full mint.',
        'first_seen_at_utc': 'Earliest received_at_utc of any detection/decision with this mint (bot or collector).',
        'first_signal_at_utc': 'Earliest received_at_utc including signature-only Helius logs later resolved to this mint (<= first_seen_at_utc).',
        'first_seen_event_id': 'Event id of the first detection.',
        'first_seen_source / first_seen_detected_by': 'Source and process of the first detection.',
        '*_at_first_detection': 'Identity fields copied ONLY from the first detection record (no later metadata = no look-ahead). Null if unknown.',
        'detection_events / duplicate_detection_events': 'All detection rows for the mint and how many were duplicates.',
        'detected_by_set / sources_set': 'Distinct processes / sources that saw the candidate.',
        'bot_*_postdetection': 'Bot outcomes recorded AFTER detection (seconds later). Useful to study the filter, NOT a T0 feature.',
        'quote_sampled': '1 if the collector selected the candidate for shadow quotes (deterministic hash sample).',
        'followups_recorded': 'Number of horizon records (any status, incl. missed).',
    },
    'features_at_detection.csv': {
        'asof_time_utc': 'Point-in-time = first_seen_at_utc. Every column is taken from the first detection message only.',
        'v_sol_in_curve / v_tokens_in_curve / market_cap_sol / initial_buy / sol_amount': 'Values from the PumpPortal create message (null if absent).',
        'has_name / has_symbol / has_uri / name_len / symbol_len': 'Simple metadata presence features.',
    },
    'labels.csv': {c: '' for c in LABEL_COLUMNS},
}
DERIVED['labels.csv'].update({
    'label_name': 'price_return_pct[...] | max_upside_pct[...] | max_drawdown_pct[...] | quote_roundtrip_return_pct | quote_roundtrip_return_minout_pct.',
    'value': 'Label value in percent; EMPTY when censored (never 0 as a placeholder).',
    'censored': '1 = future unavailable at export (not matured, no data, no route, provider error). Keep these rows; do not drop candidates.',
    'censor_reason': 'Why the label is censored.',
    'incomplete': '1 = horizon point missing; value (if any) uses fewer points.',
    'route_available': 'For quote labels: 1 sell route existed at the horizon, 0 quote failed (e.g. no_route), empty unknown.',
    'reference_*': 'Reference point (time, event, value) the label is measured from.',
    'value_*': 'Point (time, event) the label value comes from.',
    'method': 'Exact computation method.',
})


def render(table_counts: Dict[str, int] | None = None) -> str:
    lines: List[str] = ['# Data dictionary', '',
                        'All times are UTC ISO-8601. Raw integer amounts are strings. JSON columns hold serialized JSON.',
                        'Records are append-only; corrections are new rows with `supersedes_id`.', '',
                        '## Common columns (every event table)', '', '| column | description |', '|---|---|']
    for name, desc in COMMON.items():
        lines.append(f'| `{name}` | {desc} |')
    for table in schema.TYPED_COLUMNS:
        lines += ['', f'## exports/{table}.csv  (SQLite table `{table}`)', '']
        if table_counts:
            lines.append(f'Rows: {table_counts.get(table, 0)}')
            lines.append('')
        lines += ['| column | type | description |', '|---|---|---|']
        for name, sqltype in schema.TYPED_COLUMNS[table]:
            lines.append(f'| `{name}` | {sqltype.split()[0]} | {TYPED.get(name, "")} |')
    for csv_name, cols in DERIVED.items():
        lines += ['', f'## exports/{csv_name}  (derived at export time)', '', '| column | description |', '|---|---|']
        for name, desc in cols.items():
            lines.append(f'| `{name}` | {desc} |')
    return '\n'.join(lines) + '\n'
