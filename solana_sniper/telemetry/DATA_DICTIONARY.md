# Data dictionary

All times are UTC ISO-8601. Raw integer amounts are strings. JSON columns hold serialized JSON.
Records are append-only; corrections are new rows with `supersedes_id`.

## Common columns (every event table)

| column | description |
|---|---|
| `row_seq` | Monotonic insertion sequence inside this SQLite file (ordering only). |
| `event_id` | Unique id of this record. Idempotency key: re-delivery of the same event is ignored; different raw events always get different ids. |
| `candidate_id` | Stable UUIDv5 of (chain_id, mint). Same mint => same id across runs/machines. Null while the mint is not yet known (Helius log before getTransaction). |
| `chain_id` | Always solana-mainnet. |
| `mint` | Full token mint address (never shortened). |
| `source` | Discovery/venue source as reported (Pump.fun, Raydium CPMM, ...), or bot/collector for derived records. |
| `detected_by` | Which process produced the record: bot (hooks inside the paper bot) or collector (standalone passive collector). |
| `source_time_utc` | Timestamp given by the provider/chain (e.g. blockTime), UTC ISO-8601. Null if the provider gives none (PumpPortal, DexScreener snapshot). |
| `received_at_utc` | When THIS process received the data (UTC ISO-8601, microseconds). Use this for point-in-time availability: a value is known at T only if received_at_utc <= T. |
| `source_age_ms` | received_at - source_time in ms, when both exist. |
| `recorded_at_utc` | When the writer serialized the record (>= received_at). Not a feature. |
| `parser_version` | Version of the telemetry parser that produced the record. |
| `config_fingerprint` | sha256 of the secret-free configuration (bot Settings or collector config). |
| `code_version` | git commit + hash of *.py sources at process start. |
| `supersedes_id` | If set, this record corrects the record with that event_id. Old records are never updated or deleted. |
| `payload_json` | All remaining fields of the record as redacted JSON (provider payloads, context, notes). |

## exports/detections.csv  (SQLite table `detections`)

| column | type | description |
|---|---|---|
| `detection_type` | TEXT | helius_creation_log | helius_transaction_fetched | helius_mint_resolved | pumpportal_create. |
| `dedup_decision` | TEXT | Dedup verdict at that moment: first_signature/duplicate_signature, first_mint/duplicate_mint, first_seen_by_collector/duplicate_mint_collector, ignored_after_watch_stop, not_create_tx, missing_mint, invalid_reserves. Duplicates are kept as rows. |
| `signature` | TEXT | Full transaction signature (when available). |
| `slot` | INTEGER | Solana slot (when available). |
| `related_event_id` | TEXT | Links a resolved mint / fetched transaction to the earlier signature-only detection. |
| `symbol` | TEXT | Token symbol exactly as provided; null when missing/blank (never guessed). |
| `name` | TEXT | Token name exactly as provided; null when missing/blank. |
| `creator` | TEXT | Creator wallet from the creation message (public address). |
| `bonding_curve` | TEXT | Pump.fun bonding-curve account (when available). |
| `pool` | TEXT | Pool/venue field from the provider (when available). |
| `metadata_status` | TEXT | present | partial | missing | mint_only | mint_unknown_until_getTransaction | tx_only. |
| `raw_json` | TEXT | Redacted original message (JSON). |

## exports/decisions.csv  (SQLite table `decisions`)

| column | type | description |
|---|---|---|
| `stage` | TEXT | Pipeline stage: detector_*, trader_intake, mint_meta, mint_authority, freeze_authority, supply, sol_pair, honeypot_route, pump_curve, liquidity, filters_total, entry, paper_entry. |
| `decision` | TEXT | PASS | FAIL | SKIP | UNKNOWN. |
| `reason_code` | TEXT | Stable machine reason (e.g. slot_busy, liquidity_below_threshold, no_buy_quote, buy_impact_above_limit, check_exception, not_evaluated_mint_meta_missing). |
| `reason_text` | TEXT | Human text as produced by the bot (Russian). |
| `observed_value` | TEXT | Actual value the decision looked at (JSON), when exposed by the pipeline. |
| `threshold` | TEXT | Applied threshold (JSON), when the stage has one. |
| `unit` | TEXT | Unit of observed_value/threshold. |
| `data_source` | TEXT | Where the decision data came from (rpc:getAccountInfo, dexscreener, ...). |
| `outcome` | TEXT | Pipeline consequence: filters_pending, filter_stage_result, blocked, skipped, slot_busy, would_enter_paper, entry_aborted, paper_entered, queued_for_trader, not_forwarded, not_evaluated ... |
| `attempt` | INTEGER | Retry attempt index (0-based) when applicable. |

## exports/observations.csv  (SQLite table `observations`)

| column | type | description |
|---|---|---|
| `obs_type` | TEXT | market_dexscreener | curve_rpc | bot_spot_tick | filter_metric | sol_usd. |
| `provider` | TEXT | Data provider / endpoint family. |
| `status` | TEXT | ok | not_indexed | missing | account_missing | invalid_account_data | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json | invalid_payload | provider_error | no_route | not_tradable | invalid_quote | skipped_<reason> | not_sampled | collector_error ... Missing data is a status, never a zero. |
| `status_reason` | TEXT | Detail for status (redacted). |
| `http_status` | INTEGER | HTTP status code when a request was sent. |
| `latency_ms` | REAL | Request latency in ms. |
| `pair_address` | TEXT | Full pair/pool address of the selected (highest-liquidity) pair. |
| `dex_id` | TEXT | DEX id reported by the provider. |
| `quote_asset` | TEXT | Mint of the other side of the selected pair. |
| `price_usd` | REAL | Token price in USD (provider value; null if unavailable). |
| `price_native` | REAL | Token price in the quote asset of the pair. |
| `price_sol` | REAL | Token price in SOL (pair quote is WSOL, or curve reserves). |
| `liquidity_usd` | REAL | Pair liquidity in USD as reported. |
| `liquidity_base` | REAL | Base-side liquidity (token units) as reported. |
| `liquidity_quote` | REAL | Quote-side liquidity as reported. |
| `market_cap_usd` | REAL | Market cap as reported. |
| `fdv_usd` | REAL | FDV as reported. |
| `volume_json` | TEXT | Volume by interval as reported (JSON, e.g. m5/h1/h6/h24). |
| `pool_created_at_utc` | TEXT | Pair creation time reported by provider. |
| `curve_complete` | INTEGER | 1 if the Pump.fun curve is complete (migrated). |
| `curve_virtual_sol_raw` | TEXT | Virtual SOL reserves, lamports, string. |
| `curve_virtual_token_raw` | TEXT | Virtual token reserves, raw units, string. |
| `curve_real_sol_raw` | TEXT | Real SOL reserves, lamports, string. |
| `curve_real_token_raw` | TEXT | Real token reserves, raw units, string. |
| `metric` | TEXT | Name of a filter metric (stage.metric). |
| `metric_value` | TEXT | Value of the filter metric (JSON). Null = not available (never fake zero). |
| `followup_id` | TEXT | Stable id of (candidate, horizon). |
| `horizon_label` | TEXT | Follow-up horizon (1s, 5s, ..., 24h) the record belongs to. |

## exports/quotes.csv  (SQLite table `quotes`)

| column | type | description |
|---|---|---|
| `purpose` | TEXT | shadow_buy | shadow_sell_exact | followup_sell_exact | followup_buy | bot_entry_buy | bot_entry_sell_precheck | bot_exit | bot_honeypot_probe | bot_unlabeled. |
| `direction` | TEXT | buy (SOL->token) | sell (token->SOL). |
| `provider` | TEXT | Data provider / endpoint family. |
| `status` | TEXT | ok | not_indexed | missing | account_missing | invalid_account_data | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json | invalid_payload | provider_error | no_route | not_tradable | invalid_quote | skipped_<reason> | not_sampled | collector_error ... Missing data is a status, never a zero. |
| `status_reason` | TEXT | Detail for status (redacted). |
| `http_status` | INTEGER | HTTP status code when a request was sent. |
| `latency_ms` | REAL | Request latency in ms. |
| `input_mint` | TEXT | Quote input mint. |
| `output_mint` | TEXT | Quote output mint. |
| `amount_raw` | TEXT | Exact input amount in raw base units (string). |
| `input_decimals` | INTEGER | Decimals of input mint when known (9 for WSOL). |
| `output_decimals` | INTEGER | Decimals of output mint when known. |
| `notional_rub` | REAL | Hypothetical notional in RUB that defined the buy size. |
| `notional_sol` | REAL | Same notional in SOL. |
| `sol_usd_used` | REAL | SOL/USD used to convert the notional. |
| `usd_rub_used` | REAL | USD/RUB used to convert the notional. |
| `out_amount_raw` | TEXT | Quoted output amount, raw units (string). |
| `min_out_raw` | TEXT | otherAmountThreshold (slippage floor), raw units (string). |
| `slippage_bps` | INTEGER | Requested slippage in bps. |
| `price_impact_pct` | REAL | Quoted price impact in percent. |
| `route_labels` | TEXT | Route venue labels (JSON list). |
| `context_slot` | INTEGER | Quote context slot. |
| `quote_pair_id` | TEXT | Links a shadow buy with its exact-size sell and follow-up sells. |
| `attempt` | INTEGER | Retry attempt index (0-based) when applicable. |
| `followup_id` | TEXT | Stable id of (candidate, horizon). |
| `horizon_label` | TEXT | Follow-up horizon (1s, 5s, ..., 24h) the record belongs to. |
| `quote_only` | INTEGER | Always 1: this is a quote, not an execution (DB CHECK constraint). |
| `transaction_sent` | INTEGER | Always 0: no transaction was signed or sent (DB CHECK constraint). |

## exports/followups.csv  (SQLite table `followups`)

| column | type | description |
|---|---|---|
| `followup_id` | TEXT | Stable id of (candidate, horizon). |
| `horizon_label` | TEXT | Follow-up horizon (1s, 5s, ..., 24h) the record belongs to. |
| `horizon_sec` | REAL | Horizon in seconds from anchor. |
| `anchor_time_utc` | TEXT | Reference time of follow-ups: first detection known to the collector. |
| `anchor_event_id` | TEXT | Event id of that first detection. |
| `target_at_utc` | TEXT | Planned observation time = anchor + horizon. |
| `actual_at_utc` | TEXT | When the follow-up was actually started. |
| `delay_ms` | REAL | actual - target in ms. |
| `status` | TEXT | ok | not_indexed | missing | account_missing | invalid_account_data | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json | invalid_payload | provider_error | no_route | not_tradable | invalid_quote | skipped_<reason> | not_sampled | collector_error ... Missing data is a status, never a zero. |
| `status_reason` | TEXT | Detail for status (redacted). |
| `observation_event_ids` | TEXT | Observation records produced for this follow-up (JSON list). |
| `quote_event_ids` | TEXT | Quote records produced for this follow-up (JSON list). |

## exports/lifecycle.csv  (SQLite table `lifecycle`)

| column | type | description |
|---|---|---|
| `lifecycle_type` | TEXT | paper_entry_open | exit_trigger | exit_pending_no_quote | exit_pending_no_exact_amount | exit_pending_zero_amount | exit_trigger_not_confirmed_by_quote | paper_fill_partial | paper_fill_full | paper_position_closed | paper_position_restored | paper_position_open_at_shutdown. |
| `position_key` | TEXT | mint:opened_at of the paper position. |
| `reason` | TEXT | Exit reason as produced by the bot. |
| `fraction` | REAL | Fraction of the position addressed by the event. |
| `amount_raw` | TEXT | Exact input amount in raw base units (string). |
| `tokens_raw` | TEXT | Paper position size in raw units (string). |
| `remaining_raw` | TEXT | Open remainder in raw units (string). |
| `paper_calculation` | INTEGER | 1: values are paper-bot calculations, not real balances. |
| `is_real_transaction` | INTEGER | Always 0 (DB CHECK constraint). |

## exports/health.csv  (SQLite table `health`)

| column | type | description |
|---|---|---|
| `health_type` | TEXT | config_snapshot | collector_started | collector_heartbeat | discovery_connected | discovery_disconnected | data_loss | collector_stopped | bot_shutdown ... |
| `status` | TEXT | ok | not_indexed | missing | account_missing | invalid_account_data | http_429 | http_4xx | http_5xx | timeout | request_error | invalid_json | invalid_payload | provider_error | no_route | not_tradable | invalid_quote | skipped_<reason> | not_sampled | collector_error ... Missing data is a status, never a zero. |

## exports/candidates.csv  (derived at export time)

| column | description |
|---|---|
| `candidate_id` | Stable id of (chain_id, mint). |
| `mint` | Full mint. |
| `first_seen_at_utc` | Earliest received_at_utc of any detection/decision with this mint (bot or collector). |
| `first_signal_at_utc` | Earliest received_at_utc including signature-only Helius logs later resolved to this mint (<= first_seen_at_utc). |
| `first_seen_event_id` | Event id of the first detection. |
| `first_seen_source / first_seen_detected_by` | Source and process of the first detection. |
| `*_at_first_detection` | Identity fields copied ONLY from the first detection record (no later metadata = no look-ahead). Null if unknown. |
| `detection_events / duplicate_detection_events` | All detection rows for the mint and how many were duplicates. |
| `detected_by_set / sources_set` | Distinct processes / sources that saw the candidate. |
| `bot_*_postdetection` | Bot outcomes recorded AFTER detection (seconds later). Useful to study the filter, NOT a T0 feature. |
| `quote_sampled` | 1 if the collector selected the candidate for shadow quotes (deterministic hash sample). |
| `followups_recorded` | Number of horizon records (any status, incl. missed). |

## exports/features_at_detection.csv  (derived at export time)

| column | description |
|---|---|
| `asof_time_utc` | Point-in-time = first_seen_at_utc. Every column is taken from the first detection message only. |
| `v_sol_in_curve / v_tokens_in_curve / market_cap_sol / initial_buy / sol_amount` | Values from the PumpPortal create message (null if absent). |
| `has_name / has_symbol / has_uri / name_len / symbol_len` | Simple metadata presence features. |

## exports/labels.csv  (derived at export time)

| column | description |
|---|---|
| `candidate_id` |  |
| `mint` |  |
| `label_name` | price_return_pct[...] | max_upside_pct[...] | max_drawdown_pct[...] | quote_roundtrip_return_pct | quote_roundtrip_return_minout_pct. |
| `horizon_label` |  |
| `horizon_sec` |  |
| `anchor_time_utc` |  |
| `anchor_event_id` |  |
| `target_time_utc` |  |
| `reference_time_utc` |  |
| `reference_event_id` |  |
| `reference_value` |  |
| `value_time_utc` |  |
| `value_event_id` |  |
| `value` | Label value in percent; EMPTY when censored (never 0 as a placeholder). |
| `censored` | 1 = future unavailable at export (not matured, no data, no route, provider error). Keep these rows; do not drop candidates. |
| `censor_reason` | Why the label is censored. |
| `incomplete` | 1 = horizon point missing; value (if any) uses fewer points. |
| `route_available` | For quote labels: 1 sell route existed at the horizon, 0 quote failed (e.g. no_route), empty unknown. |
| `notional_rub` |  |
| `points_used` |  |
| `source` |  |
| `method` | Exact computation method. |
| `label_version` |  |
| `computed_at_utc` |  |
| `reference_*` | Reference point (time, event, value) the label is measured from. |
| `value_*` | Point (time, event) the label value comes from. |
