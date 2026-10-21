# Пассивный сборщик телеметрии (только чтение)

Пакет `solana_sniper/telemetry/` сохраняет **каждого кандидата с момента самого раннего обнаружения**
и его дальнейшее наблюдаемое поведение для офлайн-анализа. Ничего не торгует, ничего не подписывает
и не отправляет: Jupiter используется только как `GET /quote` (`quote_only=1`, `transaction_sent=0`
закреплены `CHECK`-ограничениями SQLite), RPC — только методы из allowlist для чтения.

> ⚠️ Collector и бот **не запускались** при разработке. Все тесты офлайн, на синтетических данных,
> сеть в тестах заблокирована (`tests/conftest.py`).

## 1. Найденный pipeline и точки наблюдения

```
ws_client.ws_listener ─► detector.detector_worker ─► _process_creation ─┐
   (Helius logsSubscribe)   (creation? dedup signature)  (getTransaction→mint, dedup mint)
detector.pumpportal_listener ─► on_transaction_signal (dedup mint) ─────┤
                                                                         ▼
trader.trader_worker ─► _handle_coin ─► filters.run_all_checks ─► mode/balance/slot ─► start_paper_trade
                                                                     ▼
                         manage_position ─► sell_part ─► _finish_position (wallet.json, trades.log, SQLite)
price.get_jupiter_quote — единственная точка Jupiter quote бота (+ honeypot-проба в filters)
```

| Точка | Hook | Что пишется |
|---|---|---|
| `detector_worker`, сразу после распознавания creation-лога, **до** dedup по signature | `helius_creation_log` | detection + решение dedup (`first_signature`/`duplicate_signature`) |
| `detector_worker`, ветка `watch_stop` | `helius_ignored_after_watch_stop` | creation-события, которые бот игнорирует после выбора цели |
| `fetch_mint_from_signature` | `helius_tx_fetched` | slot, `blockTime` → `source_time_utc` |
| `_process_creation` | `helius_mint_resolved` / `helius_mint_unresolved` | mint, кривая, dedup по mint, неудача извлечения |
| `pumpportal_listener`, **до** `on_transaction_signal` | `pumpportal_raw` + `pumpportal_forwarded` | каждое сырое сообщение (и повторы), решение парсера |
| `_handle_coin`, **до** фильтров/слота | `candidate_received` | intake кандидата |
| `run_all_checks` и `check_*` | `filter_stage`, `filter_metric`, `filters_not_evaluated` | PASS/FAIL/UNKNOWN каждого этапа, значения и пороги, исключения |
| `_handle_coin` | `filters_verdict`, `entry_decision` | blocked / target taken / мал баланс / **slot_busy** / slot_acquired / paper_entered |
| `start_paper_trade` (каждый `return False`) | `entry_decision(stage='paper_entry')` | decimals, SOL/USD, нет buy/sell quote, impact выше лимита… |
| `get_jupiter_quote`, `check_honeypot_route` | `jupiter_quote`, `honeypot_probe` | каждая попытка: успех и неуспех (HTTP, timeout, no route…), latency, raw amounts строкой |
| `sell_part`, `_finish_position`, restore, shutdown | `lifecycle` | trigger, pending, неподтверждённый trigger, partial/full paper-fill, закрытие, открытый остаток |
| `manage_position` | `spot_tick` | каждая цена бота (или её отсутствие) |

Cooldown в текущем коде бота отсутствует — его некому логировать.

**Почему это безопасно для торговли.** Каждый hook — одна строка, обёрнутая в `_guard`
(ловит любые исключения и считает их потерю), без `await`, без IO и сети: только `queue.put_nowait`
в ограниченную очередь. При выключенной телеметрии (нет `TELEMETRY_DIR`) hook сразу возвращает `None`.
Возвращаемые значения hooks бот не использует в решениях. Условия, пороги, порядок проверок,
`return`-значения и расчёт PnL не менялись; единственные структурные правки — промежуточные
переменные (`stage_result`, `blocked`, `stage_names`) вместо inline-выражений с тем же результатом.
Существующие 15 тестов бота проходят без изменений, а отдельный тест сравнивает итог сделки
при выключенной, рабочей и сломанной телеметрии.

## 2. Архитектура данных

```
бот (hooks) ─put_nowait─► Observer(очередь) ─поток-писатель─► TELEMETRY_DIR/spool/bot-*.jsonl
collector ─PumpPortal (своя подписка)───────────┐
         ─читает spool (по offset, идемпотентно)─┼─► один писатель SQLite ─► collector.sqlite
         ─follow-ups / shadow quotes (read-only)─┘                         └► raw_events.jsonl
export (офлайн) ─► exports/*.csv, README.md (coverage), data_dictionary.md, manifest.json, bundles/*.zip
```

* SQLite пишет только collector (или export, если collector не запущен) → один writer.
* Все таблицы событий append-only (триггеры запрещают `UPDATE`/`DELETE`), исправления — новой строкой с `supersedes_id`.
* Идемпотентность по `event_id`; разные сырые события (в т.ч. повторы mint) всегда разные строки.
* Переполнение очереди / ошибки записи → запись `health.data_loss` со счётчиком, причиной и интервалом.
* Рестарт: прошедшие горизонты помечаются `missed/collector_not_running_at_target`, будущие планируются заново.
* Redaction: ключи-секреты, URL (оставляется только схема+хост), значения секретных env, `Bearer …`, `api-key=…`, массивы-ключи из 64 байт.
* Каталог данных отдельный; collector отказывается писать внутрь `runs/` или в каталог с `wallet.json`/`sniper.db`.

## 3. Схема и data dictionary

Таблицы: `detections`, `decisions`, `observations`, `quotes`, `followups`, `lifecycle`, `health`
(+ служебные `schema_migrations`, `spool_offsets`). Полное описание колонок — в
[`DATA_DICTIONARY.md`](DATA_DICTIONARY.md); при каждом экспорте генерируется `data_dictionary.md` с числом строк.

Производные выгрузки: `candidates.csv` (одна строка на кандидата, идентичность — только из первого
обнаружения), `features_at_detection.csv` (только информация на момент T0), `labels.csv`
(будущие метки отдельно: `price_return_pct`, `max_upside_pct`, `max_drawdown_pct`,
`quote_roundtrip_return_pct`, с горизонтом, точкой отсчёта, методом, `censored`/`incomplete`).

## 4. Конфигурация (env)

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `TELEMETRY_DIR` | (не задана → hooks выключены) | каталог данных; для collector можно `--data-dir` |
| `TELEMETRY_HORIZONS` | `1s,5s,15s,30s,60s,120s,5m,15m,1h,6h,24h` | горизонты follow-up |
| `TELEMETRY_QUOTE_HORIZONS` | `5s,30s,60s,120s,5m,15m,1h` | на каких горизонтах делать quote |
| `TELEMETRY_NOTIONALS_RUB` | `250,500` | гипотетические размеры |
| `TELEMETRY_QUOTE_SAMPLE_RATE` | `0.05` | доля кандидатов (детерминированный hash-sample) для shadow quotes |
| `TELEMETRY_JUPITER_RPM` / `_DEXSCREENER_RPM` / `_RPC_RPM` | `30` / `60` / `60` | консервативные бюджеты запросов в минуту |
| `TELEMETRY_RPC_URL` | — | read-only RPC для чтения bonding curve (опционально) |
| `TELEMETRY_USE_HELIUS_RPC=on` | off | собрать RPC URL из `HELIUS_API_KEY` (тратит кредиты) |
| `TELEMETRY_DISCOVERY` | `pumpportal` | `none` — только spool бота |
| `TELEMETRY_MAX_STORAGE_MB` | `20000` | после превышения сетевые замеры ставятся на паузу (статус `skipped_storage_limit`) |
| `USD_RUB_RATE` | `90` | как в боте |

Лимиты провайдеров заданы консервативно и должны быть сверены с вашим тарифом. При 429 учитывается
`Retry-After`, иначе экспоненциальный backoff с jitter; повторы — только для timeout/5xx, максимум 3 попытки.
Если запрос не отправлен (бюджет, cooldown, нет свежего SOL/USD, лимит хранилища), это сохраняется явным статусом.

## 5. Команды (из каталога `solana_sniper/`)

```bash
# офлайн-тесты (без сети, без main.py)
python -m pytest -q tests

# офлайн-экспорт + ZIP (без передачи данных куда-либо)
python -m telemetry export --data-dir ../telemetry_data --zip
python -m telemetry status --data-dir ../telemetry_data
```

Запуск — **только после отдельного разрешения**:

```bash
# collector (read-only сеть)
python -m telemetry run --data-dir ../telemetry_data
# остановка (корректная, с flush)
python -m telemetry stop --data-dir ../telemetry_data    # или: kill -TERM "$(cat ../telemetry_data/collector.pid)"

# hooks внутри бота включаются только переменной окружения при обычном запуске бота:
TELEMETRY_DIR=../telemetry_data python main.py
```

## 6. Объём и частота (оценка)

Размер записи: detection ≈ 1.4 КБ, market observation ≈ 1.4 КБ, follow-up ≈ 1 КБ, quote ≈ 0.7–1.5 КБ.
На кандидата без quote-выборки ≈ 30–40 КБ в JSONL и примерно столько же в SQLite → **≈ 70 КБ на кандидата**.
При 20–30 тыс. созданий Pump.fun в сутки — **≈ 1.4–2.1 ГБ/сутки** (JSONL ротируется в `raw_archive/*.jsonl.gz`).
Запросы: DexScreener батчами до 30 mint (≈ ≤ 60/мин), Jupiter ≈ 8/мин при sample 5%, RPC — только если настроен.

## 7. Как проверить полноту

* `README.md` бандла: кандидаты, метаданные, quotes, каждый горизонт (созревшие vs записанные), причины пропусков.
* `health`: `data_loss` должен быть 0; `discovery_disconnected` — интервалы без собственного потока.
* `candidates.followups_recorded` ≈ числу горизонтов для созревших кандидатов.
* Сверка `detection_events` с консолью бота («Новая монета») за то же окно.
* `manifest.json`: sha256 и число строк каждого файла.

## 8. Известные ограничения полноты

1. **Helius-поток (Raydium/Meteora)** собирается только через hooks бота, пока бот слушает. В режиме
   `MODE=one` бот закрывает WebSocket после выбора цели (`watch_stop`) — дальше эти источники не видны.
   Собственная подписка collector покрывает только Pump.fun (PumpPortal). Отдельный Helius-поток
   в collector не добавлен намеренно (тратит кредиты ключа; ключ — только из env).
2. `logsNotification` содержит slot в `context`, но `ws_client` отбрасывает его до очереди: у
   Helius-детекций slot/blockTime появляются только после `getTransaction` (`helius_transaction_fetched`).
3. Значения внутри фильтров, которые бот не возвращает (например, точная ликвидность в DexScreener),
   записываются через `filter_metric`; для остальных этапов `observed_value` может быть пустым.
4. DexScreener и PumpPortal не дают времени источника → `source_time_utc` пуст, используйте `received_at_utc`.
5. Follow-up отсчитываются от первого обнаружения, **известного collector**; если бот увидел mint раньше,
   `candidates.first_seen_at_utc` покажет более раннее время (разница обычно < 1 с).
6. Пики/просадки считаются по дискретным точкам и **занижают** реальные экстремумы.
7. Quote-метки гипотетические: без комиссий/priority fee, не являются fill.
8. Если бот и collector работают одновременно, это две подписки PumpPortal (рекомендация провайдера — не плодить соединения).
9. `main.py`/`ws_client.py` используют синтаксис f-строк Python 3.12+; тесты и пакет телеметрии работают на 3.11+.
