# 🎯 solana_sniper — референс-проект курса

Эталонный мем-снайпер (paper trading) — **ориентир, к которому идём в обучении**.
Во время курса ты пишешь СВОЙ код сам, а к этому проекту сверяешься как к цели.

> ⚠️ **Это СИМУЛЯТОР.** Никаких реальных транзакций, приватных ключей и списаний
> здесь нет и не может быть — только виртуальные сделки и запись в SQLite.

## Запуск

```bash
pip install -r requirements.txt

# Linux / macOS
export HELIUS_API_KEY=ваш_ключ
python main.py

# Windows PowerShell
$env:HELIUS_API_KEY="ваш_ключ"
python main.py
```

Ключ API — ТОЛЬКО через переменную окружения (никогда не в коде!).
Ключ можно получить бесплатно на https://dev.helius.xyz/

## Режимы работы

| MODE | Что делает |
|------|-----------|
| `one` (по умолчанию) | Ловит ПЕРВУЮ монету, прошедшую фильтры → фейк-сделка → итоги → стоп |
| `portfolio` | До `MAX_POSITIONS` позиций одновременно, работает до Ctrl+C |

## 🏆 Челлендж 1500

Кошелёк живёт в `wallet.json` (JSON) и **переживает перезапуски**:
1. При первом запуске создаётся с `DEPOSIT_RUB` (1500 ₽).
2. Каждая сделка — **ровно 1/3 текущего баланса** (≈500 ₽ в старте).
3. **Честный PnL**: налог площадки 1.25% (покупка + продажа) и
   priority fee/Jito tip (0.002 SOL на вход, 0.0001 SOL на выход)
   вычитаются из виртуального счёта.
4. **Лестница выхода:**
   - `SL −20%` — весь объём сразу;
   - `TP1 +30%` — продаём 50%, SL остатка переносится в
     **истинный безубыток +3.5%** (P_entry × 1.035);
   - остаток держится **без потолка прибыли** и продаётся только
     по **трейлинг-стопу −15% от ATH** (или при застое: размах < 3%
     в течение 120 сек до TP1 — принудительный выход 100%).
5. Итоги каждой сделки пишутся в `wallet.json` и `trades.log`:
   `[11:30] Монета … Вход: 500. Выход: 650. Профит: +150. Баланс: 1650.`

Сброс челленджа: удалить `wallet.json` (создастся заново с 1500).

## 🚀 Быстрый вход (<0.8 с до решения)

1. **Pump.fun — Pumportal WSS** (`PUMP_SOURCE=pumpportal`): mint, создатель, адрес кривой и **стартовые резервы** приходят прямо в WSS-пакете. `getTransaction` не вызывается вообще — нет 429, нет ретраев. Цена входа считается из резервов пакета (`vSol / vTokens`) — 0 запросов.
2. **Фильтры параллельно** (`asyncio.gather`): mint-мета, SOL-пара, кривая, ликвидность и honeypot-голосуют одновременно.
3. **Кривая не читается дважды**: адрес и резервы из пакета едут в `Position.parsed_curve_data`.
4. Raydium/Meteora идут через Helius `logsSubscribe` + `getTransaction` (медленнее — бесплатный план Helius не даёт `transactionSubscribe`).

## 🏦 Сейф (Vault)

При балансе ≥ 5000 ₽ (потом каждые +2500 ₽) бот забирает **50% чистой прибыли** (сверх стартового депозита) в несгораемый `vault_rub`. Деньги Сейфа не участвуют в торговле и не уменьшаются при убытках.

## 💾 Надёжность

- **Открытые позиции живут в `wallet.json`** — перезапуск бота не теряет сделку: позиции восстанавливаются (`♻️ Восстановлена позиция …`) и мониторинг продолжается.
- Учёт в трёх местах (wallet.json / trades.log / sniper.db) сходится построчно.
- Индексаторная spot-цена может только подать сигнал выхода. SL/TP1/BE/trailing подтверждаются свежей Jupiter ExactIn quote точного размера; пороговый выход не фиксируется, если executable quote не пересекла нужную цену. Выручка учитывается только по `otherAmountThreshold` с 1% slippage. Timeout и Stagnation тоже требуют свежего sell quote; без него позиция остаётся открытой/pending.
- Honeypot-проверка фиксированного размера (`HONEYPOT_CHECK=on`) может получать HTTP 400/429 на свежем mint. Независимо от неё paper-вход требует точную Jupiter ExactIn quote покупки и предварительную quote продажи ровно того количества токенов, которое модель зачислит; это снижает риск входа без маршрута выхода, но не гарантирует будущий fill.
- `ENTRY_QUOTE_RETRIES` добавляет ограниченные повторы ExactIn buy-quote и exact-size sell-precheck при отсутствии маршрута; каждый повтор заново запрашивает тот же raw-размер. Повторы не обходят лимит impact и не создают fill без quote.
- `INDEXER_RETRIES` повторяет запросы DexScreener/GeckoTerminal только если оба индексатора вернули пустой ответ. Найденный пул с ликвидностью ниже порога не проходит retry и остаётся заблокирован.
- `MAX_ENTRY_PRICE_IMPACT_PCT` ограничивает impact обеих стартовых котировок. `MIN_LIQUIDITY_USD` сравнивается с `liquidity.usd` DexScreener или `reserve_in_usd` GeckoTerminal; при недостатке/отсутствии данных вход блокируется.

## Настройки (переменные окружения)

| Переменная | По умолчанию | Смысл |
|-----------|--------------|-------|
| `HELIUS_API_KEY` | — | API-ключ Helius (обязателен) |
| `MODE` | `one` | `one` / `portfolio` |
| `SOURCES` | `all` | какие площадки слушать: `pumpfun`, `raydium-v4`, `raydium-cpmm`, `meteora-dlmm`, `meteora-damm`, `meteora-dbc` — через запятую |
| `PUMP_SOURCE` | `pumpportal` | путь входа Pump.fun: `pumpportal` (минт из WSS-пакета, быстро) / `helius-logs` (getTransaction, медленно) |
| `MAX_POSITIONS` | `3` | слотов входа (Semaphore) |
| `FILTER_CONCURRENCY` | `6` | сколько фильтр-наборов параллельно |
| `DEPOSIT_RUB` | `1500` | стартовый баланс челленджа (при первом запуске), ₽ |
| `WALLET_PATH` | `wallet.json` | файл кошелька: баланс + история сделок |
| `TRADE_LOG` | `trades.log` | текстовый журнал сделок |
| `TAKE_PROFIT_PCT` | `30` | TP1: рост, при котором продаём 50%, % |
| `STOP_LOSS_PCT` | `10` | Hard Stop-Loss, % (рыночный ордер) |
| `CHECK_MIN_SEC` | `0.5` | минимум секунд между проверками цены |
| `CHECK_MAX_SEC` | `0.5` | максимум секунд между проверками цены (0.5 — 6 RPC-запросов/с при 3 позициях: бесплатный Helius может 429-лимитить) |
| `BE_OFFSET_PCT` | `3.5` | истинный безубыток после TP1, % |
| `TRAIL_PCT` | `15` | откат от ATH для трейлинг-стопа, % |
| `STAGNATION_SEC` | `180` | таймаут застоя, сек |
| `STAGNATION_RANGE_PCT` | `3` | допустимый размах цены при застое, % |
| `PROTOCOL_FEE_PCT` | `1.25` | legacy-параметр; DEX-комиссии и impact уже внутри quote Jupiter, повторно не вычитаются |
| `ENTRY_FEE_SOL` | `0.001` | сетевая/priority-комиссия на вход, SOL |
| `EXIT_FEE_SOL` | `0.0001` | сетевая/priority-комиссия на выход, SOL |
| `USD_RUB_RATE` | `90` | курс USD→RUB для перевода SOL-комиссий |
| `MAX_TRADE_RUB` | `1500` | кап размера сделки, ₽ |
| `VAULT_START_RUB` | `5000` | первая веха Сейфа, ₽ |
| `VAULT_STEP_RUB` | `2500` | шаг вех Сейфа, ₽ |
| `VAULT_SHARE` | `0.5` | доля чистой прибыли, уносимая в Сейф |
| `MAX_HOLD_SEC` | `120` | при достижении отправить выход; без quote-fill позиция остаётся открытой, выручка не начисляется |
| `MIN_LIQUIDITY_USD` | `500` | фактический порог DexScreener `liquidity.usd` / GeckoTerminal `reserve_in_usd`, $ |
| `MAX_ENTRY_PRICE_IMPACT_PCT` | `5` | максимальный impact точной quote покупки и exact-size предварительной продажи; иначе вход блокируется |
| `ENTRY_QUOTE_RETRIES` | `0` | дополнительные повторные запросы exact-size buy/sell-precheck quote; без ослабления проверок |
| `ENTRY_QUOTE_RETRY_DELAY_SEC` | `1` | базовая пауза линейного backoff для повторов Jupiter quote, сек |
| `INDEXER_RETRIES` | `0` | повторы DexScreener/GeckoTerminal только при пустых ответах обоих источников |
| `INDEXER_RETRY_DELAY_SEC` | `1` | базовая пауза повторной проверки индексаторов, сек |
| `HONEYPOT_CHECK` | `on` | дополнительная fixed-size проверка продажи; exact-size проверки входа выполняются отдельно |
| `ONLY_SOL_PAIRS` | `on` | искать только пулы с SOL (отсекает USDC/USDT-пары) |
| `DB_PATH` | `sniper.db` | путь к журналу SQLite |

## Как отключить площадки (например, Pump.fun)

**Способ 1 — настройка (без правки кода):**
```powershell
# только Raydium CPMM и Meteora DBC — без Pump.fun
$env:SOURCES="raydium-cpmm,meteora-dbc"; python main.py
```
```bash
SOURCES=raydium-cpmm,meteora-dbc python main.py
```
Доступные id: `pumpfun`, `raydium-v4`, `raydium-cpmm`, `meteora-dlmm`, `meteora-damm`, `meteora-dbc`. Пусто или `all` — слушать всё.

**Способ 2 — убрать совсем из кода:** в `detector.py` удалить (или закомментировать) строку в `PROGRAMS`, строку в `SOURCE_IDS` и блок в `CREATION_PATTERNS` — и площадки не будет нигде.

## Структура проекта (модуль → тема курса)

| Модуль | Отвечает | День курса |
|--------|----------|-----------|
| `config.py` | настройки из env, секреты | 12 |
| `models.py` | данные: dataclass | 1 |
| `console.py` | вывод, цвета, время | 1 |
| `price.py` | он-чейн кривая + индексаторы цены | 6, 9 |
| `detector.py` | ловит создания монет, извлекает CA | 8 |
| `filters.py` | анти-скам: authority, supply, ликвидность, honeypot | 10 |
| `trader.py` | paper trading: TP/SL/таймаут | 3, 4 |
| `storage.py` | SQLite-журнал сделок (aiosqlite) | 11 |
| `ws_client.py` | WebSocket-подписки, reconnect | 7 |
| `main.py` | сборка конвейера (asyncio.Queue) | 5, 13 |

## Как читать этот код

1. **Сначала `main.py`** — видно весь конвейер целиком.
2. Каждая функция начинается с `# ПЛАН:` — это дисциплина «сначала думаем».
3. Правила, по которым написан код (они же правила курса):
   - `await queue.get()` — строго ВНЕ `try`, `queue.task_done()` — строго в `finally`;
   - f-строки с одинарными кавычками: `f'{price:.2f}'`;
   - WebSocket — всегда `commitment: 'processed'`;
   - данные — `dataclass`, логика — чистые функции.

## Проверка статистики

```bash
sqlite3 sniper.db 'SELECT COUNT(*), SUM(pnl_rub) FROM trades;'
```

Или прямо в консоли Python:

```python
import asyncio
from storage import get_stats
print(asyncio.run(get_stats('sniper.db')))
```
