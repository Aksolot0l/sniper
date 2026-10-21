# sniper

Solana paper-бот (`solana_sniper/`) и пассивный сборщик телеметрии (`solana_sniper/telemetry/`, только чтение).

* Бот: [`solana_sniper/README.md`](solana_sniper/README.md). Ключ Helius — только через `HELIUS_API_KEY`.
* Телеметрия: [`solana_sniper/telemetry/README.md`](solana_sniper/telemetry/README.md),
  колонки — [`DATA_DICTIONARY.md`](solana_sniper/telemetry/DATA_DICTIONARY.md).

Архивы прогонов (`runs/`), `wallet.json` и `sniper.db` в эту ветку не переносятся; они остаются в исходном ZIP в `main`.
Данные телеметрии пишутся в отдельный каталог `telemetry_data/` (в `.gitignore`).
