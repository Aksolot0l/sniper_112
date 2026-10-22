# Прогон №1 — восстановленный архив (30 мин, build `9f2cd08`, RUN_ID `run1-30min-20261022T101923Z`)

Оригинальная папка прогона в Dropbox (`/sniper_112/_EMPTY_placeholder_run1-30min`) сейчас пуста,
локальные копии у оператора удалены. Здесь лежит всё, что сохранилось у аудитора до удаления,
и то, что из этого удалось пересобрать.

## Что оригинальное (скачано из Dropbox 22.10 в 11:31–11:33 UTC)

| файл | строк | что внутри |
|---|---:|---|
| `candidates.csv` | 411 | по одному событию создания: минт, символ, `v_sol`, `delta`, `initial_buy`, `sol_amount`, актор |
| `decisions.csv` | 2 466 | вся воронка: security → сигнал A → гейт B → вход → выход |
| `creator_probes.csv` | 405 | зонды дева: identity, подписи, ATA, burst, latency, `error_type` |
| `observations.csv` | 2 430 | цены на офсетах 0/5/10/15/60/120 с от первого события |
| `paper_fills.csv` | 44 (27 A + 17 B) | ноги выходов: цена, PnL, комиссии, причина |
| `ab_summary.json` | — | итоги A/B, комиссии, RPC-метрики |
| `SUMMARY.md` | — | отчёт оператора по прогону |

## Что пересобрано (не оригинал)

`telemetry_rebuilt.sqlite` — собран `rebuild_and_check.py` из пяти CSV по схеме `telemetry.py`
(build `8fbe5ba`). Внутри: `candidates` 411, `decisions` 2 466, `paper_fills` 44,
`creator_probes` 405, `observations` 2 430.

Проверка сходимости: PnL `portfolio_a` = **−1 035.56 ₽**, `portfolio_b` = **−672.77 ₽** —
совпадает с `ab_summary.json` до копейки. Воронка из этой БД: 411 → 236 security → 29 admits → 26 открыто.

Ограничения пересборки: в `candidates` нет колонок `dev_buy_sol` / `initial_buy_raw` (в CSV их не было,
значения лежат внутри `raw_json`), в `creator_probes` нет `error_detail` (поля тогда не существовало).

## Что потеряно безвозвратно

- `telemetry.sqlite` (оригинал) и `sniper.db` — журнал сделок;
- `run.log`, `manifest.json`, `wallet_portfolio_a.json`, `wallet_portfolio_b.json`, `preflight_tests.log`;
- выгрузки `detections.csv`, `quotes.csv`, `creator_attributions.csv`, `watchlist_events.csv`,
  `raw_events.jsonl`, `collector_meta`, `data_dictionary.md`;
- архив `run1-30min-20261022T101923Z.zip`.

На выводы разбора это не влияет: всё, что использовалось в аудите (сделки, воронка, зонды, наблюдения,
ончейн-времена), сохранено. `detections`/`quotes` нужны были только для сверки объёмов.

## `derived/` — рабочие артефакты аудитора

- `trades.json` — 42 сведённые сделки (арм, минт, символ, first_seen/open/close, held, PnL, причина, dev buy);
- `onchain_rows.json` — те же 42 сделки, сшитые с ончейн-временем создания (`firstPool.createdAt` Jupiter):
  лаг MINT→open, OPEN→CLOSE;
- `recs.json` — сделки + признаки из `decisions.csv entry_state` + `creator_probes`;
- `mints.txt` — 28 минтов для ончейн-запросов;
- `onchain.py` — скрипт сшивки (требует сети к `datapi.jup.ag`).

## Как пользоваться

```bash
python3 rebuild_and_check.py          # пересобрать БД из CSV и напечатать проверку
```

Дальше — обычные запросы к `telemetry_rebuilt.sqlite` или `git`-версии этого архива.
Он же (zip) лежит в `audit/run1-30min-20261022T101923Z_recovered.zip` в ветке аудитора:
`git show <commit>:audit/…zip > run1.zip` восстанавливает всё даже при потере рабочей папки.

## Ещё одна возможная копия

В корне Dropbox лежит `workspace-01a162cc-f869-703c-8048-44edc1bf2305.zip` (30.5 МБ, 22.10 08:58 UTC) —
возможно, это дамп рабочей папки с ранними артефактами. Скачать его напрямую из песочницы нельзя,
но у тебя он открывается: если внутри есть `runs/…/telemetry.sqlite` или `run.log` — это шанс вернуть
и потерянную часть.
