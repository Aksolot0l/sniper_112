# Прогон №2 — предполётный чеклист (build 8fbe5ba / archive f115e44)

## Перед стартом

1. Распаковать **новый** архив `dist/sniper_ab_paper_local_20261022T120052Z.zip`
   (BUILD внутри = `8fbe5ba`), не поверх прошлого раза — в новую папку.
2. Проверить, что в шелле нет устаревшей переменной (она ПЕРЕОПРЕДЕЛЯЕТ код):
   ```bash
   echo "[$ENTRY_BIRTH_MAX_SOL] [$ENTRY_DELTA_MAX_SOL]"
   ```
   Должно быть `[] []` (или `[] [2.5]`). Если увидел `50.0` — `unset ENTRY_BIRTH_MAX_SOL`.
3. Запуск на 30 минут (дефолт скрипта — 9000 с, поэтому длительность задать явно):
   ```bash
   bash START_MAC_LINUX.sh --duration 1800
   ```
4. Один процесс, paper-only, ключ только в памяти. Дашборд `127.0.0.1:8767`.

## Первые 60 секунд после старта (по run.log)

- в шапке манифеста: `entry: 30.5 <= v_sol_birth <= 34.5 ...`, `fix_7: ... does NOT create profit`;
- в `run.log` нет `A/B preflight fail-closed: missing_or_invalid_v_sol_birth` пачками.

## После прогона — 6 цифр, по которым видно, всё ли сработало

| Что | Где | Ожидание | Если не так |
|---|---|---|---|
| `v_sol_birth_max` | `manifest.json` → `strategies.portfolio_a` | `34.5` | конфиг не подхватился (env override) |
| `birth_reject` | `manifest.json` → `funnel.chain` | ~140 вместо 102 | тот же override |
| `admits` / `opened` A | там же | ~33 / 22–26 | >30 входов = фильтр не работает |
| `dev_buy_presence` | `funnel` | `bought` ≈ 35–40% | `unknown` = 100% → сломан парсер `solAmount` |
| `gate_status.unavailable` | `funnel` / `creator_gate` | ~27% (8–12 монет) | >35% = RPC деградировал |
| `error_detail` | `exports/creator_probes.csv` | впервые покажет причину (429 / bad request / RPC code) | — |

`funnel.a_only` теперь делит пропуски B на `unavailable` и `fail_risk` — именно эта таблица
отвечает, есть ли у гейта B собственный сигнал или он «работает» сбоями связи.

## Что считать результатом

- Успех прогона №2 — **исчез хвост**, а не плюс: A ≈ −500 ₽ (−33%), B ≈ −120 ₽ (−8%).
  Разброс из-за другой выборки монет: A в диапазоне −300…−700 ₽ при 22–26 входах — норма.
- Если A окажется в плюсе — это везение на 20 сделках, а не доказательство эджа.
- Сравнение с прогоном №1 корректно только по входному фильтру: изменились обе руки.
- Вопрос «работает ли creator guard» прогон №2 не решает: 70% его прошлого преимущества
  было артефактом телеметрии. Смотреть `gate_status`, а не PnL B.

## Что прислать на разбор

`telemetry.sqlite`, `sniper.db`, `exports/*.csv`, `run.log`, `manifest.json`, `SUMMARY.md`, `ab_summary.json`.
