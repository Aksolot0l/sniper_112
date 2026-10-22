> Готовый промпт для Кодера (копировать целиком)

# Задача: правка №7 — потолок `v_sol_birth` 50.0 → 34.5 + диагностика слепых зон

Контекст: прогон №1 (`9f2cd08`, 30 мин, 405 кандидатов). A −1 035.56 ₽ (−69.0%), B −672.77 ₽ (−44.9%).
Аудит сырых `candidates.csv` / `decisions.csv` / `creator_probes.csv` / `observations.csv` показал:
`v_sol_birth = 29.96 + dev_buy_SOL` (corr 0.99988, n=145) — то есть допуск `birth` до 50 SOL пускает
в торговлю монеты, где дев сам купил 4–6 SOL своего же токена. На 136 кандидатах с `solAmount`:
devBuy 0 → 2/116 монет падают ≤ −25%; 1–3 SOL → 23/86; ≥ 4 SOL → 36/50. В сделках все 4 монеты
с devBuy ≥ 5 (MONEKO, ALPK, $PONK, Моисей) — минус на −538.57 ₽ в A и −552.79 ₽ в B.

## 1. Поведенческое изменение (ровно одно, в обеих руках)

`v_sol_birth_max`: 50.0 → **34.5** (порог соответствует dev buy ≥ 4.54 SOL).

Файлы, где значение задаётся/печатается — менять согласованно:
1. `solana_sniper/config.py:162` — дефолт `ENTRY_BIRTH_MAX_SOL` `'50.0'` → `'34.5'`; комментарий на `config.py:65` переписать по факту.
2. `solana_sniper/run_local_session.py:85` — дефолт `env.get('ENTRY_BIRTH_MAX_SOL', ...)`.
3. `solana_sniper/run_rocket2_ab_paper.sh:79` — `export ENTRY_BIRTH_MAX_SOL`.
4. `solana_sniper/portfolio_ab.py:74` — дефолт dataclass `ABProfile.v_sol_birth_max`; `portfolio_ab.py:166` — fallback в `entry_band`.
5. `solana_sniper/main_ab.py:145`, `README.md:42` — строка баннера/описания правила (сейчас 50.0).
6. `solana_sniper/telemetry.py:545` — устаревшая строка `'30.5 <= v_sol_birth <= 38.0 and v_sol_now <= v_sol_birth + 0.8'`; заменить на фактическое правило.

НЕ менять: `delta_min_sol 0.10`, `delta_max_sol 2.5`, всю лестницу выходов (SL −10%, no-progress 5 c, shelf 4.5 c, TP1 +30%, rocket +50%, таймауты 35/90 c), размер 500 ₽, `MAX_POSITIONS = 3`, `duplicate_symbol = false`, paper-only. Новых RPC-запросов не добавлять.

Ожидание по прогону: входов станет 22–26 вместо 26, PnL A ≈ −500 ₽. Это изменение **срезает хвост катастрофы, а не выводит в плюс**. Если в отчёте появится «вышли в прибыль» — это ошибка.

## 2. Диагностика (поведение не меняет, но без неё следующий разбор невозможен)

7. `creator_probe.py` (`_creator_token_accounts`, `_signatures`): 108 из 405 проб (27%) уходят в `censored` с `error_type=RuntimeError` и латентностью 83 мс — быстрый отказ RPC, а не таймаут. Сохранять сырой payload ответа (`code`, `message`, первые 200 символов) в новое поле `error_detail` пробы и выводить его в экспорт. Сейчас причина неизвестна и восстановить её из архива нельзя.
8. `portfolio_ab.py:721-752`: разделить `fail` и «не смогли прочитать» по-разному. Сейчас `creator_token_ata_missing` попадает в `censored`, а `creator_history_too_short` — в `fail`, хотя оба означают «нет данных»: у монет без `pumpportal_event_actor` читается fee-кошелёк кривой с 1–2 подписями, и гейт отклоняет их всегда. В `values_json` писать `gate_status ∈ {pass, fail_risk, unavailable}`, счётчики по каждому классу — в манифест.
9. `detector.py:523-524`: `sol_amount_lamports` хранит SOL, а не лампорты (значения 1, 3, 6, 85), `initial_buy_lamports` содержит мусор (0 у 33% строк, 358, 2 115 328 при supply 1e9). Переименовать в `dev_buy_sol` / `initial_buy_raw`, добавить валидацию диапазона `0 < dev_buy_sol ≤ 50`, всё, что вне — `None`.
10. В `manifest.json` / `SUMMARY` добавить воронку одним блоком: `candidates → security_pass → birth_reject → no_live_momentum → momentum_too_late → admits → opened → cash_below_slot`, плюс разбивку `creator_probes` по `identity` (`pump_curve_creator` / `pumpportal_event_actor`), `guard_met`, `fail_risk`, `unavailable`. Сейчас эти цифры собираются только вручную из CSV.

## 3. Два расхождения, которые надо закрыть цифрами (не гипотезами)

- **26 входов против ожидаемых ≈40.** Причина найдена: `pass_if_cap_2.5` = 29 из 236 `entry_state` (12.3%), при cap 4 — 43, при cap 12 — 103. Это потолок правила, не баг. Подтвердить в отчёте прогона №2 строкой `admitted_by_current_cap`.
- **12 монет «только в A».** 8 из 12 отсеяны сбоем телеметрии (−321.48 ₽), 3 — реальным `creator_burst_activity` (−91.16 ₽), 1 — реальным `creator_history_too_short` (−45.18 ₽). После п.8 в отчёте должно быть видно, сколько отсеков пришлось на `unavailable`.

## 4. Приёмка

- 110+ тестов зелёные; новые тесты на границу: `v_sol_birth = 34.49` — вход разрешён, `34.5` — разрешён, `34.51` — `birth_out_of_band`; и на то, что `delta`-полоса не изменилась.
- Прогон 30 минут, режим `PAPER_ONLY`, те же артефакты в Dropbox (`telemetry.sqlite`, `sniper.db`, `exports/`, `run.log`, `manifest.json`, `SUMMARY.md`).
- В `SUMMARY.md` — таблица «прогон №1 vs №2» по входам, PnL, среднему PnL на сделку и числу отсеков `unavailable`.
