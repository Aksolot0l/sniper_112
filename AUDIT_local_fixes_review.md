# Проверка local-fixes.patch: оба бага подтверждены (ветка `f115e44`, до патча)

Ветка на GitHub пока на `f115e44` — патч не запушен, поэтому проверял заявленные баги на текущем коде.
Оба воспроизводятся. Плюс нашёл третий, который маскирует первые два в отчёте.

## Баг 1. Preflight лаунчера — подтверждён

`run_rocket2_ab_paper.sh:159-160`:
```python
ok, _ = PROFILE_A.entry_check(39.88, 40.41)        # PERFORMA delta 0.53
assert ok
```
`entry_check(39.88, 40.41)` → `(False, 'birth_out_of_band')`, значит `AssertionError`.
Прогнал весь preflight-блок скрипта с полным env: падает **ровно на этой строке** (строки 39–40 блока),
всё до неё проходит. Заменил только её на «33.50 → проходим, 39.88 → birth_out_of_band» —
блок печатает `A/B preflight OK`. Одна строка, больше блокеров нет.

Уточнение по охвату: `START_MAC_LINUX.sh` → `run_local_session.py`, там такой проверки нет,
поэтому запуск через START этим багом не блокировался. Ломался только прямой запуск
`run_rocket2_ab_paper.sh`. Патч всё равно нужен: скрипт должен работать.

Мелочь рядом: `portfolio_ab.py:60-64` и `README.md:60` всё ещё описывают PERFORMA/CAVELON как
«ракеты, которые режет старый анти-фильтр». После потолка 34.5 они отсекаются по рождению — текст
надо привести к факту, иначе следующий читатель решит, что эти входы ожидаются.

## Баг 2. `error_detail` список → TEXT — подтверждён, строка теряется целиком

Воспроизведение на текущем коде (collector + реальный writer):
```
row1 (list): вызов record_creator_probe НЕ упал     <- исключение не в вызывающем коде
⚠️ Телеметрия: dropped=0, write_errors=1, writer=ProgrammingError
строк в creator_probes: 1                            <- Mint1 (со списком) потерян
  ('Mint2', 'RuntimeError', 'rpc_http_429 http_status=429')   <- fallback-путь пишет нормально
collector_meta: [('writer_errors', '0')]             <- артефакт врёт: 0 при 1 ошибке
```
Механика: `creator_probe._record_result` передаёт `error_detail=<list[dict]>`; в
`telemetry.record_creator_probe` стоит `if error_detail is None:`, поэтому список уходит прямо
в параметр TEXT-колонки; ошибку ловит `_writer` (`except Exception` на строку) — запись теряется,
счётчик растёт, поток продолжается. Ровно как описано: 4 пробы за 2 минуты = 4 потерянные строки.

Почему это не поймали 125 тестов и мой smoke:
- `test_probe_result_carries_error_detail_and_never_gates_on_it` вызывает `probe.probe()` **без**
  установленного коллектора (`get_collector()` → None), поэтому до БД дело не доходит;
- в `smoke_test.py` проба подставляется заглушкой (`coin.creator_probe_result = _probe_for(...)`),
  `creator_probes` в smoke-БД = **0 строк**, писать нечего.

## Третий баг (мой, рядом). `collector_meta.writer_errors` занижает счётчик

`close()` кладёт `('writer_errors', str(self._write_errors))` в очередь **до** `queue.join()`,
поэтому ошибки, случившиеся при добивании очереди, в артефакт не попадают (в репро: лог 1, БД 0).
Худший случай — всегда 0. Единственное durable-свидетельство потери данных — одна жёлтая строка
в `run.log`. Лечение: писать meta после `queue.join()` и вывести `telemetry_write_errors`
в `manifest.json` / `SUMMARY.md`.

## Чего не хватает в фиксе

1. **Нормализовать на стороне телеметрии, а не только вызывающего.** `record_creator_probe` —
   публичный API; фикс в `creator_probe` оставляет мину под следующий вызов. Минимальная правка:
   ```python
   if not isinstance(error_detail, str):
       error_detail = _summarize_error_detail(
           error_detail if error_detail is not None else values.get('error_detail')
       )
   ```
   Тогда список, словарь и None равно безопасны, а сырой список уже лежит в `values_json`.
2. **Регрессионный тест должен ходить через БД**, а не через `probe.probe()`. Каркас:
   `await collector.start()` → `record_creator_probe(..., error_detail=[{...}])` → `await collector.close()`
   → `SELECT error_detail FROM creator_probes` = одна строка, `count(*) = 1`, `write_errors = 0`.
   Тест без коллектора (как существующий) проходит и при живом баге.
3. **Проверка «нет тихих потерь»**: в тесте же `assert collector._write_errors == 0` — иначе
   следующая несовместимость типов снова уйдёт в лог одной строкой.
