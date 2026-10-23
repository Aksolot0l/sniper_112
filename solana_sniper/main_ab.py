"""Run isolated A/B-only Pump.fun paper portfolios; never signs/sends trades."""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from config import load_settings
from console import Color, log
from creator_probe import CreatorOnchainProbe
from creator_watch import CreatorWatch
from detector import (
    PUMP_PROGRAM_ID,
    detector_worker,
    probe_pumpportal,
    pumpportal_listener,
    select_programs,
)
from portfolio_ab import ARM_IDS, ABPaperSimulator, entry_band_label, profiles_from_settings
from price import (
    ReadOnlyRpcBatcher,
    get_sol_usd,
    reset_rpc_metrics,
    set_rpc_batcher,
)
from storage import storage_worker
from telemetry import CandidateCollector, funnel_from_db, set_collector
from ws_client import ws_listener


RUN_DURATION_SEC_DEFAULT = 3600


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temporary, path)


# Run #7 budget: 6 offsets on 3 695 coins cost 27 724 requests / 3 h = 2.57 rps
# in run #6, over the 1.8 rps target. Rejected coins are now read only at 0/15
# and everyone else at 0/5/15/60, which brings the schedule to ~20 700 / 3 h.
DEFAULT_OBSERVATION_OFFSETS: tuple[int, ...] = (0, 5, 15, 60)


def _observation_offsets() -> tuple[int, ...]:
    raw = os.getenv(
        'TELEMETRY_OBSERVATION_OFFSETS_SEC',
        ','.join(str(value) for value in DEFAULT_OBSERVATION_OFFSETS),
    )
    try:
        values = sorted({int(part.strip()) for part in raw.split(',') if part.strip()})
        values = [value for value in values if value >= 0]
        return tuple(values) if values else DEFAULT_OBSERVATION_OFFSETS
    except (TypeError, ValueError):
        return DEFAULT_OBSERVATION_OFFSETS


def _enrich_enabled() -> bool:
    return os.getenv('ENTRY_ENRICH', '0').strip().lower() in {'1', 'true', 'yes', 'on'}


def _arm_bands() -> dict[str, str]:
    """{arm name: band label} of the arms this process will run."""
    try:
        profiles = profiles_from_settings(load_settings())
    except Exception:
        profiles = profiles_from_settings(object())
    return {name: entry_band_label(profile) for name, profile in profiles.items()}


def _build_commit() -> str:
    """Short code revision this process runs, without shelling out.

    Order: SNIPER_BUILD env -> pack-written BUILD file -> local git HEAD.
    The bundle ships BUILD (no .git), so a run on the user's PC still names
    the exact commit it was built from.
    """
    for env_name in ('SNIPER_BUILD', 'BUILD_COMMIT'):
        value = (os.getenv(env_name) or '').strip()
        if value:
            return value
    here = Path(__file__).resolve()
    build_file = here.with_name('BUILD')
    try:
        if build_file.is_file():
            value = build_file.read_text(encoding='utf-8').strip()
            if value:
                return value
    except OSError:
        pass
    git_dir = here.parent.parent / '.git'
    try:
        head = (git_dir / 'HEAD').read_text(encoding='utf-8').strip()
        if head.startswith('ref:'):
            ref_path = git_dir / head[4:].strip()
            if ref_path.is_file():
                head = ref_path.read_text(encoding='utf-8').strip()
        if head and not head.startswith('ref:'):
            return head[:7]
    except OSError:
        pass
    return 'unknown'


# Reference numbers from run #1 (build 9f2cd08, 30 min, 1800 s). Reported by
# the operator; kept only so SUMMARY.md can put run #2 next to run #1 without
# pretending the two were measured in one process.
BASELINE_RUN_1 = {
    'build': '9f2cd08',
    'duration_sec': 1800,
    'entry_birth_max_sol': 50.0,
    'portfolio_a': {
        'entries': 26, 'closed_trade_count': 26, 'realized_pnl_rub': -1035.56,
        'pnl_per_trade_rub': -39.83, 'conservative_100bps_pnl_rub': -1274.11,
    },
    'portfolio_b': {
        'entries': 16, 'closed_trade_count': 16, 'realized_pnl_rub': -672.77,
        'pnl_per_trade_rub': -42.05, 'conservative_100bps_pnl_rub': -818.87,
    },
    'source': 'run #1 report (30-min PAPER_ONLY run on the operator machine)',
}


def _enrich_host_status(stats: Any) -> str:
    """'ok' | 'enrich_unavailable' | 'no_rows' from the collector counters."""
    data = stats if isinstance(stats, dict) else {}
    rows = int(data.get('rows') or 0)
    ok = int(data.get('ok') or 0)
    if rows <= 0:
        return 'no_rows'
    return 'ok' if ok > 0 else 'enrich_unavailable'


def _money(value: Any) -> str:
    """Rouble cell: two decimals, or an em dash when the number is missing."""
    try:
        if value is None:
            return '—'
        return f'{float(value):.2f}'
    except (TypeError, ValueError):
        return str(value)


def _share(value: Any) -> str:
    """Percent string with two decimals, or an em dash when not measured."""
    try:
        if value is None:
            return 'доля не измерена'
        return f'{100.0 * float(value):.2f} %'
    except (TypeError, ValueError):
        return str(value)


SNAPSHOT_INTERVAL_SEC = 60.0


async def _periodic_snapshots(simulator: Any, run_dir: Path) -> None:
    """Write ticks / summary / wallets every SNAPSHOT_INTERVAL_SEC (P0-5)."""
    while True:
        try:
            await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        try:
            simulator.snapshot_artifacts(reason='periodic')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f'⚠️ Снапшот артефактов не записан ({type(exc).__name__})', Color.YELLOW)


def _summary_markdown(
    manifest: dict[str, Any], funnel: dict[str, Any], summary: dict[str, Any],
) -> str:
    """Render the one-page post-run report next to the raw artifacts."""
    lines: list[str] = []
    add = lines.append
    ab = summary.get('ab_test') or {}
    arms = [name for name, book in summary.items()
            if isinstance(book, dict) and book.get('stats') is not None]
    profiles = (summary.get('profiles') or {})
    manifest_strategies = (manifest.get('strategies') or {})

    def _profile(name: str) -> dict:
        return (profiles.get(name) or manifest_strategies.get(name) or {})

    def _arm_name(name: str) -> str:
        return str(_profile(name).get('arm_id') or name)

    add(f"# Прогон {manifest.get('run_id')}")
    add('')
    add(f"- build: `{manifest.get('build')}`")
    add(f"- длительность: {manifest.get('actual_duration_sec')} c "
        f"(план {manifest.get('planned_duration_sec')} c)")
    add(f"- paper-only: {manifest.get('paper_only')}; подписано транзакций: "
        f"{manifest.get('signed_transactions')}")
    add(f"- наблюдения, с: {manifest.get('observations_sec')}")
    stats = manifest.get('telemetry') or {}
    if stats:
        add(f"- телеметрия: dropped={stats.get('dropped_events')}, "
            f"write_errors={stats.get('write_errors')}, "
            f"writer={stats.get('writer_error') or 'ok'}")
        if stats.get('write_errors'):
            add('')
            add('> ⚠️ Часть строк телеметрии не записалась: воронка ниже '
                'построена по усечённым данным.')
    if manifest.get('early_stop_reason'):
        add(f"- досрочная остановка: `{manifest.get('early_stop_reason')}` "
            '(stop-list, не PnL; артефакты дописаны)')
    add('')

    # --- the arms and their single difference -----------------------------
    add('## Армы: каждая отличается от A одной границей')
    add('')
    add('| арм | полоса входа (v_sol_birth; delta) | что изменено против A |')
    add('|---|---|---|')
    for name in arms:
        band = _profile(name).get('entry_band_label') or '—'
        changed = ', '.join(
            ((ab.get('arms') or {}).get(name) or {}).get('single_boundary_vs_a') or []
        ) or 'эталон'
        if name == (ab.get('reference_arm') or 'portfolio_a'):
            changed = 'эталон'
        add(f'| {_arm_name(name)} (`{name}`) | {band} | {changed} |')
    add('')
    add('Выход у всех арм одинаковый — лестница Round 6 (SL −10 % до TP1/ракеты, '
        'TP1 +30 % = 50 %, rocket bypass +50 %, трейл 0.85× после +10 %, shelf 4.5 с, '
        'no-progress 5 с, таймауты 35/90 с). Ставка 500 ₽, банк 1 500 ₽, 3 слота, '
        'момент входа и частота опроса — тоже одинаковые. Последний прогон показал, '
        'что выход по времени неотличим от лестницы (парная разница −4.24 ₽, '
        'CI [−19.2; +10.7]), поэтому выход закрыт и здесь не варьируется.')
    add('')

    # --- main table --------------------------------------------------------
    add('## Результат по армам')
    add('')
    add('| арм | n | сумма ₽ | ₽/сделку | медиана | sd | CI95 среднего | '
        'без топ-2 | парная разница с A | CI98.75 парной | W/L/T |')
    add('|---|---:|---:|---:|---:|---:|---|---:|---:|---|---|')
    for name in arms:
        book = summary.get(name) or {}
        st = book.get('stats') or {}
        ci = st.get('ci95_mean')
        ci_text = f'[{ci[0]:.2f}, {ci[1]:.2f}]' if ci else '—'
        paired = book.get('paired_vs_a') or {}
        p_ci = paired.get('ci95_diff')
        p_ci_text = f'[{p_ci[0]:.2f}, {p_ci[1]:.2f}]' if p_ci else '—'
        add(f"| {_arm_name(name)} (`{name}`) | {st.get('n')} | {_money(st.get('sum'))} | "
            f"{_money(st.get('mean'))} | {_money(st.get('median'))} | "
            f"{_money(st.get('sd'))} | {ci_text} | {_money(st.get('sum_without_top2'))} | "
            f"{('эталон' if paired.get('reference') else _money(paired.get('mean_diff')))} | "
            f"{('—' if paired.get('reference') else p_ci_text)} | "
            f"{('—' if paired.get('reference') else str(paired.get('wins', 0)) + '/' + str(paired.get('losses', 0)) + '/' + str(paired.get('ties', 0)))} |")
    add('')
    add('`парная разница с A` — среднее по монетам, которые торговали ОБЕ арма: '
        'правильный тест для схемы «одна разница». Два независимых средних на '
        'пересекающихся монетах сравнивать нельзя.')
    add('')

    # --- P0 acceptance -----------------------------------------------------
    rpc = (manifest.get('rpc_metrics') or {})
    telemetry = (manifest.get('telemetry') or {})
    p0_checks = (
        ('early_stop_reason = null', not manifest.get('early_stop_reason'),
         manifest.get('early_stop_reason') or 'null'),
        ('неисправленные ошибки RPC ≤ 2 %',
         float(rpc.get('unrecovered_rate') or 0.0) <= 0.02,
         f"{100.0 * float(rpc.get('unrecovered_rate') or 0.0):.2f} % "
         f"({rpc.get('unrecovered_failures', 0)} из {rpc.get('requests', 0)})"),
        ('dropped_events = 0', int(telemetry.get('dropped_events') or 0) == 0,
         int(telemetry.get('dropped_events') or 0)),
        ('write_errors = 0', int(telemetry.get('write_errors') or 0) == 0,
         int(telemetry.get('write_errors') or 0)),
        ('requests_per_sec ≤ 1.8', float(rpc.get('requests_per_sec') or 0.0) <= 1.8,
         _money(rpc.get('requests_per_sec'))),
        ('security_fail_skipped_deep > 0 (P0-2 работает)',
         int(funnel.get('security_fail_skipped_deep') or 0) > 0,
         int(funnel.get('security_fail_skipped_deep') or 0)),
        ('теней в отчёте нет',
         not any(str(name).startswith('shadow') for name in summary),
         ', '.join(name for name in summary if str(name).startswith('shadow')) or 'нет'),
        ('enrich.csv пишется',
         bool((manifest.get('enrich') or {}).get('enabled'))
         and (manifest.get('enrich') or {}).get('status') == 'written',
         (manifest.get('enrich') or {}).get('status') or 'выключен'),
    )
    add('### P0: принят ли прогон')
    add('')
    add('| проверка | факт | статус |')
    add('|---|---|---|')
    for label, ok, value in p0_checks:
        add(f"| {label} | {value} | {'✅' if ok else '❌'} |")
    add('')
    add(f"- 429 всего: {rpc.get('http_429_count', 0)}, ретраев: "
        f"{rpc.get('retry_count', 0)}, восстановлено: {rpc.get('recovered_failures', 0)} "
        '(восстановленный 429 — это back-pressure, а не сбой)')
    per_profile = rpc.get('requests_per_profile')
    add(f"- запросы по армам: {per_profile if per_profile else 'не разложены'}")
    enrich = manifest.get('enrich') or {}
    if enrich:
        add(f"- `enrich.csv`: {enrich.get('rows', 0)} строк, "
            f"ok={enrich.get('ok', 0)}, unavailable={enrich.get('unavailable', 0)}, "
            f"host={enrich.get('host_status') or 'н/д'}")
    add('')

    # --- criteria, written before the start --------------------------------
    add('## Критерии решения (записаны ДО запуска)')
    add('')
    add('1. **P0 принят**, если все строки таблицы выше — ✅.')
    add('2. **Гипотеза арма подтверждена**, если одновременно: '
        '`paired mean(arm − A) ≥ +8 ₽`, CI98.75 не содержит нуля и знак совпал '
        'в первой и второй половине прогона.')
    add('3. **Множественность:** гипотез три → Bonferroni, α = 0.0125 '
        '(интервал 98.75 %).')
    add('4. CI накрывает ноль, но знак стабилен в обеих половинах → писать '
        '«разведочно, нужен повтор», живой конфиг не менять, победу не объявлять.')
    add('5. Все три арма в пределах ±5 ₽ → подбор полос исчерпан; дальше решает '
        'только новый признак (`enrich.csv`) или смена рынка, а не пороги.')
    add('6. **Стоп-лист не поднимается** (2 %), RPS не увеличивается, чтобы '
        '«дожить до конца».')
    add('')
    add('Ожидания по армам, зафиксированные до старта:')
    add('')
    expectations = ab.get('expectation') or {}
    for name in arms:
        if name == 'portfolio_a':
            continue
        add(f"- **{_arm_name(name)}** (`{name}`): {expectations.get(name, '—')}")
    add('')
    add(f"> ⚠️ {ab.get('power_note') or ''}")
    add('')
    add('> ⚠️ Любое решение по разнице меньше ~15 ₽/сделку на 3 часах невозможно; '
        'на 8 часах шумовой пол ≈ ±10 ₽. Ни одно число здесь не означает '
        '«стало прибыльно»: у A в прошлом прогоне медиана сделки −15.20 ₽, а весь '
        'плюс держался на двух памп-хвостах.')
    add('')

    # --- hourly blocks and stability ---------------------------------------
    add('### Почасовые блоки (шумовой пол при неизменных правилах)')
    add('')
    header_arms = ' | '.join(_arm_name(name) for name in arms)
    add(f'| час | {header_arms} |')
    add('|---|' + '---:|' * len(arms))
    hours: list[str] = []
    for name in arms:
        hours.extend(((summary.get(name) or {}).get('blocks') or {}).keys())
    for hour in sorted(set(hours), key=lambda h: int(h.lstrip('h') or 0)):
        cells = []
        for name in arms:
            slot = ((summary.get(name) or {}).get('blocks') or {}).get(hour) or {}
            cells.append(
                f"{slot.get('n', 0)} / {_money(slot.get('mean'))}" if slot else '—'
            )
        add(f'| {hour} | ' + ' | '.join(cells) + ' |')
    add('')
    add('### Стабильность знака: первая и вторая половина')
    add('')
    add('| арм | 1-я половина, ₽ | 2-я половина, ₽ | знак совпал |')
    add('|---|---:|---:|---|')
    for name in arms:
        if name == 'portfolio_a':
            continue
        stability = (summary.get(name) or {}).get('stability') or {}
        first = (stability.get('first_half') or {}).get('mean_diff')
        second = (stability.get('second_half') or {}).get('mean_diff')
        add(f'| {_arm_name(name)} | {_money(first)} | {_money(second)} | '
            f"{'да' if stability.get('sign_stable') else 'нет'} |")
    add('')

    # --- rocket accounting --------------------------------------------------
    add('### Учёт ракет: сколько результата держится на пиках ≥ +50 %')
    add('')
    add('| арм | n | ракет | доля ракет | ₽/ракету | ₽/остальные | '
        'точка безубыточности |')
    add('|---|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        rocket = (summary.get(name) or {}).get('rocket_accounting') or {}
        share = rocket.get('rocket_share')
        be = rocket.get('break_even_rocket_share')
        add(f"| {_arm_name(name)} | {rocket.get('n', 0)} | {rocket.get('rockets', 0)} | "
            f"{_share(share)} | {_money(rocket.get('rockets_mean_rub'))} | "
            f"{_money(rocket.get('others_mean_rub'))} | "
            f"{_share(be) if be is not None else '—'} |")
    add('')
    add('`точка безубыточности p* = −mean(остальных) / (mean(ракет) − mean(остальных))`: '
        'какая доля сделок должна быть ракетами, чтобы арм вышел в ноль. Сравнивать '
        'с фактической долей в колонке слева.')
    add('')

    # --- exit reasons -------------------------------------------------------
    add('### Что закрывало сделки (`exit_reason × арм`)')
    add('')
    add('| арм | причина | n | сумма ₽ | ₽/сделку | медиана удержания, с |')
    add('|---|---|---:|---:|---:|---:|')
    for name in arms:
        breakdown = (summary.get(name) or {}).get('exit_breakdown') or {}
        if not breakdown:
            add(f'| {_arm_name(name)} | — | 0 | — | — | — |')
        for kind, slot in breakdown.items():
            add(f"| {_arm_name(name)} | {kind} | {slot.get('n')} | "
                f"{_money(slot.get('sum_rub'))} | {_money(slot.get('mean_rub'))} | "
                f"{_money(slot.get('median_hold_sec'))} |")
    add('')

    # --- fly-through of the SL rule (run #7 §1.4) ---------------------------
    tick_quality = summary.get('tick_quality') or {}
    sl = tick_quality.get('stop_loss_fills') or {}
    if sl.get('n'):
        worse = int(tick_quality.get('stop_loss_worse_than_minus15') or 0)
        add('### Пролёт стоп-лосса: по какой цене сделка реально выходила')
        add('')
        add(f"- SL стоит на −10 %, фактический филл: медиана **{_money(sl.get('median'))} %**, "
            f"среднее {_money(sl.get('mean'))} %, худший {_money(sl.get('worst'))} %")
        add(f"- хуже −15 %: {worse} из {sl.get('n')} "
            f"({_share(worse / sl['n'] if sl.get('n') else None)})")
        gaps = tick_quality.get('mark_gap_ms') or {}
        if gaps.get('median') is not None:
            add(f"- шаг между метками: медиана {_money(gaps.get('median'))} мс, "
                f"худший {_money(gaps.get('worst'))} мс — это и есть причина пролёта, "
                'а не задержка решения')
        add('')
        add('`trigger_pct == fill_pct` в прошлом прогоне был структурным нулём: '
            'сработавшая метка и была филлом. Здесь филл сравнивается с '
            'предыдущей меткой, поэтому пролёт виден числом.')
        add('')

    # --- bank ---------------------------------------------------------------
    add('### Банк 1 500 ₽: пополнения и момент смерти без них')
    add('')
    add('| арм | пополнений | влито, ₽ | PnL сделок, ₽ | equity без пополнений | '
        'банк умер бы на сделке |')
    add('|---|---:|---:|---:|---:|---|')
    for name in arms:
        book = summary.get(name) or {}
        bust = book.get('bust_at_trade')
        add(f"| {_arm_name(name)} | {book.get('top_ups', 0)} | "
            f"{_money(book.get('capital_injected_rub'))} | "
            f"{_money(book.get('pnl_with_topups'))} | "
            f"{_money(book.get('equity_no_topups'))} | "
            f"{('не умирал' if bust is None else '№' + str(bust))} |")
    add('')
    add('Пополнение — только бухгалтерия: ставка, слоты и все входы/выходы не '
        'меняются; пополнение срабатывает лишь когда cash < 500 ₽ перед входом.')
    add('')

    # --- blind spot ---------------------------------------------------------
    add('### Слепая зона security-фильтра (P0-2)')
    add('')
    add(f"- отброшено security: {funnel.get('security_fail_total')}, "
        f"из них с ценой: {funnel.get('security_fail_observed')} "
        f"({_share(funnel.get('security_fail_observed_share'))})")
    add(f"- из наблюдаемых выросли ≥ +50 % за 15 с: "
        f"{funnel.get('security_fail_rockets')} "
        f"({_share(funnel.get('security_fail_rocket_share'))})")
    add(f"- причины отказа: {funnel.get('security_fail_reasons')}")
    add(f"- чтения по этим монетам: только offset 0 и 15, "
        f"`skipped_security_deep` = {funnel.get('security_fail_skipped_deep', 0)}")
    add('')

    # --- funnel -------------------------------------------------------------
    add('## Воронка')
    add('')
    add('```')
    add(str(funnel.get('chain') or 'n/a'))
    add('')
    add(f"entry_state             {funnel.get('entry_state')}")
    for arm in arms:
        for reason, count in ((funnel.get(f'{arm}_signal')) or {}).items():
            add(f"  {_arm_name(arm)} signal: {reason:<48} {count}")
    add(f"opened                  " + ', '.join(
        f"{_arm_name(arm)}={((funnel.get('opened') or {}).get(arm))}" for arm in arms
    ))
    for arm in arms:
        for reason, count in ((funnel.get('entry_skips') or {}).get(arm) or {}).items():
            add(f"  {_arm_name(arm)} skip: {reason:<48} {count}")
    add('```')
    add('')
    add(f"Бакеты delta: {funnel.get('delta_buckets')}")
    add('')

    add('## Как читать')
    add('')
    add('1. Армы сравниваются только парно и только по монетам, которые торговали '
        'оба; сырые средние двух выборок на пересекающихся монетах — не тест.')
    add('2. Почасовая таблица показывает шум: если разница между армами меньше '
        'разброса между часами, это шум, а не эффект.')
    add('3. PnL не доказывает эджа. Решение принимается только по критерию выше; '
        'отрицательный знак у всех арм ожидаем и не является сюрпризом.')
    add('')
    return '\n'.join(lines)


def _strategy_metadata() -> dict[str, Any]:
    """Declared up front so a run can never be compared against an older one."""
    return {
        'build': _build_commit(),
        'build_baseline': (
            'run #6 = 4ba22b9 (3 h, exit experiment): A +248.61 RUB / +1.49 '
            'per trade, B -375.35 / -2.27, paired B-A -4.24, CI [-19.2, +10.7]'
        ),
        'strategy_version': 'run7: entry_v3+birth_max_34.5, four entry-band arms',
        # --- run #7: one boundary per arm, one exit ladder for all ----------
        'run7_single_difference': 'entry_band',
        'entry_latency_ms': 150.0,
        'entry_fill_price': (
            'the first on-chain curve read after detection (the same '
            'getMultipleAccounts context that passed the security filters). '
            'The WSS packet price is never used for a fill; entry_lag_ms '
            'records how old the read was and entry_premium_pct compares the '
            'entry mid with the coin t0 mid of that read'
        ),
        'exit_rule': (
            'identical in every arm — the Round 6 4-mode ladder: SL -10% '
            'before TP1/rocket, TP1 +30% sells 50%, rocket bypass +50%, trail '
            '0.85x after +10%, shelf 4.5 s, no-progress 5 s, timeouts 35/90 s'
        ),
        'arms': {
            'portfolio_a': {'arm_id': 'A', 'band': '30.5-34.5 / 0.10-2.50',
                            'role': 'reference, unchanged in every symbol'},
            'portfolio_b1_narrow': {'arm_id': 'B1', 'band': '30.5-34.5 / 0.10-1.00',
                                    'changed': 'delta_max 2.50 -> 1.00',
                                    'why': 'the only band with non-negative EV at '
                                           '5-10 s in all three runs (+0.47/+1.42/'
                                           '+1.82 %), and better on the real ladder '
                                           'in run3/run5 (-11.1/-7.2 vs -23.7/-20.1); '
                                           'risk: three rockets carried the 1-2.5 '
                                           'band in run6'},
            'portfolio_b2_birth': {'arm_id': 'B2', 'band': '30.5-31.0 / 0.10-2.50',
                                   'changed': 'v_sol_birth_max 34.50 -> 31.00',
                                   'why': 'the only filter with a stable sign 4/4 on '
                                          'a fixed exit, and identical to dev_buy <= '
                                          '1 SOL; risk: on the run6 ladder it lost '
                                          '(-507 RUB vs +755 RUB for birth > 31)'},
            'portfolio_b3_t0': {'arm_id': 'B3', 'band': '30.5-34.5 / 0.00-2.50',
                                'changed': 'delta_min 0.10 -> 0.00',
                                'why': 'the only arm whose expected gain is '
                                       'arithmetic: it enters on the first curve '
                                       'read (A waits for a 0.10 SOL impulse) and '
                                       'so does not pay the measured 1.416 % '
                                       'entry_fill_premium (~7 RUB on 500 RUB); it '
                                       'also sees 438 coins instead of 167 inside '
                                       'the same birth band, with the same 6.6 % '
                                       'rocket share (60 s window). Risk: entering '
                                       'without an impulse confirmation'},
        },
        'run7_expectation': {
            'portfolio_b1_narrow': '+1..+6 RUB/trade vs A (better in 3/3 runs on a '
                                   'fixed exit and in run3/run5 on the ladder; the '
                                   'narrow band cuts the flow 1.8x)',
            'portfolio_b2_birth': '-8..+8 RUB (model says better in 2/3 runs, the '
                                  'run6 ladder lost 15.7 RUB/trade: the run decides)',
            'portfolio_b3_t0': '+3..+9 RUB from removing the 1.416 % entry premium '
                                '(~7 RUB on 500 RUB); EV itself is neutral',
        },
        'run7_decision_rule': (
            'an arm confirms its hypothesis iff the PAIRED mean(arm - A) >= +8 RUB '
            'on the coins both arms traded, the 98.75 % bootstrap CI (Bonferroni '
            'over three hypotheses, alpha 0.0125) excludes 0, and the sign is the '
            'same in both halves of the run'
        ),
        'run7_power': (
            'paired sd ~98 RUB, ~56 trades/hour per arm: the CI half width is '
            '~15 RUB at ~165 pairs, so a 3-hour run cannot resolve anything below '
            '~15 RUB/trade, and the +8 RUB threshold needs ~1 700 pairs after '
            'Bonferroni, i.e. ~21 h at this flow'
        ),
        'run7_p0_accepted': (
            'early_stop_reason = null, unrecovered RPC failures <= 2 %, '
            'dropped_events = 0, write_errors = 0, requests_per_sec <= 1.8, '
            'security_fail_skipped_deep > 0, no shadow profiles, enrich.csv written'
        ),
        'run7_p0_fixes': (
            'P0-2 verdict resolved inside the offset loop (run6 read every offset '
            'because the verdict was set after observe_candidate); ticks carry '
            'price_usd instead of a rouble price divided by sol_usd; '
            'security_fail_reasons use the LAST reason (the verdict, not the '
            'passed check); ticks gain mark_prev_pct/step_gap_pp/mark_gap_ms; '
            'observation offsets 0/5/15/60 to fit the RPC budget'
        ),
        'run7_not_doing': (
            'no exit change in any arm, no delta > 2.5 arm (EV@15 -9.25/-5.84/-5.54 % '
            'in three runs and no extra rockets inside A\'s birth band), no creator '
            'gate live (worse than average in 4/4 samples), no fourth experimental '
            'arm, no band tuning mid-run'
        ),
        'bank_rule': (
            'каждый профиль ведёт свой кэш; если перед входом cash < 500 ₽, '
            'книга пополняется до 1500 ₽ и сделка открывается. Ставка 500 ₽, '
            '3 слота и все входы/выходы не меняются — пополнение влияет только '
            'на «торговать / не торговать». Счётчик top_ups остаётся метрикой'
        ),
        'stop_list': (
            'прогон останавливается только по четырём причинам: telemetry '
            'dropped_events/write_errors > 0; доля НЕИСПРАВЛЕННЫХ ошибок RPC '
            '(после 3 ретраев 0.2/0.5/1.2 c + джиттер, с учётом Retry-After) '
            '> 2% за 10 мин или p95 > 1000 мс пять минут подряд; профиль без '
            'входов 30 мин при растущем entry_state; остаток квоты Helius < 10% '
            'или < 2 ГБ на диске. Порог 2% поднимать запрещено. PnL не является '
            'причиной остановки'
        ),
        'profiles': {
            name: {
                'arm_id': meta['arm_id'],
                'entry_band': meta['band'],
                'changed_vs_a': meta.get('changed', 'none'),
                'exit_rule': '4-mode ladder (identical in every arm)',
            }
            for name, meta in {
                'portfolio_a': {'arm_id': 'A', 'band': '30.5-34.5 / 0.10-2.50'},
                'portfolio_b1_narrow': {'arm_id': 'B1', 'band': '30.5-34.5 / 0.10-1.00',
                                        'changed': 'delta_max_sol 2.50 -> 1.00'},
                'portfolio_b2_birth': {'arm_id': 'B2', 'band': '30.5-31.0 / 0.10-2.50',
                                       'changed': 'v_sol_birth_max 34.50 -> 31.00'},
                'portfolio_b3_t0': {'arm_id': 'B3', 'band': '30.5-34.5 / 0.00-2.50',
                                    'changed': 'delta_min_sol 0.10 -> 0.00'},
            }.items()
        },
    }


def _safe_manifest(settings: Any, run_id: str, run_dir: Path) -> dict[str, Any]:
    strategy_meta = _strategy_metadata()
    common = {
        'initial_balance_rub': 1500,
        'position_size_rub': 500,
        'max_slots': 3,
        'entry_note': (
            'live-momentum band replaces the 2026-10 anti-filter '
            '(v_sol_now <= v_sol_birth + 0.8 rejected every real runner '
            'and bought dead delta==0 decoys). Birth ceiling is 34.5 since '
            'fix #7: v_sol_birth = 29.96 + dev_buy_SOL, so 50 admitted dev '
            'buys of 4-6 SOL and 36 of 50 such launches fell <= -25%'
        ),
        'stop_loss': 'before TP1/rocket bypass only, while best_pnl_pct < +10.0: pnl_pct <= -10.0; sell 100%',
        'tp1': 'when +30.0 <= pnl_pct < +50.0 before TP1/rocket bypass: sell 50%; retry same partial if exact-size quote is unavailable',
        'rocket_bypass': 'at pnl_pct >= +50.0 before TP1: retain 100%, set bypass, do not sell half',
        'trailing': (
            'once best_pnl_pct >= +10.0: sell remaining at '
            'pnl_pct <= 0.85 * best_pnl_pct (labelled SL/trail, not a take-profit)'
        ),
        'shelf_stagnation': 'sell remaining when pnl_pct >= +1.5 and no >=+0.4pp new high for 4.5s',
        'no_progress': 'once at held >= 5s: best_pnl_pct < +4.0 and pnl_pct < +1.5; evaluate once',
        'peak_timestamp_update': 'update only when pnl_pct >= best_pnl_pct + 0.4 percentage points',
        'timeout_before_trailing_sec': 35,
        'timeout_after_trailing_sec': 90,
    }
    return {
        'run_id': run_id,
        **strategy_meta,
        # Acceptance #2: the latency model and the declared difference must be
        # readable from the manifest alone, without opening a trade row.
        'entry_latency_ms': 150.0,
        'ab_test': {
            # Run #7: every experimental arm moves exactly one entry boundary.
            'single_difference': 'entry_band',
            'reference_arm': 'portfolio_a',
            'entry_latency_ms': 150.0,
            'exit_rule': strategy_meta['exit_rule'],
            'arms': strategy_meta['arms'],
            'decision_rule': strategy_meta['run7_decision_rule'],
            'expectation': strategy_meta['run7_expectation'],
            'power_note': strategy_meta['run7_power'],
        },
        'status': 'starting',
        'early_stop_reason': None,
        'created_at_utc': _utc_now(),
        'planned_duration_sec': int(os.getenv('RUN_DURATION_SEC', RUN_DURATION_SEC_DEFAULT)),
        'paper_only': True,
        'signed_transactions': 0,
        'submitted_transactions': 0,
        'portfolio_scope': list(_arm_bands()),
        'enrich': {'enabled': _enrich_enabled(), 'path': str(run_dir / 'exports' / 'enrich.csv')},
        'strategies': {
            name: {
                **common, **strategy_meta,
                **strategy_meta['profiles'][name],
                'entry_band_label': band,
                'creator_probe': 'telemetry only; no entry gate in any arm',
                'creator_gate': False,
            }
            for name, band in _arm_bands().items()
        },
        'rpc': {
            'provider': 'Helius',
            'http_host': urlsplit(settings.http_rpc_url).hostname,
            'wss_host': urlsplit(settings.wss_url).hostname,
            'methods': ['getMultipleAccounts', 'getSignaturesForAddress', 'getTokenAccountsByOwner'],
            'read_only': True,
            'credentials_stored': False,
            'http_shadows_enabled': False,
            'candidate_entry_batch_accounts': ['mint', 'curve', 'PUMP_GLOBAL_PDA', 'PUMP_FEE_CONFIG_PDA'],
            'candidate_entry_batch_commitment': 'processed',
            'null_retry': 'one retry after 120 ms',
            'latency_policy': 'measure actual latency/errors; no SLA or coverage promise',
        },
        'fees': {
            'source': 'on-chain Pump Global + FeeConfig; quote fills use SDK integer math',
            'entry_network_fee_sol_model': settings.entry_fee_sol,
            'exit_network_fee_sol_model_per_sell': settings.exit_fee_sol,
            'network_fees_are_simulation_assumptions': True,
            'on_chain_curve_fees': 'included in out_amount_raw; reported separately from network fees',
            'unsupported_or_unknown': 'fail closed',
        },
        'filters': {
            'sources': 'pumpfun', 'standard_supply': True, 'is_mayhem': False,
            'quote_mint': 'SOL/WSOL only', 'mint_authority_revoked': True,
            'freeze_authority_revoked': True, 'duplicate_symbol_gate': False,
            'creator_history_policy': 'advisory telemetry/warnings only',
            'creator_gate_in_live_arms': False,
            'creator_gate_note': (
                'the creator gate lost against the average in 4/4 independent '
                'samples (run3 -6.29 %, run5 -4.77 %, run6 both halves -4.01 / '
                '-6.44 % vs -2.99/-2.86/-4.11/-4.51) and is not used anywhere'
            ),
        },
        'observations_sec': list(_observation_offsets()),
        'observation_policy': 'scheduled real Pump curve RPC reads; no interpolation or DexScreener/GeckoTerminal fallback for Pump.fun',
        'accounting': {
            'snapshot_fill': 'out_amount_raw',
            'conservative_shadow': 'min_out_raw at QUOTE_SLIPPAGE_BPS=100',
            'quote_slippage_bps': 100,
            'gross_curve_pnl_rub': 'realized snapshot PnL before modeled network fees; on-chain curve fee remains embedded in quote output',
            'network_fees': 'tracked separately; deducted from realized returns and shown as open fee liability',
            'usd_rub_rate': settings.usd_rub_rate,
        },
        'wallet_state_files': [
            str(run_dir / f'wallet_{name}.json') for name in _arm_bands()
        ],
        'data_files': {
            'sniper_db': str(run_dir / 'sniper.db'),
            'telemetry_db': str(run_dir / 'telemetry.sqlite'),
            'exports': str(run_dir / 'exports'),
        },
        'api_key_or_rpc_url_with_query': False,
    }


async def main() -> None:
    settings = load_settings()
    loop = asyncio.get_running_loop()
    external_stop = asyncio.Event()
    installed_signals: list[signal.Signals] = []
    try:
        loop.add_signal_handler(signal.SIGTERM, external_stop.set)
        installed_signals.append(signal.SIGTERM)
    except (NotImplementedError, RuntimeError, ValueError):
        pass
    run_id = os.getenv('RUN_ID', '').strip()
    run_dir_raw = os.getenv('RUN_DIR', '').strip()
    if not run_id or not run_dir_raw or not re.fullmatch(r'[A-Za-z0-9._-]+', run_id):
        raise SystemExit('RUN_ID and RUN_DIR are required and RUN_ID must be path-safe.')
    project_root = Path(__file__).resolve().parent
    runs_root = (project_root / 'runs').resolve()
    run_dir = Path(run_dir_raw).resolve()
    if run_dir.parent != runs_root or run_dir.name != f'rocket2-ab-paper-{run_id}':
        raise SystemExit('RUN_DIR must be the unique project runs/rocket2-ab-paper-<RUN_ID> archive.')
    if not run_dir.is_dir():
        raise SystemExit('RUN_DIR must already exist and be unique for this run.')
    if (run_dir / 'manifest.json').exists():
        raise SystemExit('RUN_DIR already has a manifest; refusing to overwrite an archive.')
    for existing in run_dir.iterdir():
        if existing.name == 'exports' and existing.is_dir() and not any(existing.iterdir()):
            continue
        # Files written by our own local runner before/while the bot starts.
        if existing.name in {
            'preflight_tests.log', 'run.log', 'supervisor_heartbeat.json',
            'supervisor_heartbeat.tmp',
        } and existing.is_file():
            continue
        raise SystemExit('RUN_DIR contains unexpected preexisting files; refusing to overwrite an archive.')
    if not settings.api_key:
        raise SystemExit('HELIUS_API_KEY is not present in the process environment; no RPC/run started.')
    rpc_host = (urlsplit(settings.http_rpc_url).hostname or '').lower()
    if 'helius' not in rpc_host:
        raise SystemExit('The configured HTTP RPC host is not Helius; no RPC/run started.')
    if settings.sources.strip().lower() != 'pumpfun':
        raise SystemExit('SOURCES must be exactly pumpfun for the isolated A/B run.')
    if settings.deposit_rub != 1500 or settings.max_positions != 3 or settings.max_trade_rub != 500:
        raise SystemExit('A/B wallet sizing must be 1,500 ₽ with 3 slots × 500 ₽.')
    if settings.honeypot_check:
        raise SystemExit('HTTP shadow/honeypot quote checks must be disabled for this run.')
    if os.getenv('PAPER_ONLY', '1').strip() != '1':
        raise SystemExit('PAPER_ONLY must remain enabled.')
    # Network fee model must be the real Solana cost. config.py defaults are
    # 0.001 / 0.0001 SOL for legacy callers, which would inflate fees 12.5x on a
    # direct `python main_ab.py`; refuse to run with anything but the paper model.
    if (
        not math.isclose(settings.entry_fee_sol, 0.00008, rel_tol=0, abs_tol=1e-12)
        or not math.isclose(settings.exit_fee_sol, 0.00005, rel_tol=0, abs_tol=1e-12)
    ):
        raise RuntimeError(
            'network fee model must be 0.00008 / 0.00005 SOL '
            f'(got {settings.entry_fee_sol} / {settings.exit_fee_sol}); '
            'use run_rocket2_ab_paper.sh or run_local_session.py'
        )

    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = _safe_manifest(settings, run_id, run_dir)
    _write_json(run_dir / 'manifest.json', manifest)
    reset_rpc_metrics()

    creator_history_raw = os.getenv('CREATOR_HISTORY_DIR', str(project_root / 'runs'))
    creator_history_dir = Path(creator_history_raw)
    if not creator_history_dir.is_absolute():
        creator_history_dir = project_root / creator_history_dir
    blocklist_raw = os.getenv('CREATOR_BLOCKLIST_PATH', str(creator_history_dir / 'known_scam_sellers.txt'))
    blocklist_path = Path(blocklist_raw)
    if not blocklist_path.is_absolute():
        blocklist_path = project_root / blocklist_path
    creator_watch = CreatorWatch(creator_history_dir, blocklist_path)
    try:
        history = await asyncio.to_thread(creator_watch.load_history)
        log(
            '👤 Creator history loaded: '
            f"wallets={history.get('creator_wallets', 0)}, "
            f"repeats={history.get('repeat_creator_wallets', 0)}, "
            f"files={history.get('files_scanned', 0)}; telemetry/warn-only.",
            Color.CYAN,
        )
    except Exception as exc:
        log(f'⚠️ Creator history unavailable ({type(exc).__name__}); probes remain non-blocking.', Color.YELLOW)

    telemetry_path = Path(os.getenv('TELEMETRY_DB_PATH', str(run_dir / 'telemetry.sqlite')))
    export_path = Path(os.getenv('TELEMETRY_EXPORT_DIR', str(run_dir / 'exports')))
    collector = CandidateCollector(
        str(telemetry_path), str(export_path),
        shadow_notional_rub=0.0,
        observation_offsets_sec=_observation_offsets(),
        creator_watch=creator_watch,
        enable_shadow_quotes=False,
    )
    await collector.start()
    set_collector(collector)
    collector.record_meta('run_id', run_id)
    collector.record_meta('portfolio_scope', ['portfolio_a', 'portfolio_b'])
    collector.record_meta('shadow_quotes_enabled', False)
    collector.record_meta('paper_only', True)
    collector.record_meta('rpc_host', rpc_host)

    pump_fast = False
    programs = select_programs(settings.sources)
    if settings.pump_source == 'pumpportal':
        pump_fast = await probe_pumpportal()
        if pump_fast:
            programs = {pid: name for pid, name in programs.items() if pid != PUMP_PROGRAM_ID}
        else:
            log('⚠️ PumpPortal unavailable; falling back to Helius Pump.fun program logs (read-only).', Color.YELLOW)

    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    trades_queue: asyncio.Queue = asyncio.Queue()
    coins_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    watch_stop = asyncio.Event()
    candidate_tasks: set[asyncio.Task] = set()
    storage_task: asyncio.Task | None = None
    producers: list[asyncio.Task] = []
    candidate_worker_task: asyncio.Task | None = None
    creator_probe: CreatorOnchainProbe | None = None
    simulator: ABPaperSimulator | None = None
    batcher: ReadOnlyRpcBatcher | None = None
    run_error: Optional[str] = None
    telemetry_stats: dict[str, Any] = {}
    external_stop_reason: Optional[str] = None
    actual_start_epoch = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        batcher = ReadOnlyRpcBatcher(
            session, settings.http_rpc_url,
            flush_window_sec=float(os.getenv('RPC_BATCH_WINDOW_SEC', '0.008')),
        )
        batcher.start()
        set_rpc_batcher(batcher)
        try:
            sol_usd = await asyncio.wait_for(get_sol_usd(session), timeout=12.5)
            if sol_usd and sol_usd > 0:
                log(f'📈 SOL/USD conversion ready (${sol_usd:.2f}); USD/RUB={settings.usd_rub_rate:.2f}.', Color.DIM)
            else:
                log('⚠️ SOL/USD unavailable; entries remain fail-closed.', Color.YELLOW)
        except asyncio.TimeoutError:
            log('⚠️ SOL/USD warm-up exceeded 12.5s; entries remain fail-closed until available.', Color.YELLOW)

        creator_probe = CreatorOnchainProbe(
            session, settings.http_rpc_url, timeout_sec=0.35,
            concurrency=2,
            signature_cache_sec=settings.creator_cache_ttl_sec,
            guard_min_signatures=settings.creator_guard_min_sig,
            burst_window_sec=settings.creator_burst_window_sec,
            creator_watch=creator_watch,
        )
        simulator = ABPaperSimulator(
            session, settings, run_id, run_dir, trades_queue,
            creator_watch, creator_probe,
        )
        storage_task = asyncio.create_task(storage_worker(trades_queue, settings.db_path), name='ab-trade-storage')

        try:
            await simulator.start()
            collector.record_meta('run_started_at_utc', _utc_now())
            collector.record_meta('rpc_preflight_host', rpc_host)
            manifest.update({
                'status': 'running',
                'started_at_utc': _utc_now(),
                'pumpportal_fast_source': pump_fast,
                'helius_program_fallback': not pump_fast,
                'sources': 'pumpfun',
            })
            _write_json(run_dir / 'manifest.json', manifest)

            requested_duration = max(0, int(os.getenv('RUN_DURATION_SEC', str(RUN_DURATION_SEC_DEFAULT))))
            deadline_raw = os.getenv('OVERNIGHT_RUN_DEADLINE_EPOCH', '').strip()
            if deadline_raw:
                try:
                    deadline_remaining = max(0, int(float(deadline_raw) - time.time()))
                except (TypeError, ValueError, OverflowError):
                    deadline_remaining = 0
                duration = min(requested_duration, deadline_remaining)
            else:
                duration = requested_duration
            if duration < 1:
                raise RuntimeError('session_deadline_elapsed_before_run')

            async def candidate_worker() -> None:
                while True:
                    coin = await coins_queue.get()
                    try:
                        try:
                            collector.observe_candidate(coin, session, settings)
                        except Exception as exc:
                            log(f'⚠️ Telemetry candidate enqueue failed ({type(exc).__name__}).', Color.YELLOW)
                        task = asyncio.create_task(simulator.handle_candidate(coin), name=f'ab-candidate-{coin.mint[:8]}')
                        candidate_tasks.add(task)
                        task.add_done_callback(candidate_tasks.discard)
                    finally:
                        coins_queue.task_done()

            candidate_worker_task = asyncio.create_task(candidate_worker(), name='ab-candidate-worker')
            # Helius program-log fallback uses a shared event queue. In the
            # normal PumpPortal path, no other source or strategy is started.
            if not pump_fast:
                events_queue: asyncio.Queue = asyncio.Queue()
                # Replace the placeholder task, if any, with the actual paired workers.
                producers = []
                producers.append(asyncio.create_task(
                    detector_worker(events_queue, coins_queue, session,
                                    settings.http_rpc_url, watch_stop),
                    name='ab-helius-detector',
                ))
                producers.append(asyncio.create_task(
                    ws_listener(events_queue, settings, programs, watch_stop),
                    name='ab-helius-ws-listener',
                ))
            if pump_fast:
                producers.append(asyncio.create_task(
                    pumpportal_listener(coins_queue, watch_stop), name='ab-pumpportal-listener'
                ))

            if deadline_raw:
                duration = min(duration, max(0, int(float(deadline_raw) - time.time())))
            if duration < 1:
                raise RuntimeError('session_deadline_elapsed_before_listener_start')
            actual_start_epoch = time.time()
            simulator.started_at_epoch = actual_start_epoch
            manifest['started_at_utc'] = _utc_now()
            _write_json(run_dir / 'manifest.json', manifest)
            log(
                f'▶️ RUN_ID={run_id} started; planned duration={duration}s; '
                'only portfolio_a + portfolio_b; no signed/submitted transactions.',
                Color.GREEN,
            )
            # P0-5: artefacts every 60 s, so a stop-list trigger (or a killed
            # process) still leaves a readable summary, ticks and wallets.
            snapshot_task = asyncio.create_task(
                _periodic_snapshots(simulator, run_dir), name='ab-artifact-snapshot',
            )
            duration_task = asyncio.create_task(asyncio.sleep(duration), name='ab-run-duration')
            early_stop_task = asyncio.create_task(simulator.early_stop_event.wait(), name='ab-early-stop-watch')
            signal_task = asyncio.create_task(external_stop.wait(), name='ab-signal-watch')
            done, pending = await asyncio.wait(
                {duration_task, early_stop_task, signal_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)
            if early_stop_task in done and simulator.early_stop_reason:
                log(f'⏹ Early stop: {simulator.early_stop_reason}; finalizing this RUN_ID.', Color.YELLOW)
            elif signal_task in done and external_stop.is_set():
                external_stop_reason = 'SIGTERM'
                log('⏹ Session supervisor requested stop; finalizing without synthetic exits.', Color.YELLOW)
        except asyncio.CancelledError:
            run_error = 'cancelled'
            raise
        except Exception as exc:
            run_error = type(exc).__name__
            log(f'❌ A/B run stopped by {run_error}; no transaction was sent.', Color.RED)
        finally:
            watch_stop.set()
            for task in producers:
                task.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
            if candidate_worker_task is not None:
                try:
                    await asyncio.wait_for(coins_queue.join(), timeout=3.0)
                except asyncio.TimeoutError:
                    pass
                candidate_worker_task.cancel()
                await asyncio.gather(candidate_worker_task, return_exceptions=True)
            if candidate_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*list(candidate_tasks), return_exceptions=True), timeout=12.0)
                except asyncio.TimeoutError:
                    for task in list(candidate_tasks):
                        task.cancel()
                    await asyncio.gather(*list(candidate_tasks), return_exceptions=True)
            summary: dict[str, Any] = {}
            if simulator is not None:
                try:
                    summary = await simulator.close()
                except Exception as exc:
                    log(f'⚠️ A/B state save failed ({type(exc).__name__}); no synthetic exit counted.', Color.YELLOW)
            if creator_probe is not None:
                try:
                    await creator_probe.close()
                except Exception as exc:
                    log(f'⚠️ Creator probe shutdown ({type(exc).__name__}); probes are non-blocking.', Color.YELLOW)
            try:
                await asyncio.wait_for(trades_queue.join(), timeout=5.0)
            except asyncio.TimeoutError:
                log('⚠️ Trade DB queue did not fully drain before shutdown.', Color.YELLOW)
            if storage_task is not None:
                storage_task.cancel()
                await asyncio.gather(storage_task, return_exceptions=True)
            collector.record_meta('rpc_metrics_final', summary.get('rpc_metrics', {}))
            collector.record_meta('run_error_type', run_error or 'none')
            collector.record_meta('run_ended_at_utc', _utc_now())
            await collector.close()
            telemetry_stats = collector.stats()
            set_collector(None)
            set_rpc_batcher(None)
            if batcher is not None:
                await batcher.close()
            ended = time.time()
            manifest.update({
                'status': (
                    'stopped_with_error' if run_error else
                    'completed_early' if simulator is not None and simulator.early_stop_reason else
                    'stopped_by_signal' if external_stop_reason else 'completed'
                ),
                'ended_at_utc': _utc_now(),
                'actual_duration_sec': round(ended - actual_start_epoch, 3),
                'early_stop_reason': simulator.early_stop_reason if simulator is not None else None,
                'external_stop_reason': external_stop_reason,
                'run_error_type': run_error,
                'rpc_metrics': summary.get('rpc_metrics', {}),
                # Run #7: passive feature collection (plain HTTPS, no RPC).
                'enrich': {
                    **(summary.get('enrich') or {}),
                    'status': 'written' if (summary.get('enrich') or {}).get('rows')
                              else 'empty',
                    # Reachability of datapi.jup.ag, stated as a status: an
                    # unreachable host produces rows with
                    # status=enrich_unavailable, never a crashed run.
                    'host_status': _enrich_host_status(summary.get('enrich')),
                },
                'portfolio_summary_file': str(run_dir / 'ab_summary.json'),
                'signed_transactions': 0,
                'submitted_transactions': 0,
                # Lost telemetry rows were previously visible only as one yellow
                # log line; now they reach the manifest and SUMMARY.md.
                'telemetry': telemetry_stats,
            })
            # Funnel + gate split, computed from the finished telemetry DB
            # (read-only, no RPC). A blind spot in the probe must stay visible
            # instead of being reported as a risk verdict.
            try:
                funnel = funnel_from_db(telemetry_path)
            except Exception as exc:
                funnel = {'available': False, 'reason': type(exc).__name__}
            manifest['funnel'] = funnel
            manifest['stop_policy'] = summary.get('stop_policy', {})
            _write_json(run_dir / 'manifest.json', manifest)
            try:
                (run_dir / 'SUMMARY.md').write_text(
                    _summary_markdown(manifest, funnel, summary), encoding='utf-8',
                )
                log(f'📄 SUMMARY.md записан: {run_dir / "SUMMARY.md"}', Color.CYAN)
            except Exception as exc:
                log(f'⚠️ SUMMARY.md не записан ({type(exc).__name__})', Color.YELLOW)
            log(f'⏹ A/B run finalized: {run_id}; open and pending positions remain separately recorded.', Color.CYAN)

    for installed_signal in installed_signals:
        loop.remove_signal_handler(installed_signal)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\nStopped. Paper-only; no transactions submitted.')
        sys.exit(0)
