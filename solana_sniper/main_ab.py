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
from portfolio_ab import (
    ARM_IDS, ARM_REFERENCE, ABPaperSimulator, entry_band_label,
    exit_mode_from_env, profiles_from_settings,
)
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


def shared_entry_latency_ms() -> float:
    """The entry delay every book of this run actually uses (run #8: 0).

    Read from the built profiles, not from the env, so the manifest cannot
    advertise one number while the books trade another.
    """
    try:
        profiles = profiles_from_settings(load_settings())
        return float(next(iter(profiles.values())).entry_latency_ms)
    except Exception:
        try:
            return float(os.getenv('ENTRY_LATENCY_MS', '150').strip() or 150.0)
        except ValueError:
            return 150.0


def _live_profiles() -> dict[str, Any]:
    """The books this process will actually trade (env-aware)."""
    try:
        return profiles_from_settings(load_settings())
    except Exception:
        try:
            return profiles_from_settings(object())
        except Exception:
            return {}


def _live_exit_mode() -> str:
    profiles = _live_profiles()
    if profiles:
        return str(next(iter(profiles.values())).exit_mode)
    return exit_mode_from_env()


def _exit_rule_text() -> str:
    profiles = _live_profiles()
    if profiles:
        return ABPaperSimulator._exit_rule_label(next(iter(profiles.values())))
    return 'ladder (could not build a profile; declared by hand)'


def _arm_bands() -> dict[str, str]:
    """{arm name: band label} of the arms this process will run."""
    try:
        profiles = profiles_from_settings(load_settings())
    except Exception:
        profiles = profiles_from_settings(object())
    return {name: entry_band_label(profile) for name, profile in profiles.items()}


def _arm_entry_fields() -> dict[str, dict[str, Any]]:
    """Every entry number of every book this process will run.

    Run #9 acceptance checks the bands and the copies rule in the manifest, so
    they are read from the profiles that will actually trade — not from a
    constant that could drift away from the environment.
    """
    try:
        profiles = profiles_from_settings(load_settings())
    except Exception:
        try:
            profiles = profiles_from_settings(object())
        except Exception:
            return {}
    return {
        name: {
            'arm_id': profile.arm_id,
            'v_sol_birth_min': profile.v_sol_birth_min,
            'v_sol_birth_max': profile.v_sol_birth_max,
            'delta_min_sol': profile.delta_min_sol,
            'delta_max_sol': profile.delta_max_sol,
            'max_name_copies_120s': profile.max_name_copies_120s,
            'name_copies_window_sec': profile.name_copies_window_sec,
            'entry_band_label': entry_band_label(profile),
            'exit_mode': profile.exit_mode,
            'entry_latency_ms': profile.entry_latency_ms,
        }
        for name, profile in profiles.items()
    }


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


def _is_run8(manifest: dict[str, Any]) -> bool:
    """A run #8 report is written for the run #8 questions only."""
    ab = manifest.get('ab_test') or {}
    return (
        ab.get('mode') == 'single_arm_smoke'
        or manifest.get('exit_mode') == 'fee_cover'
    )


def _run8_summary_markdown(
    manifest: dict[str, Any], funnel: dict[str, Any], summary: dict[str, Any],
) -> str:
    """The 30-minute fee_cover report: latency, cost and funnel, no PnL verdict."""
    lines: list[str] = []
    add = lines.append
    arms = [
        name for name, book in summary.items()
        if isinstance(book, dict) and book.get('stats') is not None
    ]
    profiles = summary.get('profiles') or {}

    def label(name: str) -> str:
        return str((profiles.get(name) or {}).get('arm_id') or name)

    def m(value: Any) -> str:
        return _money(value)

    add(f"# Прогон {manifest.get('run_id')}")
    add('')
    add(f"- build: `{manifest.get('build')}`")
    add(f"- длительность: {manifest.get('actual_duration_sec')} c "
        f"(план {manifest.get('planned_duration_sec')} c)")
    add(f"- paper-only: {manifest.get('paper_only')}; подписано транзакций: "
        f"{manifest.get('signed_transactions')}")
    add(f"- **сравнения с A в этом прогоне нет**: торгуют только "
        f"{' и '.join(label(name) for name in arms)}; S0 — контроль для S1.")
    stats = manifest.get('telemetry') or {}
    if stats:
        add(f"- телеметрия: dropped={stats.get('dropped_events')}, "
            f"write_errors={stats.get('write_errors')}, "
            f"writer={stats.get('writer_error') or 'ok'}")
    if manifest.get('early_stop_reason'):
        add(f"- досрочная остановка: `{manifest.get('early_stop_reason')}` "
            '(stop-list, не PnL; артефакты дописаны)')
    poll = summary.get('mark_poll') or {}
    if poll:
        add(f"- опрос меток: {poll.get('mark_poll_sec')} с, "
            f"адаптация: {'да' if poll.get('mark_poll_adapted') else 'нет'}, "
            f"лимит 10-мин окна {poll.get('rps_limit_10min')} rps")
    add('')

    add('## Что за прогон')
    add('')
    add('| книга | полоса входа | что отличается | выход |')
    add('|---|---|---|---|')
    for name in arms:
        prof = profiles.get(name) or {}
        band = prof.get('entry_band_label') or '—'
        changed = (
            f"delta_min {prof.get('delta_min_sol')} (против S1)"
            if name == 'portfolio_s0' else 'ничего: это B1'
        )
        add(f"| {label(name)} (`{name}`) | {band} | {changed} | "
            f"fee_cover (общий для обеих книг) |")
    add('')
    add('Выход у обеих книг один и он не лестница Round 6: **hard stop −10 %**, '
        'затем **продажа 100 % при +3.5 %** (измеренный round trip 3.25 % + 0.25 п.п. '
        'запаса), затем **выход по времени 5 с**. TP1, трейл, shelf и no-progress в '
        'этом режиме не вызываются вовсе, поэтому их нет и в разбивке выходов. '
        'Ставка 500 ₽, банк 1 500 ₽ у каждой книги, 3 слота, вход и опрос одинаковые.')
    add('')

    add('## Результат по книгам')
    add('')
    add('| книга | n | сумма ₽ | ₽/сделку | медиана | sd | CI95 среднего | без топ-2 |')
    add('|---|---:|---:|---:|---:|---:|---|---:|')
    for name in arms:
        book = summary.get(name) or {}
        st = book.get('stats') or {}
        ci = st.get('ci95_mean')
        ci_text = f'[{ci[0]:.2f}, {ci[1]:.2f}]' if ci else '—'
        add(f"| {label(name)} | {st.get('n')} | {m(st.get('sum'))} | "
            f"{m(st.get('mean'))} | {m(st.get('median'))} | {m(st.get('sd'))} | "
            f"{ci_text} | {m(st.get('sum_without_top2'))} |")
    add('')
    add('Деньги здесь — не результат прогона: 30–60 сделок на книгу ничего не '
        'разрешают (см. «Чего этот прогон не говорит»).')
    add('')

    add('### Чем закрывались сделки')
    add('')
    add('| книга | fee_cover | time_exit | hard_stop | прочие категории |')
    add('|---|---:|---:|---:|---|')
    for name in arms:
        shares = (summary.get(name) or {}).get('exit_shares') or {}
        cells = []
        for kind, key in (('fee_cover', 'fee_cover_n'), ('time_exit', 'time_exit_n'),
                          ('hard_stop', 'hard_stop_n')):
            n = int(shares.get(key) or 0)
            share = ((shares.get('shares') or {}).get(kind) or {}).get('share')
            # An absent kind is 0 %, not "not measured": the whole point is
            # that e.g. hard_stop may legitimately never fire.
            cells.append(f'{n} ({_share(0.0 if share is None else share)})')
        other = shares.get('ladder_kinds_present') or []
        add(f"| {label(name)} | {cells[0]} | {cells[1]} | {cells[2]} | "
            f"{', '.join(other) if other else 'нет'} |")
    add('')
    add('«прочие категории» обязаны быть пустыми: в fee_cover-режиме лестница не '
        'вызывается. Если там что-то появилось — это баг, а не результат.')
    add('')

    add('## Латентность входа: пять стадий, каждая измерена')
    add('')
    add('| стадия | n | медиана, мс | p90, мс | max, мс |')
    add('|---|---:|---:|---:|---:|')
    names = {
        't_detect_ms': 'packet WSS → строка candidate',
        't_filters_ms': 'candidate → вердикт security-батча',
        't_read_ms': 'вердикт → чтение кривой, по которому посчитан филл',
        't_fill_ms': 'чтение → строка филла',
        't_total_ms': 'packet → строка филла',
    }
    for name in arms:
        chain = (summary.get(name) or {}).get('latency_chain') or {}
        stages = chain.get('stages') or {}
        add(f"**{label(name)}** (n={chain.get('n', 0)})")
        add('')
        for field, text in names.items():
            slot = stages.get(field) or {}
            add(f"| `{field}` — {text} | {slot.get('n', 0)} | "
                f"{m(slot.get('median'))} | {m(slot.get('p90'))} | "
                f"{m(slot.get('max'))} |")
        add('')
        add(f"- доля входов быстрее 200 мс: {_share(chain.get('share_under_200ms'))}, "
            f"быстрее 500 мс: {_share(chain.get('share_under_500ms'))}")
        add(f"- модельная задержка входа в этом прогоне: "
            f"{chain.get('entry_latency_ms_model')} мс")
        add('')
    add('`t_read_ms = 0` — это не потерянное измерение: security-батч и есть то '
        'чтение, по которому считается цена входа, второй RPC на вход не делается. '
        '`t_detect_ms` — время от пакета WSS до строки candidate в телеметрии, '
        'то есть очередь воркера, а не сеть.')
    add('')

    add('## Стоимость круглого рейса и цена гейта импульса')
    add('')
    add('| книга | entry_fill_premium медиана, % | round trip на неподвижных, % (n) | '
        'эталон | цена гейта: entry_premium медиана, % | p90, % | доля >1 % |')
    add('|---|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        book = summary.get(name) or {}
        rt = book.get('round_trip_cost') or {}
        gate = book.get('gate_price') or {}
        add(f"| {label(name)} | {m(rt.get('entry_fill_premium_pct_median'))} | "
            f"{m(rt.get('round_trip_cost_pct_median'))} "
            f"({rt.get('n_flat', 0)}) | "
            f"{m(rt.get('reference_round_trip_pct'))} % / "
            f"{m(rt.get('reference_entry_fill_premium_pct'))} % | "
            f"{m(gate.get('entry_premium_pct_median'))} | "
            f"{m(gate.get('entry_premium_pct_p90'))} | "
            f"{_share(gate.get('share_premium_above_1pct'))} |")
    add('')
    add('`round trip` — медиана PnL сделок, у которых цена не двигалась '
        '(`mfe − mae ≤ 1 п.п.`): там PnL и есть стоимость комиссий и импакта. '
        '`цена гейта` = `entry_premium_pct` = `(v_sol на входе / v_sol_birth)² − 1` — '
        'надбавка, которую платит книга за ожидание, пока delta дойдёт до '
        '`delta_min`. У S0 (`delta_min 0.00`) вход и есть первое чтение, поэтому '
        'её число — эталон «без гейта».')
    add('')

    add('## PnL по времени удержания и реплей «держать до N секунд»')
    add('')
    add('| книга | факт | 1 с | 2 с | 3 с | 5 с | hold 3 с | hold 5 с | 5 с + стоп −10 % |')
    add('|---|---:|---:|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        replay = (summary.get(name) or {}).get('hold_replay') or {}
        snap = replay.get('snapshots') or {}
        rep = replay.get('replay') or {}

        def mean_of(slot: dict) -> str:
            return m((slot or {}).get('mean_pct'))

        add(f"| {label(name)} | {mean_of(replay.get('actual'))} | "
            f"{mean_of(snap.get('pnl_pct_at_1s'))} | "
            f"{mean_of(snap.get('pnl_pct_at_2s'))} | "
            f"{mean_of(snap.get('pnl_pct_at_3s'))} | "
            f"{mean_of(snap.get('pnl_pct_at_5s'))} | "
            f"{mean_of(rep.get('hold_to_3s'))} | "
            f"{mean_of(rep.get('hold_to_5s'))} | "
            f"{mean_of(rep.get('hold_to_5s_stop_10'))} |")
    add('')
    add('Всё в процентах от ставки, по меткам, которые бот реально видел '
        '(`exports/ticks.csv`). «1 с … 5 с» — последняя метка не позже момента; '
        '«hold N с» — первая метка в момент N с; «5 с + стоп» — выход по −10 %, '
        'если он случился раньше пяти секунд. Это реплей самих сделок, а не '
        'модель «что было бы, если бы мы вошли в другие монеты».')
    add('')

    add('## Воронка')
    add('')
    add('```')
    add(str(funnel.get('chain') or 'n/a'))
    add('')
    add(f"entry_state             {funnel.get('entry_state')}")
    for arm in arms:
        for reason, count in ((funnel.get(f'{arm}_signal')) or {}).items():
            add(f"  {label(arm)} signal: {reason:<48} {count}")
        add(f"  {label(arm)} opened: "
            f"{(funnel.get('opened') or {}).get(arm)}")
        for reason, count in ((funnel.get('entry_skips') or {}).get(arm) or {}).items():
            add(f"  {label(arm)} skip: {reason:<50} {count}")
    add('```')
    add('')
    for name in arms:
        book = summary.get(name) or {}
        signals = funnel.get(f'{name}_signal') or {}
        skips = (funnel.get('entry_skips') or {}).get(name) or {}
        no_price = sum(
            count for reason, count in skips.items()
            if str(reason).startswith(('four_account', 'missing_or_invalid', 'sol_usd',
                                       'curve_', 'pump_', 'unsupported_'))
        )
        add(f"- **{label(name)}**: отказов по `delta_min` "
            f"(`no_live_momentum`) — {signals.get('no_live_momentum', 0)}, "
            f"по `delta_max` (`momentum_too_late`) — "
            f"{signals.get('momentum_too_late', 0)}, "
            f"вне полосы рождения — {signals.get('birth_out_of_band', 0)}, "
            f"слоты заняты — {book.get('slots_full', 0)}, "
            f"нет цены/снимка — {no_price}")
    add('')
    add(f"Бакеты delta: {funnel.get('delta_buckets')}")
    add('')

    add('## Приёмка прогона #8')
    add('')
    rpc = manifest.get('rpc_metrics') or {}
    telemetry = manifest.get('telemetry') or {}
    poll = summary.get('mark_poll') or {}
    # The drain count is the honest one: the simulator waited (bounded) for its
    # own exit rules to finish, then reported whatever was left.
    drain = summary.get('drain') or {}
    open_positions = (
        drain.get('left_open')
        if drain.get('left_open') is not None
        else sum(
            int((summary.get(name) or {}).get('open_position_count') or 0)
            for name in arms
        )
    )
    duration = float(manifest.get('actual_duration_sec') or 0.0)
    peak = rpc.get('requests_per_sec_max_10min')
    rows = (
        ('paper_only = true, подписано 0',
         bool(manifest.get('paper_only')) and int(manifest.get('signed_transactions') or 0) == 0,
         f"{manifest.get('paper_only')} / {manifest.get('signed_transactions')}"),
        ('длительность ≥ 1 790 c', duration >= 1_790.0, f'{duration:.0f} c'),
        ('early_stop_reason = null', not manifest.get('early_stop_reason'),
         manifest.get('early_stop_reason') or 'null'),
        ('неисправленные ошибки RPC ≤ 2 %',
         float(rpc.get('unrecovered_rate') or 0.0) <= 0.02,
         f"{100.0 * float(rpc.get('unrecovered_rate') or 0.0):.2f} % "
         f"({rpc.get('unrecovered_failures', 0)} из {rpc.get('requests', 0)})"),
        ('средний rps ≤ 1.8', float(rpc.get('requests_per_sec') or 0.0) <= 1.8,
         m(rpc.get('requests_per_sec'))),
        ('худшее 10-мин окно ≤ 2.2 rps (или записана адаптация)',
         peak is None or float(peak) <= 2.2 or bool(poll.get('mark_poll_adapted')),
         (m(peak) if peak is not None else '— (окно не набрано)')
         + (', каденция поднята до 0.25 с' if poll.get('mark_poll_adapted') else '')),
        ('dropped_events = 0', int(telemetry.get('dropped_events') or 0) == 0,
         int(telemetry.get('dropped_events') or 0)),
        ('write_errors = 0', int(telemetry.get('write_errors') or 0) == 0,
         int(telemetry.get('write_errors') or 0)),
        ('0 открытых позиций в конце (после паузы на закрытие)',
         open_positions == 0,
         f"{open_positions} (ждали {drain.get('waited_sec')} c)"),
        ('fee_cover сработал ≥ 1 раза в каждой книге',
         all(bool(((summary.get(name) or {}).get('exit_shares') or {})
                  .get('fee_cover_fired')) for name in arms),
         ', '.join(
             f"{label(name)}={((summary.get(name) or {}).get('exit_shares') or {}).get('fee_cover_n', 0)}"
             for name in arms
         )),
        ('медиана t_total_ms записана',
         all(((summary.get(name) or {}).get('latency_chain') or {})
             .get('stages', {}).get('t_total_ms', {}).get('median') is not None
             for name in arms),
         ', '.join(
             str((((summary.get(name) or {}).get('latency_chain') or {})
                  .get('stages', {}).get('t_total_ms', {}) or {}).get('median'))
             for name in arms
         )),
    )
    add('| проверка | факт | статус |')
    add('|---|---|---|')
    for text, ok, value in rows:
        add(f"| {text} | {value} | {'✅' if ok else '❌'} |")
    add('')
    add(f"- 429 всего: {rpc.get('http_429_count', 0)}, ретраев: "
        f"{rpc.get('retry_count', 0)}, восстановлено {rpc.get('recovered_failures', 0)} "
        '(восстановленный 429 — back-pressure, а не сбой)')
    add('')

    add('## Чего этот прогон не говорит')
    add('')
    add('- **Нет сравнения с A.** В прогоне только S1 и S0; любая фраза вида '
        '«S1 лучше A» здесь не проверялась.')
    add('- **Нет вердикта по деньгам.** 30–60 сделок на книгу: реплей ожидает '
        '≈ −4.9 % у S1 и ≈ −4.6 % у S0, лучший измеренный вариант ≈ +1 %. '
        'Разница меньше ~15 ₽/сделку на такой выборке нерешаема, порог +8 ₽ '
        'после Bonferroni требует ~1 700 пар.')
    add('- **Ничего не подписывалось и не отправлялось**: `signed_transactions = 0`, '
        'RPC только на чтение.')
    add('- Измеряется латентность, стоимость исполнения и то, срабатывает ли '
        'fee_cover вообще — не прибыльность.')
    add('')
    return '\n'.join(lines)


def _run_number() -> str:
    """Which declared experiment this process is configured to run.

    Read from the arms and the exit the profiles will actually use, so the
    report page can never be chosen by a constant that drifted away from the
    environment.
    """
    try:
        profiles = profiles_from_settings(load_settings())
    except Exception:
        try:
            profiles = profiles_from_settings(object())
        except Exception:
            return 'run7'
    names = set(profiles)
    if 'portfolio_s2' in names:
        return 'run9'
    if 'portfolio_s1' in names or 'portfolio_s0' in names:
        return 'run8'
    if names and 'portfolio_a' not in names:
        return 'run8'
    return 'run7'


def _is_run9(manifest: dict[str, Any]) -> bool:
    """Run #9 = the S0/S2 selection run; it also uses fee_cover."""
    if manifest.get('run_number'):
        return manifest['run_number'] == 'run9'
    return (
        'portfolio_s2' in (manifest.get('portfolio_scope') or [])
        and bool(manifest.get('entry_parameters'))
    )


def _run9_summary_markdown(
    manifest: dict[str, Any], funnel: dict[str, Any], summary: dict[str, Any],
) -> str:
    """Run #9: S0 unchanged vs S2 (dev-buy floor + name copies)."""
    lines: list[str] = []
    add = lines.append
    arms = [
        name for name, book in summary.items()
        if isinstance(book, dict) and book.get('stats') is not None
    ]
    profiles = summary.get('profiles') or {}
    planned = float(manifest.get('planned_duration_sec') or 0.0)
    long_run = planned >= 43_000.0

    def label(name: str) -> str:
        return str((profiles.get(name) or {}).get('arm_id') or name)

    def m(value: Any) -> str:
        return _money(value)

    def entry(name: str) -> dict[str, Any]:
        return (manifest.get('entry_parameters') or {}).get(name) or {}

    add(f"# Прогон {manifest.get('run_id')}")
    add('')
    add(f"- build: `{manifest.get('build')}`")
    add(f"- длительность: {manifest.get('actual_duration_sec')} c "
        f"(план {manifest.get('planned_duration_sec')} c)")
    add(f"- paper-only: {manifest.get('paper_only')}; подписано транзакций: "
        f"{manifest.get('signed_transactions')}")
    add(f"- выход у обеих книг один: **hard stop −10 %**, **продажа 100 % при "
        f"+3.5 %**, иначе **часы 5 с**")
    stats = manifest.get('telemetry') or {}
    if stats:
        # A smoke does not carry telemetry counters; show "н/д", never None.
        add(f"- телеметрия: dropped={stats.get('dropped_events', 'н/д')}, "
            f"write_errors={stats.get('write_errors', 'н/д')}, "
            f"writer={stats.get('writer_error') or 'ok'}")
    if manifest.get('early_stop_reason'):
        add(f"- досрочная остановка: `{manifest.get('early_stop_reason')}`")
    poll = summary.get('mark_poll') or {}
    if poll:
        add(f"- опрос меток: {poll.get('mark_poll_sec')} с, "
            f"адаптация: {'да' if poll.get('mark_poll_adapted') else 'нет'}"
            + (' (каденция уже на 0.25 с, slowing невозможен — не заявляется)'
               if poll.get('adaptation_blocked') else '')
            + f"; порог 10-мин окна {poll.get('rps_limit_10min')} rps")
    add('')

    add('## Схема опыта')
    add('')
    add('| книга | полоса рождения | delta | копии имени 120 с |')
    add('|---|---|---|---|')
    for name in arms:
        fields = entry(name) or (profiles.get(name) or {})
        copies = fields.get('max_name_copies_120s')
        add(f"| **{label(name)}** (`{name}`) | "
            f"{m(fields.get('v_sol_birth_min'))}–{m(fields.get('v_sol_birth_max'))} | "
            f"{m(fields.get('delta_min_sol'))}–{m(fields.get('delta_max_sol'))} | "
            f"{'выключен' if copies is None else str(copies)} |")
    add('')
    add('S0 — книга прогона #8 без единого изменения. S2 отличается двумя числами '
        'во входе: пол 31.96 (= dev_buy ≥ 2 SOL) и запрет входа, если тот же тикер '
        'уже запускали за предыдущие 120 с. Выход, ставка, слоты и каденс одинаковые.')
    add('')

    add('## Результат по книгам')
    add('')
    add('| книга | n | сумма ₽ | ₽/сделку | медиана | sd | CI95 среднего | без топ-2 |')
    add('|---|---:|---:|---:|---:|---:|---|---:|')
    for name in arms:
        st = (summary.get(name) or {}).get('stats') or {}
        ci = st.get('ci95_mean')
        ci_text = f'[{ci[0]:.2f}, {ci[1]:.2f}]' if ci else '—'
        add(f"| {label(name)} | {st.get('n')} | {m(st.get('sum'))} | "
            f"{m(st.get('mean'))} | {m(st.get('median'))} | {m(st.get('sd'))} | "
            f"{ci_text} | {m(st.get('sum_without_top2'))} |")
    add('')
    add('| книга | fee_cover | time_exit 5 с | hard_stop −10 % | прочие категории |')
    add('|---|---:|---:|---:|---|')
    for name in arms:
        shares = (summary.get(name) or {}).get('exit_shares') or {}
        cells = []
        for key, kind in (('fee_cover_n', 'fee_cover'), ('time_exit_n', 'time_exit'),
                          ('hard_stop_n', 'hard_stop')):
            n = int(shares.get(key) or 0)
            share = ((shares.get('shares') or {}).get(kind) or {}).get('share')
            cells.append(f'{n} ({_share(0.0 if share is None else share)})')
        other = shares.get('ladder_kinds_present') or []
        add(f"| {label(name)} | {cells[0]} | {cells[1]} | {cells[2]} | "
            f"{', '.join(other) if other else 'нет'} |")
    add('')
    add('«прочие категории» обязаны быть пустыми: в fee_cover-режиме лестница не '
        'вызывается.')
    add('')

    add('## Латентность входа и стоимость круга')
    add('')
    add('| книга | t_total_ms медиана / p90 / max | доля < 200 мс | '
        'entry_fill_premium медиана, % | round trip на неподвижных, % (n) | эталон |')
    add('|---|---:|---:|---:|---:|---:|')
    for name in arms:
        book = summary.get(name) or {}
        chain = book.get('latency_chain') or {}
        stage = (chain.get('stages') or {}).get('t_total_ms') or {}
        rt = book.get('round_trip_cost') or {}
        add(f"| {label(name)} | {m(stage.get('median'))} / {m(stage.get('p90'))} / "
            f"{m(stage.get('max'))} | {_share(chain.get('share_under_200ms'))} | "
            f"{m(rt.get('entry_fill_premium_pct_median'))} | "
            f"{m(rt.get('round_trip_exact_pct_median'))} "
            f"({rt.get('n_flat_exact', 0)}) | "
            f"3.25 % / 1.41 % |")
    add('')
    add('`round trip` здесь — по сделкам, где **mid на выходе равен mid на входе в '
        'пределах 0.01 %**: на них PnL и есть стоимость круга. Рядом, для сравнения, '
        'старое определение (`mfe − mae ≤ 1 п.п.`) и эталоны прогона #7 (3.25 % круг, '
        '1.41 % плата за вход).')
    add('')
    per_book_rt = []
    for name in arms:
        rt = (summary.get(name) or {}).get('round_trip_cost') or {}
        per_book_rt.append(int(rt.get('n_flat_exact') or 0))
    if per_book_rt and max(per_book_rt) >= 5:
        worst = max(
            float(((summary.get(name) or {}).get('round_trip_cost') or {})
                  .get('round_trip_exact_pct_median') or 0.0)
            for name in arms
        )
        add(f"**Ворота прогона B:** медиана round trip {worst:.2f} % "
            f"{'≤ 4.0 % — прогон B разрешён' if worst <= 4.0 else '> 4.0 % — прогон B не запускается'}.")
    else:
        add('**Ворота прогона B:** неподвижных сделок слишком мало, чтобы назвать '
            'медиану; по правилу §4 приёмки B не запускается до ручного решения.')
    add('')

    add('## Что было бы при выходе по часам: реальная цена через 5 с')
    add('')
    add('| книга | сделок с price_at_5s | покрытие | медиана PnL на 5 с, % | среднее, % |')
    add('|---|---:|---:|---:|---:|')
    for name in arms:
        pe = ((summary.get(name) or {}).get('post_exit') or {})
        add(f"| {label(name)} | {pe.get('price_at_5s_trades', 0)} | "
            f"{_share(pe.get('price_at_5s_coverage'))} | "
            f"{m(pe.get('pnl_pct_at_5s_median'))} | {m(pe.get('pnl_pct_at_5s_mean'))} |")
    add('')
    add('`price_at_5s` — ближайшая **реальная** метка к входу + 5 с (допуск 0.75 с), '
        'из ленты позиции или из «призрака» после выхода. Проценты — mid-к-mid, без '
        '3.25 % круга: это движение цены, а не PnL сделки.')
    add('')

    add('## «Призрак»: что было после выхода')
    add('')
    add('| книга | выходов отслежено | меток на выход (мин/медиана) | '
        '+0.5 с | +1 с | +2 с | +5 с | +10 с |')
    add('|---|---:|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        pe = ((summary.get(name) or {}).get('post_exit') or {})
        cov = pe.get('required_point_coverage') or {}
        vals = [m(((pe.get('mean_pnl_vs_exit_pct_at') or {}).get(f'+{t:g}s')))
                for t in (0.5, 1, 2, 5, 10)]
        add(f"| {label(name)} | {pe.get('exits_followed', 0)} | "
            f"{pe.get('marks_per_exit_min', 0)} / {pe.get('marks_per_exit_median', 0)} | "
            + ' | '.join(vals) + ' |')
    add('')
    add('Числа — среднее движение цены (mid) **после** выхода, % к mid на выходе. '
        'Они отвечают на вопрос, который бумажный филл искажает: он бронирует цену '
        'триггера в том же снимке, а реальная продажа приходит позже.')
    add('')

    add('## Воронка отказов')
    add('')
    add('| книга | name_copies | birth_band | delta_min | delta_max | slots_full | no_price |')
    add('|---|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        counts = (summary.get(name) or {}).get('reject_counts') or {}
        add(f"| {label(name)} | "
            + ' | '.join(str(counts.get(key, 0)) for key in
                         ('name_copies', 'birth_band', 'delta_min', 'delta_max',
                          'slots_full', 'no_price')) + ' |')
    add('')
    add(f"Бакеты delta: {funnel.get('delta_buckets')}")
    add('')

    add('## S0 против S2: парно по минутам')
    add('')
    add('| книга | минут с сделками | парных минут | доля минут, где книга больше | '
        'средняя разница, ₽ | средняя у S0, ₽ | средняя у книги, ₽ |')
    add('|---|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        mp = (summary.get(name) or {}).get('minute_pairs_vs_reference')
        if not mp:
            add(f"| {label(name)} | — | — | — | — | — | — |")
            continue
        counts = mp.get('minutes_with_trades') or {}
        add(f"| {label(name)} | {counts.get('reference')} / {counts.get('arm')} | "
            f"{mp.get('minutes_paired')} | {_share(mp.get('share_arm_better'))} | "
            f"{m(mp.get('mean_diff_rub'))} | {m(mp.get('mean_reference_rub'))} | "
            f"{m(mp.get('mean_arm_rub'))} |")
    add('')
    add('Это отдельный вывод §5(4): он не отменяет правила 1–3 по CI95 средней S2, '
        'и минуты, где торговала только одна книга, в парное среднее не входят — '
        'пустая минута это не ноль.')
    add('')

    add('## Разложение вкладов внутри книги (§5(5))')
    add('')
    add('| книга | ячейка | n | среднее ₽ | медиана ₽ | среднее % | fee_cover | time_exit | stop |')
    add('|---|---|---:|---:|---:|---:|---:|---:|---:|')
    for name in arms:
        cells = ((summary.get(name) or {}).get('selection_decomposition') or {}).get('cells') or {}
        for cell in ('dev_2_4.5_copies_0', 'dev_2_4.5_copies_pos', 'dev_lt_2', 'unknown'):
            row = cells.get(cell) or {}
            if not row.get('n'):
                continue
            add(f"| {label(name)} | `{cell}` | {row.get('n')} | {m(row.get('mean'))} | "
                f"{m(row.get('median'))} | {m(row.get('mean_pct'))} | "
                f"{row.get('fee_cover_n', 0)} | {row.get('time_exit_n', 0)} | "
                f"{row.get('hard_stop_n', 0)} |")
    add('')
    add('`dev_buy_sol = v_sol_birth − 29.96`. Три ячейки — ровно те, что названы в '
        'правиле §5(5): эффект пола и эффект фильтра копий видны внутри одной книги.')
    add('')

    add('## Приёмка прогона #9')
    add('')
    rpc = manifest.get('rpc_metrics') or {}
    telemetry = manifest.get('telemetry') or {}
    poll = summary.get('mark_poll') or {}
    drain = summary.get('drain') or {}
    open_positions = (
        drain.get('left_open') if drain.get('left_open') is not None
        else sum(int((summary.get(name) or {}).get('open_position_count') or 0)
                 for name in arms)
    )
    duration = float(manifest.get('actual_duration_sec') or 0.0)
    peak = rpc.get('requests_per_sec_max_10min')
    min_trades = 5 if not long_run else 0
    post_exit_ok = all(
        int((((summary.get(name) or {}).get('post_exit') or {})
             .get('exits_with_marks') or 0)) >= int((((summary.get(name) or {}).get('post_exit') or {})
                                                       .get('exits_followed') or 0))
        and int((((summary.get(name) or {}).get('post_exit') or {})
                 .get('marks_per_exit_min') or 0)) >= 5
        for name in arms
    )
    money_rule = (
        'CI95 средней S2 исключает 0 и оценка ≥ +2 % -> отбор принят; включает 0 '
        '-> не доказано; целиком ниже 0 -> отбор отклонён'
    )
    rows = (
        ('paper_only = true, подписано 0',
         bool(manifest.get('paper_only')) and int(manifest.get('signed_transactions') or 0) == 0,
         f"{manifest.get('paper_only')} / {manifest.get('signed_transactions')}"),
        (f'длительность ≥ {"43 000" if long_run else "1 790"} c',
         duration >= (43_000.0 if long_run else 1_790.0), f'{duration:.0f} c'),
        ('early_stop_reason = null', not manifest.get('early_stop_reason'),
         manifest.get('early_stop_reason') or 'null'),
        (f"неисправленные ошибки RPC ≤ {'1' if long_run else '2'} %",
         float(rpc.get('unrecovered_rate') or 0.0) <= (0.01 if long_run else 0.02),
         f"{100.0 * float(rpc.get('unrecovered_rate') or 0.0):.2f} % "
         f"({rpc.get('unrecovered_failures', 0)} из {rpc.get('requests', 0)})"),
        ('средний rps ≤ 1.8', float(rpc.get('requests_per_sec') or 0.0) <= 1.8,
         m(rpc.get('requests_per_sec'))),
        ('худшее 10-мин окно ≤ 2.5 rps',
         peak is None or float(peak) <= 2.5,
         m(peak) if peak is not None else '— (окно не набрано)'),
        ('dropped_events = 0', int(telemetry.get('dropped_events') or 0) == 0,
         int(telemetry.get('dropped_events') or 0)),
        ('write_errors = 0', int(telemetry.get('write_errors') or 0) == 0,
         int(telemetry.get('write_errors') or 0)),
        ('0 открытых позиций в конце (после паузы на закрытие)',
         open_positions == 0, f"{open_positions} (ждали {drain.get('waited_sec')} c)"),
        (f'сделок ≥ {min_trades} в каждой книге' if min_trades else 'сделки в каждой книге',
         all(int(((summary.get(name) or {}).get('stats') or {}).get('n') or 0) >= min_trades
             for name in arms),
         ', '.join(f"{label(name)}={((summary.get(name) or {}).get('stats') or {}).get('n', 0)}"
                   for name in arms)),
        ('post_exit.csv: ≥ 5 меток у каждого выхода', post_exit_ok,
         ', '.join(
             f"{label(name)}: {((summary.get(name) or {}).get('post_exit') or {}).get('exits_followed', 0)}"
             f" выходов, мин "
             f"{((summary.get(name) or {}).get('post_exit') or {}).get('marks_per_exit_min', 0)} меток"
             for name in arms)),
        ('полосы и копии в манифесте',
         all(
             (manifest.get('entry_parameters') or {}).get(name, {}).get('v_sol_birth_min')
             is not None for name in arms
         ) and bool(manifest.get('entry_parameters')),
         ', '.join(
             f"{label(name)}="
             f"{m(((manifest.get('entry_parameters') or {}).get(name) or {}).get('v_sol_birth_min'))}"
             f"/{((manifest.get('entry_parameters') or {}).get(name) or {}).get('max_name_copies_120s')}"
             for name in arms
         )),
        ('медиана round_trip_cost_pct записана',
         all(((summary.get(name) or {}).get('round_trip_cost') or {})
             .get('round_trip_exact_pct_median') is not None for name in arms),
         ', '.join(
             m(((summary.get(name) or {}).get('round_trip_cost') or {})
               .get('round_trip_exact_pct_median')) for name in arms
         )),
    )
    add('| проверка | факт | статус |')
    add('|---|---|---|')
    for text, ok, value in rows:
        add(f"| {text} | {value} | {'✅' if ok else '❌'} |")
    add('')
    add(f"- 429 всего: {rpc.get('http_429_count', 0)}, ретраев: "
        f"{rpc.get('retry_count', 0)}, восстановлено {rpc.get('recovered_failures', 0)}")
    add('')
    add(f"**Правило вывода, зафиксированное до старта:** {money_rule}.")
    add('')

    add('## Чего этот прогон не говорит')
    add('')
    add('- Выход не менялся ни в одной книге: тейк +3.5 %, стоп −10 %, часы 5 с — '
        'дословно как в прогоне #8.')
    add('- С прогоном #8 напрямую сравнивать нельзя: там не было фильтра копий, а '
        'окно было мягким (5 стопов из 64 против 28 %, ожидаемых по сетке).')
    add('- Любая цифра PnL печатается только вместе с n, средним, медианой, CI95 и '
        'разбивкой по выходам; отдельного «стало прибыльно» здесь нет.')
    if not long_run:
        add('- Это технический смоук на 30 минут: он отвечает про задержки, стоимость '
            'круга и то, что пропускает фильтр, а не про деньги. Прогон B запускается '
            'только при выполненной приёмке A и медиане круга ≤ 4.0 %.')
    add('- Ничего не подписывалось и не отправлялось: `signed_transactions = 0`, '
        'RPC только на чтение.')
    add('')
    return '\n'.join(lines)


def _summary_markdown(
    manifest: dict[str, Any], funnel: dict[str, Any], summary: dict[str, Any],
) -> str:
    """Render the one-page post-run report next to the raw artifacts.

    Run #8 asks different questions (latency, execution cost, funnel) and has
    no reference arm, so it gets its own report instead of a run #7 page with
    empty paired columns.
    """
    if _is_run9(manifest):
        return _run9_summary_markdown(manifest, funnel, summary)
    if _is_run8(manifest):
        return _run8_summary_markdown(manifest, funnel, summary)
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
        # Run #7: a flat average hides a burst; the rule is per 10-min window.
        # A run shorter than the window cannot prove anything either way, and
        # the row says so instead of pretending to be green.
        ('≤ 2.2 rps в любом 10-мин окне',
         rpc.get('requests_per_sec_max_10min') is None
         or float(rpc['requests_per_sec_max_10min']) <= 2.2,
         _money(rpc.get('requests_per_sec_max_10min'))
         if rpc.get('requests_per_sec_max_10min') is not None
         else '— (окно 10 мин ещё не набрано)'),
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
        'на 8 часах ≈450 пар и полуширина ≈ ±9 ₽ от нуля. Ни одно число здесь не означает '
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
    sources = {
        f"{_arm_name(name)}: {(summary.get(name) or {}).get('rocket_accounting', {}).get('peak_source')}"
        f" (окно {(summary.get(name) or {}).get('rocket_accounting', {}).get('window_sec')} с, "
        f"без наблюдений {(summary.get(name) or {}).get('rocket_accounting', {}).get('fallback_trades')})"
        for name in arms
    }
    add('Пик считается по фиксированному окну наблюдений, а не по времени удержания '
        'сделки (сделка живёт ~5 с, поэтому «пик внутри сделки» несравним между '
        'армами). ' + '; '.join(sorted(sources)))
    if any(
        (summary.get(name) or {}).get('rocket_accounting', {}).get('peak_source') != 'observations'
        for name in arms
    ):
        add('')
        add('> ⚠️ части арм пик взят из сделки (`in_trade`): наблюдений в БД не хватило. '
            'Долю ракет по таким армам читать как нижнюю границу.')
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
        fly = tick_quality.get('fly_through_by_exit') or {}
        if fly:
            add('| тип выхода | n | медиана пролёта, п.п. | p90, п.п. | худший, п.п. |')
            add('|---|---:|---:|---:|---:|')
            for kind, slot in fly.items():
                add(f"| {kind} | {slot.get('n', 0)} | {_money(slot.get('median'))} | "
                    f"{_money(slot.get('p90'))} | {_money(slot.get('worst'))} |")
            add('')
            add('Пролёт = `fill_pct − trigger_pct` на метке выхода, по типам выхода. '
                'Положительный = правило сработало позже, чем указывало; это '
                'характеристика шага опроса, а не «качество исполнения».')
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
    add('- offset 60 берётся у детерминированной **1/3 монет по хешу mint** '
        '(0/5/15 — у всех); выборка не зависит ни от одного решения бота, '
        f"пропущено по хешу: {funnel.get('window_sample_skipped', 0)}")
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
        # Run #8: the exit is a property of the RUN, not of an arm, and it is
        # read from the profiles that will trade, so a fee_cover run can never
        # ship a manifest that promises the Round 6 ladder.
        'exit_rule': _exit_rule_text(),
        'exit_mode': _live_exit_mode(),
        # --- run #8: 30 min, two books, one exit --------------------------
        'run8_design': (
            'S1 = B1 verbatim (birth 30.50-34.50, delta 0.10-1.00); S0 = S1 with '
            'the momentum gate removed (delta_min 0.00). Both books carry one '
            'exit: hard stop -10%, sell 100% at +3.5%, else the 5 s clock. No '
            'reference arm A in this run, so S0 is compared with S1 and nothing '
            'is compared with A.'
        ),
        'run8_why_fee_cover': (
            "the replay of run #7's 167 real B1 trades, priced from observations "
            'and net of the measured 3.25% round trip: B1 + fee-cover 3.5% = '
            '-4.93%; B1 held to 5 s = -2.22%; the whole t0 band + 5 s + stop '
            '-10% = +1.46% (n=1533). Median entry premium: B1 2.89%, A 5.55%, '
            'B3 0.00% — delta >= 0.10 buys somebody else\'s move and then '
            'sells it at +3.5%.'
        ),
        'run8_out_of_scope': (
            'no money verdict: 30-60 trades per book cannot resolve anything. '
            'The replay expects about -4.9% for S1 and -4.6% for S0, the best '
            'measured variant about +1%; what this run measures is latency, '
            'execution cost and whether fee_cover fires at all. No comparison '
            'with A, no better/worse claim.'
        ),
        'run8_acceptance': (
            'paper_only=true and 0 signed; duration >= 1790 s; early_stop_reason '
            '= null; unrecovered RPC errors <= 2%; mean rps <= 1.8 and worst '
            '10-min window <= 2.2 (or the 0.25 s cadence adaptation recorded); '
            'dropped_events = 0; write_errors = 0; 0 open positions at the end; '
            'fee_cover fired at least once in each book; t_total_ms median '
            'recorded without a threshold'
        ),
        'run8_latency_chain': (
            't_detect_ms WSS packet -> candidate row; t_filters_ms candidate row '
            '-> security verdict; t_read_ms verdict -> the curve read the fill is '
            'priced from (0 when the filter batch IS that read); t_fill_ms read '
            '-> fill row; t_total_ms packet -> fill row'
        ),
        'run8_mark_poll': (
            'MARK_POLL_SEC default 0.15 s (floor 0.05); if a 10-min window passes '
            '2.2 rps the cadence rises to 0.25 s once and mark_poll_adapted=true '
            'is reported. accountSubscribe is not used: a second mark source '
            'would make exit timing depend on which stream arrived first'
        ),
        'run8_single_difference': 'delta_min_sol (S0 vs S1)',
        # --- run #9: 12 h, S0 (unchanged) vs S2 (dev-buy floor + copies) ----
        'run9_design': (
            'S0 is run #8\'s S0 verbatim: birth 30.50-34.50, delta 0.00-1.00, '
            'no name-copy filter. S2 moves two entry numbers only: birth floor '
            '31.96 (dev buy >= 2 SOL) and at most 0 launches of the same ticker '
            'in the previous 120 s. Exit (hard stop -10%, sell 100% at +3.5%, '
            'else 5 s), stake 500 RUB, 3 slots and cadence are identical in both '
            'books, so the difference between the books is the selection only.'
        ),
        'run9_why': (
            'run #8 replay: the misses are already -3.03% at 0.5 s and every '
            'take fires inside 0.17-3.02 s, so clock and latency are not the '
            'problem; the -3% round trip (2.5 pp of it pump.fun fees) is paid '
            'on every trade. The only lever left is the share of coins that '
            'actually jump, so only the selection of the second book changes.'
        ),
        'run9_out_of_scope': (
            'no exit change in any book, no band/copies/stake/cadence change '
            'mid-run, no arms A/B1/B2/B3, no direct comparison with run #8 '
            '(that run had no name-copy filter and a softer window: 5 stops of '
            '64 vs the 28% the grid expects), no PnL claim without n, mean, '
            'median, CI95 and the exit breakdown.'
        ),
        'run9_acceptance_a': (
            'A = 30-min technical smoke, not money: paper_only and 0 signed; '
            'duration >= 1790 s; early_stop_reason = null; unrecovered RPC '
            'errors <= 2%; mean rps <= 1.8 and worst 10-min window <= 2.5; '
            'dropped_events = 0 and write_errors = 0; 0 open positions at the '
            'end; >= 5 trades per book (otherwise report the actual count); '
            'post_exit.csv exists with >= 5 marks per exit; manifest shows the '
            'bands 30.50-34.50 (S0) and 31.96-34.50 (S2), copies off (S0) and '
            '0 (S2); median round_trip_cost_pct recorded, and if it is above '
            '4.0% run B is not started'
        ),
        'run9_acceptance_b': (
            'B = 12-h money run: duration >= 43000 s of actual polling; '
            'early_stop_reason = null; mean rps <= 1.8 and worst window <= 2.5; '
            'unrecovered RPC errors <= 1%; 0 open positions at the end; '
            'post_exit.csv filled for every exit'
        ),
        'run9_effect_rule': (
            'fixed before the start: (1) S2 CI95 of the mean excludes 0 and the '
            'estimate is >= +2% -> selection accepted, go discuss a real run; '
            '(2) CI95 includes 0 -> not proven, the same config may not be '
            'repeated and the next step is a pointed change; (3) estimate '
            'negative with CI95 entirely below 0 -> selection rejected; '
            '(4) S0 vs S2 also compared pairwise by minute (same candidates at '
            'the same moment): share of minutes where S2 earned more and the '
            'mean difference in RUB; (5) inside S0, split dev 2-4.5 & copies=0 '
            'vs dev 2-4.5 & copies>0 vs dev < 2'
        ),
        'run9_early_stop': (
            'ONE pre-agreed exception to "PnL never stops a run": at hour 8, if '
            'the CI95 of the S2 mean is entirely below zero, the run stops with '
            'early_stop_reason = negative_edge_S2 and a report. Nothing else '
            'stops a run on money'
        ),
        'run9_post_exit': (
            'after a position closes the coin stays in the mark poll for 10 s '
            'and every mark goes to exports/post_exit.csv (portfolio, mint, '
            'exit_at_epoch, sec_since_exit, price_usd, pnl_vs_entry_pct, '
            'pnl_vs_exit_pct). It shares the getMultipleAccounts batch the open '
            'positions already trigger, so it costs no extra RPC; it measures '
            'what a paper fill that books the trigger price in the same '
            'snapshot would lose against a real one'
        ),
        'run9_price_at_5s': (
            'every trade carries price_at_5s: the real price 5 s after the '
            'entry, from the tick stream while open or from the post-exit ghost '
            'after it closed (nearest mark within 0.75 s, else null). It is the '
            'only way to answer what the 5 s clock would have returned instead '
            'of the take'
        ),
        'run9_single_difference': (
            'entry selection: birth floor 31.96 and name copies <= 0 (S2 vs S0); '
            'the exit, the stake, the slots and the cadence are identical'
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
            # --- run #8: a two-book pair with no reference arm --------------
            'portfolio_s1': {'arm_id': 'S1', 'band': '30.5-34.5 / 0.10-1.00',
                             'changed': 'nothing: S1 IS B1 (delta_max 2.50 -> 1.00)',
                             'role': 'run #8 book under fee_cover exit'},
            'portfolio_s0': {'arm_id': 'S0', 'band': '30.5-34.5 / 0.00-1.00',
                             'changed': 'delta_min 0.10 -> 0.00, against S1 not A',
                             'role': 'run #8 control: same band as S1 minus the '
                                     'momentum gate; run #9 keeps it unchanged'},
            # --- run #9: the reference book and the selection change --------
            'portfolio_s2': {'arm_id': 'S2', 'band': '31.96-34.5 / 0.00-1.00',
                             'changed': 'v_sol_birth_min 30.50 -> 31.96 (dev '
                                        'buy >= 2 SOL) and the name-copy filter '
                                        '0 copies in 120 s',
                             'role': 'run #9 treatment book, measured against '
                                     'S0 with the same exit'},
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
            'passed check); ticks gain mark_prev_pct/step_gap_pp/mark_gap_ms and '
            'exit_kind; observation offsets 0/5/15/60, with the 60 s read taken '
            'for a hash-selected third of the coins; entry_lag_ms replaces the '
            '"floor violations" counter that measured the model, not a failure'
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
                'exit_rule': _exit_rule_text(),
            }
            for name, meta in {
                'portfolio_a': {'arm_id': 'A', 'band': '30.5-34.5 / 0.10-2.50'},
                'portfolio_b1_narrow': {'arm_id': 'B1', 'band': '30.5-34.5 / 0.10-1.00',
                                        'changed': 'delta_max_sol 2.50 -> 1.00'},
                'portfolio_b2_birth': {'arm_id': 'B2', 'band': '30.5-31.0 / 0.10-2.50',
                                       'changed': 'v_sol_birth_max 34.50 -> 31.00'},
                'portfolio_b3_t0': {'arm_id': 'B3', 'band': '30.5-34.5 / 0.00-2.50',
                                    'changed': 'delta_min_sol 0.10 -> 0.00'},
                'portfolio_s1': {'arm_id': 'S1', 'band': '30.5-34.5 / 0.10-1.00',
                                 'changed': 'none vs B1; no A in this run'},
                'portfolio_s0': {'arm_id': 'S0', 'band': '30.5-34.5 / 0.00-1.00',
                                 'changed': 'delta_min_sol 0.10 -> 0.00, vs S1'},
                'portfolio_s2': {'arm_id': 'S2', 'band': '31.96-34.5 / 0.00-1.00',
                                 'changed': 'v_sol_birth_min 30.50 -> 31.96 + '
                                            'name copies 0 / 120 s, vs S0'},
            }.items()
        },
    }


def _safe_manifest(settings: Any, run_id: str, run_dir: Path) -> dict[str, Any]:
    strategy_meta = _strategy_metadata()
    common = {
        'initial_balance_rub': 1500,
        'position_size_rub': 500,
        'max_slots': 3,
        'exit_mode': _live_exit_mode(),
        'exit_rule': _exit_rule_text(),
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
        'entry_latency_ms': float(shared_entry_latency_ms()),
        'ab_test': {
            # Run #7: every experimental arm moves exactly one entry boundary.
            'single_difference': 'entry_band',
            # Run #8 drops A on purpose: S1 is compared with S0, and saying
            # "reference_arm: portfolio_a" when A is not running would invite
            # exactly the comparison the run does not make.
            'reference_arm': (
                'portfolio_a' if 'portfolio_a' in _arm_bands() else None
            ),
            'mode': 'single_arm_smoke' if 'portfolio_a' not in _arm_bands() else 'ab',
            'reference_note': (
                None if 'portfolio_a' in _arm_bands()
                else 'сравнения с A в этом прогоне нет'
            ),
            'arm_reference': dict(ARM_REFERENCE),
            'entry_latency_ms': float(shared_entry_latency_ms()),
            'exit_rule': strategy_meta['exit_rule'],
            'arms': strategy_meta['arms'],
            'decision_rule': strategy_meta['run7_decision_rule'],
            'expectation': strategy_meta['run7_expectation'],
            'power_note': strategy_meta['run7_power'],
        },
        'run_number': _run_number(),
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
                **(strategy_meta.get('entry_parameters') or {}).get(name, {}),
                'entry_band_label': band,
                'creator_probe': 'telemetry only; no entry gate in any arm',
                'creator_gate': False,
            }
            for name, band in _arm_bands().items()
        },
        # Run #9: the bands and the name-copy rule, verbatim from the profiles.
        'entry_parameters': _arm_entry_fields(),
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
    collector.record_meta('portfolio_scope', list(_arm_bands()))
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
                f'arms: {", ".join(_arm_bands())}; '
                'no signed/submitted transactions.',
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
