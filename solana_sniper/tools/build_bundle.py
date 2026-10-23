#!/usr/bin/env python3
"""Build the self-contained local-run bundle (zip) for the user's PC.

The bundle contains code only: no keys, no runs, no databases, no logs.
Launchers: START_WINDOWS.bat and START_MAC_LINUX.sh.
"""
from __future__ import annotations

import argparse
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DIST = PROJECT.parent / 'dist'

INCLUDE_FILES = [
    'config.py', 'console.py', 'creator_probe.py', 'creator_watch.py', 'detector.py',
    'enrich.py', 'filters.py', 'live_ab_dashboard.py', 'main_ab.py', 'make_ab_checkpoint.py',
    'models.py', 'portfolio_ab.py', 'price.py', 'pump_curve_quote.py',
    'pumpportal_validation.py', 'run_local_session.py', 'run_overnight_ab_paper.py',
    'smoke_test.py', 'sqlite_snapshots.py', 'storage.py', 'telemetry.py',
    'trade_logger.py', 'trader.py', 'wallet.py', 'watchlist.py', 'ws_client.py',
    'requirements.txt', 'paper_ab_dashboard.html',
    'run_rocket2_ab_paper.sh', 'run_overnight_ab_paper.sh',
    'START_MAC_LINUX.sh', 'START_WINDOWS.bat', '.gitignore', 'README.md',
]
INCLUDE_GLOBS = ['tests/test_*.py', 'tools/preflight_budget.py']
EXCLUDE_DIRS = {'.venv', 'runs', '__pycache__', 'log_exports', '.git', 'dist', 'exports'}

READ_ME_FIRST = """\
SOLANA PUMP.FUN — PAPER — ПРОГОН №8 (30 минут по умолчанию)
=======================================================

ЧТО ЭТО
  Бумажный (paper-only) прогон на Pump.fun: ни одна транзакция не
  подписывается и не отправляется, RPC — только чтение.
  Две книги, у каждой свой банк 1500 ₽ и 3 слота по 500 ₽:

    S1 portfolio_s1  30.50-34.50 / 0.10-1.00   ровно B1
    S0 portfolio_s0  30.50-34.50 / 0.00-1.00   S1 без гейта импульса

  S0 — контроль для S1, а не для A. Арма A в этом прогоне НЕТ:
  manifest.reference_arm = null, mode = single_arm_smoke,
  «сравнения с A в этом прогоне нет».

  Выход один для обеих книг (fee_cover), до всех правил Round 6:
    hard stop -10 %  ->  продать 100 %
    +3.5 %           ->  продать 100 % (измеренный круглый рейс 3.25 %)
    иначе 5 с        ->  продать 100 %
  TP1, трейл, shelf, no-progress и таймауты 35/90 с в этом режиме
  не вызываются вовсе.

ЗАПУСК
  Windows:      двойной клик по START_WINDOWS.bat
  macOS/Linux:  bash START_MAC_LINUX.sh

  Скрипт сам создаст .venv, поставит зависимости и спросит HELIUS_API_KEY
  (ключ живёт только в памяти процесса, в файлы не пишется).

  Проверочный короткий прогон:
      bash START_MAC_LINUX.sh --smoke 150
      START_WINDOWS.bat --smoke 150

  Длительность: DURATION_SEC=1800 по умолчанию (30 минут), 10800 = 3 часа.
  Окно терминала НЕ закрывать: процесс идёт в foreground.
  Остановка — Ctrl+C: бот завершится аккуратно, без синтетических выходов.

ГДЕ СМОТРЕТЬ
  Дашборд открывается сам: http://127.0.0.1:8767/ — по вкладке на каждую книгу,
  плюс «Сравнение» и «Логи».
  Данные прогона: runs/rocket2-ab-paper-<RUN_ID>/
      wallet_portfolio_s1.json / wallet_portfolio_s0.json
      exports/ticks.csv    — своя лента меток каждой позиции (0.15 с)
      exports/decisions.csv — воронка + цепочка латентности входа (в архиве
                             прогона лежит усечённой: decisions.csv.gz.part*)
      exports/enrich.csv   — пассивные признаки монет (0 RPC, на решения не влияет)
      SUMMARY.md           — отчёт прогона №8: латентность, стоимость, воронка
      ab_summary.json      — те же блоки машинно
      telemetry.sqlite     — вся воронка решений
      sniper.db            — закрытые сделки
      run.log              — лог

ЧТО ИЗМЕРЯЕТСЯ (и что нет)
  Измеряется:
    1) латентность входа по пяти стадиям: t_detect_ms (пакет WSS -> строка
       candidate), t_filters_ms (строка -> вердикт security), t_read_ms
       (вердикт -> чтение кривой, по которому посчитана цена; 0, потому что
       security-батч и есть это чтение), t_fill_ms (чтение -> филл),
       t_total_ms (пакет -> филл); медиана, p90, max и доля входов < 200 мс;
    2) стоимость: entry_fill_premium и круглый рейс на сделках, где цена не
       двигалась (эталоны прогона №7: 1.41 % и 3.25 %);
    3) цена гейта импульса: entry_premium_pct = (v_sol входа / v_sol_birth)^2 - 1,
       у S1 против S0 (у S0 вход и есть первое чтение);
    4) PnL по времени удержания 1/2/3/5 с и реплей «держать 3 с», «держать 5 с»,
       «5 с + стоп -10 %» — по реальным меткам ticks.csv;
    5) воронка: кандидаты, входы S1/S0, отказы по delta_min/delta_max,
       занятые слоты, отсутствие цены;
    6) одна строка приёмки: paper_only, signed_transactions = 0,
       early_stop_reason, средний rps, худшее 10-мин окно, http_429_count.
  НЕ измеряется:
    деньги. 30-60 сделок на книгу ничего не разрешают (реплей ожидает
    около -4.9 % у S1 и -4.6 % у S0); разница меньше ~15 ₽/сделку нерешаема,
    порог +8 ₽ после Bonferroni требует ~1700 пар. Сравнения с A нет.

ЧТО ПРОВЕРИТЬ ПОСЛЕ ПРОГОНА
  1) SUMMARY.md: таблицы по книгам, выходы, пять стадий латентности,
     стоимость и цена гейта, реплей, воронка.
  2) Приёмка: early_stop_reason = null, длительность >= 1790 c, неисправленные
     ошибки RPC <= 2 %, dropped_events = 0, write_errors = 0, средний rps <= 1.8,
     худшее 10-мин окно <= 2.2 (или mark_poll_adapted = true), 0 открытых
     позиций, fee_cover >= 1 раза в каждой книге, медиана t_total_ms записана.
  3) Банк: top_ups / capital_injected_rub / equity_no_topups / bust_at_trade
     по каждой книге.

ОГРАНИЧЕНИЯ
  * Один процесс за раз (лимит Helius RPS). Второй запуск будет отклонён.
  * 3 слота × 500 ₽ на книгу, старт 1500 ₽, пополнение только при cash < 500 ₽.
  * Наблюдения только 0 и 5 с; опрос меток 0.15 с (при > 2.2 rps в 10-мин
    окне — 0.25 с, это фиксируется в отчёте).
  * Ничего не подписывается и не отправляется: signed_transactions = 0.
"""


def iter_files() -> list[Path]:
    files: list[Path] = []
    for name in INCLUDE_FILES:
        path = PROJECT / name
        if path.is_file():
            files.append(path)
    for pattern in INCLUDE_GLOBS:
        files.extend(sorted(PROJECT.glob(pattern)))
    return files


def _git_commit() -> str:
    """Short hash of the code being packed; 'unknown' outside a git checkout."""
    try:
        return subprocess.run(
            ['git', '-C', str(PROJECT), 'rev-parse', '--short', 'HEAD'],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip() or 'unknown'
    except (OSError, subprocess.SubprocessError):
        return 'unknown'


def build(out_dir: Path, commit: str | None = None) -> Path:
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f'sniper_ab_paper_local_{stamp}.zip'
    files = iter_files()
    missing = [name for name in INCLUDE_FILES if not (PROJECT / name).is_file()]
    if missing:
        raise SystemExit(f'missing required files: {missing}')
    commit = (commit or _git_commit()).strip()
    if not commit:
        commit = 'unknown'
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('READ_ME_FIRST.txt', READ_ME_FIRST)
        # No .git inside the bundle: this file is how a run names its build.
        archive.writestr('BUILD', commit + '\n')
        for path in files:
            rel = path.relative_to(PROJECT).as_posix()
            data = path.read_bytes()
            # Windows batch files must use CRLF; shell scripts must not.
            if path.suffix == '.bat':
                data = data.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
            elif path.suffix == '.sh':
                data = data.replace(b'\r\n', b'\n')
            info = zipfile.ZipInfo(rel, date_time=datetime.now().timetuple()[:6])
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o755 if path.suffix in {'.sh', '.py'} else 0o644) << 16
            archive.writestr(info, data)
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default=str(DIST))
    parser.add_argument('--commit', default=None)
    args = parser.parse_args()
    target = build(Path(args.out), args.commit)
    size_kb = target.stat().st_size / 1024
    print(f'built: {target} ({size_kb:.1f} KB)')
    with zipfile.ZipFile(target) as archive:
        for info in archive.infolist():
            print(f'  {info.file_size:>8}  {info.filename}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
