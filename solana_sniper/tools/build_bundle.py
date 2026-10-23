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
SOLANA PUMP.FUN — PAPER A/B — ЛОКАЛЬНЫЙ ПРОГОН (8 часов по умолчанию)
=======================================================

ЧТО ЭТО
  Бумажный (paper-only) прогон на Pump.fun: ни одна транзакция не
  подписывается и не отправляется, RPC — только чтение.
  Четыре арма, у каждого свой банк 1500 ₽ и 3 слота по 500 ₽:

    A  portfolio_a         30.50-34.50 / 0.10-2.50   эталон, не меняется
    B1 portfolio_b1_narrow 30.50-34.50 / 0.10-1.00   потолок delta ниже
    B2 portfolio_b2_birth  30.50-31.00 / 0.10-2.50   потолок рождения ниже
    B3 portfolio_b3_t0     30.50-34.50 / 0.00-2.50   вход на первом чтении (t0)

  Каждый экспериментальный арм отличается от A РОВНО одной границей входа.
  Выход, момент входа, слоты, ставка и частота опроса — одинаковые.

ЗАПУСК
  Windows:      двойной клик по START_WINDOWS.bat
  macOS/Linux:  bash START_MAC_LINUX.sh

  Скрипт сам создаст .venv, поставит зависимости и спросит HELIUS_API_KEY
  (ключ живёт только в памяти процесса, в файлы не пишется).

  Проверочный короткий прогон:
      bash START_MAC_LINUX.sh --smoke 150
      START_WINDOWS.bat --smoke 150

  Длительность: DURATION_SEC=10800 для 3 часов, по умолчанию 28800 (8 часов).
  Окно терминала НЕ закрывать: процесс идёт в foreground.
  Остановка — Ctrl+C: бот завершится аккуратно, без синтетических выходов.

ГДЕ СМОТРЕТЬ
  Дашборд открывается сам: http://127.0.0.1:8767/ — по вкладке на каждый арм,
  плюс «Сравнение арм» и «Логи».
  Данные прогона: runs/rocket2-ab-paper-<RUN_ID>/
      wallet_portfolio_a.json / wallet_portfolio_b1_narrow.json /
      wallet_portfolio_b2_birth.json / wallet_portfolio_b3_t0.json
      exports/ticks.csv    — своя лента меток каждой позиции
                             (price_usd — цена в USD как есть; mark_prev_pct,
                             step_gap_pp, mark_gap_ms — пролёт правила)
      exports/enrich.csv   — пассивные признаки монет (0 RPC, на решения не влияет)
      SUMMARY.md           — итоговый отчёт: армы, парный тест, P0, блоки, ракеты
      ab_summary.json      — машинные итоги
      telemetry.sqlite     — вся воронка решений
      sniper.db            — закрытые сделки
      run.log              — лог

В ЧЁМ ЭКСПЕРИМЕНТ (прогон №7)
  Гипотеза проверяется ПАРНО: берутся только монеты, которые торговали оба
  арма, и считается среднее разностей (arm − A). Арм подтверждён, если
  paired mean >= +8 ₽, интервал 98.75 % (Bonferroni на три гипотезы) не
  содержит нуля и знак совпал в обеих половинах прогона.
  Ожидания, записанные ДО старта: B1 +1…+6 ₽, B2 −8…+8 ₽, B3 +3…+9 ₽.
  Если все три в пределах ±5 ₽ — подбор полос исчерпан.
  Парная полуширина CI ±15 ₽ при ~165 парах, поэтому разница меньше ~15 ₽ на 3 часах
  нерешаема (порог +8 ₽ после Bonferroni требует ~1700 пар); «стало прибыльно»
  по этому прогону сказать нельзя.

ЧТО ПРОВЕРИТЬ ПОСЛЕ ПРОГОНА
  1) SUMMARY.md: таблица арм (парная разница + CI), таблица P0, критерии.
  2) P0: early_stop_reason = null, неисправленные ошибки RPC <= 2 %,
     dropped_events = 0, write_errors = 0, requests_per_sec <= 1.8,
     security_fail_skipped_deep > 0, теней нет, enrich.csv записан.
  3) В telemetry.sqlite (decisions) стадии <arm>_signal: live_momentum_entry
     против birth_out_of_band / momentum_too_late / no_live_momentum.
  4) Банк: top_ups / capital_injected_rub / equity_no_topups / bust_at_trade
     по каждому арму.
  5) Досрочная остановка: manifest.early_stop_reason — техническая причина
     stop-list с численным условием; PnL остановкой не является.

ОГРАНИЧЕНИЯ
  * Один процесс за раз (лимит Helius RPS). Второй запуск будет отклонён.
  * 3 слота × 500 ₽ на арм, старт 1500 ₽, пополнение только при cash < 500 ₽.
  * Наблюдения 0/5/15/60 с; отброшенные security читаются только на 0 и 15 с.
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
