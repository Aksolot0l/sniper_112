#!/usr/bin/env python3
"""Build the self-contained local-run bundle (zip) for the user's PC.

The bundle contains code only: no keys, no runs, no databases, no logs.
Launchers: START_WINDOWS.bat and START_MAC_LINUX.sh.
"""
from __future__ import annotations

import argparse
import zipfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DIST = PROJECT.parent / 'dist'

INCLUDE_FILES = [
    'config.py', 'console.py', 'creator_probe.py', 'creator_watch.py', 'detector.py',
    'filters.py', 'live_ab_dashboard.py', 'main_ab.py', 'make_ab_checkpoint.py',
    'models.py', 'portfolio_ab.py', 'price.py', 'pump_curve_quote.py',
    'pumpportal_validation.py', 'run_local_session.py', 'run_overnight_ab_paper.py',
    'smoke_test.py', 'sqlite_snapshots.py', 'storage.py', 'telemetry.py',
    'trade_logger.py', 'trader.py', 'wallet.py', 'watchlist.py', 'ws_client.py',
    'requirements.txt', 'paper_ab_dashboard.html',
    'run_rocket2_ab_paper.sh', 'run_overnight_ab_paper.sh',
    'START_MAC_LINUX.sh', 'START_WINDOWS.bat', '.gitignore', 'README.md',
]
INCLUDE_GLOBS = ['tests/test_*.py']
EXCLUDE_DIRS = {'.venv', 'runs', '__pycache__', 'log_exports', '.git', 'dist', 'exports'}

READ_ME_FIRST = """\
SOLANA PUMP.FUN — PAPER A/B — ЛОКАЛЬНЫЙ ПРОГОН (2ч30м)
=======================================================

ЧТО ЭТО
  Бумажный (paper-only) двойной портфель A/B на Pump.fun.
  Ни одна транзакция не подписывается и не отправляется. RPC — только чтение.
  Портфель A: вход по живому импульсу.
  Портфель B: тот же вход + creator-guard (идентификация дева, отсев раг-ферм).

ЗАПУСК
  Windows:      двойной клик по START_WINDOWS.bat
  macOS/Linux:  bash START_MAC_LINUX.sh

  Скрипт сам создаст .venv, поставит зависимости и спросит HELIUS_API_KEY
  (ключ живёт только в памяти процесса, в файлы не пишется).

  Проверочный короткий прогон:
      bash START_MAC_LINUX.sh --smoke 150
      START_WINDOWS.bat --smoke 150

  Окно терминала НЕ закрывать: процесс идёт в foreground.
  Остановка — Ctrl+C: бот завершится аккуратно, без синтетических выходов.

ГДЕ СМОТРЕТЬ
  Дашборд открывается сам: http://127.0.0.1:8767/
  Можно открыть файл paper_ab_dashboard.html напрямую (он сам найдёт монитор
  на 127.0.0.1:8767).
  Данные прогона: runs/rocket2-ab-paper-<RUN_ID>/
      wallet_portfolio_a.json / wallet_portfolio_b.json  — состояние портфелей
      ab_summary.json                                    — итоги
      telemetry.sqlite                                   — вся воронка решений
      sniper.db                                          — закрытые сделки
      run.log                                            — лог

ЧТО ПРОВЕРИТЬ ПОСЛЕ ПРОГОНА
  1) В telemetry.sqlite (таблица decisions) стадии portfolio_a_signal и
     portfolio_b_signal: сколько раз сработало live_momentum_entry против
     no_live_momentum / birth_out_of_band / momentum_too_late.
  2) portfolio_b_creator_gate: pass / fail / censored и причины
     (creator_burst_activity = раг-ферма, creator_history_too_short и т.д.).
  3) Сравнить realized_pnl_rub портфелей A и B за один и тот же период.

ОГРАНИЧЕНИЯ
  * Один процесс за раз (лимит Helius RPS). Второй запуск будет отклонён.
  * 3 слота × 500 ₽ на портфель, старт 1500 ₽.
  * фильтр дублей по символу по символу отключён.
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


def build(out_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f'sniper_ab_paper_local_{stamp}.zip'
    files = iter_files()
    missing = [name for name in INCLUDE_FILES if not (PROJECT / name).is_file()]
    if missing:
        raise SystemExit(f'missing required files: {missing}')
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('READ_ME_FIRST.txt', READ_ME_FIRST)
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
    args = parser.parse_args()
    target = build(Path(args.out))
    size_kb = target.stat().st_size / 1024
    print(f'built: {target} ({size_kb:.1f} KB)')
    with zipfile.ZipFile(target) as archive:
        for info in archive.infolist():
            print(f'  {info.file_size:>8}  {info.filename}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
