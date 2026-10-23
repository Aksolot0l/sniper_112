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
SOLANA PUMP.FUN — PAPER — ПРОГОН №9 (30 минут; 12 часов — стадия B)
===================================================================

ЧТО ЭТО
  Бумажный (paper-only) прогон на Pump.fun: ни одна транзакция не
  подписывается и не отправляется, RPC — только чтение.
  Две книги, у каждой свой банк 1500 ₽ и 3 слота по 500 ₽.
  Книги различаются РОВНО двумя вещами: нижней границей рождения и
  фильтром копий тикера. Выход у них байт-в-байт одинаковый.

    S0 portfolio_s0  30.50-34.50 / 0.00-1.00 / копии: выкл
                     — книга прогона №8 дословно (она дала +558.02 ₽
                       за 30 минут); это контроль, а не «стало прибыльно»
    S2 portfolio_s2  31.96-34.50 / 0.00-1.00 / копии <= 0 за 120 с
                     — отбор дева: 31.96 = 29.96 + 2 SOL, то есть вход
                       только там, где девелопер купил >= 2 SOL

  Копии считаются без RPC: словарь «тикер -> моменты запуска», окно 120 с,
  пустой тикер никогда не фильтруется, причина отказа name_copies_120s.

  Выход один для обеих книг (fee_cover), до всех правил Round 6:
    hard stop -10 %  ->  продать 100 %
    +3.5 %           ->  продать 100 % (измеренный круглый рейс 3.25 %)
    иначе 5 с        ->  продать 100 %
  TP1, трейл, shelf, no-progress и таймауты 35/90 с в этом режиме
  не вызываются вовсе. Менять выход внутри прогона нельзя.

ЗАПУСК
  Windows:      двойной клик по START_WINDOWS.bat
  macOS/Linux:  bash START_MAC_LINUX.sh

  Скрипт сам создаст .venv, поставит зависимости и спросит HELIUS_API_KEY
  (ключ живёт только в памяти процесса, в файлы не пишется).

  Проверочный короткий прогон:
      bash START_MAC_LINUX.sh --smoke 150
      START_WINDOWS.bat --smoke 150

  Стадия A: DURATION_SEC=1800 (30 минут, по умолчанию) — это проверка
            техники, а не денег.
  Стадия B: DURATION_SEC=43200 (12 часов) — только после того, как
            приёмка A выполнена и round_trip_cost_pct <= 4.0 %.
  Окно терминала НЕ закрывать: процесс идёт в foreground.
  Остановка — Ctrl+C: бот завершится аккуратно, без синтетических выходов.

ГДЕ СМОТРЕТЬ
  Дашборд открывается сам: http://127.0.0.1:8767/ — по вкладке на каждую книгу,
  плюс «Сравнение» и «Логи».
  Данные прогона: runs/rocket2-ab-paper-<RUN_ID>/
      wallet_portfolio_s0.json / wallet_portfolio_s2.json
      exports/ticks.csv     — своя лента меток каждой позиции
      exports/post_exit.csv — призрак после выхода: 10 с меток на каждый
                              выход (portfolio, mint, sec_since_exit,
                              price_usd, pnl_vs_entry_pct, pnl_vs_exit_pct)
      exports/decisions.csv — воронка + цепочка латентности входа (в архиве
                              прогона лежит усечённой: decisions.csv.gz.part*)
      exports/enrich.csv    — пассивные признаки монет (0 RPC)
      SUMMARY.md            — отчёт прогона №9; ab_summary.json — то же машинно
      telemetry.sqlite      — вся воронка решений
      sniper.db             — закрытые сделки
      run.log               — лог

ЧТО ИЗМЕРЯЕТСЯ
  1) отбор: доля монет, где девелопер купил >= 2 SOL (рождение >= 31.96),
     и доля тех, где тикер запускали повторно за 120 с; обе доли считаются
     по воронке, а не по сделкам;
  2) стоимость круглого рейса на сделках, где цена не двигалась
     (round_trip_cost_pct; эталон 3.25 %). Медиана > 4.0 % => стадию B
     не запускать, чинить стоимость;
  3) price_at_5s у каждой сделки: реальная цена через 5.0 с после входа
     (из призрака, если позиция уже закрыта) — это цена, которую видит
     выход по времени, а не цена триггера;
  4) призрак 10 с после выхода: среднее движение mid после выхода на
     +0.5/+1/+2/+5/+10 с. Отвечает на вопрос, который бумажный филл
     искажает: он бронирует цену триггера в том же снимке;
  5) латентность входа по пяти стадиям: t_detect_ms / t_filters_ms /
     t_read_ms / t_fill_ms / t_total_ms — медиана, p90, max;
  6) воронка и отказы по каждой книге: name_copies, birth_band, delta_max,
     slots_full, no_price;
  7) парные минуты S0 против S2 (доля минут, где S2 > S0, и средняя разница
     в ₽) и разложение S0 на три клетки: дев 2-4.5 и копий нет,
     дев 2-4.5 и копии есть, дев < 2.
  НЕ измеряется:
    «стало прибыльно». Бумажный PnL — это не доказательство: любое число
    идёт только с n, средним, медианой, CI95 и разбивкой по выходам.

ЧТО РЕШИТ СТАДИЯ B (правило зафиксировано ДО запуска)
  1) CI95 средней S2 не включает 0 и >= +2 % -> отбор дева принимается;
  2) CI95 включает 0 -> не доказано, конфиг не повторять;
  3) CI95 целиком < 0 -> S2 отвергнут;
  4) S0 против S2 — отдельный вывод по парным минутам;
  5) внутри S0 — три клетки разложения (дев/копии).
  Ожидание: S0 ~900-1400 сделок, S2 ~350-700; 12 часов дают разрешение
  2.6-3.5 %. В отчёте — ФАКТИЧЕСКОЕ число сделок, а не ожидание.
  Единственная денежная остановка: на 8-м часу, если CI95 S2 целиком
  ниже нуля -> negative_edge_S2.

ПРИЁМКА A (30 минут)
  paper_only, signed_transactions = 0; длительность >= 1790 c;
  early_stop_reason = null; неисправленных ошибок RPC <= 2 %;
  средний rps <= 1.8 и худшее 10-мин окно <= 2.5; dropped_events = 0;
  write_errors = 0; 0 открытых позиций; >= 5 сделок на книгу (иначе в
  отчёт идёт фактическое число); >= 5 меток призрака на выход; в манифесте
  полосы S0 30.50-34.50 и S2 31.96-34.50, копии «выкл» и «0».

ПРИЁМКА B (12 часов)
  >= 43 000 c реального опроса; early_stop_reason = null; средний rps <= 1.8,
  худшее 10-мин окно <= 2.5; неисправленных ошибок RPC <= 1 %;
  0 открытых позиций; post_exit.csv полный.

ОГРАНИЧЕНИЯ
  * Один процесс за раз (лимит Helius RPS). Второй запуск будет отклонён.
  * 3 слота × 500 ₽ на книгу, старт 1500 ₽, пополнение только при cash < 500 ₽
    (факт пополнения записывается).
  * Наблюдения только 0 и 5 с; опрос меток 0,25 с под окно 2.5 rps.
    Поднимать rps, чтобы «дожить до конца», запрещено.
  * Внутри прогона не меняются: выход, полосы, копии, ставка, каденция.
  * Сравнивать напрямую с прогоном №8 нельзя.
  * Ничего не подписывается и не отправляется: signed_transactions = 0.

СБОРКА
  Файл BUILD рядом с этим текстом — хеш кода. В отчёте называть его дословно.
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
