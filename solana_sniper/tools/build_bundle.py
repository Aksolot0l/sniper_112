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
    'enrich.py', 'entry_reads.py', 'filters.py', 'live_ab_dashboard.py', 'main_ab.py',
    'make_ab_checkpoint.py',
    'models.py', 'portfolio_ab.py', 'price.py', 'pump_curve_quote.py',
    'pumpportal_validation.py', 'run_local_session.py', 'run_overnight_ab_paper.py',
    'smoke_test.py', 'sqlite_snapshots.py', 'storage.py', 'telemetry.py',
    'trade_logger.py', 'trader.py', 'wallet.py', 'watchlist.py', 'ws_client.py',
    'requirements.txt', 'paper_ab_dashboard.html',
    'run_rocket2_ab_paper.sh', 'run_overnight_ab_paper.sh',
    'START_MAC_LINUX.sh', 'START_WINDOWS.bat', '.gitignore', 'README.md',
]
INCLUDE_GLOBS = ['tests/test_*.py', 'tools/preflight_budget.py',
                 'tools/rpc_decomposition.py']
EXCLUDE_DIRS = {'.venv', 'runs', '__pycache__', 'log_exports', '.git', 'dist', 'exports'}

READ_ME_FIRST = """\
SOLANA PUMP.FUN — PAPER — ПРОГОН №10 (10a: 1 час, затем 10b: 12 часов)
======================================================================

ЧТО ЭТО
  Бумажный (paper-only) прогон на Pump.fun: ни одна транзакция не
  подписывается и не отправляется, RPC — только чтение.
  Две книги, у каждой свой банк 1500 ₽ и 3 слота по 500 ₽.
  Книги различаются РОВНО двумя вещами: нижней границей рождения и
  фильтром копий тикера. Выход у них байт-в-байт одинаковый.

    S0 portfolio_s0  30.50-34.50 / 0.00-1.00 / копии: выкл
                     — книга прогона №8 дословно; это контроль, а не
                       «стало прибыльно»
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

ПОРЯДОК СТРОГО: 10a (1 час) -> приёмка -> 10b (12 часов)
  Оба этапа — один прогон №10: RUN_NUMBER=run10 (он же в манифесте),
  различаются только каталогом и длительностью.

  10a: DURATION_SEC=3600,  --run-id run10a-1h-<UTC>
  10b: DURATION_SEC=43200, --run-id run10b-12h-<UTC>   (только после приёмки 10a)

ЗАПУСК
  Windows:      двойной клик по START_WINDOWS.bat
  macOS/Linux:  bash START_MAC_LINUX.sh

  Скрипт сам создаст .venv, поставит зависимости и спросит HELIUS_API_KEY
  (ключ живёт только в памяти процесса, в файлы не пишется).
  Проверочный короткий прогон: bash START_MAC_LINUX.sh --smoke 150
  Окно терминала НЕ закрывать: процесс идёт в foreground.
  Остановка — Ctrl+C: бот завершится аккуратно, без синтетических выходов.

ГДЕ СМОТРЕТЬ
  Дашборд: http://127.0.0.1:8767/ — по вкладке на книгу, плюс «Сравнение».
  Данные прогона: runs/rocket2-ab-paper-<RUN_ID>/
      wallet_portfolio_s0.json / wallet_portfolio_s2.json
      exports/ticks.csv     — лента меток каждой позиции
      exports/post_exit.csv — призрак после выхода: 8 меток на выход
                              (0.5/1/1.5/2/3/5/7/10 с), поля portfolio, mint,
                              exit_at_epoch, sec_since_exit, price_usd,
                              pnl_vs_entry_pct, pnl_vs_exit_pct
      exports/decisions.csv — воронка + цепочка латентности входа
      exports/enrich.csv    — пассивные признаки монет (0 RPC)
      SUMMARY.md            — отчёт; ab_summary.json — то же машинно
      telemetry.sqlite      — вся воронка решений
      sniper.db             — закрытые сделки
      run.log               — лог

ПОЧЕМУ RPS ТЕПЕРЬ ДРУГОЙ
  Призрак читается по расписанию, а не на каждом цикле меток; цикл, в котором
  нет ни открытой позиции, ни созревшего призрака, не делает запроса вовсе.
  В 10a этого оказалось мало: 6 565 запросов за час (1.82 rps, 2.97 в худшем
  окне), из них 3 094 — двойное чтение одних и тех же четырёх аккаунтов
  (проверка безопасности + наблюдение "+0 с" у одного кандидата). Теперь на
  кандидата за цикл опроса приходится ОДИН батч через кэш entry_reads.py на
  0.25 с, в памяти и без записи на диск. Ожидание: ~5 000 запросов за час,
  ~1.4 rps, в худшем окне ~1.6. Число печатается в отчёте как
  entry_reads_per_candidate, а не считается на глаз.

ЧТО ИЗМЕРЯЕТСЯ
  1) отбор: доля монет, где девелопер купил >= 2 SOL, и доля повторных
     тикеров за 120 с — по воронке, а не по сделкам;
  2) стоимость круглого рейса на сделках, где цена не двигалась
     (round_trip_cost_pct; эталон 3.25 %). Медиана > 4.0 % => 10b не стартует;
  3) price_at_5s у каждой сделки: реальная цена через 5.0 с после входа
     (из призрака, если позиция уже закрыта);
  4) призрак 10 с после выхода: +0.5/+1/+2/+5/+10 с. Отвечает на вопрос,
     который бумажный филл искажает: он бронирует цену триггера в том же
     снимке, а реальная продажа приходит позже;
  5) латентность входа: t_detect_ms / t_filters_ms / t_read_ms / t_fill_ms /
     t_total_ms;
  6) воронка по книгам в порядке проверок: копии -> полоса -> момент, каждая
     строка — доля от числа кандидатов (множества не пересекаются; в 10a
     525 отказов по копиям — это 49.3 % от 1 064 кандидатов, а не 96.5 % от
     544 строк решений), плюс одно итоговое число "кандидатов на 1 сделку";
  7) парные минуты S0 против S2 и разложение S0 на три клетки:
     дев 2-4.5 и копий нет, дев 2-4.5 и копии есть, дев < 2.
  НЕ измеряется: «стало прибыльно». Любое число PnL — только с n, средним,
  медианой, CI95 и разбивкой по выходам.

ПРИЁМКА 10a (техника; деньги не читаются)
  paper_only, signed_transactions = 0; длительность >= 3 500 c фактического
  опроса; early_stop_reason = null; невосстановленные ошибки RPC <= 1 %;
  средний rps <= 1.8 и худшее 10-мин окно <= 2.2; 429 <= 0.5 % запросов;
  dropped_events = 0; write_errors = 0; 0 открытых позиций; у каждого выхода
  записаны ВСЕ плановые точки призрака, недобрано не больше max(2, 1 % от
  плана), медиана меток 8, минимум >= 7 (на смоуке >= 5). "Недобрано" — это
  точка, к которой не нашлось метки в пределах 0.75 с; в 10a потерянных точек
  было ноль, а порог падал из-за опозданий (+10 с пришла на 11.35 с), поэтому
  отчёт печатает ещё и максимальное опоздание и p95 по каждой точке;
  полосы в манифесте S0 30.50-34.50 копии off, S2 31.96-34.50 копии 0;
  сделок S0 >= 80 и S2 >= 8; медиана round_trip_cost_pct записана.
  Час по деньгам не решает ничего: CI95 S2 там шириной около ±51 ₽, поэтому
  гейтить запуск 10b по PnL запрещено.

ПРИЁМКА 10b
  >= 43 000 c реального опроса; early_stop_reason = null; средний rps <= 1.8,
  худшее окно <= 2.2; невосстановленные ошибки <= 1 %; 429 <= 0.5 %;
  0 открытых позиций; post_exit.csv полный; сделок S0 >= 400, S2 >= 80.
  Единственная денежная остановка: на 8-м часу, если CI95 средней S2 целиком
  ниже нуля -> negative_edge_S2.

ПРАВИЛА ВЫВОДА (зафиксированы до старта)
  1) отбор: CI95 средней S2 не включает 0 и оценка >= +10 ₽ -> принят;
     включает 0 -> не доказано, тот же конфиг не повторять;
  2) фильтр копий читается по разнице клеток внутри S0 (дев >= 2: без копий
     против с копиями), разница >= 20 ₽ с CI95, не включающим 0 -> оставить,
     < 10 ₽ -> снять;
  3) пролёт стопа: медиана филла стопа сверяется с -14.5 %, хуже -17 % ->
     пересчитать конфиг;
  4) задержка исполнения: pnl_vs_exit на +0.5 и +1 с;
  5) почасовая разбивка и знак по половинам — по каждой книге.

ОГРАНИЧЕНИЯ
  * Один процесс за раз (лимит Helius RPS). Второй запуск будет отклонён.
  * 3 слота × 500 ₽ на книгу, старт 1500 ₽, пополнение только при cash < 500 ₽
    (число пополнений по каждой книге — в отчёт отдельно).
  * Наблюдения только 0 и 5 с; опрос меток 0,25 с под окно 2.5 rps.
    Поднимать rps, чтобы «дожить до конца», запрещено.
  * Между 10a и 10b не меняется ни одно число: ни выход, ни полосы, ни копии,
    ни ставка, ни каденс.
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
