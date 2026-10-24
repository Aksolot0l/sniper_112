#!/usr/bin/env bash
# PAPER A/B launcher for macOS / Linux.
# Everything runs in the FOREGROUND of this terminal: no nohup, no detached
# process, so nothing is frozen by the sandbox when the answer ends.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=============================================================="
echo " Solana Pump.fun · PAPER · 2 книги S0/S2 · выход fee_cover"
echo " длительность из DURATION_SEC (по умолчанию 30 мин; 12 ч = 43200)"
echo " paper-only: транзакции не подписываются и не отправляются"
echo "=============================================================="

PY="${PYTHON:-python3}"
if ! command -v "$PY" >/dev/null 2>&1; then
    echo "ERROR: python3 не найден. Установи Python 3.10+ и повтори."
    exit 1
fi

if [[ ! -d .venv ]]; then
    echo "→ создаю виртуальное окружение .venv…"
    "$PY" -m venv .venv
fi
VENV_PY=".venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
    echo "ERROR: не удалось создать .venv/bin/python"
    exit 1
fi
if ! "$VENV_PY" -c "import aiohttp, websockets, aiosqlite" >/dev/null 2>&1; then
    echo "→ ставлю зависимости…"
    "$VENV_PY" -m pip install --quiet --upgrade pip
    "$VENV_PY" -m pip install --quiet -r requirements.txt
fi

if [[ -z "${HELIUS_API_KEY:-}" ]]; then
    read -rsp "Вставь HELIUS_API_KEY (ввод скрыт, в файлы не пишется): " HELIUS_API_KEY
    echo
    export HELIUS_API_KEY
fi

# Прогон №9: две книги, эталона A в этом прогоне нет (сравнения с A нет).
# S0 — ровно книга прогона №8 (30.50–34.50 / 0.00–1.00), без фильтра копий.
# S2 — те же числа плюс два изменения входа: пол рождения 31.96 (dev_buy ≥ 2 SOL)
# и запрет входа, если тот же тикер запускали за предыдущие 120 с.
# Выход у обеих книг один и не менялся: hard stop −10 %, покрытие +3.5 %, иначе 5 с.
# Прогон №10: тот же опыт, номер объявлен явно — иначе страница отчёта
# соберётся как run9. 10a (1 ч) и 10b (12 ч) — один номер, разные каталоги.
export RUN_NUMBER="${RUN_NUMBER:-run10}"
export ARMS="${ARMS:-portfolio_s0,portfolio_s2}"
export ALLOW_SINGLE_ARM="${ALLOW_SINGLE_ARM:-1}"
export ENTRY_BIRTH_MIN_SOL="${ENTRY_BIRTH_MIN_SOL:-30.5}"
export ENTRY_BIRTH_MAX_SOL="${ENTRY_BIRTH_MAX_SOL:-34.5}"
export ENTRY_DELTA_MIN_SOL="${ENTRY_DELTA_MIN_SOL:-0.00}"
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-1.00}"
export ENTRY_DELTA_MAX_SOL_S1="${ENTRY_DELTA_MAX_SOL_S1:-1.00}"
export ENTRY_DELTA_MIN_SOL_S0="${ENTRY_DELTA_MIN_SOL_S0:-0.00}"
# По книгам: S0 — как в прогоне №8, S2 — пол 31.96 и ноль копий имени за 120 с.
export ENTRY_BIRTH_MIN_S0="${ENTRY_BIRTH_MIN_S0:-30.50}"
export MAX_NAME_COPIES_S0="${MAX_NAME_COPIES_S0:-off}"
export ENTRY_BIRTH_MIN_S2="${ENTRY_BIRTH_MIN_S2:-31.96}"
export MAX_NAME_COPIES_S2="${MAX_NAME_COPIES_S2:-0}"
export EXIT_MODE="${EXIT_MODE:-fee_cover}"
export EXIT_FEE_COVER_PCT="${EXIT_FEE_COVER_PCT:-3.5}"
export EXIT_TIME_SEC="${EXIT_TIME_SEC:-5.0}"
export EXIT_HARD_STOP_PCT="${EXIT_HARD_STOP_PCT:-10.0}"
# Быстрый вход и опрос: без искусственной задержки, метка каждые 0.25 с
# (в прогоне №8 на 0.15 rps доходил до 3.21 — это и есть причина 0.25).
export ENTRY_LATENCY_MS="${ENTRY_LATENCY_MS:-0}"
export MARK_POLL_SEC="${MARK_POLL_SEC:-0.25}"
export MARK_POLL_RPS_LIMIT="${MARK_POLL_RPS_LIMIT:-2.5}"
# Единственная оговорённая заранее денежная остановка: на 8-м часу, если CI95
# средней S2 целиком ниже нуля. Ничего другого деньги не останавливают.
export EARLY_STOP_NEGATIVE_EDGE_AT_SEC="${EARLY_STOP_NEGATIVE_EDGE_AT_SEC:-28800}"
export EARLY_STOP_NEGATIVE_EDGE_BOOK="${EARLY_STOP_NEGATIVE_EDGE_BOOK:-portfolio_s2}"
# Слоты и ставка заморожены: 3 слота по 500 ₽, банк 1 500 ₽ у каждой книги.
export MAX_POSITIONS="${MAX_POSITIONS:-3}"
export MAX_TRADE_RUB="${MAX_TRADE_RUB:-500}"
export DEPOSIT_RUB="${DEPOSIT_RUB:-1500}"
# Пассивный сбор признаков Jupiter datapi (0 RPC, на решения не влияет).
export ENTRY_ENRICH="${ENTRY_ENRICH:-1}"
# Расписание наблюдений в этом прогоне — только 0 и 5 с (бюджет RPS).
export TELEMETRY_OBSERVATION_OFFSETS_SEC="${TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}"

# --smoke 150 → короткая проверка; без аргументов → 30 минут (прогон A).
# Длительность задаётся переменной DURATION_SEC: 43200 = 12 часов (прогон B).
exec "$VENV_PY" run_local_session.py \
    --duration-sec "${DURATION_SEC:-1800}" \
    --open-browser \
    "$@"
