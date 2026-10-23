#!/usr/bin/env bash
# PAPER A/B launcher for macOS / Linux.
# Everything runs in the FOREGROUND of this terminal: no nohup, no detached
# process, so nothing is frozen by the sandbox when the answer ends.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=============================================================="
echo " Solana Pump.fun · PAPER · 2 книги S1/S0 · выход fee_cover"
echo " длительность из DURATION_SEC (по умолчанию 30 мин)"
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

# Прогон №8: две книги, эталона A в этом прогоне нет (сравнения с A нет).
# S1 = ровно B1 (30.5–34.5 / 0.10–1.00); S0 = S1 без гейта импульса (0.00–1.00).
# Выход у обеих книг один: hard stop −10 %, покрытие комиссий +3.5 %, иначе 5 с.
export ARMS="${ARMS:-portfolio_s1,portfolio_s0}"
export ALLOW_SINGLE_ARM="${ALLOW_SINGLE_ARM:-1}"
export ENTRY_BIRTH_MIN_SOL="${ENTRY_BIRTH_MIN_SOL:-30.5}"
export ENTRY_BIRTH_MAX_SOL="${ENTRY_BIRTH_MAX_SOL:-34.5}"
export ENTRY_DELTA_MIN_SOL="${ENTRY_DELTA_MIN_SOL:-0.10}"
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-2.5}"
export ENTRY_DELTA_MAX_SOL_S1="${ENTRY_DELTA_MAX_SOL_S1:-1.00}"
export ENTRY_DELTA_MIN_SOL_S0="${ENTRY_DELTA_MIN_SOL_S0:-0.00}"
export EXIT_MODE="${EXIT_MODE:-fee_cover}"
export EXIT_FEE_COVER_PCT="${EXIT_FEE_COVER_PCT:-3.5}"
export EXIT_TIME_SEC="${EXIT_TIME_SEC:-5.0}"
export EXIT_HARD_STOP_PCT="${EXIT_HARD_STOP_PCT:-10.0}"
# Быстрый вход и опрос: без искусственной задержки, метка каждые 0.15 с.
export ENTRY_LATENCY_MS="${ENTRY_LATENCY_MS:-0}"
export MARK_POLL_SEC="${MARK_POLL_SEC:-0.15}"
# Слоты и ставка заморожены: 3 слота по 500 ₽, банк 1 500 ₽ у каждой книги.
export MAX_POSITIONS="${MAX_POSITIONS:-3}"
export MAX_TRADE_RUB="${MAX_TRADE_RUB:-500}"
export DEPOSIT_RUB="${DEPOSIT_RUB:-1500}"
# Пассивный сбор признаков Jupiter datapi (0 RPC, на решения не влияет).
export ENTRY_ENRICH="${ENTRY_ENRICH:-1}"
# Расписание наблюдений в этом прогоне — только 0 и 5 с (бюджет RPS).
export TELEMETRY_OBSERVATION_OFFSETS_SEC="${TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}"

# --smoke 150 → короткая проверка; без аргументов → 30 минут.
# Длительность задаётся переменной DURATION_SEC (например, 10800 для 3 часов).
exec "$VENV_PY" run_local_session.py \
    --duration-sec "${DURATION_SEC:-1800}" \
    --open-browser \
    "$@"
