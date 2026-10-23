#!/usr/bin/env bash
# PAPER A/B launcher for macOS / Linux.
# Everything runs in the FOREGROUND of this terminal: no nohup, no detached
# process, so nothing is frozen by the sandbox when the answer ends.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=============================================================="
echo " Solana Pump.fun · PAPER · 4 арма · длительность из DURATION_SEC (по умолчанию 8 ч)"
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

# Прогон №7: четыре арма, каждый отличается от A ОДНОЙ границей входа.
# A — эталон (30.5–34.5 / 0.10–2.50), выход, слоты и ставка у всех одинаковые.
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-2.5}"
export ENTRY_DELTA_MAX_SOL_B1="${ENTRY_DELTA_MAX_SOL_B1:-1.00}"
export ENTRY_BIRTH_MAX_SOL_B2="${ENTRY_BIRTH_MAX_SOL_B2:-31.00}"
export ENTRY_DELTA_MIN_SOL_B3="${ENTRY_DELTA_MIN_SOL_B3:-0.00}"
# Пассивный сбор признаков Jupiter datapi (0 RPC, на решения не влияет).
export ENTRY_ENRICH="${ENTRY_ENRICH:-1}"
# Расписание наблюдений: 0/5/15/60 с — 4 чтения вместо 6.
export TELEMETRY_OBSERVATION_OFFSETS_SEC="${TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5,15,60}"

# --smoke 150 → короткая проверка; без аргументов → 8 часов.
# Длительность задаётся переменной DURATION_SEC (например, 10800 для 3 часов).
exec "$VENV_PY" run_local_session.py \
    --duration-sec "${DURATION_SEC:-28800}" \
    --open-browser \
    "$@"
