#!/usr/bin/env bash
# PAPER A/B launcher for macOS / Linux.
# Everything runs in the FOREGROUND of this terminal: no nohup, no detached
# process, so nothing is frozen by the sandbox when the answer ends.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=============================================================="
echo " Solana Pump.fun · PAPER A/B · 2ч30м локальный прогон"
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

# Прогон №3: единственное различие плеч — потолок delta. A = 2.5 (как раньше),
# B = 3.0; гейт креатора вынесен в тень shadow_gate с нулевым капиталом.
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-2.5}"
export ENTRY_DELTA_MAX_SOL_B="${ENTRY_DELTA_MAX_SOL_B:-3.0}"

# --smoke 150 → короткая проверка; без аргументов → 8 часов.
# Длительность задаётся переменной DURATION_SEC (например, 10800 для 3 часов).
exec "$VENV_PY" run_local_session.py \
    --duration-sec "${DURATION_SEC:-28800}" \
    --open-browser \
    "$@"
