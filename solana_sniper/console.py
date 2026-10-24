"""
console.py — красивый вывод в консоль: время, цвета, короткие адреса.
"""

from __future__ import annotations

import os
from datetime import datetime

os.system('')  # включает ANSI-цвета в современных Windows-терминалах


# ПЛАН:
# 1. Задать палитру цветов (ANSI-коды)
# 2. ts() — текущее время строкой ЧЧ:ММ:СС
# 3. log() — печать строки с меткой времени и цветом
# 4. short() — сократить длинный адрес до вида Abcd…wxyz

class Color:
    RESET = '\033[0m'
    DIM = '\033[2m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    YELLOW = '\033[33m'
    MAGENTA = '\033[35m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    GRAY = '\033[90m'


def ts() -> str:
    # ПЛАН:
    # 1. Взять текущее локальное время
    # 2. Отформатировать как ЧЧ:ММ:СС
    return datetime.now().strftime('%H:%M:%S')


def log(msg: str, color: str = '') -> None:
    # ПЛАН:
    # 1. Собрать строку: серая метка времени + цветное сообщение
    # 2. Напечатать сразу (flush), чтобы видеть в реальном времени
    prefix = f'{Color.GRAY}[{ts()}]{Color.RESET}'
    print(f'{prefix} {color}{msg}{Color.RESET}', flush=True)


def short(addr: str, n: int = 4) -> str:
    # ПЛАН:
    # 1. Если адрес короче 2*n+1 — вернуть как есть
    # 2. Иначе оставить начало и конец, середину заменить на …
    if len(addr) <= 2 * n + 1:
        return addr
    return f'{addr[:n]}…{addr[-n:]}'
