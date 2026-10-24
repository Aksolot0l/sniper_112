"""
trade_logger.py — текстовый журнал сделок «Челлендж 1500».

Каждая закрытая сделка дописывается строкой в файл (trades.log):
[11:30] Монета 3521HXs1…pump. Вход: 500. Выход: 650. Профит: +150. Баланс: 1650.
"""

from __future__ import annotations

from datetime import datetime


def log_trade(
    path: str,
    mint: str,
    entry_rub: float,
    exit_rub: float,
    pnl_rub: float,
    balance_rub: float,
) -> None:
    # ПЛАН:
    # 1. Собрать строку в формате журнала:
    #    [время] Монета mint. Вход: N. Выход: N. Профит: +N. Баланс: N.
    # 2. Дописать в конец файла (режим 'a' — история не затирается)
    line = (
        f'[{datetime.now():%H:%M}] Монета {mint}. '
        f'Вход: {entry_rub:.0f}. Выход: {exit_rub:.0f}. '
        f'Профит: {pnl_rub:+.0f}. Баланс: {balance_rub:.0f}.'
    )
    with open(path, 'a', encoding='utf-8') as f:
        f.write(line + '\n')
