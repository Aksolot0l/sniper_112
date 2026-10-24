"""
storage.py — журнал сделок в SQLite (aiosqlite).

Зачем БД, а не JSON-файл: переживает перезапуск, не теряет записи
при сбое, умеет считать статистику (winrate, PnL) прямо запросом.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

import aiosqlite

from console import Color, log
from models import TradeRecord

CREATE_TABLE_SQL = '''
    CREATE TABLE IF NOT EXISTS trades (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        mint        TEXT    NOT NULL,
        source      TEXT    NOT NULL,
        entry_price REAL    NOT NULL,
        exit_price  REAL    NOT NULL,
        pct         REAL    NOT NULL,
        pnl_rub     REAL    NOT NULL,
        reason      TEXT    NOT NULL,
        hold_sec    REAL    NOT NULL,
        created_at  TEXT    NOT NULL,
        portfolio   TEXT    NOT NULL DEFAULT 'main'
    )
'''

INSERT_SQL = '''
    INSERT INTO trades
        (mint, source, entry_price, exit_price, pct, pnl_rub, reason, hold_sec, created_at, portfolio)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

STATS_SQL = '''
    SELECT
        COUNT(*)                                    AS total,
        SUM(CASE WHEN pct >= 0 THEN 1 ELSE 0 END)   AS wins,
        COALESCE(SUM(pnl_rub), 0)                   AS total_pnl,
        COALESCE(MAX(pct), 0)                       AS best_pct,
        COALESCE(MIN(pct), 0)                       AS worst_pct
    FROM trades
'''


# ПЛАН:
# 1. storage_worker() — воркер: очередь сделок → INSERT в SQLite
# 2. get_stats()      — агрегаты по таблице: сделки, победы, PnL

async def storage_worker(
    trades_queue: 'asyncio.Queue[TradeRecord]', db_path: str
) -> None:
    # ПЛАН:
    # 1. Открыть базу и создать таблицу, если её ещё нет
    # 2. Бесконечно брать сделки из очереди (get() — СТРОГО вне try)
    # 3. Вставлять запись и сразу коммитить (не терять при сбое)
    # 4. ВСЕГДА отмечать обработку (task_done() — в finally)
    async with aiosqlite.connect(db_path) as db:
        await db.execute(CREATE_TABLE_SQL)
        cursor = await db.execute('PRAGMA table_info(trades)')
        columns = {row[1] for row in await cursor.fetchall()}
        if 'portfolio' not in columns:
            await db.execute("ALTER TABLE trades ADD COLUMN portfolio TEXT NOT NULL DEFAULT 'main'")
        await db.commit()
        log(f'💾 Журнал сделок готов: {db_path}', Color.GREEN)

        while True:
            record = await trades_queue.get()
            try:
                await db.execute(
                    INSERT_SQL,
                    (
                        record.mint,
                        record.source,
                        record.entry_price,
                        record.exit_price,
                        record.pct,
                        record.pnl_rub,
                        record.reason,
                        record.hold_sec,
                        record.created_at,
                        str(getattr(record, 'portfolio', 'main') or 'main'),
                    ),
                )
                await db.commit()
            finally:
                trades_queue.task_done()


async def get_stats(db_path: str) -> Dict[str, Any]:
    # ПЛАН:
    # 1. Открыть базу и выполнить агрегирующий запрос
    # 2. Достать строку результата (total, wins, pnl, best, worst)
    # 3. Вернуть словарь со статистикой
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(STATS_SQL) as cursor:
            row = await cursor.fetchone()
    total, wins, total_pnl, best_pct, worst_pct = row
    return {
        'total': int(total or 0),
        'wins': int(wins or 0),
        'total_pnl': float(total_pnl or 0.0),
        'best_pct': float(best_pct or 0.0),
        'worst_pct': float(worst_pct or 0.0),
    }
