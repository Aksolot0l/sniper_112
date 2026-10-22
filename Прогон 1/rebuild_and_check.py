#!/usr/bin/env python3
"""Пересборка telemetry.sqlite прогона №1 из CSV-выгрузок + базовая проверка.

Зачем: оригинальные `telemetry.sqlite`, `sniper.db`, `run.log`, `manifest.json` и выгрузки
`detections/quotes/creator_attributions` были потеряны. Из шести CSV восстанавливаются пять
таблиц (candidates, decisions, paper_fills, creator_probes, observations) — этого хватает,
чтобы пересчитать воронку, PnL по портфелям и все выводы разбора.

Запуск:  python3 rebuild_and_check.py [папка_с_csv]   (по умолчанию — текущая)
Никаких RPC, никакой сети, только файлы.
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from pathlib import Path

SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS candidates (
        candidate_id TEXT PRIMARY KEY, chain_id TEXT NOT NULL, mint TEXT NOT NULL,
        source TEXT NOT NULL, first_seen_at_utc TEXT NOT NULL, signature TEXT,
        curve TEXT, creator TEXT, program_creator TEXT, tx_payer TEXT,
        name TEXT, symbol TEXT, decimals INTEGER,
        dev_buy_sol REAL, initial_buy_raw INTEGER,
        raw_json TEXT NOT NULL, config_json TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        stage TEXT NOT NULL, decision TEXT NOT NULL, reasons_json TEXT NOT NULL,
        values_json TEXT NOT NULL, occurred_at_utc TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS creator_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        program_creator TEXT, tx_payer TEXT, status TEXT NOT NULL,
        requested_at_utc TEXT NOT NULL, observed_at_utc TEXT NOT NULL,
        latency_ms REAL, signature_count INTEGER, oldest_block_time INTEGER,
        ata_exists INTEGER, token_balance_raw TEXT, token_balance_ui REAL,
        error_type TEXT, error_detail TEXT, values_json TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        target_offset_sec INTEGER NOT NULL, target_at_utc TEXT NOT NULL,
        observed_at_utc TEXT NOT NULL, status TEXT NOT NULL, price_usd REAL,
        price_source TEXT, latency_ms REAL, error_type TEXT)''',
    '''CREATE TABLE IF NOT EXISTS paper_fills (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        quote_event_id TEXT, candidate_id TEXT, mint TEXT NOT NULL,
        source TEXT NOT NULL, occurred_at_utc TEXT NOT NULL, reason TEXT NOT NULL,
        sold_fraction REAL NOT NULL, fill_price_usd REAL NOT NULL,
        net_return_rub REAL NOT NULL, pnl_leg_rub REAL NOT NULL,
        fee_rub REAL NOT NULL, out_amount_raw TEXT, min_out_raw TEXT NOT NULL,
        snapshot_pnl_rub REAL, conservative_100bps_pnl_rub REAL,
        conservative_out_amount_raw TEXT, conservative_min_out_raw TEXT,
        conservative_sold_fraction REAL, conservative_error TEXT,
        price_impact_pct REAL, route_json TEXT NOT NULL,
        paper_only INTEGER NOT NULL CHECK (paper_only = 1),
        portfolio TEXT NOT NULL DEFAULT 'main')''',
)

TABLES = (
    ('candidates', 'candidates.csv'),
    ('decisions', 'decisions.csv'),
    ('paper_fills', 'paper_fills.csv'),
    ('creator_probes', 'creator_probes.csv'),
    ('observations', 'observations.csv'),
)


def rebuild(src: Path, out: Path) -> None:
    for suffix in ('', '-wal', '-shm'):
        p = Path(str(out) + suffix)
        if p.exists():
            p.unlink()
    con = sqlite3.connect(out)
    for statement in SCHEMA:
        con.execute(statement)
    for table, name in TABLES:
        rows = list(csv.DictReader((src / name).open(encoding='utf-8-sig')))
        columns = [c[1] for c in con.execute(f'PRAGMA table_info({table})')]
        common = [c for c in columns if rows and c in rows[0]]
        sql = (f"INSERT OR IGNORE INTO {table} ({','.join(common)}) "
               f"VALUES ({','.join('?' * len(common))})")
        for row in rows:
            con.execute(sql, [row[c] if row[c] != '' else None for c in common])
        con.commit()
        got = con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f'{table:16} csv={len(rows):5d}  db={got:5d}')
    con.close()


def check(out: Path) -> None:
    con = sqlite3.connect(f'file:{out.resolve()}?mode=ro', uri=True)
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    print('\nПортфели (paper_fills):')
    for arm in ('portfolio_a', 'portfolio_b'):
        rows = q("SELECT pnl_leg_rub FROM paper_fills WHERE portfolio=?", ) if False else \
            con.execute("SELECT pnl_leg_rub FROM paper_fills WHERE portfolio=?", (arm,)).fetchall()
        total = sum(float(r[0] or 0) for r in rows)
        trades = con.execute(
            "SELECT COUNT(*) FROM decisions WHERE stage=? AND decision='opened'",
            (f'{arm}_entry',)).fetchone()[0]
        print(f'  {arm:12} открыто={trades:3d}  ног в fills={len(rows):3d}  PnL={total:9.2f} ₽')
    print('\nПричины выходов:')
    for reason, n in con.execute(
            "SELECT reason, COUNT(*) FROM paper_fills WHERE portfolio='portfolio_a' "
            "GROUP BY reason ORDER BY 2 DESC"):
        print(f'  {n:3d}  {reason}')
    print('\nВоронка A:')
    for reason, n in con.execute(
            "SELECT reasons_json, COUNT(*) FROM decisions WHERE stage='portfolio_a_signal' "
            "GROUP BY 1 ORDER BY 2 DESC"):
        print(f'  {n:4d}  {reason}')
    con.close()


if __name__ == '__main__':
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent
    out = src / 'telemetry_rebuilt.sqlite'
    rebuild(src, out)
    check(out)
    print(f'\nготово: {out}')
