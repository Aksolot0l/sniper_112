"""
models.py — все данные проекта как dataclass.

Здесь только «что хранится». Вся логика — в других модулях
(чистые функции: данные входят, данные выходят).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ПЛАН:
# 1. NewCoin — монета, которую только что создали в блокчейне
# 2. FilterResult — вердикт фильтров безопасности с причинами
# 3. Position — открытая фейк-сделка, которую мы держим
# 4. TradeRecord — закрытая сделка для записи в SQLite

@dataclass
class NewCoin:
    mint: str                     # CA — адрес монеты (главный приз!)
    source: str                   # где создали: 'Pump.fun', 'Raydium CPMM', ...
    signature: str                # подпись транзакции создания
    curve: Optional[str] = None   # бондинг-кривая Pump.fun (для он-чейн цены)
    creator: Optional[str] = None  # legacy attribution alias; not necessarily the Pump program creator
    packet: Dict[str, Any] = field(default_factory=dict)  # данные из WSS-пакета
    found_at: float = field(default_factory=time.time)
    decimals: Optional[int] = None  # число знаков mint из RPC-метаданных
    program_creator: Optional[str] = None  # Pump bonding-curve creator from verified raw[49:81]
    tx_payer: Optional[str] = None  # first transaction signer / payer; provenance kept separately
    curve_raw: Optional[bytes] = field(default=None, repr=False, compare=False)
    curve_observed_at: Optional[float] = None  # first successful curve read used as Watchlist t0
    entry_snapshot: Optional[Dict[str, Any]] = field(default=None, repr=False, compare=False)
    creator_probe_result: Optional[Dict[str, Any]] = field(default=None, repr=False, compare=False)
    # Run #8 §1.3: when the candidate row itself was written. `found_at` is the
    # WSS packet, this is the row in telemetry, and the difference is the
    # detection stage the latency chain reports. Set by the collector.
    candidate_row_epoch: Optional[float] = field(default=None, repr=False, compare=False)


@dataclass
class FilterResult:
    passed: bool
    reasons: List[str] = field(default_factory=list)  # почему пропустили/заблокировали

    def merge(self, other: 'FilterResult') -> 'FilterResult':
        # ПЛАН:
        # 1. Объединить два вердикта: прошли оба — прошли
        # 2. Склеить списки причин в один
        # 3. Вернуть новый FilterResult
        return FilterResult(
            passed=self.passed and other.passed,
            reasons=self.reasons + other.reasons,
        )


@dataclass
class Position:
    mint: str
    source: str
    entry_price: float            # цена входа в USD
    simulated: bool               # True — цена симулировалась (источников не было)
    opened_at: float
    deposit_rub: float            # размер фейк-покупки в рублях (до комиссий)
    curve: Optional[str] = None   # бондинг-кривая (Pump.fun)
    price_origin: str = ''        # откуда взята текущая цена
    # --- «Челлендж 1500»: честные комиссии и лестница выхода ---
    tokens: float = 0.0           # токены в удобных для чтения единицах
    decimals: int = 6             # число знаков mint
    tokens_raw: int = 0           # точное количество токенов в минимальных единицах
    sold_tokens_raw: int = 0      # сколько минимальных единиц уже продано
    invested_rub: float = 0.0     # всего потрачено на вход (с сетевой комиссией)
    fees_rub: float = 0.0         # сетевые комиссии (накопленно)
    realized_rub: float = 0.0     # чистыми вернулось от продаж (накопленно)
    realized_exit_value_usd: float = 0.0  # сумма quote-выручки до сетевых комиссий
    sold_frac: float = 0.0        # какая доля позиции уже продана
    ath_price: float = 0.0        # локальный пик после входа (для трейлинга)
    tp1_done: bool = False        # TP1 отработал (50% продано)?
    last_move_ts: float = 0.0     # время последнего движения > 3%
    ref_price: float = 0.0        # цена на момент last_move_ts (окно застоя)
    exit_pending: bool = False    # выход запрошен, но пока не подтверждён котировкой
    pending_exit_reason: str = ''
    pending_exit_frac: float = 0.0
    last_exit_attempt: float = 0.0
    last_exit_log: float = 0.0
    parsed_curve_data: Dict[str, Any] = field(default_factory=dict)  # кривая из WSS-пакета


@dataclass(frozen=True)
class SwapQuote:
    input_mint: str
    output_mint: str
    in_amount_raw: int
    out_amount_raw: int
    min_out_raw: int
    price_impact_pct: Optional[float] = None
    route_labels: tuple[str, ...] = ()
    context_slot: Optional[int] = None


@dataclass
class TradeRecord:
    mint: str
    source: str
    entry_price: float
    exit_price: float
    pct: float                    # результат в %
    pnl_rub: float                # результат в рублях
    reason: str                   # тейк-профит / стоп-лосс / таймаут ...
    hold_sec: float               # сколько держали
    created_at: str               # время закрытия (ISO-строка для БД)
    portfolio: str = 'main'       # independent paper account label (A/B, main, etc.)
