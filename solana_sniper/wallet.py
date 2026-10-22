"""
wallet.py — виртуальный кошелёк «Челлендж 1500» (файл wallet.json).

Хранит баланс, несгораемый СЕЙФ (vault) и историю сделок в JSON,
поэтому данные переживают перезапуски бота.

Формат wallet.json (пример):
{
  "balance_rub": 1650.0,
  "vault_rub": 0.0,
  "initial_balance": 1500.0,
  "currency": "RUB",
  "position_parts": 3,
  "max_trade_rub": 1500.0,
  "next_vault_at": 5000.0,
  "vault_share": 0.5,
  "vault_step": 2500.0,
  "total_pnl": 150.0,
  "total_swept_rub": 0.0,
  "fees_paid_rub": 9.04,
  "trades": [
    {
      "time": "11:30",
      "mint": "66Xg…pump",
      "source": "Pump.fun",
      "entry_rub": 507.5,
      "exit_rub": 657.77,
      "pnl_rub": 150.28,
      "fees_rub": 9.04,
      "reason": "TP1 + Trailing Stop"
    }
  ]
}
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_BALANCE = 1500.0
PARTS = 3  # на сколько частей делим баланс (заходи по частям!)


# ПЛАН:
# 1. Wallet._load_or_create()   — прочитать wallet.json или создать
# 2. Wallet.position_size()     — min(1/3 баланса, кап MAX_TRADE_RUB)
# 3. Wallet.open_position()     — списать вход при открытии сделки
# 4. Wallet.close_position()    — зачислить чистый возврат от продажи
# 5. Wallet.update_wallet_and_vault() — вехи Сейфа + запись сделки
# 6. Wallet.record_trade()      — дописать сделку в историю
# 7. Wallet.save()              — сохранить всё в wallet.json

class Wallet:
    # ПЛАН:
    # 1. Запомнить путь к файлу и параметры Сейфа
    # 2. Загрузить кошелёк (или создать со стартового баланса)
    def __init__(
        self,
        path: str,
        initial: float = DEFAULT_BALANCE,
        max_trade_rub: float = 1500.0,
        vault_start: float = 5000.0,
        vault_step: float = 2500.0,
        vault_share: float = 0.5,
    ):
        self.path = Path(path)
        self.initial_balance = initial
        self.balance_rub = initial
        self.vault_rub = 0.0
        self.max_trade_rub = max_trade_rub
        self.next_vault_at = vault_start
        self.vault_step = vault_step
        self.vault_share = vault_share
        self.total_pnl = 0.0
        self.total_swept_rub = 0.0
        self.fees_paid_rub = 0.0
        self.trades: List[Dict[str, Any]] = []
        # Открытые позиции — чтобы пережить перезапуск бота
        self.open_positions: List[Dict[str, Any]] = []
        self._load_or_create()

    def _load_or_create(self) -> None:
        # ПЛАН:
        # 1. Файл есть — прочитать JSON и восстановить поля
        # 2. Иначе — создать файл со стартовым балансом
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding='utf-8'))
                self.initial_balance = float(
                    data.get('initial_balance', self.initial_balance)
                )
                self.balance_rub = float(data.get('balance_rub', 0.0))
                self.vault_rub = float(data.get('vault_rub', 0.0))
                self.total_pnl = float(data.get('total_pnl', 0.0))
                self.total_swept_rub = float(
                    data.get('total_swept_rub', 0.0)
                )
                self.fees_paid_rub = float(data.get('fees_paid_rub', 0.0))
                self.next_vault_at = float(
                    data.get('next_vault_at', self.next_vault_at)
                )
                self.trades = list(data.get('trades', []))
                self.open_positions = list(
                    data.get('open_positions', [])
                )
                return
            except (ValueError, json.JSONDecodeError):
                pass  # мусор в файле — пересоздаём
        self.save()

    def position_size(self) -> float:
        # ПЛАН: 1/3 баланса, но не выше капа (против проскальзывания
        # на пулах с низкой ликвидностью)
        return min(self.balance_rub / PARTS, self.max_trade_rub)

    def open_position(
        self, size_rub: float, pos_dict: Optional[Dict[str, Any]] = None
    ) -> None:
        # ПЛАН:
        # 1. Одним обновлением записать позицию и списать её полную стоимость
        # 2. Добавить входную сетевую комиссию из записи позиции в агрегат
        # 3. Сохранить кошелёк, чтобы при сбое не потерять одну сторону записи
        entry_fee_rub = 0.0
        if pos_dict is not None:
            self.open_positions = [
                pos for pos in self.open_positions
                if pos.get('mint') != pos_dict.get('mint')
            ]
            if pos_dict.get('sold_frac', 0.0) < 1.0 - 1e-9:
                self.open_positions.append(pos_dict)
            entry_fee_rub = float(pos_dict.get('fees_rub', 0.0) or 0.0)
        self.balance_rub -= size_rub
        self.fees_paid_rub += entry_fee_rub
        self.save()

    def close_position(
        self,
        returned_rub: float,
        pnl_rub: float,
        fees_rub: float,
        pos_dict: Optional[Dict[str, Any]] = None,
    ) -> None:
        # ПЛАН:
        # 1. Зачислить только подтверждённую quote-выручку за вычетом сети
        # 2. Обновить позицию вместе с балансом, исключая повторную продажу после сбоя
        # 3. Прибавить PnL и сетевые комиссии и сохранить кошелёк
        self.balance_rub += returned_rub
        self.total_pnl += pnl_rub
        self.fees_paid_rub += fees_rub
        if pos_dict is not None:
            self.open_positions = [
                pos for pos in self.open_positions
                if pos.get('mint') != pos_dict.get('mint')
            ]
            if pos_dict.get('sold_frac', 0.0) < 1.0 - 1e-9:
                self.open_positions.append(pos_dict)
        self.save()

    async def update_wallet_and_vault(
        self,
        mint: str,
        source: str,
        entry_rub: float,
        exit_rub: float,
        pnl_rub: float,
        fees_rub: float,
        reason: str,
    ) -> float:
        # ПЛАН:
        # 1. Проверить вехи Сейфа (5000, 7500, 10000…):
        #    баланс >= веха → 50% ЧИСТОЙ прибыли (сверх депозита)
        #    уносит в несгораемый vault_rub
        # 2. Деньги Сейфа заблокированы для торговли
        # 3. Дописать сделку в историю и сохранить wallet.json
        # 4. Вернуть сумму, унесённую в Сейф
        swept = 0.0
        while self.balance_rub >= self.next_vault_at:
            profit = self.balance_rub - self.initial_balance
            if profit <= 0:
                break
            part = profit * self.vault_share
            self.vault_rub += part
            self.balance_rub -= part
            self.total_swept_rub += part
            self.next_vault_at += self.vault_step
            swept += part

        self.record_trade(
            mint, source, entry_rub, exit_rub, pnl_rub, fees_rub, reason
        )
        return swept

    def record_trade(
        self,
        mint: str,
        source: str,
        entry_rub: float,
        exit_rub: float,
        pnl_rub: float,
        fees_rub: float,
        reason: str,
    ) -> None:
        # ПЛАН:
        # 1. Собрать запись о сделке
        # 2. Дописать в историю
        # 3. Сохранить wallet.json
        self.trades.append({
            'time': datetime.now().strftime('%H:%M'),
            'mint': mint,
            'source': source,
            'entry_rub': round(entry_rub, 2),
            'exit_rub': round(exit_rub, 2),
            'pnl_rub': round(pnl_rub, 2),
            'fees_rub': round(fees_rub, 2),
            'reason': reason,
        })
        self.save()

    def sync_open_position(self, pos_dict: Dict[str, Any]) -> None:
        # ПЛАН:
        # 1. Заменить запись позиции (по mint) свежей
        # 2. Если позиция полностью продана — удалить запись
        # 3. Сохранить wallet.json
        self.open_positions = [
            p for p in self.open_positions
            if p.get('mint') != pos_dict.get('mint')
        ]
        if pos_dict.get('sold_frac', 0.0) < 1.0 - 1e-9:
            self.open_positions.append(pos_dict)
        self.save()

    def save(self) -> None:
        # ПЛАН: сохранить всё состояние кошелька в JSON
        data = {
            'balance_rub': round(self.balance_rub, 2),
            'vault_rub': round(self.vault_rub, 2),
            'initial_balance': round(self.initial_balance, 2),
            'currency': 'RUB',
            'position_parts': PARTS,
            'max_trade_rub': self.max_trade_rub,
            'next_vault_at': self.next_vault_at,
            'vault_share': self.vault_share,
            'vault_step': self.vault_step,
            'total_pnl': round(self.total_pnl, 2),
            'total_swept_rub': round(self.total_swept_rub, 2),
            'fees_paid_rub': round(self.fees_paid_rub, 2),
            'open_positions': self.open_positions,
            'trades': self.trades,
        }
        self.path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
