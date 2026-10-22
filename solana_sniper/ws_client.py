"""
ws_client.py — WebSocket-клиент: уши бота.

Подписывается на логи программ (logsSubscribe) и складывает каждое
уведомление в очередь для детектора. При обрыве — переподключение
с экспоненциальным backoff (1с → 2с → 4с … до 60с).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Tuple

import websockets

from config import Settings
from console import Color, log


# ПЛАН:
# 1. ws_listener() — подключиться, подписаться, слушать, переподключаться

async def ws_listener(
    events_queue: 'asyncio.Queue[Tuple[str, Dict[str, Any]]]',
    settings: Settings,
    programs: Dict[str, str],
    watch_stop: asyncio.Event,
) -> None:
    # ПЛАН:
    # 1. Бесконечный цикл с переподключением (backoff при ошибке)
    # 2. Подключиться к WSS и разослать подписки logsSubscribe
    #    (по одной на каждую программу, commitment: 'processed')
    # 3. Сопоставлять id подписки и имя программы
    # 4. Каждое уведомление класть в очередь: (имя_программы, результат)
    # 5. Если слушать больше не нужно (watch_stop) — закрыть WS и выйти
    # 6. При ошибке — лог, пауза, попытка снова (не молча и не мгновенно)
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                settings.wss_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2 ** 23,
            ) as ws:
                log('✅ WebSocket подключён. Подписываемся на программы…', Color.GREEN)

                pending: Dict[int, str] = {}
                sub_names: Dict[int, str] = {}
                for req_id, (program_id, name) in enumerate(programs.items(), start=1):
                    await ws.send(
                        json.dumps(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'method': 'logsSubscribe',
                                'params': [
                                    {'mentions': [program_id]},
                                    {'commitment': 'processed'},
                                ],
                            }
                        )
                    )
                    pending[req_id] = name

                backoff = 1.0  # соединение живое — сбрасываем задержку

                async for raw in ws:
                    if watch_stop.is_set():
                        # Целевая монета выбрана: поток событий больше не нужен —
                        # закрываем соединение, чтобы не сжигать кредиты RPC.
                        log('📡 Слушать больше не нужно — отключаем WebSocket (экономим кредиты)', Color.CYAN)
                        await ws.close()
                        return

                    msg = json.loads(raw)

                    # Ответ на подписку: {'id': N, 'result': <sub_id>}
                    req_id = msg.get('id')
                    if req_id in pending and 'result' in msg:
                        sub_names[msg['result']] = pending.pop(req_id)
                        if not pending:
                            log(
                                f'📡 Активных подписок: {len(sub_names)} — '
                                ', '.join(sub_names.values()),
                                Color.GREEN,
                            )
                        continue

                    if msg.get('method') != 'logsNotification':
                        continue

                    params = msg.get('params') or {}
                    source = sub_names.get(params.get('subscription'), 'Unknown')
                    # Solana заворачивает полезную нагрузку в {context, value}
                    result = params.get('result') or {}
                    if 'value' in result:
                        result = result.get('value') or {}
                    await events_queue.put((source, result))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f'❌ WebSocket ошибка: {exc}', Color.RED)
            log(f'↻ Переподключение через {backoff:.0f} с…', Color.YELLOW)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
