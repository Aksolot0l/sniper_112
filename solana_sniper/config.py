"""
config.py — настройки проекта.

Секреты передаются только через переменные окружения. Если Helius key не
задан, используются настраиваемые публичные read-only RPC endpoints.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


# ПЛАН:
# 1. Описать все настройки проекта как неизменяемый dataclass
# 2. Прочитать переменные окружения с разумными умолчаниями
# 3. Без Helius key использовать только заданные public read-only RPC endpoints
# 4. Проверить режим работы и вернуть готовые настройки

@dataclass(frozen=True)
class Settings:
    api_key: str
    wss_url: str
    http_rpc_url: str
    deposit_rub: float          # стартовый баланс челленджа, ₽
    take_profit_pct: float      # TP1: рост, при котором продаём 50%, %
    stop_loss_pct: float        # порог запуска SL-выхода; не гарантирует max loss, %
    check_min_sec: float        # минимум секунд между проверками цены
    check_max_sec: float        # максимум секунд между проверками цены
    max_hold_sec: float         # принудительная продажа через N секунд
    mode: str                   # 'one' — одна монета и стоп | 'portfolio' — портфель
    sources: str                # какие площадки слушать: 'all' | 'pumpfun,raydium-cpmm,...'
    max_positions: int          # максимум одновременных позиций (слотов)
    min_liquidity_usd: float    # минимальная ликвидность для фильтра, $
    honeypot_check: bool        # включить предварительную route-проверку фиксированного размера
    max_entry_price_impact_pct: float  # предел impact для точной round-trip quote перед входом
    entry_quote_retries: int    # повторы точных Jupiter quote для свежего пула
    entry_quote_retry_delay_sec: float  # базовая пауза линейного backoff, сек
    indexer_retries: int        # короткие повторы при пустых ответах индексаторов
    indexer_retry_delay_sec: float  # пауза между запросами DexScreener/Gecko, сек
    only_sol_pairs: bool        # искать только пулы с SOL (не USDC/USDT)
    # --- честный PnL: комиссии ---
    protocol_fee_pct: float     # legacy: не вычитаем повторно, quote Jupiter уже учитывает DEX fee
    entry_fee_sol: float        # priority fee / Jito tip на вход, SOL
    exit_fee_sol: float         # priority fee / Jito tip на выход, SOL
    usd_rub_rate: float         # курс USD→RUB (для перевода SOL-комиссий)
    # --- лестница выхода ---
    be_offset_pct: float        # истинный безубыток после TP1, %
    trail_pct: float            # откат от ATH для трейлинг-стопа, %
    stagnation_sec: float       # таймаут застоя, сек
    stagnation_range_pct: float # допустимый размах цены при застое, %
    wallet_path: str            # wallet.json (кошелёк челленджа)
    trade_log_path: str         # текстовый журнал сделок
    # --- быстрый вход и Сейф ---
    max_trade_rub: float        # кап размера сделки, ₽
    vault_start_rub: float      # первая веха Сейфа, ₽
    vault_step_rub: float       # шаг вех Сейфа, ₽
    vault_share: float          # доля чистой прибыли в Сейф
    pump_source: str            # 'pumpportal' | 'helius-logs'
    filter_concurrency: int     # сколько фильтр-наборов одновременно
    db_path: str                # путь к файлу SQLite
    creator_risk_policy: str    # block/warn/off для повторных независимых признаков риска
    # --- вход v3: живой импульс вместо статической дельты ---
    entry_birth_min_sol: float  # нижняя граница виртуальных SOL на рождении кривой
    entry_birth_max_sol: float  # верхняя граница (было 38 — резало реальные ракеты)
    entry_delta_min_sol: float  # минимум притока SOL после создания (0.0 = мёртвая приманка)
    entry_delta_max_sol: float  # верхний предохранитель от позднего входа
    # --- creator guard (только портфель B) ---
    creator_guard_min_sig: int      # минимум транзакций в истории кошелька дева
    creator_burst_window_sec: float # окно: 25+ транзакций за это время = раг-ферма
    creator_cache_ttl_sec: float    # in-memory кэш getSignaturesForAddress, сек


def load_settings() -> Settings:
    # ПЛАН:
    # 1. Прочитать optional Helius key и безопасные read-only RPC endpoints
    # 2. Выбрать Helius URL или публичные endpoints/явно заданные URL
    # 3. Прочитать числовые настройки с умолчаниями
    # 4. Проверить режим ('one' / 'portfolio')
    # 5. Вернуть заполненный dataclass Settings

    api_key = os.getenv('HELIUS_API_KEY', '').strip()
    if api_key:
        default_http = f'https://mainnet.helius-rpc.com/?api-key={api_key}'
        default_wss = f'wss://mainnet.helius-rpc.com/?api-key={api_key}'
        print('RPC provider: Helius (ключ из окружения; значение скрыто).')
    else:
        default_http = 'https://api.mainnet-beta.solana.com'
        default_wss = 'wss://api.mainnet-beta.solana.com'
        print('⚠️ HELIUS_API_KEY не задан — использую публичный RPC без ключа; возможны лимиты провайдера.')

    http_rpc_url = os.getenv('SOLANA_HTTP_RPC_URL', default_http).strip()
    wss_url = os.getenv('SOLANA_WSS_RPC_URL', default_wss).strip()

    mode = os.getenv('MODE', 'one').strip().lower()
    if mode not in ('one', 'portfolio'):
        print(f'⚠️ Неизвестный MODE={mode!r} — используем "one"')
        mode = 'one'

    creator_risk_policy = os.getenv('CREATOR_RISK_POLICY', 'warn').strip().lower()
    if creator_risk_policy not in ('block', 'warn', 'off'):
        print(f'⚠️ Неизвестный CREATOR_RISK_POLICY={creator_risk_policy!r} — используем "warn"')
        creator_risk_policy = 'warn'

    return Settings(
        api_key=api_key,
        wss_url=wss_url,
        http_rpc_url=http_rpc_url,
        deposit_rub=float(os.getenv('DEPOSIT_RUB', '1500')),
        take_profit_pct=float(os.getenv('TAKE_PROFIT_PCT', '30')),
        stop_loss_pct=float(os.getenv('STOP_LOSS_PCT', '10')),
        check_min_sec=float(os.getenv('CHECK_MIN_SEC', '0.5')),
        check_max_sec=float(os.getenv('CHECK_MAX_SEC', '0.5')),
        max_hold_sec=float(os.getenv('MAX_HOLD_SEC', '120')),
        mode=mode,
        sources=os.getenv('SOURCES', 'all'),
        max_positions=int(os.getenv('MAX_POSITIONS', '3')),
        min_liquidity_usd=float(os.getenv('MIN_LIQUIDITY_USD', '500')),
        honeypot_check=os.getenv('HONEYPOT_CHECK', 'on').strip().lower() != 'off',
        max_entry_price_impact_pct=float(
            os.getenv('MAX_ENTRY_PRICE_IMPACT_PCT', '5')
        ),
        entry_quote_retries=min(
            max(int(os.getenv('ENTRY_QUOTE_RETRIES', '0')), 0), 5
        ),
        entry_quote_retry_delay_sec=max(
            float(os.getenv('ENTRY_QUOTE_RETRY_DELAY_SEC', '1.0')), 0.0
        ),
        indexer_retries=min(
            max(int(os.getenv('INDEXER_RETRIES', '0')), 0), 3
        ),
        indexer_retry_delay_sec=max(
            float(os.getenv('INDEXER_RETRY_DELAY_SEC', '1.0')), 0.0
        ),
        only_sol_pairs=os.getenv('ONLY_SOL_PAIRS', 'on').strip().lower() != 'off',
        protocol_fee_pct=float(os.getenv('PROTOCOL_FEE_PCT', '1.25')),
        # 0.001 SOL ≈ обычный своп-тip (Jito-флор 0.0001–0.001);
        # 0.002 — гонка за горячим митом, бумаге не нужен:
        # на позиции 500₽ экономит ~1.9% кругового хода
        entry_fee_sol=float(os.getenv('ENTRY_FEE_SOL', '0.001')),
        exit_fee_sol=float(os.getenv('EXIT_FEE_SOL', '0.0001')),
        usd_rub_rate=float(os.getenv('USD_RUB_RATE', '90')),
        be_offset_pct=float(os.getenv('BE_OFFSET_PCT', '3.5')),
        trail_pct=float(os.getenv('TRAIL_PCT', '15')),
        # 180с вместо 120: ракеты часто консолидируются
        # 2–4 мин перед взлётом; 120с резало их раньше SL
        stagnation_sec=float(os.getenv('STAGNATION_SEC', '180')),
        stagnation_range_pct=float(os.getenv('STAGNATION_RANGE_PCT', '3')),
        wallet_path=os.getenv('WALLET_PATH', 'wallet.json'),
        trade_log_path=os.getenv('TRADE_LOG', 'trades.log'),
        max_trade_rub=float(os.getenv('MAX_TRADE_RUB', '1500')),
        vault_start_rub=float(os.getenv('VAULT_START_RUB', '5000')),
        vault_step_rub=float(os.getenv('VAULT_STEP_RUB', '2500')),
        vault_share=float(os.getenv('VAULT_SHARE', '0.5')),
        pump_source=os.getenv('PUMP_SOURCE', 'pumpportal').strip().lower(),
        # 6 вместо 10: меньше параллельных honeypot-проверок
        # → меньше HTTP 429 от Jupiter на горячих монетах
        filter_concurrency=int(os.getenv('FILTER_CONCURRENCY', '6')),
        db_path=os.getenv('DB_PATH', 'sniper.db'),
        creator_risk_policy=creator_risk_policy,
        entry_birth_min_sol=float(os.getenv('ENTRY_BIRTH_MIN_SOL', '30.5')),
        entry_birth_max_sol=float(os.getenv('ENTRY_BIRTH_MAX_SOL', '50.0')),
        entry_delta_min_sol=float(os.getenv('ENTRY_DELTA_MIN_SOL', '0.10')),
        entry_delta_max_sol=float(os.getenv('ENTRY_DELTA_MAX_SOL', '12.0')),
        creator_guard_min_sig=int(os.getenv('CREATOR_GUARD_MIN_SIG', '25')),
        creator_burst_window_sec=float(os.getenv('CREATOR_BURST_WINDOW_SEC', '3600')),
        creator_cache_ttl_sec=float(os.getenv('CREATOR_CACHE_TTL_SEC', '300')),
    )
