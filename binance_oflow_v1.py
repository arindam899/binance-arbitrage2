#!/usr/bin/env python3
"""
Binance Futures Order-Flow + Liquidity Sweep Scanner v1

What it does
------------
- Connects to Binance USD-M Futures public WebSocket streams
- Maintains a local order book per symbol
- Computes:
    * rolling delta
    * book imbalance
    * bid/ask refill
    * liquidation bursts
    * equal-high / equal-low liquidity pools
    * liquidity-sweep / failed-breakout signals
    * absorption-style reversal signals
- Sends console alerts
- Optionally sends Telegram alerts
- Logs alerts to CSV

Notes
-----
- This is an alert scanner, not an execution bot.
- It uses public market data only.
- Start with a few liquid symbols: BTCUSDT, ETHUSDT, SOLUSDT.
- Run in paper/alert mode first.

Python
------
Recommended: Python 3.11+

Install
-------
pip install aiohttp websockets orjson python-dotenv

Run
---
python binance_oflow_v1.py

Optional environment variables
------------------------------
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import signal
import statistics
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import aiohttp
import orjson
import websockets

# =========================
# Configuration
# =========================

@dataclass
class ScannerConfig:
    symbols: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    rest_base: str = "https://fapi.binance.com"
    ws_base: str = "wss://fstream.binance.com/stream?streams="
    orderbook_snapshot_limit: int = 1000

    # Timing
    scanner_interval_sec: float = 0.5
    candle_interval_sec: int = 60
    oi_poll_interval_sec: int = 15
    heartbeat_log_interval_sec: int = 30

    # Buffers
    trade_buffer_sec: int = 15
    liquid_buffer_sec: int = 20
    refill_buffer_sec: int = 20
    max_candles: int = 500

    # Depth
    depth_levels_for_imbalance: int = 5

    # Liquidity pool detection
    eq_lookback_candles: int = 40
    eq_min_touches: int = 2
    eq_tolerance_pct_default: float = 0.0008   # 0.08%
    eq_tolerance_pct_map: Dict[str, float] = field(default_factory=lambda: {
        "BTCUSDT": 0.0005,
        "ETHUSDT": 0.0007,
        "SOLUSDT": 0.0010,
    })

    # Sweep logic
    sweep_max_hold_seconds: float = 25.0
    sweep_min_break_pct_default: float = 0.00025
    sweep_min_break_pct_map: Dict[str, float] = field(default_factory=lambda: {
        "BTCUSDT": 0.00015,
        "ETHUSDT": 0.0002,
        "SOLUSDT": 0.00035,
    })
    sweep_reclaim_distance_pct: float = 0.00005

    # Absorption logic
    absorption_max_progress_pct: float = 0.0008  # 0.08%
    micro_reclaim_lookback_sec: float = 5.0

    # Alert scoring thresholds
    min_absorption_score: float = 3.0
    min_sweep_score: float = 3.0
    min_liq_burst_notional: float = 100000.0  # quote notional in last few sec
    min_trade_notional_1s: float = 100000.0

    # Risk / quality filters
    max_spread_bps_map: Dict[str, float] = field(default_factory=lambda: {
        "BTCUSDT": 2.0,
        "ETHUSDT": 3.0,
        "SOLUSDT": 6.0,
    })
    cooldown_after_signal_sec: int = 20
    reconnect_backoff_sec: int = 5
    resync_on_book_gap: bool = True

    # Logging / output
    log_dir: str = "logs"
    csv_filename: str = "signal_log.csv"
    console_table: bool = True
    telegram_enabled: bool = bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))

    # Optional extra settings
    dry_run: bool = True

    def eq_tolerance_for(self, symbol: str) -> float:
        return self.eq_tolerance_pct_map.get(symbol, self.eq_tolerance_pct_default)

    def min_break_pct_for(self, symbol: str) -> float:
        return self.sweep_min_break_pct_map.get(symbol, self.sweep_min_break_pct_default)

    def max_spread_bps_for(self, symbol: str) -> float:
        return self.max_spread_bps_map.get(symbol, 8.0)


CONFIG = ScannerConfig()


# =========================
# Logging setup
# =========================

def setup_logging(log_dir: str) -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "scanner.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


# =========================
# Data models
# =========================

@dataclass
class TradeEvent:
    ts: float
    symbol: str
    price: float
    qty: float
    side: str          # "buy" or "sell" aggressor
    notional: float

@dataclass
class BookTickerEvent:
    ts: float
    symbol: str
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float

@dataclass
class LiquidationEvent:
    ts: float
    symbol: str
    side: str
    price: float
    qty: float
    notional: float

@dataclass
class Candle:
    start_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    quote_volume: float = 0.0

    def update(self, price: float, qty: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += qty
        self.quote_volume += price * qty

@dataclass
class LiquidityLevel:
    level: float
    kind: str  # "equal_highs" or "equal_lows"
    touches: int
    first_seen_ts: float
    last_seen_ts: float
    active: bool = True

@dataclass
class SignalEvent:
    ts: float
    symbol: str
    signal_type: str
    price: float
    level: float
    stop: float
    tp1: float
    tp2: float
    score: float
    delta_1s: float
    delta_5s: float
    buy_notional_1s: float
    sell_notional_1s: float
    imbalance_5: float
    bid_refill_score: float
    ask_refill_score: float
    liq_burst_notional_5s: float
    oi: Optional[float]
    oi_change_pct_15s: Optional[float]
    spread_bps: float
    note: str = ""

    def to_csv_row(self) -> Dict[str, Any]:
        return asdict(self)


# =========================
# Utilities
# =========================

def now_ts() -> float:
    return time.time()

def fmt_pct(x: float, digits: int = 2) -> str:
    return f"{x * 100:.{digits}f}%"

def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0

def bps(a: float, b: float) -> float:
    mid = (a + b) / 2.0
    if mid == 0:
        return 0.0
    return abs(a - b) / mid * 10000.0

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =========================
# CSV logger
# =========================

class CsvSignalLogger:
    def __init__(self, log_dir: str, filename: str):
        self.path = Path(log_dir) / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_file()

    def _init_file(self) -> None:
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(SignalEvent(
                    ts=0, symbol="", signal_type="", price=0, level=0, stop=0, tp1=0, tp2=0,
                    score=0, delta_1s=0, delta_5s=0, buy_notional_1s=0, sell_notional_1s=0,
                    imbalance_5=0, bid_refill_score=0, ask_refill_score=0,
                    liq_burst_notional_5s=0, oi=None, oi_change_pct_15s=None, spread_bps=0,
                    note=""
                ).to_csv_row().keys()))
                writer.writeheader()

    def log(self, signal: SignalEvent) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(signal.to_csv_row().keys()))
            writer.writerow(signal.to_csv_row())


# =========================
# Telegram alert sender
# =========================

class TelegramAlerter:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id)

    async def send(self, session: aiohttp.ClientSession, text: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logging.warning("Telegram send failed with status %s", resp.status)
        except Exception as e:
            logging.warning("Telegram error: %s", e)


# =========================
# Local order book
# =========================

class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: Optional[int] = None
        self.prev_u: Optional[int] = None
        self.is_synced: bool = False
        self.last_sync_ts: Optional[float] = None

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.prev_u = None
        self.is_synced = False
        self.last_sync_ts = None

    def load_snapshot(self, snapshot: Dict[str, Any]) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.prev_u = None
        self.is_synced = False
        self.last_sync_ts = now_ts()

    def apply_depth_event(self, event: Dict[str, Any]) -> bool:
        """
        Binance diff-depth event fields:
        U = first update ID in event
        u = final update ID in event
        pu = final update ID in previous event
        b = bids
        a = asks
        """
        if self.last_update_id is None:
            return False

        U = int(event["U"])
        u = int(event["u"])
        pu = int(event.get("pu", 0))

        if u < self.last_update_id:
            return False

        if not self.is_synced:
            if U <= self.last_update_id <= u:
                self.is_synced = True
            else:
                return False
        else:
            if self.prev_u is not None and pu != self.prev_u:
                self.is_synced = False
                return False

        for p, q in event["b"]:
            p = float(p)
            q = float(q)
            if q == 0.0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for p, q in event["a"]:
            p = float(p)
            q = float(q)
            if q == 0.0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.prev_u = u
        return True

    def top_n(self, n: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return top_bids, top_asks

    def best_bid(self) -> Optional[Tuple[float, float]]:
        bids, _ = self.top_n(1)
        return bids[0] if bids else None

    def best_ask(self) -> Optional[Tuple[float, float]]:
        _, asks = self.top_n(1)
        return asks[0] if asks else None


# =========================
# Order flow engine
# =========================

class RollingTradeStore:
    def __init__(self, max_age_sec: int):
        self.max_age_sec = max_age_sec
        self.trades: Deque[TradeEvent] = deque()

    def add(self, trade: TradeEvent) -> None:
        self.trades.append(trade)
        self.trim(trade.ts)

    def trim(self, now: Optional[float] = None) -> None:
        now = now or now_ts()
        cutoff = now - self.max_age_sec
        while self.trades and self.trades[0].ts < cutoff:
            self.trades.popleft()

    def metrics(self, window_sec: float) -> Dict[str, float]:
        now = now_ts()
        buy_qty = sell_qty = 0.0
        buy_notional = sell_notional = 0.0
        pxs: List[float] = []
        for t in reversed(self.trades):
            if now - t.ts > window_sec:
                break
            pxs.append(t.price)
            if t.side == "buy":
                buy_qty += t.qty
                buy_notional += t.notional
            else:
                sell_qty += t.qty
                sell_notional += t.notional
        return {
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "delta_qty": buy_qty - sell_qty,
            "delta_notional": buy_notional - sell_notional,
            "trade_count": len(pxs),
            "min_price": min(pxs) if pxs else 0.0,
            "max_price": max(pxs) if pxs else 0.0,
        }

    def recent_prices(self, window_sec: float) -> List[float]:
        now = now_ts()
        out: List[float] = []
        for t in reversed(self.trades):
            if now - t.ts > window_sec:
                break
            out.append(t.price)
        return list(reversed(out))


class RefillTracker:
    def __init__(self, max_age_sec: int):
        self.max_age_sec = max_age_sec
        self.events: Deque[Tuple[float, float, float]] = deque()  # (ts, bid_refill, ask_refill)
        self.prev_best_bid_qty: Optional[float] = None
        self.prev_best_ask_qty: Optional[float] = None

    def update(self, bid_qty: float, ask_qty: float, ts: Optional[float] = None) -> Tuple[float, float]:
        ts = ts or now_ts()
        bid_refill = 0.0
        ask_refill = 0.0

        if self.prev_best_bid_qty is not None and bid_qty > self.prev_best_bid_qty:
            bid_refill = bid_qty - self.prev_best_bid_qty

        if self.prev_best_ask_qty is not None and ask_qty > self.prev_best_ask_qty:
            ask_refill = ask_qty - self.prev_best_ask_qty

        self.prev_best_bid_qty = bid_qty
        self.prev_best_ask_qty = ask_qty

        self.events.append((ts, bid_refill, ask_refill))
        self.trim(ts)
        return bid_refill, ask_refill

    def trim(self, now: Optional[float] = None) -> None:
        now = now or now_ts()
        cutoff = now - self.max_age_sec
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def score(self, window_sec: float) -> Tuple[float, float]:
        now = now_ts()
        bid = ask = 0.0
        for ts, b, a in reversed(self.events):
            if now - ts > window_sec:
                break
            bid += b
            ask += a
        return bid, ask


class LiquidationStore:
    def __init__(self, max_age_sec: int):
        self.max_age_sec = max_age_sec
        self.events: Deque[LiquidationEvent] = deque()

    def add(self, event: LiquidationEvent) -> None:
        self.events.append(event)
        self.trim(event.ts)

    def trim(self, now: Optional[float] = None) -> None:
        now = now or now_ts()
        cutoff = now - self.max_age_sec
        while self.events and self.events[0].ts < cutoff:
            self.events.popleft()

    def burst(self, window_sec: float) -> Dict[str, float]:
        now = now_ts()
        buy_notional = sell_notional = 0.0
        count = 0
        for e in reversed(self.events):
            if now - e.ts > window_sec:
                break
            count += 1
            if e.side == "buy":
                buy_notional += e.notional
            else:
                sell_notional += e.notional
        return {
            "count": count,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "net_notional": buy_notional - sell_notional,
            "gross_notional": buy_notional + sell_notional,
        }


# =========================
# Candle builder + liquidity mapper
# =========================

class CandleBuilder:
    def __init__(self, interval_sec: int, max_candles: int):
        self.interval_sec = interval_sec
        self.max_candles = max_candles
        self.candles: Deque[Candle] = deque()

    def update_with_trade(self, ts: float, price: float, qty: float) -> None:
        bucket = math.floor(ts / self.interval_sec) * self.interval_sec
        if not self.candles or self.candles[-1].start_ts != bucket:
            c = Candle(start_ts=bucket, open=price, high=price, low=price, close=price)
            c.update(price, qty)
            self.candles.append(c)
            while len(self.candles) > self.max_candles:
                self.candles.popleft()
        else:
            self.candles[-1].update(price, qty)

    def list(self) -> List[Candle]:
        return list(self.candles)


class LiquidityMapper:
    def __init__(self, symbol: str, config: ScannerConfig):
        self.symbol = symbol
        self.config = config
        self.levels: List[LiquidityLevel] = []

    def rebuild(self, candles: List[Candle]) -> None:
        levels: List[LiquidityLevel] = []
        if len(candles) < 5:
            self.levels = []
            return

        lookback = candles[-self.config.eq_lookback_candles:]
        tol = self.config.eq_tolerance_for(self.symbol)

        highs: List[Tuple[float, float]] = [(c.start_ts, c.high) for c in lookback]
        lows: List[Tuple[float, float]] = [(c.start_ts, c.low) for c in lookback]

        levels.extend(self._cluster_levels(highs, tol, "equal_highs"))
        levels.extend(self._cluster_levels(lows, tol, "equal_lows"))
        self.levels = levels

    def _cluster_levels(self, pts: List[Tuple[float, float]], tol_pct: float, kind: str) -> List[LiquidityLevel]:
        clusters: List[List[Tuple[float, float]]] = []
        for ts, px in pts:
            placed = False
            for cluster in clusters:
                mean_px = sum(v for _, v in cluster) / len(cluster)
                if abs(px - mean_px) / mean_px <= tol_pct:
                    cluster.append((ts, px))
                    placed = True
                    break
            if not placed:
                clusters.append([(ts, px)])

        out: List[LiquidityLevel] = []
        for cluster in clusters:
            if len(cluster) >= self.config.eq_min_touches:
                prices = [px for _, px in cluster]
                times = [ts for ts, _ in cluster]
                out.append(
                    LiquidityLevel(
                        level=sum(prices) / len(prices),
                        kind=kind,
                        touches=len(cluster),
                        first_seen_ts=min(times),
                        last_seen_ts=max(times),
                        active=True,
                    )
                )
        return out

    def nearest_levels(self, price: float) -> Dict[str, Optional[LiquidityLevel]]:
        highs = [lvl for lvl in self.levels if lvl.kind == "equal_highs" and lvl.active]
        lows = [lvl for lvl in self.levels if lvl.kind == "equal_lows" and lvl.active]

        nearest_high = min(highs, key=lambda x: abs(x.level - price), default=None)
        nearest_low = min(lows, key=lambda x: abs(x.level - price), default=None)
        return {"equal_highs": nearest_high, "equal_lows": nearest_low}


# =========================
# Symbol state
# =========================

class SymbolState:
    def __init__(self, symbol: str, config: ScannerConfig):
        self.symbol = symbol
        self.config = config

        self.order_book = LocalOrderBook(symbol)
        self.trade_store = RollingTradeStore(config.trade_buffer_sec)
        self.refill_tracker = RefillTracker(config.refill_buffer_sec)
        self.liq_store = LiquidationStore(config.liquid_buffer_sec)
        self.candles = CandleBuilder(config.candle_interval_sec, config.max_candles)
        self.liquidity = LiquidityMapper(symbol, config)

        self.last_trade_price: Optional[float] = None
        self.book_ticker: Optional[BookTickerEvent] = None
        self.last_depth_event_ts: Optional[float] = None

        self.current_oi: Optional[float] = None
        self.oi_history: Deque[Tuple[float, float]] = deque()  # (ts, oi)

        self.last_signal_ts: float = 0.0
        self.pending_sweep_high: Optional[Dict[str, Any]] = None
        self.pending_sweep_low: Optional[Dict[str, Any]] = None

    def on_trade(self, trade: TradeEvent) -> None:
        self.last_trade_price = trade.price
        self.trade_store.add(trade)
        self.candles.update_with_trade(trade.ts, trade.price, trade.qty)

    def on_book_ticker(self, event: BookTickerEvent) -> None:
        self.book_ticker = event
        self.refill_tracker.update(event.bid_qty, event.ask_qty, event.ts)

    def on_liquidation(self, event: LiquidationEvent) -> None:
        self.liq_store.add(event)

    def on_oi(self, oi: float, ts: Optional[float] = None) -> None:
        ts = ts or now_ts()
        self.current_oi = oi
        self.oi_history.append((ts, oi))
        cutoff = ts - 600
        while self.oi_history and self.oi_history[0][0] < cutoff:
            self.oi_history.popleft()

    def oi_change_pct(self, window_sec: float = 15.0) -> Optional[float]:
        if self.current_oi is None or not self.oi_history:
            return None
        target_ts = now_ts() - window_sec
        base: Optional[float] = None
        for ts, oi in self.oi_history:
            if ts <= target_ts:
                base = oi
            else:
                break
        if base is None or base == 0:
            return None
        return (self.current_oi - base) / base

    def spread_bps(self) -> float:
        if self.book_ticker is None:
            return 0.0
        return bps(self.book_ticker.bid, self.book_ticker.ask)

    def imbalance(self, depth_levels: int = 5) -> float:
        bids, asks = self.order_book.top_n(depth_levels)
        bid_qty = sum(q for _, q in bids)
        ask_qty = sum(q for _, q in asks)
        denom = bid_qty + ask_qty
        if denom == 0:
            return 0.0
        return (bid_qty - ask_qty) / denom


# =========================
# Binance REST client
# =========================

class BinanceRestClient:
    def __init__(self, session: aiohttp.ClientSession, config: ScannerConfig):
        self.session = session
        self.config = config

    async def fetch_depth_snapshot(self, symbol: str) -> Dict[str, Any]:
        url = f"{self.config.rest_base}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": self.config.orderbook_snapshot_limit}
        async with self.session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_open_interest(self, symbol: str) -> float:
        url = f"{self.config.rest_base}/fapi/v1/openInterest"
        params = {"symbol": symbol}
        async with self.session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return float(data["openInterest"])


# =========================
# Market data gateway
# =========================

class BinanceMarketGateway:
    def __init__(self, config: ScannerConfig, states: Dict[str, SymbolState], rest_client: BinanceRestClient):
        self.config = config
        self.states = states
        self.rest_client = rest_client
        self.depth_buffers: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self.stop_event = asyncio.Event()

    def build_stream_url(self) -> str:
        streams = []
        for symbol in self.config.symbols:
            s = symbol.lower()
            streams.extend([
                f"{s}@aggTrade",
                f"{s}@bookTicker",
                f"{s}@depth@100ms",
                f"{s}@forceOrder",
                f"{s}@markPrice@1s",
            ])
        return self.config.ws_base + "/".join(streams)

    async def initial_sync_all_books(self) -> None:
        tasks = [self.sync_order_book(symbol) for symbol in self.config.symbols]
        await asyncio.gather(*tasks)

    async def sync_order_book(self, symbol: str) -> None:
        st = self.states[symbol]
        logging.info("Syncing order book for %s", symbol)
        snapshot = await self.rest_client.fetch_depth_snapshot(symbol)
        st.order_book.load_snapshot(snapshot)

        # Apply buffered events if any
        buffer = self.depth_buffers[symbol]
        kept: List[Dict[str, Any]] = []
        while buffer:
            ev = buffer.popleft()
            u = int(ev["u"])
            if u < st.order_book.last_update_id:
                continue
            kept.append(ev)

        kept.sort(key=lambda e: int(e["U"]))
        for ev in kept:
            ok = st.order_book.apply_depth_event(ev)
            if not ok and self.config.resync_on_book_gap:
                logging.warning("Depth sync failed during replay for %s; resyncing", symbol)
                return await self.sync_order_book(symbol)

        if not st.order_book.is_synced:
            logging.warning("Book not yet synced after snapshot replay for %s", symbol)
        else:
            logging.info("Order book synced for %s", symbol)

    async def run_ws(self) -> None:
        url = self.build_stream_url()
        while not self.stop_event.is_set():
            try:
                logging.info("Connecting WS: %s", url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=10_000_000) as ws:
                    await self.initial_sync_all_books()
                    async for raw in ws:
                        msg = orjson.loads(raw)
                        await self.handle_message(msg)
                        if self.stop_event.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception("WS connection error: %s", e)
                await asyncio.sleep(self.config.reconnect_backoff_sec)

    async def handle_message(self, msg: Dict[str, Any]) -> None:
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        if not stream:
            return
        parts = stream.split("@")
        symbol = parts[0].upper()
        st = self.states.get(symbol)
        if st is None:
            return

        if stream.endswith("@aggTrade"):
            await self._handle_agg_trade(st, data)
        elif stream.endswith("@bookTicker"):
            await self._handle_book_ticker(st, data)
        elif "@depth" in stream:
            await self._handle_depth(st, data)
        elif stream.endswith("@forceOrder"):
            await self._handle_force_order(st, data)
        elif stream.endswith("@markPrice@1s"):
            await self._handle_mark_price(st, data)

    async def _handle_agg_trade(self, st: SymbolState, data: Dict[str, Any]) -> None:
        ts = float(data["E"]) / 1000.0
        price = float(data["p"])
        qty = float(data["q"])
        # Binance aggTrade "m" True means buyer is maker -> aggressive sell
        side = "sell" if data.get("m", False) else "buy"
        trade = TradeEvent(ts=ts, symbol=st.symbol, price=price, qty=qty, side=side, notional=price * qty)
        st.on_trade(trade)

    async def _handle_book_ticker(self, st: SymbolState, data: Dict[str, Any]) -> None:
        ts = now_ts()
        event = BookTickerEvent(
            ts=ts,
            symbol=st.symbol,
            bid=float(data["b"]),
            bid_qty=float(data["B"]),
            ask=float(data["a"]),
            ask_qty=float(data["A"]),
        )
        st.on_book_ticker(event)

    async def _handle_depth(self, st: SymbolState, data: Dict[str, Any]) -> None:
        st.last_depth_event_ts = now_ts()
        if st.order_book.last_update_id is None:
            self.depth_buffers[st.symbol].append(data)
            return

        ok = st.order_book.apply_depth_event(data)
        if not ok and self.config.resync_on_book_gap:
            logging.warning("Depth gap / desync for %s, resyncing", st.symbol)
            await self.sync_order_book(st.symbol)

    async def _handle_force_order(self, st: SymbolState, data: Dict[str, Any]) -> None:
        if "o" not in data:
            return
        o = data["o"]
        ts = float(o["T"]) / 1000.0
        price = float(o["ap"])
        qty = float(o["q"])
        side = "buy" if o["S"] == "BUY" else "sell"
        st.on_liquidation(LiquidationEvent(
            ts=ts, symbol=st.symbol, side=side, price=price, qty=qty, notional=price * qty
        ))

    async def _handle_mark_price(self, st: SymbolState, data: Dict[str, Any]) -> None:
        # Placeholder hook if you later want current funding or mark-vs-last logic
        return


# =========================
# Feature + signal engine
# =========================

class SignalEngine:
    def __init__(self, config: ScannerConfig):
        self.config = config

    def compute_feature_snapshot(self, st: SymbolState) -> Dict[str, Any]:
        now = now_ts()
        t1 = st.trade_store.metrics(1.0)
        t5 = st.trade_store.metrics(5.0)
        bid_refill_5s, ask_refill_5s = st.refill_tracker.score(5.0)
        liq_5s = st.liq_store.burst(5.0)
        spread = st.spread_bps()
        imbalance_5 = st.imbalance(self.config.depth_levels_for_imbalance)

        last_price = st.last_trade_price or 0.0
        prices_5s = st.trade_store.recent_prices(self.config.micro_reclaim_lookback_sec)
        micro_high = max(prices_5s) if prices_5s else last_price
        micro_low = min(prices_5s) if prices_5s else last_price

        candles = st.candles.list()
        if len(candles) >= 3:
            st.liquidity.rebuild(candles)
        nearby = st.liquidity.nearest_levels(last_price) if last_price else {"equal_highs": None, "equal_lows": None}
        eq_high = nearby["equal_highs"]
        eq_low = nearby["equal_lows"]

        return {
            "ts": now,
            "last_price": last_price,
            "spread_bps": spread,
            "imbalance_5": imbalance_5,
            "buy_notional_1s": t1["buy_notional"],
            "sell_notional_1s": t1["sell_notional"],
            "delta_1s": t1["delta_notional"],
            "delta_5s": t5["delta_notional"],
            "buy_qty_1s": t1["buy_qty"],
            "sell_qty_1s": t1["sell_qty"],
            "bid_refill_score": bid_refill_5s,
            "ask_refill_score": ask_refill_5s,
            "liq_burst_notional_5s": liq_5s["gross_notional"],
            "liq_buy_notional_5s": liq_5s["buy_notional"],
            "liq_sell_notional_5s": liq_5s["sell_notional"],
            "micro_high": micro_high,
            "micro_low": micro_low,
            "eq_high": eq_high,
            "eq_low": eq_low,
            "oi": st.current_oi,
            "oi_change_pct_15s": st.oi_change_pct(15.0),
        }

    def should_skip(self, st: SymbolState, f: Dict[str, Any]) -> bool:
        if not st.order_book.is_synced:
            return True
        if st.book_ticker is None or st.last_trade_price is None:
            return True
        if f["spread_bps"] > self.config.max_spread_bps_for(st.symbol):
            return True
        if now_ts() - st.last_signal_ts < self.config.cooldown_after_signal_sec:
            return True
        return False

    def evaluate(self, st: SymbolState) -> List[SignalEvent]:
        f = self.compute_feature_snapshot(st)
        if self.should_skip(st, f):
            return []

        signals: List[SignalEvent] = []
        self._update_pending_sweeps(st, f)

        abs_long = self._absorption_long(st, f)
        if abs_long:
            signals.append(abs_long)

        abs_short = self._absorption_short(st, f)
        if abs_short:
            signals.append(abs_short)

        sw_short = self._sweep_short(st, f)
        if sw_short:
            signals.append(sw_short)

        sw_long = self._sweep_long(st, f)
        if sw_long:
            signals.append(sw_long)

        if signals:
            st.last_signal_ts = now_ts()
        return signals

    def _update_pending_sweeps(self, st: SymbolState, f: Dict[str, Any]) -> None:
        px = f["last_price"]
        eq_high: Optional[LiquidityLevel] = f["eq_high"]
        eq_low: Optional[LiquidityLevel] = f["eq_low"]
        break_pct = self.config.min_break_pct_for(st.symbol)

        if eq_high:
            broken_up = px > eq_high.level * (1 + break_pct)
            near_high = abs(px - eq_high.level) / eq_high.level <= break_pct * 3
            if broken_up:
                if st.pending_sweep_high is None or abs(st.pending_sweep_high["level"] - eq_high.level) / eq_high.level > 1e-9:
                    st.pending_sweep_high = {
                        "level": eq_high.level,
                        "break_ts": now_ts(),
                        "break_high": px,
                    }
                else:
                    st.pending_sweep_high["break_high"] = max(st.pending_sweep_high["break_high"], px)
            elif st.pending_sweep_high and not near_high:
                if now_ts() - st.pending_sweep_high["break_ts"] > self.config.sweep_max_hold_seconds:
                    st.pending_sweep_high = None

        if eq_low:
            broken_down = px < eq_low.level * (1 - break_pct)
            near_low = abs(px - eq_low.level) / eq_low.level <= break_pct * 3
            if broken_down:
                if st.pending_sweep_low is None or abs(st.pending_sweep_low["level"] - eq_low.level) / eq_low.level > 1e-9:
                    st.pending_sweep_low = {
                        "level": eq_low.level,
                        "break_ts": now_ts(),
                        "break_low": px,
                    }
                else:
                    st.pending_sweep_low["break_low"] = min(st.pending_sweep_low["break_low"], px)
            elif st.pending_sweep_low and not near_low:
                if now_ts() - st.pending_sweep_low["break_ts"] > self.config.sweep_max_hold_seconds:
                    st.pending_sweep_low = None

    def _absorption_long(self, st: SymbolState, f: Dict[str, Any]) -> Optional[SignalEvent]:
        px = f["last_price"]
        eq_low: Optional[LiquidityLevel] = f["eq_low"]
        if px <= 0 or eq_low is None:
            return None

        near_support = abs(px - eq_low.level) / eq_low.level <= self.config.eq_tolerance_for(st.symbol) * 2.5
        downside_progress_pct = safe_div(max(eq_low.level - px, 0.0), eq_low.level)
        reclaimed_micro = px > f["micro_low"] * (1 + self.config.sweep_reclaim_distance_pct)
        trade_notional = f["buy_notional_1s"] + f["sell_notional_1s"]

        score = 0.0
        if near_support:
            score += 1.0
        if f["delta_1s"] < -self.config.min_trade_notional_1s:
            score += 1.0
        if downside_progress_pct <= self.config.absorption_max_progress_pct:
            score += 1.0
        if f["bid_refill_score"] > f["ask_refill_score"]:
            score += 1.0
        if f["imbalance_5"] > 0.05:
            score += 1.0
        if reclaimed_micro:
            score += 1.0
        if f["liq_sell_notional_5s"] >= self.config.min_liq_burst_notional:
            score += 1.0
        if trade_notional < self.config.min_trade_notional_1s:
            score -= 1.0

        if score < self.config.min_absorption_score:
            return None

        stop = min(eq_low.level, f["micro_low"]) * (1 - 0.0004)
        risk = max(px - stop, px * 0.0004)
        tp1 = px + risk
        tp2 = px + 2 * risk

        return SignalEvent(
            ts=now_ts(),
            symbol=st.symbol,
            signal_type="ABSORPTION_LONG",
            price=px,
            level=eq_low.level,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            score=score,
            delta_1s=f["delta_1s"],
            delta_5s=f["delta_5s"],
            buy_notional_1s=f["buy_notional_1s"],
            sell_notional_1s=f["sell_notional_1s"],
            imbalance_5=f["imbalance_5"],
            bid_refill_score=f["bid_refill_score"],
            ask_refill_score=f["ask_refill_score"],
            liq_burst_notional_5s=f["liq_burst_notional_5s"],
            oi=f["oi"],
            oi_change_pct_15s=f["oi_change_pct_15s"],
            spread_bps=f["spread_bps"],
            note="Sell aggression absorbed near equal lows / support.",
        )

    def _absorption_short(self, st: SymbolState, f: Dict[str, Any]) -> Optional[SignalEvent]:
        px = f["last_price"]
        eq_high: Optional[LiquidityLevel] = f["eq_high"]
        if px <= 0 or eq_high is None:
            return None

        near_resistance = abs(px - eq_high.level) / eq_high.level <= self.config.eq_tolerance_for(st.symbol) * 2.5
        upside_progress_pct = safe_div(max(px - eq_high.level, 0.0), eq_high.level)
        rejected_micro = px < f["micro_high"] * (1 - self.config.sweep_reclaim_distance_pct)
        trade_notional = f["buy_notional_1s"] + f["sell_notional_1s"]

        score = 0.0
        if near_resistance:
            score += 1.0
        if f["delta_1s"] > self.config.min_trade_notional_1s:
            score += 1.0
        if upside_progress_pct <= self.config.absorption_max_progress_pct:
            score += 1.0
        if f["ask_refill_score"] > f["bid_refill_score"]:
            score += 1.0
        if f["imbalance_5"] < -0.05:
            score += 1.0
        if rejected_micro:
            score += 1.0
        if f["liq_buy_notional_5s"] >= self.config.min_liq_burst_notional:
            score += 1.0
        if trade_notional < self.config.min_trade_notional_1s:
            score -= 1.0

        if score < self.config.min_absorption_score:
            return None

        stop = max(eq_high.level, f["micro_high"]) * (1 + 0.0004)
        risk = max(stop - px, px * 0.0004)
        tp1 = px - risk
        tp2 = px - 2 * risk

        return SignalEvent(
            ts=now_ts(),
            symbol=st.symbol,
            signal_type="ABSORPTION_SHORT",
            price=px,
            level=eq_high.level,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            score=score,
            delta_1s=f["delta_1s"],
            delta_5s=f["delta_5s"],
            buy_notional_1s=f["buy_notional_1s"],
            sell_notional_1s=f["sell_notional_1s"],
            imbalance_5=f["imbalance_5"],
            bid_refill_score=f["bid_refill_score"],
            ask_refill_score=f["ask_refill_score"],
            liq_burst_notional_5s=f["liq_burst_notional_5s"],
            oi=f["oi"],
            oi_change_pct_15s=f["oi_change_pct_15s"],
            spread_bps=f["spread_bps"],
            note="Buy aggression absorbed near equal highs / resistance.",
        )

    def _sweep_short(self, st: SymbolState, f: Dict[str, Any]) -> Optional[SignalEvent]:
        pending = st.pending_sweep_high
        if not pending:
            return None

        level = pending["level"]
        px = f["last_price"]
        held = now_ts() - pending["break_ts"]
        reclaim_below = px < level * (1 - self.config.sweep_reclaim_distance_pct)
        broken_amount_pct = safe_div(max(pending["break_high"] - level, 0.0), level)

        score = 0.0
        if held <= self.config.sweep_max_hold_seconds:
            score += 1.0
        if reclaim_below:
            score += 1.0
        if broken_amount_pct >= self.config.min_break_pct_for(st.symbol):
            score += 1.0
        if f["ask_refill_score"] > f["bid_refill_score"]:
            score += 1.0
        if f["imbalance_5"] < -0.05:
            score += 1.0
        if f["liq_buy_notional_5s"] >= self.config.min_liq_burst_notional:
            score += 1.0
        if f["delta_1s"] < 0:
            score += 1.0

        if score < self.config.min_sweep_score:
            return None

        stop = pending["break_high"] * (1 + 0.0004)
        risk = max(stop - px, px * 0.0004)
        tp1 = px - risk
        tp2 = px - 2 * risk
        st.pending_sweep_high = None

        return SignalEvent(
            ts=now_ts(),
            symbol=st.symbol,
            signal_type="SWEEP_SHORT",
            price=px,
            level=level,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            score=score,
            delta_1s=f["delta_1s"],
            delta_5s=f["delta_5s"],
            buy_notional_1s=f["buy_notional_1s"],
            sell_notional_1s=f["sell_notional_1s"],
            imbalance_5=f["imbalance_5"],
            bid_refill_score=f["bid_refill_score"],
            ask_refill_score=f["ask_refill_score"],
            liq_burst_notional_5s=f["liq_burst_notional_5s"],
            oi=f["oi"],
            oi_change_pct_15s=f["oi_change_pct_15s"],
            spread_bps=f["spread_bps"],
            note="Upside liquidity sweep failed and reclaimed below equal highs.",
        )

    def _sweep_long(self, st: SymbolState, f: Dict[str, Any]) -> Optional[SignalEvent]:
        pending = st.pending_sweep_low
        if not pending:
            return None

        level = pending["level"]
        px = f["last_price"]
        held = now_ts() - pending["break_ts"]
        reclaim_above = px > level * (1 + self.config.sweep_reclaim_distance_pct)
        broken_amount_pct = safe_div(max(level - pending["break_low"], 0.0), level)

        score = 0.0
        if held <= self.config.sweep_max_hold_seconds:
            score += 1.0
        if reclaim_above:
            score += 1.0
        if broken_amount_pct >= self.config.min_break_pct_for(st.symbol):
            score += 1.0
        if f["bid_refill_score"] > f["ask_refill_score"]:
            score += 1.0
        if f["imbalance_5"] > 0.05:
            score += 1.0
        if f["liq_sell_notional_5s"] >= self.config.min_liq_burst_notional:
            score += 1.0
        if f["delta_1s"] > 0:
            score += 1.0

        if score < self.config.min_sweep_score:
            return None

        stop = pending["break_low"] * (1 - 0.0004)
        risk = max(px - stop, px * 0.0004)
        tp1 = px + risk
        tp2 = px + 2 * risk
        st.pending_sweep_low = None

        return SignalEvent(
            ts=now_ts(),
            symbol=st.symbol,
            signal_type="SWEEP_LONG",
            price=px,
            level=level,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            score=score,
            delta_1s=f["delta_1s"],
            delta_5s=f["delta_5s"],
            buy_notional_1s=f["buy_notional_1s"],
            sell_notional_1s=f["sell_notional_1s"],
            imbalance_5=f["imbalance_5"],
            bid_refill_score=f["bid_refill_score"],
            ask_refill_score=f["ask_refill_score"],
            liq_burst_notional_5s=f["liq_burst_notional_5s"],
            oi=f["oi"],
            oi_change_pct_15s=f["oi_change_pct_15s"],
            spread_bps=f["spread_bps"],
            note="Downside liquidity sweep failed and reclaimed above equal lows.",
        )


# =========================
# Console dashboard / formatting
# =========================

def render_signal(sig: SignalEvent) -> str:
    oi_change = "n/a" if sig.oi_change_pct_15s is None else f"{sig.oi_change_pct_15s * 100:.2f}%"
    lines = [
        f"[{sig.signal_type}] {sig.symbol}",
        f"Price: {sig.price:.4f}",
        f"Level: {sig.level:.4f}",
        f"Stop: {sig.stop:.4f}",
        f"TP1: {sig.tp1:.4f}",
        f"TP2: {sig.tp2:.4f}",
        f"Score: {sig.score:.2f}",
        f"Delta(1s): {sig.delta_1s:,.2f}",
        f"Delta(5s): {sig.delta_5s:,.2f}",
        f"BuyNotional(1s): {sig.buy_notional_1s:,.2f}",
        f"SellNotional(1s): {sig.sell_notional_1s:,.2f}",
        f"Imbalance(5): {sig.imbalance_5:.3f}",
        f"BidRefill(5s): {sig.bid_refill_score:,.2f}",
        f"AskRefill(5s): {sig.ask_refill_score:,.2f}",
        f"LiqBurst(5s): {sig.liq_burst_notional_5s:,.2f}",
        f"OI change(15s): {oi_change}",
        f"Spread(bps): {sig.spread_bps:.2f}",
        f"Note: {sig.note}",
    ]
    return "\n".join(lines)

def render_dashboard(states: Dict[str, SymbolState], engine: SignalEngine) -> str:
    headers = [
        "SYMBOL", "PRICE", "SPREAD", "DELTA1S", "DELTA5S", "IMB5", "BIDREF", "ASKREF", "LIQ5S", "OIΔ15S", "BOOK"
    ]
    rows = []
    for symbol, st in states.items():
        f = engine.compute_feature_snapshot(st)
        price = f["last_price"]
        oi_change = st.oi_change_pct(15.0)
        rows.append([
            symbol,
            f"{price:.4f}" if price else "-",
            f"{f['spread_bps']:.2f}",
            f"{f['delta_1s'] / 1000:.1f}k",
            f"{f['delta_5s'] / 1000:.1f}k",
            f"{f['imbalance_5']:.2f}",
            f"{f['bid_refill_score']:.1f}",
            f"{f['ask_refill_score']:.1f}",
            f"{f['liq_burst_notional_5s'] / 1000:.1f}k",
            f"{oi_change * 100:.2f}%" if oi_change is not None else "n/a",
            "OK" if st.order_book.is_synced else "SYNC",
        ])

    widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows]) for i, h in enumerate(headers)]
    out = []
    out.append(" | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    out.append("-+-".join("-" * w for w in widths))
    for r in rows:
        out.append(" | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))))
    return "\n".join(out)


# =========================
# Scanner application
# =========================

class ScannerApp:
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.states = {symbol: SymbolState(symbol, config) for symbol in config.symbols}
        self.csv_logger = CsvSignalLogger(config.log_dir, config.csv_filename)
        self.telegram = TelegramAlerter()
        self.signal_engine = SignalEngine(config)
        self.stop = False

    async def run(self) -> None:
        setup_logging(self.config.log_dir)
        logging.info("Starting Binance order-flow + liquidity sweep scanner")
        async with aiohttp.ClientSession() as session:
            self.session = session
            self.rest_client = BinanceRestClient(session, self.config)
            self.gateway = BinanceMarketGateway(self.config, self.states, self.rest_client)

            loop = asyncio.get_running_loop()
            for sig_name in ("SIGINT", "SIGTERM"):
                if hasattr(signal, sig_name):
                    loop.add_signal_handler(getattr(signal, sig_name), self.request_stop)

            tasks = [
                asyncio.create_task(self.gateway.run_ws(), name="ws"),
                asyncio.create_task(self.poll_open_interest(), name="oi"),
                asyncio.create_task(self.signal_loop(), name="signal_loop"),
                asyncio.create_task(self.heartbeat_loop(), name="heartbeat"),
            ]
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                raise
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    def request_stop(self) -> None:
        logging.info("Stop requested")
        self.stop = True
        if hasattr(self, "gateway"):
            self.gateway.stop_event.set()

    async def poll_open_interest(self) -> None:
        while not self.stop:
            try:
                for symbol, st in self.states.items():
                    oi = await self.rest_client.fetch_open_interest(symbol)
                    st.on_oi(oi)
                await asyncio.sleep(self.config.oi_poll_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning("Open interest polling error: %s", e)
                await asyncio.sleep(5)

    async def signal_loop(self) -> None:
        while not self.stop:
            try:
                for symbol, st in self.states.items():
                    signals = self.signal_engine.evaluate(st)
                    for sig in signals:
                        logging.info("\n%s", render_signal(sig))
                        self.csv_logger.log(sig)
                        await self.telegram.send(self.session, render_signal(sig))
                await asyncio.sleep(self.config.scanner_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception("Signal loop error: %s", e)
                await asyncio.sleep(1)

    async def heartbeat_loop(self) -> None:
        while not self.stop:
            try:
                if self.config.console_table:
                    print()
                    print(render_dashboard(self.states, self.signal_engine))
                    print()
                await asyncio.sleep(self.config.heartbeat_log_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning("Heartbeat loop error: %s", e)
                await asyncio.sleep(2)


# =========================
# Main entry
# =========================

def main() -> None:
    app = ScannerApp(CONFIG)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
