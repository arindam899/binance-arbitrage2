import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from functools import partial
from threading import Lock
from types import SimpleNamespace
from typing import Any, Deque, Dict, List, Optional
from urllib import error, parse, request

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

try:
    import config
    ARBITRAGE_THRESHOLD = config.ARBITRAGE_THRESHOLD
    DEFAULT_EXPIRY = getattr(config, "DEFAULT_FUTURES_EXPIRY", "PERPETUAL")
except ImportError:
    ARBITRAGE_THRESHOLD = 1.0
    DEFAULT_EXPIRY = "PERPETUAL"
    logging.warning("config.py not found, using default Binance settings")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logger = logging.getLogger(__name__)
SUPPORTS_REALTIME_WEBSOCKET = False


class BinanceRealBroker:
    """Binance spot + USD-M futures broker adapter with REST throttling."""

    DEFAULT_SYMBOLS = ["0GUSDT","1000CATUSDT","1000CHEEMSUSDT","1000SATSUSDT","1INCHUSDT","1MBABYDOGEUSDT","A2ZUSDT","AAVEUSDT","ACHUSDT","ACMUSDT","ACXUSDT","ACTUSDT","ADAUSDT","ADXUSDT","AEURUSDT","AEVOUSDT","AGLDUSDT","AIUSDT","AIXBTUSDT","AKROUSDT","ALCXUSDT","ALGOUSDT","ALICEUSDT","ALLOUSDT","ALPINEUSDT","ALTUSDT","AMPUSDT","ANIMEUSDT","ANKRUSDT","APEUSDT","API3USDT","APTUSDT","ARBUSDT","ARDRUSDT","ARPAUSDT","ARUSDT","ASTRUSDT","ATAUSDT","ATOMUSDT","ATMUSDT","ATUSDT","AUDIOUSDT","AUCTIONUSDT","AUSDT","AVAXUSDT","AVAUSDT","AVNTUSDT","AXLUSDT","AXSUSDT","BABYUSDT","BALUSDT","BANDUSDT","BANKUSDT","BANANAUSDT","BANANAS31USDT","BARUSDT","BATATUSDT","BATUSDT","BBUSDT","BCHUSDT","BEAMXUSDT","BELUSDT","BERAUSDT","BFUSDUSDT","BICOUSDT","BIGTIMEUSDT","BIFIUSDT","BIOUSDT","BLURUSDT","BMTUSDT","BNBUSDT","BNSOLUSDT","BOMEUSDT","BONKUSDT","BREVUSDT","BROCCOLI714USDT","BTTCUSDT","CAKEUSDT","CATIUSDT","CELOUSDT","CELRUSDT","CETUSUSDT","CFXUSDT","CGPTUSDT","CHZUSDT","CHRUSDT","CITYUSDT","CKBUSDT","COMPUSDT","COOKIEUSDT","COSUSDT","COTIUSDT","COWUSDT","CRVUSDT","CTKUSDT","CTSIUSDT","CUSDT","CVXUSDT","CYBERUSDT","DASHUSDT","DCRUSDT","DEGOUSDT","DENTUSDT","DEXEUSDT","DGBUSDT","DIAUSDT","DODOUSDT","DOGEUSDT","DOGSUSDT","DOLOUSDT","DOTUSDT","DUSDT","DUSKUSDT","DYDXUSDT","DYMUSDT","EDENUSDT","EDUUSDT","EGLDUSDT","EIGENUSDT","ENAUSDT","ENJUSDT","ENSOUSDT","ENSUSDT","EPICUSDT","ERAUSDT","ESPUSDT","ETCUSDT","ETHFIUSDT","ETHUSDT","EURUSDT","EURIUSDT","EULUSDT","FARMUSDT","FDUSDUSDT","FETUSDT","FFUSDT","FIDAUSDT","FILUSDT","FIOUSDT","FLOKIUSDT","FLOWUSDT","FLUXUSDT","FOGOUSDT","FORTHUSDT","FORMUSDT","FRAXUSDT","FTTUSDT","FUNUSDT","FUSDT","GALAUSDT","GASUSDT","GIGGLEUSDT","GLMRUSDT","GLMUSDT","GMTUSDT","GNOUSDT","GNSUSDT","GPSUSDT","GRTUSDT","GTCUSDT","GUNUSDT","GUSDT","HAEDALUSDT","HBARUSDT","HEIUSDT","HEMIUSDT","HIGHUSDT","HFTUSDT","HOMEUSDT","HOOKUSDT","HOLOUSDT","HOTUSDT","HUMAUSDT","HYPERUSDT","ICPUSDT","ICXUSDT","IDEXUSDT","IDUSDT","ILVUSDT","IMXUSDT","INJUSDT","INITUSDT","IOSTUSDT","IOTAUSDT","IOUSDT","IQUSDT","IOTXUSDT","JASMYUSDT","JOEUSDT","JTOUSDT","JUPUSDT","JUVUSDT","KAIAUSDT","KAITOUSDT","KAVAUSDT","KERNELUSDT","KGSTUSDT","KITEUSDT","KMNOUSDT","KNCUSDT","KSMUSDT","LAUSDT","LAZIOUSDT","LAYERUSDT","LDOUSDT","LINEAUSDT","LINKUSDT","LISTAUSDT","LPTUSDT","LQTYUSDT","LRCUSDT","LSKUSDT","LTCUSDT","LUMIAUSDT","LUNAUSDT","LUNCUSDT","MAGICUSDT","MANTRAUSDT","MANAUSDT","MANTAUSDT","MASKUSDT","MAVUSDT","MBLUSDT","MDTUSDT","MEUSDT","MEMEUSDT","METISUSDT","METUSDT","MINAUSDT","MIRAUSDT","MITOUSDT","MBOXUSDT","MLNUSDT","MMTUSDT","MORPHOUSDT","MOVRUSDT","MTLUSDT","MUBARAKUSDT","NEXOUSDT","NEARUSDT","NEIROUSDT","NEOUSDT","NEWTUSDT","NFPUSDT","NILUSDT","NMRUSDT","NOMUSDT","NOTUSDT","NTRNUSDT","NXPCUSDT","OGNUSDT","OGUSDT","ONDOUSDT","ONEUSDT","ONGUSDT","OPNUSDT","OPUSDT","ORDIUSDT","ORCAUSDT","OSMOUSDT","OXTUSDT","PAXGUSDT","PENDLEUSDT","PENGUUSDT","PEOPLEUSDT","PEPEUSDT","PHAUSDT","PHBUSDT","PIVXUSDT","PIXELUSDT","PLUMEUSDT","PNUTUSDT","POLUSDT","POLYXUSDT","PONDUSDT","PORTALUSDT","PORTOUSDT","POWRUSDT","PROMUSDT","PROVEUSDT","PSGUSDT","PYRUSDT","PYTHUSDT","QKCUSDT","QIUSDT","QNTUSDT","QTUMUSDT","QUICKUSDT","RADUSDT","RAREUSDT","RAYUSDT","RDNTUSDT","REDUSDT","RENDERUSDT","REQUSDT","RESOLVUSDT","REZUSDT","RIFUSDT","RLUSDUSDT","RLCUSDT","RONINUSDT","ROBOUSDT","ROSEUSDT","RPLUSDT","RSRUSDT","RUNEUSDT","RVNUSDT","SAHARAUSDT","SAGAUSDT","SANDUSDT","SAPIENUSDT","SCUSDT","SCRTUSDT","SCRUSDT","SEIUSDT","SENTUSDT","SFPUSDT","SHIBUSDT","SHELLUSDT","SIGNUSDT","SKLUSDT","SLPUSDT","SNXUSDT","SOLUSDT","SOLVUSDT","SOMIUSDT","SOPHUSDT","SPELLUSDT","SPKUSDT","SSVUSDT","STGUSDT","STOUSDT","STORJUSDT","STRAXUSDT","STRKUSDT","STXUSDT","SUNUSDT","SUPERUSDT","SUIUSDT","SUSHIUSDT","SUSDT","SYNUSDT","SYRUPUSDT","TAOUSDT","TFUELUSDT","THEUSDT","THETAUSDT","TIAUSDT","TLMUSDT","TNSRUSDT","TONUSDT","TOWNSUSDT","TRBUSDT","TREEUSDT","TRUUSDT","TRUMPUSDT","TRXUSDT","TSTUSDT","TUSDUSDT","TURBOUSDT","TURTLEUSDT","TUTUSDT","TWTUSDT","UMAUSDT","UNIUSDT","USDCUSDT","USDEUSDT","USD1USDT","USDPUSDT","USTCUSDT","UTKUSDT","VANAUSDT","VANRYUSDT","VELODROMEUSDT","VETUSDT","VICUSDT","VIRTUALUSDT","VTHOUSDT","WANUSDT","WAXPUSDT","WBETHUSDT","WBTCUSDT","WCTUSDT","WIFUSDT","WINUSDT","WLDUSDT","WLFIUSDT","WOOUSDT","WUSDT","XAIUSDT","XECUSDT","XLMUSDT","XNOUSDT","XPLUSDT","XRPUSDT","XUSDUSDT","XTZUSDT","XVGUSDT","YBUSDT","YFIUSDT","YGGUSDT","ZAMAUSDT","ZENUSDT","ZILUSDT","ZKCUSDT","ZKPUSDT","ZKUSDT","ZROUSDT","ZRXUSDT","å¸å®‰äººç”ŸUSDT"]

    def __init__(self) -> None:
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.secret_key = os.getenv("BINANCE_SECRET_KEY")
        self.quote_asset = (os.getenv("BINANCE_QUOTE_ASSET") or "USDT").upper()
        self.use_testnet = str(os.getenv("BINANCE_USE_TESTNET", "true")).strip().lower() in {"1", "true", "yes", "on"}
        self.default_order_notional = float(os.getenv("BINANCE_DEFAULT_ORDER_NOTIONAL_USDT", "13") or 13)
        self.spot_conversion_fee_buffer_ratio = max(0.0, float(os.getenv("BINANCE_SPOT_CONVERSION_FEE_BUFFER_RATIO", "0.002") or 0.002))
        default_convert_mode = "false" if self.use_testnet else "true"
        self.prefer_spot_convert = str(os.getenv("BINANCE_PREFER_SPOT_CONVERT", default_convert_mode)).strip().lower() in {"1", "true", "yes", "on"}
        self.spot_convert_fallback_to_market = str(os.getenv("BINANCE_SPOT_CONVERT_FALLBACK_TO_MARKET", "true")).strip().lower() in {"1", "true", "yes", "on"}
        self.spot_convert_valid_time = str(os.getenv("BINANCE_SPOT_CONVERT_VALID_TIME", "10s")).strip()
        if self.spot_convert_valid_time not in {"10s", "30s", "1m"}:
            self.spot_convert_valid_time = "10s"
        self.spot_convert_poll_timeout_seconds = max(1.0, float(os.getenv("BINANCE_SPOT_CONVERT_POLL_TIMEOUT_SECONDS", "8") or 8))
        self.spot_convert_poll_interval_seconds = max(0.2, float(os.getenv("BINANCE_SPOT_CONVERT_POLL_INTERVAL_SECONDS", "0.5") or 0.5))
        self.min_arbitrage_investment_quote = float(os.getenv("BINANCE_MIN_ARBITRAGE_INVESTMENT_USDT", "0") or 0)
        self.futures_leverage = max(1, int(float(os.getenv("BINANCE_FUTURES_LEVERAGE", "2") or 2)))
        self.set_futures_margin_type = str(os.getenv("BINANCE_SET_FUTURES_MARGIN_TYPE", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self.futures_margin_type = str(os.getenv("BINANCE_FUTURES_MARGIN_TYPE", "ISOLATED")).strip().upper()
        if self.futures_margin_type not in {"ISOLATED", "CROSSED"}:
            self.futures_margin_type = "ISOLATED"
        self.auto_transfer_spot_to_futures = str(os.getenv("BINANCE_AUTO_TRANSFER_SPOT_TO_FUTURES", "true")).strip().lower() in {"1", "true", "yes", "on"}
        self.futures_margin_buffer_ratio = max(0.0, float(os.getenv("BINANCE_FUTURES_MARGIN_BUFFER_RATIO", "0.05") or 0.05))
        self.enable_margin_shorting = str(os.getenv("BINANCE_ENABLE_MARGIN_SHORT", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self.margin_collateral_ratio = max(0.0, float(os.getenv("BINANCE_MARGIN_COLLATERAL_RATIO", "1.0") or 1.0))
        self.margin_short_available = True
        self.margin_short_error: Optional[str] = None
        self.market_data_ttl = max(1.0, float(os.getenv("BINANCE_MARKET_DATA_TTL_SECONDS", "5") or 5))
        self.funding_data_ttl = max(60.0, float(os.getenv("BINANCE_FUNDING_DATA_TTL_SECONDS", "900") or 900))
        self.funding_history_limit = max(1, int(os.getenv("BINANCE_FUNDING_HISTORY_LIMIT", "9") or 9))
        self.funding_window_days = max(1.0, float(os.getenv("BINANCE_FUNDING_WINDOW_DAYS", "3") or 3))
        self.default_funding_interval_hours = max(1, int(os.getenv("BINANCE_DEFAULT_FUNDING_INTERVAL_HOURS", "8") or 8))
        self.funding_batch_size = max(1, int(os.getenv("BINANCE_FUNDING_BATCH_SIZE", "12") or 12))
        self.funding_refresh_budget_seconds = max(1.0, float(os.getenv("BINANCE_FUNDING_REFRESH_BUDGET_SECONDS", "8") or 8))
        self.funding_request_timeout = max(1, int(float(os.getenv("BINANCE_FUNDING_REQUEST_TIMEOUT_SECONDS", "3") or 3)))
        self.recv_window = int(float(os.getenv("BINANCE_RECV_WINDOW_MS", "5000") or 5000))
        self.access_token = None
        self.connected = False
        self.live_data: Dict[str, Any] = {}
        self.fyers_ws = None
        self.expiry_date = DEFAULT_EXPIRY
        self.auth_error: Optional[str] = None
        self.rate_limit_error: Optional[str] = None
        self.market_data_cache: Optional[Dict[str, Any]] = None
        self.market_data_cache_ts = 0.0
        self.market_data_backoff_until = 0.0
        self.funding_metrics_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self.funding_metrics_cache_ts = 0.0
        self.funding_refresh_cursor = 0
        self.funding_interval_hours_by_symbol: Dict[str, int] = {}
        self.futures_position_mode = str(os.getenv("BINANCE_FUTURES_POSITION_MODE", "ONE_WAY")).strip().upper()
        if self.futures_position_mode not in {"ONE_WAY", "HEDGE"}:
            self.futures_position_mode = "ONE_WAY"

        if self.use_testnet:
            self.spot_base_url = os.getenv("BINANCE_SPOT_BASE_URL", "https://testnet.binance.vision")
            self.futures_base_url = os.getenv("BINANCE_FUTURES_BASE_URL", "https://testnet.binancefuture.com")
        else:
            self.spot_base_url = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com")
            self.futures_base_url = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")

        logger.info("Initializing Binance broker in %s mode", "TESTNET" if self.use_testnet else "LIVE")

        self.symbols = self._init_all_symbols()
        self.equity_symbols = self.symbols.copy()
        self.futures_symbols = self.symbols.copy()
        self.all_symbols = self.symbols.copy()
        self.log_path = os.path.join(BASE_DIR, "logs")
        os.makedirs(self.log_path, exist_ok=True)

        self.exchange_filters: Dict[str, Dict[str, Dict[str, float]]] = {"EQUITY": {}, "FUTURES": {}}
        self.lot_sizes: Dict[str, float] = {}
        self._futures_leverage_by_symbol: Dict[str, int] = {}
        self._futures_margin_type_by_symbol: Dict[str, str] = {}
        self._futures_multi_assets_mode: Optional[bool] = None
        self._load_exchange_metadata()
        self._load_funding_info_metadata()

    def _init_all_symbols(self) -> List[str]:
        configured = os.getenv("BINANCE_BASE_SYMBOLS") or os.getenv("BINANCE_SYMBOLS")
        symbols = [item.strip().upper() for item in configured.split(",")] if configured else []
        unique_symbols: List[str] = []
        seen = set()
        for symbol in symbols:
            if symbol and symbol.endswith(self.quote_asset) and symbol not in seen:
                seen.add(symbol)
                unique_symbols.append(symbol)
        if configured:
            logger.info("Initialized %s configured Binance symbols", len(unique_symbols))
        else:
            logger.info("No symbol override configured, will load live Binance %s pairs from exchange metadata", self.quote_asset)
        return unique_symbols

    def _http_json(
        self,
        method: str,
        base_url: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> Any:
        query = parse.urlencode({k: v for k, v in (params or {}).items() if v is not None}, doseq=True)
        url = f"{base_url}{path}"
        if query:
            url = f"{url}?{query}"
        req = request.Request(url=url, method=method.upper(), headers=headers or {})
        try:
            with request.urlopen(req, timeout=timeout) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload) if payload else {}
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.error("Binance HTTP %s %s failed: %s", exc.code, path, body)
            raise RuntimeError(body or f"HTTP {exc.code} for {path}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Network error calling Binance {path}: {exc}") from exc

    def _signed_request(
        self,
        method: str,
        market: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if not self.api_key or not self.secret_key:
            raise RuntimeError("Missing Binance credentials. Set BINANCE_API_KEY and BINANCE_SECRET_KEY in .env.")

        payload = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000)
        payload["recvWindow"] = self.recv_window
        query = parse.urlencode(payload, doseq=True)
        signature = hmac.new(self.secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        payload["signature"] = signature

        base_url = self.spot_base_url if market == "spot" else self.futures_base_url
        headers = {"X-MBX-APIKEY": self.api_key}
        return self._http_json(method, base_url, path, params=payload, headers=headers, timeout=15)

    def _api_key_permissions(self) -> Dict[str, Any]:
        if "demo-api.binance.com" in str(self.spot_base_url).lower():
            return {}
        return self._signed_request("GET", "spot", "/sapi/v1/account/apiRestrictions")

    def _margin_short_environment_supported(self) -> bool:
        spot_base = str(self.spot_base_url).lower()
        return "demo-api.binance.com" not in spot_base

    def _spot_account_balances(self) -> Dict[str, float]:
        response = self._signed_request("GET", "spot", "/api/v3/account")
        balances: Dict[str, float] = {}
        for item in response.get("balances", []) or []:
            asset = str(item.get("asset", "")).upper()
            if not asset:
                continue
            balances[asset] = float(item.get("free", 0.0) or 0.0)
        return balances

    def _base_asset_for_symbol(self, symbol: str) -> str:
        if symbol.endswith(self.quote_asset):
            return symbol[:-len(self.quote_asset)]
        return symbol

    def _available_spot_pair_quantity(self, symbol: str) -> float:
        balances = self._spot_account_balances()
        base_asset = self._base_asset_for_symbol(symbol)
        free_amount = float(balances.get(base_asset.upper(), 0.0) or 0.0)
        return float(self._normalize_pair_quantity(symbol, free_amount) or 0.0)

    def _ensure_spot_balance(self, asset: str, required_amount: float, reason: str) -> None:
        balances = self._spot_account_balances()
        free_amount = float(balances.get(asset.upper(), 0.0) or 0.0)
        if free_amount + 1e-12 < float(required_amount):
            raise RuntimeError(
                f"Insufficient spot balance for {reason}. Need {required_amount:.8f} {asset.upper()}, have {free_amount:.8f}"
            )

    def _futures_account_balances(self) -> Dict[str, Dict[str, float]]:
        response = self._signed_request("GET", "futures", "/fapi/v3/balance")
        balances: Dict[str, Dict[str, float]] = {}
        for item in response if isinstance(response, list) else []:
            asset = str(item.get("asset", "")).upper()
            if not asset:
                continue
            balances[asset] = {
                "balance": float(item.get("balance", 0.0) or 0.0),
                "availableBalance": float(item.get("availableBalance", 0.0) or 0.0),
                "maxWithdrawAmount": float(item.get("maxWithdrawAmount", 0.0) or 0.0),
            }
        return balances

    def _get_futures_multi_assets_mode(self, refresh: bool = False) -> Optional[bool]:
        if self._futures_multi_assets_mode is not None and not refresh:
            return self._futures_multi_assets_mode

        previous = self._futures_multi_assets_mode
        try:
            response = self._signed_request("GET", "futures", "/fapi/v1/multiAssetsMargin")
            value = response.get("multiAssetsMargin")
            if isinstance(value, str):
                value = value.strip().lower() == "true"
            self._futures_multi_assets_mode = bool(value)
        except Exception as exc:
            logger.warning("Failed to detect Binance futures Multi-Assets mode: %s", exc)
            return previous

        return self._futures_multi_assets_mode

    def _required_futures_margin_quote(self, price: float, quantity: float) -> float:
        notional = max(float(price) * float(quantity), 0.0)
        leverage = max(float(self.futures_leverage), 1.0)
        return max((notional / leverage) * (1.0 + float(self.futures_margin_buffer_ratio or 0.0)), 0.0)

    def _ensure_futures_quote_balance(self, required_quote_amount: float, reason: str) -> float:
        required_quote_amount = max(float(required_quote_amount), 0.0)
        balances = self._futures_account_balances()
        available = float(balances.get(self.quote_asset.upper(), {}).get("availableBalance", 0.0) or 0.0)
        if available + 1e-12 >= required_quote_amount:
            return available

        if not self.auto_transfer_spot_to_futures:
            raise RuntimeError(
                f"Insufficient futures balance for {reason}. Need {required_quote_amount:.8f} {self.quote_asset}, have {available:.8f} in USD-M Futures wallet"
            )

        deficit = required_quote_amount - available
        self._ensure_spot_balance(self.quote_asset, deficit, f"futures wallet top-up for {reason}")
        self._margin_transfer("MAIN_UMFUTURE", self.quote_asset, deficit)
        refreshed = self._futures_account_balances()
        refreshed_available = float(refreshed.get(self.quote_asset.upper(), {}).get("availableBalance", 0.0) or 0.0)
        if refreshed_available + 1e-12 < required_quote_amount:
            raise RuntimeError(
                f"Futures wallet top-up incomplete for {reason}. Need {required_quote_amount:.8f} {self.quote_asset}, have {refreshed_available:.8f} after transfer"
            )
        return refreshed_available

    def _reverse_carry_entry_block_reason(
        self,
        symbol: str,
        quantity: float,
        equity_price: float,
        futures_price: float,
        spot_balances: Optional[Dict[str, float]] = None,
        futures_balances: Optional[Dict[str, Dict[str, float]]] = None,
        margin_assets: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> str:
        active_quantity = float(self._normalize_pair_quantity(symbol, quantity) or 0.0)
        if active_quantity <= 0:
            return f"No valid reverse carry entry quantity for {symbol}"

        spot_balances = spot_balances if spot_balances is not None else self._spot_account_balances()
        futures_balances = futures_balances if futures_balances is not None else self._futures_account_balances()
        margin_assets = margin_assets if margin_assets is not None else {}

        spot_available = float(spot_balances.get(self.quote_asset.upper(), 0.0) or 0.0)
        futures_available = float(
            (futures_balances.get(self.quote_asset.upper(), {}) or {}).get("availableBalance", 0.0) or 0.0
        )
        required_futures_quote = self._required_futures_margin_quote(futures_price, active_quantity)
        futures_spot_deficit = max(required_futures_quote - futures_available, 0.0)

        if not self.auto_transfer_spot_to_futures and futures_spot_deficit > 1e-12:
            return (
                f"Need about {required_futures_quote:.8f} {self.quote_asset} in USD-M Futures "
                f"for the futures-first entry, have {futures_available:.8f}"
            )

        if self.enable_margin_shorting:
            if not self.margin_short_available:
                return self.margin_short_error or "Cross-margin shorting is unavailable for this account."

            margin_free = float((margin_assets.get(self.quote_asset.upper(), {}) or {}).get("free", 0.0) or 0.0)
            required_margin_quote = max(float(equity_price) * active_quantity * max(self.margin_collateral_ratio, 0.0), 0.0)
            margin_spot_deficit = max(required_margin_quote - margin_free, 0.0)
            total_spot_needed = margin_spot_deficit + futures_spot_deficit
            if spot_available + 1e-12 < total_spot_needed:
                return (
                    f"Need about {total_spot_needed:.8f} {self.quote_asset} in spot to fund the futures leg first "
                    f"and top up margin collateral, have {spot_available:.8f}"
                )
            return ""

        required_conversion_quote = self._quote_amount_for_target_base_quantity(symbol, active_quantity, float(equity_price))
        total_spot_needed = required_conversion_quote + futures_spot_deficit
        if spot_available + 1e-12 < total_spot_needed:
            return (
                f"Need about {total_spot_needed:.8f} {self.quote_asset} in spot to fund the futures leg first "
                f"and convert for the spot sell, have {spot_available:.8f}"
            )
        return ""

    def _margin_account_details(self) -> Dict[str, Any]:
        return self._signed_request("GET", "spot", "/sapi/v1/margin/account")

    def _margin_account_assets(self) -> Dict[str, Dict[str, float]]:
        response = self._margin_account_details()
        assets: Dict[str, Dict[str, float]] = {}
        for item in response.get("userAssets", []) or []:
            asset = str(item.get("asset", "")).upper()
            if not asset:
                continue
            assets[asset] = {
                "free": float(item.get("free", 0.0) or 0.0),
                "borrowed": float(item.get("borrowed", 0.0) or 0.0),
                "interest": float(item.get("interest", 0.0) or 0.0),
                "netAsset": float(item.get("netAsset", 0.0) or 0.0),
            }
        return assets

    def _margin_asset_snapshot(self, asset: str) -> Dict[str, float]:
        return dict(self._margin_account_assets().get(asset.upper(), {}))

    def _margin_transfer(self, transfer_type: str, asset: str, amount: float) -> Dict[str, Any]:
        if amount <= 0:
            return {"tranId": None, "amount": 0.0}
        return self._signed_request(
            "POST",
            "spot",
            "/sapi/v1/asset/transfer",
            params={
                "type": transfer_type,
                "asset": asset.upper(),
                "amount": self._format_quote_amount(amount),
            },
        )

    def _ensure_margin_quote_collateral(self, required_quote_amount: float, reason: str) -> float:
        required_quote_amount = max(float(required_quote_amount), 0.0)
        if required_quote_amount <= 0:
            snapshot = self._margin_asset_snapshot(self.quote_asset)
            return float(snapshot.get("free", 0.0) or 0.0)

        snapshot = self._margin_asset_snapshot(self.quote_asset)
        margin_free = float(snapshot.get("free", 0.0) or 0.0)
        if margin_free + 1e-12 >= required_quote_amount:
            return margin_free

        deficit = required_quote_amount - margin_free
        self._ensure_spot_balance(self.quote_asset, deficit, f"margin collateral for {reason}")
        self._margin_transfer("MAIN_MARGIN", self.quote_asset, deficit)
        refreshed = self._margin_asset_snapshot(self.quote_asset)
        refreshed_free = float(refreshed.get("free", 0.0) or 0.0)
        if refreshed_free + 1e-12 < required_quote_amount:
            raise RuntimeError(
                f"Margin collateral transfer incomplete for {reason}. Need {required_quote_amount:.8f} {self.quote_asset}, have {refreshed_free:.8f}"
            )
        return refreshed_free

    def _margin_max_borrowable(self, asset: str) -> float:
        response = self._signed_request("GET", "spot", "/sapi/v1/margin/maxBorrowable", params={"asset": asset.upper()})
        return float(response.get("amount", 0.0) or 0.0)

    def _margin_borrow(self, asset: str, amount: float) -> Dict[str, Any]:
        if amount <= 0:
            return {"tranId": None, "amount": 0.0}
        return self._signed_request(
            "POST",
            "spot",
            "/sapi/v1/margin/borrow-repay",
            params={
                "asset": asset.upper(),
                "amount": self._format_quote_amount(amount),
                "type": "BORROW",
            },
        )

    def _margin_repay(self, asset: str, amount: float) -> Dict[str, Any]:
        if amount <= 0:
            return {"tranId": None, "amount": 0.0}
        return self._signed_request(
            "POST",
            "spot",
            "/sapi/v1/margin/borrow-repay",
            params={
                "asset": asset.upper(),
                "amount": self._format_quote_amount(amount),
                "type": "REPAY",
            },
        )

    def _margin_liability(self, asset: str) -> float:
        snapshot = self._margin_asset_snapshot(asset)
        return max(float(snapshot.get("borrowed", 0.0) or 0.0) + float(snapshot.get("interest", 0.0) or 0.0), 0.0)

    def _execute_margin_trade(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        validation = self.validate_order(symbol, "EQUITY", quantity)
        if not validation.get("valid"):
            return {"status": "ERROR", "message": validation.get("message", "Margin order validation failed")}

        normalized_quantity = float(validation.get("normalized_quantity", quantity) or quantity)
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": self._format_quantity(symbol, "EQUITY", normalized_quantity),
            "isIsolated": "FALSE",
            "newOrderRespType": "RESULT",
        }
        response = self._signed_request("POST", "spot", "/sapi/v1/margin/order", params=params)
        trade_id = response.get("orderId") or response.get("clientOrderId") or "unknown"
        return {
            "status": "EXECUTED",
            "trade_id": str(trade_id),
            "message": f"Binance margin {side.upper()} {normalized_quantity} {symbol} executed",
            "details": {
                "symbol": symbol,
                "side": side.upper(),
                "instrument": "EQUITY",
                "price": price,
                "quantity": normalized_quantity,
                "market": "margin-cross",
            },
            "response": response,
        }

    def _execute_margin_short_sell(self, symbol: str, quantity: float, price: float) -> Dict[str, Any]:
        if not self.enable_margin_shorting:
            return {"status": "ERROR", "message": "Cross-margin shorting is disabled. Set BINANCE_ENABLE_MARGIN_SHORT=true."}
        if not self.margin_short_available:
            return {"status": "ERROR", "message": self.margin_short_error or "Cross-margin shorting is unavailable for this account."}

        base_asset = self._base_asset_for_symbol(symbol)
        validation = self.validate_order(symbol, "EQUITY", quantity)
        if not validation.get("valid"):
            return {"status": "ERROR", "message": validation.get("message", "Margin short validation failed")}
        normalized_quantity = float(validation.get("normalized_quantity", quantity) or quantity)
        required_quote = max(float(price) * normalized_quantity * max(self.margin_collateral_ratio, 0.0), 0.0)

        try:
            self._ensure_margin_quote_collateral(required_quote, f"reverse carry short on {symbol}")
            max_borrowable = self._margin_max_borrowable(base_asset)
            if max_borrowable + 1e-12 < normalized_quantity:
                return {
                    "status": "ERROR",
                    "message": f"Margin borrow limit too low for {symbol}. Need {normalized_quantity:.8f} {base_asset}, max borrowable is {max_borrowable:.8f}",
                }
            borrow_response = self._margin_borrow(base_asset, normalized_quantity)
            order_result = self._execute_margin_trade(symbol, "SELL", normalized_quantity, price)
            if order_result.get("status") != "EXECUTED":
                free_base = float(self._margin_asset_snapshot(base_asset).get("free", 0.0) or 0.0)
                repay_amount = min(free_base, self._margin_liability(base_asset))
                if repay_amount > 0:
                    self._margin_repay(base_asset, repay_amount)
                return order_result
            order_result["details"].update({
                "borrowed_asset": base_asset,
                "borrowed_quantity": normalized_quantity,
                "borrow_tran_id": borrow_response.get("tranId"),
                "market": "margin-cross",
                "margin_short": True,
            })
            return order_result
        except Exception as exc:
            logger.error("Binance margin short sell error for %s: %s", symbol, exc)
            return {"status": "ERROR", "message": str(exc)}

    def _execute_margin_short_cover(self, symbol: str, quantity: float, price: float) -> Dict[str, Any]:
        if not self.enable_margin_shorting:
            return {"status": "ERROR", "message": "Cross-margin shorting is disabled. Set BINANCE_ENABLE_MARGIN_SHORT=true."}
        if not self.margin_short_available:
            return {"status": "ERROR", "message": self.margin_short_error or "Cross-margin shorting is unavailable for this account."}

        base_asset = self._base_asset_for_symbol(symbol)
        try:
            liability = self._margin_liability(base_asset)
            target_quantity = max(float(quantity), liability)
            validation = self.validate_order(symbol, "EQUITY", target_quantity)
            if not validation.get("valid"):
                return {"status": "ERROR", "message": validation.get("message", "Margin cover validation failed")}
            normalized_quantity = float(validation.get("normalized_quantity", target_quantity) or target_quantity)
            required_quote = max(float(price) * normalized_quantity, 0.0)
            self._ensure_margin_quote_collateral(required_quote, f"reverse carry cover on {symbol}")
            order_result = self._execute_margin_trade(symbol, "BUY", normalized_quantity, price)
            if order_result.get("status") != "EXECUTED":
                return order_result
            snapshot = self._margin_asset_snapshot(base_asset)
            repay_amount = min(float(snapshot.get("free", 0.0) or 0.0), self._margin_liability(base_asset))
            repay_response = self._margin_repay(base_asset, repay_amount) if repay_amount > 0 else {"tranId": None}
            residual_liability = self._margin_liability(base_asset)
            order_result["details"].update({
                "repaid_asset": base_asset,
                "repaid_quantity": repay_amount,
                "repay_tran_id": repay_response.get("tranId"),
                "residual_liability": residual_liability,
                "market": "margin-cross",
                "margin_short": True,
            })
            if residual_liability > 1e-8:
                order_result["message"] = (
                    f"{order_result['message']}. Residual margin liability for {base_asset}: {residual_liability:.8f}"
                )
            return order_result
        except Exception as exc:
            logger.error("Binance margin short cover error for %s: %s", symbol, exc)
            return {"status": "ERROR", "message": str(exc)}

    def _refresh_futures_position_mode(self) -> None:
        try:
            response = self._signed_request("GET", "futures", "/fapi/v1/positionSide/dual")
            dual_side = response.get("dualSidePosition")
            if isinstance(dual_side, str):
                dual_side = dual_side.strip().lower() == "true"
            self.futures_position_mode = "HEDGE" if dual_side else "ONE_WAY"
        except Exception as exc:
            logger.warning("Failed to detect Binance futures position mode, using %s: %s", self.futures_position_mode, exc)

    def _ensure_futures_leverage(self, symbol: str) -> None:
        cached = self._futures_leverage_by_symbol.get(symbol)
        if cached == self.futures_leverage:
            return
        response = self._signed_request(
            "POST",
            "futures",
            "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": self.futures_leverage},
        )
        applied = int(response.get("leverage") or self.futures_leverage)
        self._futures_leverage_by_symbol[symbol] = applied
        if applied != self.futures_leverage:
            raise RuntimeError(f"Requested {self.futures_leverage}x leverage for {symbol}, Binance applied {applied}x")

    def _ensure_futures_margin_type(self, symbol: str) -> None:
        if self.futures_margin_type == "ISOLATED":
            multi_assets_mode = self._get_futures_multi_assets_mode()
            if multi_assets_mode is True:
                raise RuntimeError(
                    "Binance USD-M Futures is currently in Multi-Assets mode, so ISOLATED margin cannot be used. "
                    "Switch Futures Asset Mode to Single-Asset mode, or set BINANCE_FUTURES_MARGIN_TYPE=CROSSED."
                )

        cached = self._futures_margin_type_by_symbol.get(symbol)
        if cached == self.futures_margin_type:
            return
        try:
            self._signed_request(
                "POST",
                "futures",
                "/fapi/v1/marginType",
                params={"symbol": symbol, "marginType": self.futures_margin_type},
            )
        except Exception as exc:
            message = str(exc)
            if "-4168" in message or "Multi-Assets mode" in message:
                self._futures_multi_assets_mode = True
                raise RuntimeError(
                    "Binance rejected ISOLATED margin because the USD-M Futures account is in Multi-Assets mode. "
                    "Turn off Multi-Assets mode in Binance Futures settings, or use BINANCE_FUTURES_MARGIN_TYPE=CROSSED."
                ) from exc
            if "-4046" not in message and "No need to change margin type" not in message:
                raise
        self._futures_margin_type_by_symbol[symbol] = self.futures_margin_type

    def _load_exchange_metadata(self) -> None:
        try:
            spot_info = self._http_json("GET", self.spot_base_url, "/api/v3/exchangeInfo")
            futures_info = self._http_json("GET", self.futures_base_url, "/fapi/v1/exchangeInfo")
        except Exception as exc:
            logger.warning("Failed to load Binance exchange metadata: %s", exc)
            return

        self.exchange_filters["EQUITY"] = self._extract_symbol_filters(spot_info.get("symbols", []), market_type="spot")
        self.exchange_filters["FUTURES"] = self._extract_symbol_filters(futures_info.get("symbols", []), market_type="futures")

        if self.symbols:
            intersection = [
                symbol for symbol in self.symbols
                if symbol in self.exchange_filters["EQUITY"] and symbol in self.exchange_filters["FUTURES"]
            ]
        else:
            intersection = sorted(set(self.exchange_filters["EQUITY"]).intersection(self.exchange_filters["FUTURES"]))
        self.symbols = intersection or self.symbols
        self.equity_symbols = self.symbols.copy()
        self.futures_symbols = self.symbols.copy()
        self.all_symbols = self.symbols.copy()

        for symbol in self.symbols:
            self.lot_sizes[symbol] = max(float(self._common_contract_spec(symbol).get("min_qty", 0) or 0), 0.0)

        logger.info("Loaded Binance metadata for %s tracked symbols", len(self.symbols))

    def _extract_symbol_filters(self, symbols: List[Dict[str, Any]], market_type: str) -> Dict[str, Dict[str, float]]:
        extracted: Dict[str, Dict[str, float]] = {}
        target = set(self.symbols)
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper()
            if target and symbol not in target:
                continue
            if not symbol.endswith(self.quote_asset):
                continue
            if market_type == "spot" and item.get("status") != "TRADING":
                continue
            if market_type == "futures" and item.get("contractType") != "PERPETUAL":
                continue
            filters = {flt.get("filterType"): flt for flt in item.get("filters", [])}
            lot_filter = filters.get("LOT_SIZE") or {}
            min_notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
            min_qty = float(lot_filter.get("minQty", item.get("minQty", 0)) or 0)
            step_size = float(lot_filter.get("stepSize", 0) or 0)
            min_notional = float(
                min_notional_filter.get("minNotional", min_notional_filter.get("notional", 0)) or 0
            )
            extracted[symbol] = {"min_qty": min_qty, "step_size": step_size, "min_notional": min_notional}
        return extracted

    def _quantize(self, quantity: float, step_size: float) -> float:
        if step_size <= 0:
            return float(quantity)
        qty = Decimal(str(quantity))
        step = Decimal(str(step_size))
        quantized = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
        return float(quantized)

    def _quantize_up(self, quantity: float, step_size: float) -> float:
        if step_size <= 0:
            return float(quantity)
        qty = Decimal(str(quantity))
        step = Decimal(str(step_size))
        quantized = (qty / step).to_integral_value(rounding=ROUND_UP) * step
        return float(quantized)

    def _instrument_contract_spec(self, symbol: str, instrument: str) -> Dict[str, float]:
        filters = self.exchange_filters.get(instrument.upper(), {}).get(symbol, {})
        step_size = float(filters.get("step_size", 0) or 0)
        min_qty = max(float(filters.get("min_qty", 0) or 0), step_size)
        return {"step_size": step_size, "min_qty": min_qty}

    def _common_contract_spec(self, symbol: str) -> Dict[str, float]:
        spot = self.exchange_filters["EQUITY"].get(symbol, {})
        futures = self.exchange_filters["FUTURES"].get(symbol, {})
        step_size = max(float(spot.get("step_size", 0) or 0), float(futures.get("step_size", 0) or 0))
        min_qty = max(float(spot.get("min_qty", 0) or 0), float(futures.get("min_qty", 0) or 0), step_size)
        min_notional = max(float(spot.get("min_notional", 0) or 0), float(futures.get("min_notional", 0) or 0))
        return {"step_size": step_size, "min_qty": min_qty, "min_notional": min_notional}

    def _normalize_pair_quantity(self, symbol: str, quantity: float) -> float:
        spec = self._common_contract_spec(symbol)
        step_size = float(spec.get("step_size", 0) or 0)
        min_qty = float(spec.get("min_qty", 0) or 0)
        normalized = float(self._quantize(quantity, step_size) or 0.0)
        if normalized <= 0:
            return 0.0
        if min_qty > 0 and normalized < min_qty:
            return 0.0
        return normalized

    def get_minimum_entry_quantity(self, symbol: str, equity_price: float, futures_price: float) -> float:
        spot = self._instrument_contract_spec(symbol, "EQUITY")
        futures = self._instrument_contract_spec(symbol, "FUTURES")
        common = self._common_contract_spec(symbol)
        candidates = [
            float(spot.get("min_qty", 0) or 0),
            float(futures.get("min_qty", 0) or 0),
        ]
        spot_min_notional = float(self.exchange_filters.get("EQUITY", {}).get(symbol, {}).get("min_notional", 0) or 0)
        futures_min_notional = float(self.exchange_filters.get("FUTURES", {}).get(symbol, {}).get("min_notional", 0) or 0)
        effective_min_quote = max(
            float(self.min_arbitrage_investment_quote or 0.0),
            spot_min_notional,
            futures_min_notional,
        )
        if equity_price > 0 and effective_min_quote > 0:
            candidates.append(effective_min_quote / equity_price)
        if futures_price > 0 and futures_min_notional > 0:
            candidates.append(futures_min_notional / futures_price)
        required_qty = max(candidates + [0.0])
        step_size = float(common.get("step_size", 0) or 0)
        if step_size > 0:
            required_qty = self._quantize_up(required_qty, step_size)
        return float(required_qty)

    def _empty_market_data(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        target_symbols = list(dict.fromkeys(symbols or self.symbols))
        payload = {
            "equity": {},
            "futures": {},
            "timestamp": datetime.now().isoformat(),
            "symbols_fetched": len(target_symbols),
            "symbols_with_both_quotes": 0,
            "source": "rest-empty",
        }
        if self.auth_error:
            payload["auth_error"] = self.auth_error
        if self.rate_limit_error:
            payload["rate_limit_error"] = self.rate_limit_error
        return payload

    def _filter_market_data(self, market_data: Dict[str, Any], symbols: Optional[List[str]]) -> Dict[str, Any]:
        target_symbols = list(dict.fromkeys(symbols or self.symbols))
        equity = {symbol: market_data.get("equity", {}).get(symbol) for symbol in target_symbols if symbol in market_data.get("equity", {})}
        futures = {symbol: market_data.get("futures", {}).get(symbol) for symbol in target_symbols if symbol in market_data.get("futures", {})}
        payload = {
            "equity": equity,
            "futures": futures,
            "timestamp": market_data.get("timestamp", datetime.now().isoformat()),
            "symbols_fetched": len(target_symbols),
            "symbols_with_both_quotes": len(set(equity.keys()) & set(futures.keys())),
            "source": market_data.get("source", "rest-cache"),
        }
        if self.auth_error:
            payload["auth_error"] = self.auth_error
        if self.rate_limit_error:
            payload["rate_limit_error"] = self.rate_limit_error
        return payload

    def _extract_ban_until(self, message: str) -> Optional[float]:
        match = re.search(r"banned until (\d+)", message)
        if not match:
            return None
        try:
            return int(match.group(1)) / 1000.0
        except ValueError:
            return None

    def update_futures_symbols(self, expiry_date: Optional[str] = None) -> None:
        if expiry_date:
            self.expiry_date = expiry_date

    def load_token_from_file(self) -> Optional[str]:
        return None

    def load_token_from_env(self) -> Optional[str]:
        return None

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def verify_token(self) -> bool:
        if not self.api_key or not self.secret_key:
            self.auth_error = "Missing Binance credentials. Set BINANCE_API_KEY and BINANCE_SECRET_KEY in .env."
            logger.warning("%s", self.auth_error)
            return False

        restrictions: Dict[str, Any] = {}
        try:
            restrictions = self._api_key_permissions()
        except Exception as exc:
            logger.warning("Could not read Binance API key restrictions: %s", exc)

        try:
            self._signed_request("GET", "spot", "/api/v3/account")
        except Exception as exc:
            restriction_bits = []
            if restrictions:
                restriction_bits = [
                    f"enableReading={restrictions.get('enableReading')}",
                    f"enableSpotAndMarginTrading={restrictions.get('enableSpotAndMarginTrading')}",
                    f"ipRestrict={restrictions.get('ipRestrict')}",
                ]
            restriction_text = f" API restrictions: {', '.join(restriction_bits)}." if restriction_bits else ""
            self.auth_error = (
                "Binance spot credential verification failed. "
                f"Mode={'TESTNET' if self.use_testnet else 'LIVE'}. "
                "The bot could not access GET /api/v3/account. "
                "Check spot read or trading permissions, the IP whitelist, and whether the key matches the selected environment."
                f"{restriction_text} Binance response: {exc}"
            )
            logger.warning("%s", self.auth_error)
            return False

        try:
            self._signed_request("GET", "futures", "/fapi/v2/account")
            self._refresh_futures_position_mode()
            self._get_futures_multi_assets_mode(refresh=True)
            self.auth_error = None
            if self.enable_margin_shorting:
                if not self._margin_short_environment_supported():
                    self.margin_short_available = False
                    self.margin_short_error = (
                        "Binance cross-margin shorting is not available on the configured demo/testnet spot endpoint. "
                        "The bot can still trade regular spot/futures carry, but reverse carry spot shorts are blocked in this environment. "
                        "Use a Binance environment that supports cross-margin endpoints to enable true reverse carry."
                    )
                    logger.warning("%s", self.margin_short_error)
                else:
                    try:
                        self._margin_account_details()
                        self.margin_short_available = True
                        self.margin_short_error = None
                    except Exception as exc:
                        self.margin_short_available = False
                        self.margin_short_error = (
                            "Binance cross-margin verification failed. The bot can still trade regular spot/futures carry, "
                            f"but reverse carry spot shorts are blocked. Binance response: {exc}"
                        )
                        logger.warning("%s", self.margin_short_error)
            logger.info("Binance spot and futures API credentials verified in %s futures mode", self.futures_position_mode)
            return True
        except Exception as exc:
            restriction_bits = []
            if restrictions:
                restriction_bits = [
                    f"enableReading={restrictions.get('enableReading')}",
                    f"enableFutures={restrictions.get('enableFutures')}",
                    f"enableSpotAndMarginTrading={restrictions.get('enableSpotAndMarginTrading')}",
                    f"ipRestrict={restrictions.get('ipRestrict')}",
                ]
            restriction_text = f" API restrictions: {', '.join(restriction_bits)}." if restriction_bits else ""
            self.auth_error = (
                "Binance futures credential verification failed. "
                f"Mode={'TESTNET' if self.use_testnet else 'LIVE'}. "
                "The bot could not access GET /fapi/v2/account. "
                "Check futures permission, the IP whitelist, whether the futures account is activated, and whether the key matches the selected environment."
                f"{restriction_text} Binance response: {exc}"
            )
            logger.warning("%s", self.auth_error)
            return False

    def _book_ticker_map(self, market: str) -> Dict[str, Dict[str, Any]]:
        base_url = self.spot_base_url if market == "spot" else self.futures_base_url
        path = "/api/v3/ticker/bookTicker" if market == "spot" else "/fapi/v1/ticker/bookTicker"
        response = self._http_json("GET", base_url, path)
        items = response if isinstance(response, list) else []
        target = set(self.symbols)
        mapping: Dict[str, Dict[str, Any]] = {}
        for item in items:
            symbol = str(item.get("symbol", "")).upper()
            if symbol not in target:
                continue
            mapping[symbol] = {
                "bid": float(item.get("bidPrice", 0) or 0),
                "ask": float(item.get("askPrice", 0) or 0),
                "bid_qty": float(item.get("bidQty", 0) or 0),
                "ask_qty": float(item.get("askQty", 0) or 0),
            }
        return mapping

    def get_market_data(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        target_symbols = list(dict.fromkeys(symbols or self.symbols))
        if not target_symbols:
            return self._empty_market_data(symbols=[])

        now = time.time()
        if self.market_data_cache and now < self.market_data_backoff_until:
            return self._filter_market_data(self.market_data_cache, target_symbols)

        if self.market_data_cache and (now - self.market_data_cache_ts) < self.market_data_ttl:
            return self._filter_market_data(self.market_data_cache, target_symbols)

        try:
            spot_map = self._book_ticker_map("spot")
            futures_map = self._book_ticker_map("futures")
            self.rate_limit_error = None
            self.market_data_cache = {
                "equity": spot_map,
                "futures": futures_map,
                "timestamp": datetime.now().isoformat(),
                "source": "rest-live",
            }
            self.market_data_cache_ts = now
            return self._filter_market_data(self.market_data_cache, target_symbols)
        except Exception as exc:
            message = str(exc)
            if "-1003" in message or "418" in message:
                ban_until = self._extract_ban_until(message)
                self.market_data_backoff_until = ban_until or (now + 60.0)
                until_text = datetime.fromtimestamp(self.market_data_backoff_until).isoformat()
                self.rate_limit_error = f"Binance rate limit active until {until_text}. Using cached market data when available."
                logger.warning("%s", self.rate_limit_error)
            else:
                logger.warning("Falling back to cached Binance market data after fetch failure: %s", exc)

            if self.market_data_cache:
                cached = self._filter_market_data(self.market_data_cache, target_symbols)
                cached["source"] = "rest-cache"
                return cached
            return self._empty_market_data(symbols=target_symbols)

    def get_symbol_snapshots(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        snapshots: Dict[str, Dict[str, Any]] = {}
        market_data = self.get_market_data(symbols=symbols)
        for symbol in symbols:
            equity = market_data["equity"].get(symbol)
            futures = market_data["futures"].get(symbol)
            if not equity or not futures:
                continue
            equity_bid = float(equity.get("bid", 0) or 0)
            equity_ask = float(equity.get("ask", 0) or 0)
            futures_bid = float(futures.get("bid", 0) or 0)
            futures_ask = float(futures.get("ask", 0) or 0)
            if min(equity_bid, equity_ask, futures_bid, futures_ask) <= 0:
                continue
            snapshots[symbol] = {
                "symbol": symbol,
                "equity_bid": round(equity_bid, 8),
                "equity_ask": round(equity_ask, 8),
                "futures_bid": round(futures_bid, 8),
                "futures_ask": round(futures_ask, 8),
                "buy_equity_sell_futures_spread": round(futures_bid - equity_ask, 8),
                "sell_equity_buy_futures_spread": round(equity_bid - futures_ask, 8),
                "timestamp": market_data.get("timestamp", datetime.now().isoformat()),
            }
        return snapshots

    def _fetch_symbol_funding_history(self, symbol: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        response = self._http_json(
            "GET",
            self.futures_base_url,
            "/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": max(1, int(limit or self.funding_history_limit))},
            timeout=self.funding_request_timeout,
        )
        return response if isinstance(response, list) else []

    def _load_funding_info_metadata(self) -> None:
        try:
            response = self._http_json(
                "GET",
                self.futures_base_url,
                "/fapi/v1/fundingInfo",
                timeout=self.funding_request_timeout,
            )
        except Exception as exc:
            logger.warning("Failed to load Binance funding interval metadata: %s", exc)
            return

        if not isinstance(response, list):
            return

        intervals: Dict[str, int] = {}
        for item in response:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue
            try:
                interval_hours = max(1, int(item.get("fundingIntervalHours") or self.default_funding_interval_hours))
            except (TypeError, ValueError):
                interval_hours = self.default_funding_interval_hours
            intervals[symbol] = interval_hours

        self.funding_interval_hours_by_symbol = intervals

    def _funding_history_limit_for_symbol(self, symbol: str) -> int:
        interval_hours = self.funding_interval_hours_by_symbol.get(symbol, self.default_funding_interval_hours)
        required_points = max(1, int(math.ceil((self.funding_window_days * 24.0) / float(interval_hours))))
        return max(self.funding_history_limit, required_points)

    def _fetch_current_funding_rates(self, symbols: Optional[List[str]] = None) -> Dict[str, float]:
        response = self._http_json(
            "GET",
            self.futures_base_url,
            "/fapi/v1/premiumIndex",
            timeout=self.funding_request_timeout,
        )

        target_symbols = {symbol.upper() for symbol in (symbols or [])}
        items = response if isinstance(response, list) else [response]
        rates: Dict[str, float] = {}

        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            if not symbol or (target_symbols and symbol not in target_symbols):
                continue
            try:
                rates[symbol] = float(item.get("lastFundingRate", 0.0) or 0.0) * 100.0
            except (TypeError, ValueError):
                continue

        return rates

    def _build_funding_metrics(
        self,
        symbol: str,
        history: List[Dict[str, Any]],
        current_rate_percent: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        if not history:
            return None

        rates = [float(item.get("fundingRate", 0.0) or 0.0) for item in history]
        if not rates:
            return None

        cumulative_rate_percent = sum(rates) * 100.0
        apr_percent = (abs(cumulative_rate_percent) / 3.0) * 365.0
        next_rate_percent = (
            float(current_rate_percent)
            if current_rate_percent is not None
            else rates[-1] * 100.0
        )

        if math.isclose(cumulative_rate_percent, 0.0, abs_tol=1e-12):
            return None

        if cumulative_rate_percent > 0:
            opportunity_type = "BUY_EQUITY_SELL_FUTURES"
            action = "Buy Spot / Sell Perp"
            funding_side = "SHORT_FUTURES_RECEIVES"
        else:
            opportunity_type = "SELL_EQUITY_BUY_FUTURES"
            action = "Sell Spot / Buy Perp"
            funding_side = "LONG_FUTURES_RECEIVES"

        last_time = history[-1].get("fundingTime")
        funding_time = (
            datetime.fromtimestamp(int(last_time) / 1000.0).isoformat()
            if last_time not in (None, "")
            else datetime.now().isoformat()
        )

        return {
            "symbol": symbol,
            "funding_3d_percent": round(cumulative_rate_percent, 6),
            "apr_3d_percent": round(apr_percent, 3),
            "next_funding_rate_percent": round(next_rate_percent, 6),
            "funding_side": funding_side,
            "opportunity_type": opportunity_type,
            "trade_buttons": action,
            "funding_updated_at": funding_time,
        }

    def get_funding_metrics(self, symbols: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        target_symbols = list(dict.fromkeys(symbols or self.symbols))
        if not target_symbols:
            return {}

        now = time.time()
        cache = dict(self.funding_metrics_cache or {})
        cache_covers_targets = bool(target_symbols) and all(symbol in cache for symbol in target_symbols)
        cache_is_fresh = cache_covers_targets and (now - self.funding_metrics_cache_ts) < self.funding_data_ttl
        if cache_is_fresh:
            return {symbol: cache[symbol] for symbol in target_symbols if symbol in cache}

        deadline = now + self.funding_refresh_budget_seconds
        batch_count = 0
        start_index = self.funding_refresh_cursor % len(target_symbols)
        rotated_symbols = [target_symbols[(start_index + idx) % len(target_symbols)] for idx in range(len(target_symbols))]
        missing_symbols = [symbol for symbol in rotated_symbols if symbol not in cache]
        cached_symbols = [symbol for symbol in rotated_symbols if symbol in cache]
        refresh_order = missing_symbols + cached_symbols
        processed = 0
        try:
            current_funding_rates = self._fetch_current_funding_rates(target_symbols)
        except Exception as exc:
            logger.warning("Failed to fetch current Binance funding rates: %s", exc)
            current_funding_rates = {}

        while processed < len(refresh_order) and batch_count < self.funding_batch_size and time.time() < deadline:
            symbol = refresh_order[processed]
            processed += 1
            try:
                history = self._fetch_symbol_funding_history(symbol, limit=self._funding_history_limit_for_symbol(symbol))
                item = self._build_funding_metrics(
                    symbol,
                    history,
                    current_rate_percent=current_funding_rates.get(symbol),
                )
                if item:
                    cache[symbol] = item
                else:
                    cache.pop(symbol, None)
                batch_count += 1
            except Exception as exc:
                logger.warning("Failed to fetch 3-day funding history for %s: %s", symbol, exc)

        self.funding_refresh_cursor = (start_index + processed) % len(target_symbols)
        if cache:
            self.funding_metrics_cache = cache
            self.funding_metrics_cache_ts = time.time()

        return {symbol: cache[symbol] for symbol in target_symbols if symbol in cache}

    def calculate_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        opportunities: List[Dict[str, Any]] = []
        market_data = self.get_market_data(symbols=self.symbols)
        funding_metrics = self.get_funding_metrics(self.symbols)
        try:
            spot_balances = self._spot_account_balances()
        except Exception as exc:
            logger.warning("Failed to load spot balances for opportunity gating: %s", exc)
            spot_balances = {}
        try:
            futures_balances = self._futures_account_balances()
        except Exception as exc:
            logger.warning("Failed to load futures balances for opportunity gating: %s", exc)
            futures_balances = {}
        try:
            margin_assets = self._margin_account_assets() if self.enable_margin_shorting and self.margin_short_available else {}
        except Exception as exc:
            logger.warning("Failed to load margin balances for opportunity gating: %s", exc)
            margin_assets = {}
        for symbol in self.symbols:
            equity = market_data["equity"].get(symbol)
            futures = market_data["futures"].get(symbol)
            funding = funding_metrics.get(symbol)
            if not equity or not futures or not funding:
                continue
            opp = self._calculate_opportunity(
                symbol,
                equity.get("bid", 0),
                equity.get("ask", 0),
                futures.get("bid", 0),
                futures.get("ask", 0),
                funding,
            )
            if opp:
                opp["can_place_order"] = True
                opp["trade_blocked_reason"] = ""
                if opp.get("opportunity_type") == "SELL_EQUITY_BUY_FUTURES":
                    spot_reference_price = float(opp.get("equity_ask", 0) or 0)
                    required_qty = self.get_pair_quantity_for_notional(
                        symbol,
                        self.default_order_notional,
                        spot_reference_price,
                        float(opp.get("futures_ask", 0) or 0),
                    )
                    block_reason = self._reverse_carry_entry_block_reason(
                        symbol,
                        required_qty,
                        spot_reference_price,
                        float(opp.get("futures_ask", 0) or 0.0),
                        spot_balances=spot_balances,
                        futures_balances=futures_balances,
                        margin_assets=margin_assets,
                    )
                    if block_reason:
                        opp["can_place_order"] = False
                        opp["trade_blocked_reason"] = block_reason
                opportunities.append(opp)
        opportunities.sort(
            key=lambda item: (
                float(item.get("apr_3d_percent", 0.0) or 0.0),
                float(item.get("spread_percent", 0.0) or 0.0),
                item.get("symbol", ""),
            ),
            reverse=True,
        )
        return opportunities[:10]

    def _calculate_opportunity(
        self,
        symbol: str,
        equity_bid: float,
        equity_ask: float,
        futures_bid: float,
        futures_ask: float,
        funding: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if min(equity_bid, equity_ask, futures_bid, futures_ask) <= 0:
            return None

        equity_mid = (equity_bid + equity_ask) / 2.0
        futures_mid = (futures_bid + futures_ask) / 2.0
        opportunity_type = funding.get("opportunity_type")
        if opportunity_type == "BUY_EQUITY_SELL_FUTURES":
            buy_price = equity_ask
            sell_price = futures_bid
            spread = futures_mid - equity_mid
            spread_percent = (spread / equity_mid) * 100.0 if equity_mid > 0 else 0.0
            action = f"Buy Spot @ {equity_ask:.6f}, Sell Perp @ {futures_bid:.6f}"
        else:
            opportunity_type = "SELL_EQUITY_BUY_FUTURES"
            buy_price = futures_ask
            sell_price = equity_bid
            spread = equity_mid - futures_mid
            spread_percent = (spread / equity_mid) * 100.0 if equity_mid > 0 else 0.0
            action = f"Sell Spot @ {equity_bid:.6f}, Buy Perp @ {futures_ask:.6f}"
        execution_spread = sell_price - buy_price
        execution_spread_percent = (execution_spread / buy_price) * 100.0 if buy_price > 0 else 0.0
        trade_buttons = funding.get("trade_buttons", "")

        if spread_percent < ARBITRAGE_THRESHOLD:
            return None

        return {
            "symbol": symbol,
            "equity_bid": round(equity_bid, 8),
            "equity_ask": round(equity_ask, 8),
            "futures_bid": round(futures_bid, 8),
            "futures_ask": round(futures_ask, 8),
            "spread": round(spread, 8),
            "spread_percent": round(spread_percent, 3),
            "execution_spread": round(execution_spread, 8),
            "execution_spread_percent": round(execution_spread_percent, 3),
            "opportunity_type": opportunity_type,
            "action": action,
            "trade_buttons": trade_buttons,
            "funding_3d_percent": float(funding.get("funding_3d_percent", 0.0) or 0.0),
            "apr_3d_percent": float(funding.get("apr_3d_percent", 0.0) or 0.0),
            "next_funding_rate_percent": float(funding.get("next_funding_rate_percent", 0.0) or 0.0),
            "funding_side": funding.get("funding_side"),
            "funding_updated_at": funding.get("funding_updated_at"),
            "timestamp": datetime.now().isoformat(),
        }

    def get_default_quantity(self, symbol: str) -> float:
        spec = self._common_contract_spec(symbol)
        min_qty = float(spec.get("min_qty", 0) or 0)
        step_size = float(spec.get("step_size", 0) or 0)
        snapshot = self.get_symbol_snapshots([symbol]).get(symbol)
        ask_price = float(snapshot.get("equity_ask", 0) or 0) if snapshot else 0.0
        if ask_price > 0:
            qty = max(self.default_order_notional / ask_price, min_qty)
        else:
            qty = max(min_qty, step_size, 0.0)
        qty = self._quantize(qty, step_size)
        if qty <= 0:
            qty = max(min_qty, step_size, 0.0)
        return float(qty or 0.0)

    def get_pair_quantity_for_notional(
        self,
        symbol: str,
        target_notional_quote: float,
        equity_price: float,
        futures_price: float,
    ) -> float:
        if target_notional_quote <= 0:
            return 0.0

        candidates = []
        if equity_price > 0:
            candidates.append(float(target_notional_quote) / float(equity_price))
        if futures_price > 0:
            candidates.append(float(target_notional_quote) / float(futures_price))

        if not candidates:
            return 0.0

        spec = self._common_contract_spec(symbol)
        step_size = float(spec.get("step_size", 0) or 0)
        minimum_entry_qty = self.get_minimum_entry_quantity(symbol, equity_price, futures_price)
        quantity = max(max(candidates), minimum_entry_qty, 0.0)
        if step_size > 0:
            quantity = self._quantize_up(quantity, step_size)
        return float(quantity or 0.0)

    def validate_order(self, symbol: str, instrument: Optional[str] = None, quantity: Optional[float] = None) -> Dict[str, Any]:
        try:
            if quantity is None and isinstance(instrument, (int, float)):
                quantity = float(instrument)
                instrument = "EQUITY"
            instrument = (instrument or "EQUITY").upper()
            if symbol not in self.symbols:
                return {"valid": False, "message": f"Symbol {symbol} not found in tracked Binance symbols"}
            qty = float(quantity or 0)
            if qty <= 0:
                return {"valid": False, "message": f"Quantity must be positive. Received: {quantity}"}
            spec = self._instrument_contract_spec(symbol, instrument) if instrument in {"EQUITY", "FUTURES"} else {}
            min_qty = float(spec.get("min_qty", 0) or 0)
            step_size = float(spec.get("step_size", 0) or 0)
            normalized = self._quantize(qty, step_size)
            if min_qty > 0 and normalized < min_qty:
                return {"valid": False, "message": f"Quantity {qty} is below Binance minimum {min_qty} for {symbol}"}
            if step_size > 0 and not math.isclose(normalized, qty, rel_tol=0, abs_tol=max(step_size / 1000.0, 1e-12)):
                return {"valid": False, "message": f"Quantity {qty} does not align with Binance step size {step_size} for {symbol}"}
            return {"valid": True, "message": "Order validated", "normalized_quantity": normalized}
        except Exception as exc:
            logger.error("Error validating Binance order: %s", exc)
            return {"valid": False, "message": f"Validation error: {exc}"}

    def check_funds(self, required_amount: float) -> bool:
        return True

    def _format_quantity(self, symbol: str, instrument: str, quantity: float) -> str:
        filters = self.exchange_filters.get(instrument.upper(), {}).get(symbol, {})
        step_size = float(filters.get("step_size", 0) or 0)
        normalized = self._quantize(quantity, step_size)
        if step_size <= 0:
            return format(normalized, ".8f").rstrip("0").rstrip(".")
        decimals = max(0, -Decimal(str(step_size)).as_tuple().exponent)
        return f"{normalized:.{decimals}f}"

    def _format_quote_amount(self, amount: float) -> str:
        return format(max(float(amount), 0.0), ".8f").rstrip("0").rstrip(".")

    def _quote_amount_for_target_base_quantity(self, symbol: str, target_quantity: float, reference_price: float) -> float:
        if target_quantity <= 0 or reference_price <= 0:
            return 0.0
        spot_spec = self._instrument_contract_spec(symbol, "EQUITY")
        step_size = float(spot_spec.get("step_size", 0) or 0)
        fee_buffer = float(self.spot_conversion_fee_buffer_ratio or 0.0)
        gross_quantity = float(target_quantity)
        if fee_buffer > 0 and fee_buffer < 1:
            gross_quantity = gross_quantity / (1.0 - fee_buffer)
        gross_quantity += step_size
        return max(gross_quantity * float(reference_price), 0.0)

    def _spot_convert_supported(self) -> bool:
        if not self.prefer_spot_convert:
            return False
        spot_base = str(self.spot_base_url).lower()
        return "demo-api.binance.com" not in spot_base and "testnet.binance.vision" not in spot_base

    def _wait_for_spot_convert_order(self, order_id: Optional[str], quote_id: Optional[str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if order_id:
            params["orderId"] = order_id
        elif quote_id:
            params["quoteId"] = quote_id
        else:
            return {}

        deadline = time.time() + max(float(self.spot_convert_poll_timeout_seconds or 0.0), 0.0)
        latest: Dict[str, Any] = {}
        while True:
            response = self._signed_request("GET", "spot", "/sapi/v1/convert/orderStatus", params=params)
            if isinstance(response, dict):
                latest = response
            order_status = str(latest.get("orderStatus", "") or "").upper()
            if order_status in {"SUCCESS", "FAIL"}:
                return latest
            if time.time() >= deadline:
                return latest
            time.sleep(self.spot_convert_poll_interval_seconds)

    def _execute_spot_convert_buy(self, symbol: str, quote_amount: float, price: float) -> Dict[str, Any]:
        if quote_amount <= 0:
            return {"status": "ERROR", "message": f"Quote amount must be positive. Received: {quote_amount}"}

        base_asset = symbol[:-len(self.quote_asset)] if symbol.endswith(self.quote_asset) else symbol
        quote_params = {
            "fromAsset": self.quote_asset,
            "toAsset": base_asset,
            "fromAmount": self._format_quote_amount(quote_amount),
            "walletType": "SPOT",
            "validTime": self.spot_convert_valid_time,
        }

        quote_id = ""
        accept_attempted = False

        try:
            self._ensure_spot_balance(self.quote_asset, quote_amount, f"spot convert buy for {symbol}")
            pre_balances = self._spot_account_balances()
            pre_base_free = float(pre_balances.get(base_asset.upper(), 0.0) or 0.0)

            quote_response = self._signed_request("POST", "spot", "/sapi/v1/convert/getQuote", params=quote_params)
            quote_id = str((quote_response or {}).get("quoteId", "") or "").strip()
            if not quote_id:
                return {
                    "status": "ERROR",
                    "message": f"Binance Convert did not return a quoteId for {symbol}",
                    "response": {"quote": quote_response},
                    "fallback_safe": True,
                }

            accept_attempted = True
            accept_response = self._signed_request("POST", "spot", "/sapi/v1/convert/acceptQuote", params={"quoteId": quote_id})
            order_id = str((accept_response or {}).get("orderId", "") or "").strip()
            order_status = str((accept_response or {}).get("orderStatus", "") or "").upper()
            final_status = accept_response if isinstance(accept_response, dict) else {}
            if order_status not in {"SUCCESS", "FAIL"}:
                polled_status = self._wait_for_spot_convert_order(order_id or None, quote_id)
                if polled_status:
                    final_status = polled_status
                    order_id = str(polled_status.get("orderId", "") or order_id).strip()
                    order_status = str(polled_status.get("orderStatus", "") or order_status).upper()

            if order_status != "SUCCESS":
                return {
                    "status": "ERROR",
                    "message": f"Binance Convert order for {symbol} did not complete successfully (status: {order_status or 'UNKNOWN'})",
                    "response": {
                        "quote": quote_response,
                        "accept": accept_response,
                        "order_status": final_status,
                    },
                    "fallback_safe": order_status == "FAIL",
                }

            post_balances = self._spot_account_balances()
            post_base_free = float(post_balances.get(base_asset.upper(), 0.0) or 0.0)
            available_delta = max(post_base_free - pre_base_free, 0.0)
            converted_qty = float((final_status or {}).get("toAmount", (quote_response or {}).get("toAmount", 0.0)) or 0.0)

            if converted_qty > 0 and available_delta > 0:
                net_qty = min(converted_qty, available_delta)
            else:
                net_qty = max(converted_qty, available_delta, 0.0)
            if net_qty <= 0:
                return {"status": "ERROR", "message": f"Binance Convert returned zero net spot quantity for {symbol}"}

            normalized_qty = self._normalize_pair_quantity(symbol, net_qty)
            if normalized_qty <= 0:
                common_spec = self._common_contract_spec(symbol)
                min_qty = float(common_spec.get("min_qty", 0) or 0)
                return {"status": "ERROR", "message": f"Net arbitrage quantity {net_qty} is below the common Binance minimum {min_qty} for {symbol}"}

            trade_id = order_id or quote_id or "unknown"
            return {
                "status": "EXECUTED",
                "trade_id": str(trade_id),
                "message": f"Binance Convert exchanged {quote_amount:.6f} {self.quote_asset} into {symbol}",
                "details": {
                    "symbol": symbol,
                    "side": "BUY",
                    "instrument": "EQUITY",
                    "price": price,
                    "quantity": normalized_qty,
                    "gross_quantity": converted_qty,
                    "post_buy_free_balance": post_base_free,
                    "delta_free_balance": available_delta,
                    "quote_amount": quote_amount,
                    "market": "convert",
                    "conversion": True,
                    "convert_quote_id": quote_id,
                    "convert_order_id": order_id or None,
                    "convert_order_status": order_status,
                    "convert_from_amount": (final_status or {}).get("fromAmount", (quote_response or {}).get("fromAmount")),
                    "convert_to_amount": (final_status or {}).get("toAmount", (quote_response or {}).get("toAmount")),
                    "convert_ratio": (final_status or {}).get("ratio", (quote_response or {}).get("ratio")),
                    "convert_inverse_ratio": (final_status or {}).get("inverseRatio", (quote_response or {}).get("inverseRatio")),
                },
                "response": {
                    "quote": quote_response,
                    "accept": accept_response,
                    "order_status": final_status,
                },
            }
        except Exception as exc:
            logger.error("Binance Convert error for %s: %s", symbol, exc)
            return {"status": "ERROR", "message": str(exc), "fallback_safe": not accept_attempted and not quote_id}

    def _execute_spot_market_quote_buy(self, symbol: str, quote_amount: float, price: float) -> Dict[str, Any]:
        if quote_amount <= 0:
            return {"status": "ERROR", "message": f"Quote amount must be positive. Received: {quote_amount}"}

        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": self._format_quote_amount(quote_amount),
            "newOrderRespType": "RESULT",
        }

        try:
            self._ensure_spot_balance(self.quote_asset, quote_amount, f"spot conversion buy for {symbol}")
            base_asset = symbol[:-len(self.quote_asset)] if symbol.endswith(self.quote_asset) else symbol
            pre_balances = self._spot_account_balances()
            pre_base_free = float(pre_balances.get(base_asset.upper(), 0.0) or 0.0)
            response = self._signed_request("POST", "spot", "/api/v3/order", params=params)
            executed_qty = float(response.get("executedQty", 0.0) or 0.0)
            if executed_qty <= 0:
                return {"status": "ERROR", "message": f"Binance returned zero filled quantity for spot conversion on {symbol}"}

            commission_in_base = 0.0
            for fill in response.get("fills", []) or []:
                if str(fill.get("commissionAsset", "")).upper() == base_asset.upper():
                    commission_in_base += float(fill.get("commission", 0.0) or 0.0)

            post_balances = self._spot_account_balances()
            post_base_free = float(post_balances.get(base_asset.upper(), 0.0) or 0.0)
            available_delta = max(post_base_free - pre_base_free, 0.0)
            net_qty = max(min(executed_qty - commission_in_base, available_delta if available_delta > 0 else executed_qty), 0.0)
            if net_qty <= 0:
                return {"status": "ERROR", "message": f"Binance returned zero net spot quantity for conversion on {symbol}"}

            normalized_qty = self._normalize_pair_quantity(symbol, net_qty)
            if normalized_qty <= 0:
                common_spec = self._common_contract_spec(symbol)
                min_qty = float(common_spec.get("min_qty", 0) or 0)
                return {"status": "ERROR", "message": f"Net arbitrage quantity {net_qty} is below the common Binance minimum {min_qty} for {symbol}"}
            trade_id = response.get("orderId") or response.get("clientOrderId") or "unknown"
            return {
                "status": "EXECUTED",
                "trade_id": str(trade_id),
                "message": f"Binance spot market buy exchanged {quote_amount:.6f} {self.quote_asset} into {symbol}",
                "details": {
                    "symbol": symbol,
                    "side": "BUY",
                    "instrument": "EQUITY",
                    "price": price,
                    "quantity": normalized_qty,
                    "gross_quantity": executed_qty,
                    "commission_in_base": commission_in_base,
                    "post_buy_free_balance": post_base_free,
                    "delta_free_balance": available_delta,
                    "quote_amount": quote_amount,
                    "market": "spot",
                    "conversion": True,
                },
                "response": response,
            }
        except Exception as exc:
            logger.error("Binance spot market quote buy error for %s: %s", symbol, exc)
            return {"status": "ERROR", "message": str(exc)}

    def _execute_spot_quote_buy(self, symbol: str, quote_amount: float, price: float) -> Dict[str, Any]:
        if quote_amount <= 0:
            return {"status": "ERROR", "message": f"Quote amount must be positive. Received: {quote_amount}"}

        if self._spot_convert_supported():
            convert_result = self._execute_spot_convert_buy(symbol, quote_amount, price)
            if convert_result.get("status") == "EXECUTED":
                return convert_result
            if not self.spot_convert_fallback_to_market or not convert_result.get("fallback_safe", False):
                return convert_result
            logger.warning(
                "Binance Convert failed for %s, falling back to spot market buy: %s",
                symbol,
                convert_result.get("message", "Unknown error"),
            )

        return self._execute_spot_market_quote_buy(symbol, quote_amount, price)

    def execute_trade(self, symbol: str, side: str, instrument: str, price: float, quantity: float) -> Dict[str, Any]:
        instrument = instrument.upper()
        side = side.upper()
        if self.auth_error:
            return {"status": "ERROR", "message": self.auth_error}
        validation = self.validate_order(symbol, instrument, quantity)
        if not validation.get("valid"):
            return {"status": "ERROR", "message": validation.get("message", "Order validation failed")}

        normalized_quantity = float(validation.get("normalized_quantity", quantity) or quantity)
        filters = self.exchange_filters.get(instrument, {}).get(symbol, {})
        min_notional = float(filters.get("min_notional", 0) or 0)
        estimated_notional = max(float(price) * normalized_quantity, 0.0)
        if min_notional > 0 and estimated_notional + 1e-12 < min_notional:
            return {
                "status": "ERROR",
                "message": f"Estimated notional {estimated_notional:.8f} is below Binance minimum {min_notional:.8f} for {symbol} {instrument}",
            }

        market = "spot" if instrument == "EQUITY" else "futures"
        path = "/api/v3/order" if instrument == "EQUITY" else "/fapi/v1/order"
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self._format_quantity(symbol, instrument, normalized_quantity),
            "newOrderRespType": "RESULT",
        }
        if instrument == "FUTURES":
            if self.set_futures_margin_type:
                self._ensure_futures_margin_type(symbol)
            self._ensure_futures_leverage(symbol)
            if self.futures_position_mode == "HEDGE":
                params["positionSide"] = "LONG" if side == "BUY" else "SHORT"
            else:
                params["positionSide"] = "BOTH"

        try:
            if instrument == "EQUITY":
                base_asset = symbol[:-len(self.quote_asset)] if symbol.endswith(self.quote_asset) else symbol
                if side == "BUY":
                    estimated_quote_needed = max(float(price) * normalized_quantity, 0.0)
                    self._ensure_spot_balance(self.quote_asset, estimated_quote_needed, f"spot buy for {symbol}")
                else:
                    self._ensure_spot_balance(base_asset, normalized_quantity, f"spot sell for {symbol}")
            response = self._signed_request("POST", market, path, params=params)
            trade_id = response.get("orderId") or response.get("clientOrderId") or "unknown"
            return {
                "status": "EXECUTED",
                "trade_id": str(trade_id),
                "message": f"Binance {side} {normalized_quantity} {symbol} {instrument} executed",
                "details": {
                    "symbol": symbol,
                    "side": side,
                    "instrument": instrument,
                    "price": price,
                    "quantity": normalized_quantity,
                    "market": market,
                },
                "response": response,
            }
        except Exception as exc:
            logger.error("Binance trade execution error for %s %s %s: %s", symbol, side, instrument, exc)
            return {"status": "ERROR", "message": str(exc)}

    def execute_arbitrage_pair(
        self,
        symbol: str,
        opportunity_type: str,
        quantity: float,
        equity_price: float,
        futures_price: float,
        action: str = "ENTRY",
    ) -> Dict[str, Any]:
        normalized_action = action.upper()
        if normalized_action not in {"ENTRY", "EXIT"}:
            return {"status": "ERROR", "message": f"Unsupported arbitrage action: {action}"}

        if opportunity_type == "SELL_EQUITY_BUY_FUTURES":
            active_quantity = float(quantity)
            if self.enable_margin_shorting:
                if normalized_action == "ENTRY":
                    active_quantity = float(self._normalize_pair_quantity(symbol, active_quantity) or 0.0)
                    if active_quantity <= 0:
                        return {
                            "status": "ERROR",
                            "message": f"ENTRY failed because the normalized pair quantity is invalid for {symbol}",
                            "leg_results": [],
                        }
                    block_reason = self._reverse_carry_entry_block_reason(symbol, active_quantity, equity_price, futures_price)
                    if block_reason:
                        return {"status": "ERROR", "message": block_reason, "leg_results": []}
                    self._ensure_futures_quote_balance(self._required_futures_margin_quote(futures_price, active_quantity), f"entry futures long on {symbol}")
                    futures_result = self.execute_trade(symbol, "BUY", "FUTURES", futures_price, active_quantity)
                    if futures_result.get("status") != "EXECUTED":
                        return {
                            "status": "ERROR",
                            "message": f"ENTRY failed on leg 1: {futures_result.get('message', 'Unknown error')}",
                            "leg_results": [futures_result],
                        }
                    spot_result = self._execute_margin_short_sell(symbol, active_quantity, equity_price)
                    if spot_result.get("status") != "EXECUTED":
                        revert_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
                        return {
                            "status": "ERROR",
                            "message": f"ENTRY failed on leg 2: {spot_result.get('message', 'Unknown error')}",
                            "leg_results": [futures_result, spot_result],
                            "revert_results": [revert_result],
                        }
                    active_quantity = float(spot_result.get("details", {}).get("quantity", active_quantity) or active_quantity)
                    return {
                        "status": "EXECUTED",
                        "message": f"{normalized_action} completed for {symbol}",
                        "leg_results": [futures_result, spot_result],
                        "executed_quantity": active_quantity,
                    }

                spot_result = self._execute_margin_short_cover(symbol, active_quantity, equity_price)
                if spot_result.get("status") != "EXECUTED":
                    return {
                        "status": "ERROR",
                        "message": f"EXIT failed on leg 1: {spot_result.get('message', 'Unknown error')}",
                        "leg_results": [spot_result],
                    }
                futures_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
                if futures_result.get("status") != "EXECUTED":
                    revert_result = self._execute_margin_short_sell(symbol, active_quantity, equity_price)
                    return {
                        "status": "ERROR",
                        "message": f"EXIT failed on leg 2: {futures_result.get('message', 'Unknown error')}",
                        "leg_results": [spot_result, futures_result],
                        "revert_results": [revert_result],
                    }
                return {
                    "status": "EXECUTED",
                    "message": f"{normalized_action} completed for {symbol}",
                    "leg_results": [spot_result, futures_result],
                    "executed_quantity": active_quantity,
                }

            if normalized_action == "ENTRY":
                active_quantity = float(self._normalize_pair_quantity(symbol, active_quantity) or 0.0)
                if active_quantity <= 0:
                    return {
                        "status": "ERROR",
                        "message": f"ENTRY failed because the normalized pair quantity is invalid for {symbol}",
                        "leg_results": [],
                    }
                block_reason = self._reverse_carry_entry_block_reason(symbol, active_quantity, equity_price, futures_price)
                if block_reason:
                    return {"status": "ERROR", "message": block_reason, "leg_results": []}
                self._ensure_futures_quote_balance(self._required_futures_margin_quote(futures_price, active_quantity), f"entry futures long on {symbol}")
                futures_result = self.execute_trade(symbol, "BUY", "FUTURES", futures_price, active_quantity)
                if futures_result.get("status") != "EXECUTED":
                    return {
                        "status": "ERROR",
                        "message": f"ENTRY failed on leg 1: {futures_result.get('message', 'Unknown error')}",
                        "leg_results": [futures_result],
                    }
                conversion_quote_amount = self._quote_amount_for_target_base_quantity(symbol, active_quantity, float(equity_price))
                conversion_result = self._execute_spot_quote_buy(symbol, conversion_quote_amount, equity_price)
                if conversion_result.get("status") != "EXECUTED":
                    revert_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
                    return {
                        "status": "ERROR",
                        "message": f"ENTRY failed on leg 2: {conversion_result.get('message', 'Unknown error')}",
                        "leg_results": [futures_result, conversion_result],
                        "revert_results": [revert_result],
                    }
                converted_quantity = float(conversion_result.get("details", {}).get("quantity", 0.0) or 0.0)
                if converted_quantity + 1e-12 < active_quantity:
                    revert_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
                    return {
                        "status": "ERROR",
                        "message": f"ENTRY failed because converted spot quantity is lower than the futures entry size for {symbol}",
                        "leg_results": [futures_result, conversion_result],
                        "revert_results": [revert_result],
                    }
                spot_result = self.execute_trade(symbol, "SELL", "EQUITY", equity_price, active_quantity)
                if spot_result.get("status") != "EXECUTED":
                    revert_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
                    return {
                        "status": "ERROR",
                        "message": f"ENTRY failed on leg 3: {spot_result.get('message', 'Unknown error')}",
                        "leg_results": [futures_result, conversion_result, spot_result],
                        "revert_results": [revert_result],
                    }
                return {
                    "status": "EXECUTED",
                    "message": f"{normalized_action} completed for {symbol}",
                    "leg_results": [futures_result, conversion_result, spot_result],
                    "executed_quantity": active_quantity,
                }

            futures_result = self.execute_trade(symbol, "SELL", "FUTURES", futures_price, active_quantity)
            if futures_result.get("status") != "EXECUTED":
                return {
                    "status": "ERROR",
                    "message": f"EXIT failed on leg 1: {futures_result.get('message', 'Unknown error')}",
                    "leg_results": [futures_result],
                }
            return {
                "status": "EXECUTED",
                "message": f"{normalized_action} completed for {symbol}",
                "leg_results": [futures_result],
                "executed_quantity": active_quantity,
            }

        if opportunity_type == "BUY_EQUITY_SELL_FUTURES":
            entry_legs = [
                {"side": "BUY", "instrument": "EQUITY", "price": equity_price},
                {"side": "SELL", "instrument": "FUTURES", "price": futures_price},
            ]
        else:
            return {"status": "ERROR", "message": f"Unsupported opportunity type: {opportunity_type}"}

        legs: List[Dict[str, Any]] = []
        if normalized_action == "EXIT":
            for leg in entry_legs:
                legs.append({
                    "side": "SELL" if leg["side"] == "BUY" else "BUY",
                    "instrument": leg["instrument"],
                    "price": leg["price"],
                })
        else:
            legs = entry_legs.copy()

        active_quantity = float(quantity)
        legs = sorted(legs, key=lambda leg: 0 if leg.get("instrument") == "EQUITY" else 1)

        leg_results: List[Dict[str, Any]] = []
        executed_legs: List[Dict[str, Any]] = []
        for index, leg in enumerate(legs, start=1):
            if normalized_action == "ENTRY" and leg["instrument"] == "FUTURES":
                futures_reason = "entry futures short" if leg["side"] == "SELL" else "entry futures long"
                self._ensure_futures_quote_balance(self._required_futures_margin_quote(leg["price"], active_quantity), f"{futures_reason} on {symbol}")
            leg_result = self.execute_trade(symbol, leg["side"], leg["instrument"], leg["price"], active_quantity)
            leg_results.append(leg_result)
            if leg_result.get("status") != "EXECUTED":
                revert_results: List[Dict[str, Any]] = []
                for executed_leg in reversed(executed_legs):
                    revert_side = "SELL" if executed_leg["side"] == "BUY" else "BUY"
                    revert_quantity = float(executed_leg.get("quantity", active_quantity) or active_quantity)
                    revert_result = self.execute_trade(symbol, revert_side, executed_leg["instrument"], executed_leg["price"], revert_quantity)
                    revert_results.append(revert_result)
                return {
                    "status": "ERROR",
                    "message": f"{normalized_action} failed on leg {index}: {leg_result.get('message', 'Unknown error')}",
                    "leg_results": leg_results,
                    "revert_results": revert_results,
                }

            executed_quantity = float(leg_result.get("details", {}).get("quantity", active_quantity) or active_quantity)
            executed_leg = dict(leg)
            executed_leg["quantity"] = executed_quantity
            executed_legs.append(executed_leg)

        return {
            "status": "EXECUTED",
            "message": f"{normalized_action} completed for {symbol}",
            "leg_results": leg_results,
            "executed_quantity": active_quantity,
        }

    def connect_websocket(self) -> bool:
        logger.info("Binance WebSocket adapter not enabled in this build; using REST polling")
        self.connected = False
        return False

    def disconnect_websocket(self) -> None:
        self.connected = False


_real_broker: Optional[BinanceRealBroker] = None


def get_real_broker() -> BinanceRealBroker:
    global _real_broker
    if _real_broker is None:
        _real_broker = BinanceRealBroker()
        _real_broker.verify_token()
    return _real_broker


def get_cached_real_broker() -> Optional[BinanceRealBroker]:
    return _real_broker


def get_real_market_data() -> Dict[str, Any]:
    broker = get_real_broker()
    market_data = broker.get_market_data()
    if broker.auth_error:
        market_data["auth_error"] = broker.auth_error
    if broker.rate_limit_error:
        market_data["rate_limit_error"] = broker.rate_limit_error
    return market_data


def get_real_opportunities() -> List[Dict[str, Any]]:
    broker = get_real_broker()
    return broker.calculate_arbitrage_opportunities()


def get_real_symbol_snapshots(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    broker = get_real_broker()
    return broker.get_symbol_snapshots(symbols)


def get_runtime_broker_settings() -> Dict[str, Any]:
    broker = get_real_broker()
    return {
        "default_trade_notional_usdt": float(getattr(broker, "default_order_notional", 0.0) or 0.0),
        "set_futures_margin_type": bool(getattr(broker, "set_futures_margin_type", False)),
        "futures_margin_type": str(getattr(broker, "futures_margin_type", "ISOLATED") or "ISOLATED"),
        "futures_multi_assets_mode": broker._get_futures_multi_assets_mode() if getattr(broker, "set_futures_margin_type", False) else None,
        "futures_leverage": int(getattr(broker, "futures_leverage", 1) or 1),
        "margin_short_enabled": bool(getattr(broker, "enable_margin_shorting", False)),
    }


def build_broker_precheck_report(broker: BinanceRealBroker) -> Dict[str, Any]:
    environment = {
        "use_testnet": bool(broker.use_testnet),
        "spot_base_url": str(broker.spot_base_url),
        "futures_base_url": str(broker.futures_base_url),
        "set_futures_margin_type": bool(getattr(broker, "set_futures_margin_type", False)),
        "futures_margin_type": str(getattr(broker, "futures_margin_type", "ISOLATED") or "ISOLATED"),
        "futures_multi_assets_mode": broker._get_futures_multi_assets_mode() if getattr(broker, "set_futures_margin_type", False) else None,
        "margin_short_enabled": bool(getattr(broker, "enable_margin_shorting", False)),
        "margin_short_environment_supported": bool(getattr(broker, "_margin_short_environment_supported", lambda: False)()),
    }
    report: Dict[str, Any] = {
        "environment": environment,
        "permissions": {},
        "checks": {},
    }

    try:
        restrictions = broker._api_key_permissions()
        report["permissions"] = {
            "enableReading": restrictions.get("enableReading"),
            "enableSpotAndMarginTrading": restrictions.get("enableSpotAndMarginTrading"),
            "enableFutures": restrictions.get("enableFutures"),
            "permitsUniversalTransfer": restrictions.get("permitsUniversalTransfer"),
            "ipRestrict": restrictions.get("ipRestrict"),
        }
        report["checks"]["api_restrictions"] = {"ok": True}
    except Exception as exc:
        report["checks"]["api_restrictions"] = {"ok": False, "message": str(exc)}

    try:
        broker._signed_request("GET", "spot", "/api/v3/account")
        report["checks"]["spot_account"] = {"ok": True}
    except Exception as exc:
        report["checks"]["spot_account"] = {"ok": False, "message": str(exc)}

    try:
        broker._signed_request("GET", "futures", "/fapi/v2/account")
        report["checks"]["futures_account"] = {"ok": True}
    except Exception as exc:
        report["checks"]["futures_account"] = {"ok": False, "message": str(exc)}

    if environment["margin_short_enabled"]:
        if not environment["margin_short_environment_supported"]:
            report["checks"]["cross_margin_account"] = {
                "ok": False,
                "message": broker.margin_short_error or "Cross-margin endpoints are not available in this environment.",
            }
        else:
            try:
                margin_account = broker._margin_account_details()
                assets = margin_account.get("userAssets", []) or []
                report["checks"]["cross_margin_account"] = {
                    "ok": True,
                    "asset_count": len(assets),
                }
            except Exception as exc:
                report["checks"]["cross_margin_account"] = {"ok": False, "message": str(exc)}
    else:
        report["checks"]["cross_margin_account"] = {
            "ok": False,
            "message": "Cross-margin shorting is disabled in config.",
        }

    report["ready_for_live_carry"] = bool(
        report["checks"].get("spot_account", {}).get("ok") and report["checks"].get("futures_account", {}).get("ok")
    )
    report["ready_for_live_reverse_carry"] = bool(
        report["ready_for_live_carry"] and report["checks"].get("cross_margin_account", {}).get("ok")
    )
    return report


def execute_real_arbitrage_pair(
    symbol: str,
    opportunity_type: str,
    quantity: float,
    equity_price: float,
    futures_price: float,
    action: str = "ENTRY",
) -> Dict[str, Any]:
    broker = get_real_broker()
    return broker.execute_arbitrage_pair(
        symbol=symbol,
        opportunity_type=opportunity_type,
        quantity=quantity,
        equity_price=equity_price,
        futures_price=futures_price,
        action=action,
    )


def connect_real_broker_websocket() -> bool:
    broker = get_real_broker()
    broker.update_futures_symbols(DEFAULT_EXPIRY)
    return broker.connect_websocket()


def disconnect_real_broker_websocket() -> None:
    broker = get_real_broker()
    broker.disconnect_websocket()

real_broker = SimpleNamespace(
    SUPPORTS_REALTIME_WEBSOCKET=SUPPORTS_REALTIME_WEBSOCKET,
    ARBITRAGE_THRESHOLD=ARBITRAGE_THRESHOLD,
    DEFAULT_EXPIRY=DEFAULT_EXPIRY,
    get_real_broker=get_real_broker,
    get_cached_real_broker=get_cached_real_broker,
    get_real_market_data=get_real_market_data,
    get_real_opportunities=get_real_opportunities,
    get_real_symbol_snapshots=get_real_symbol_snapshots,
    execute_real_arbitrage_pair=execute_real_arbitrage_pair,
    connect_real_broker_websocket=connect_real_broker_websocket,
    disconnect_real_broker_websocket=disconnect_real_broker_websocket,
)

logger = logging.getLogger(__name__)


class AutoTrader:
    def __init__(self) -> None:
        self._lock = Lock()
        self.enabled = False
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.history: Dict[str, Deque[Dict[str, Any]]] = {}
        self.last_errors: List[Dict[str, Any]] = []
        self.last_actions: List[Dict[str, Any]] = []
        self.config: Dict[str, Any] = {
            "entry_threshold_percent": 0.0,
            "exit_shrink_ratio": 0.60,
            "max_open_positions": 10,
            "default_quantity_mode": "lot_size",
            "fixed_quantity": 0.001,
            "persistence_scans": 1,
            "estimated_cost_percent": 0.0,
            "min_net_spread_percent": 0.0,
            "max_bid_ask_width_percent": 0.0,
            "top_candidate_count": 10,
            "min_exit_profit_percent": 0.0,
            "exit_spread_target_percent": -0.1,
        }

    def start(self) -> Dict[str, Any]:
        with self._lock:
            self.enabled = True
            return self._snapshot_locked()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            self.enabled = False
            return self._snapshot_locked()

    def update_config(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if "entry_threshold_percent" in updates:
                self.config["entry_threshold_percent"] = float(updates["entry_threshold_percent"])
            if "exit_shrink_ratio" in updates:
                ratio = float(updates["exit_shrink_ratio"])
                if ratio > 1:
                    ratio = ratio / 100.0
                self.config["exit_shrink_ratio"] = max(0.01, min(ratio, 0.95))
            if "max_open_positions" in updates:
                self.config["max_open_positions"] = max(1, int(updates["max_open_positions"]))
            if "default_quantity_mode" in updates:
                mode = str(updates["default_quantity_mode"]).lower()
                if mode in {"lot_size", "fixed"}:
                    self.config["default_quantity_mode"] = mode
            if "fixed_quantity" in updates:
                self.config["fixed_quantity"] = max(0.00000001, float(updates["fixed_quantity"]))
            if "persistence_scans" in updates:
                self.config["persistence_scans"] = max(1, int(updates["persistence_scans"]))
            if "estimated_cost_percent" in updates:
                self.config["estimated_cost_percent"] = max(0.0, float(updates["estimated_cost_percent"]))
            if "min_net_spread_percent" in updates:
                self.config["min_net_spread_percent"] = max(0.0, float(updates["min_net_spread_percent"]))
            if "max_bid_ask_width_percent" in updates:
                self.config["max_bid_ask_width_percent"] = max(0.0, float(updates["max_bid_ask_width_percent"]))
            if "top_candidate_count" in updates:
                self.config["top_candidate_count"] = max(1, int(updates["top_candidate_count"]))
            if "min_exit_profit_percent" in updates:
                self.config["min_exit_profit_percent"] = float(updates["min_exit_profit_percent"])
            if "exit_spread_target_percent" in updates:
                self.config["exit_spread_target_percent"] = float(updates["exit_spread_target_percent"])
            return self._snapshot_locked()

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return self._snapshot_locked()

    def process_opportunities(self, opportunities: List[Dict[str, Any]]) -> Dict[str, Any]:
        with self._lock:
            opp_map = {opp.get("symbol"): opp for opp in opportunities if opp.get("symbol")}
            self._update_history_locked(opp_map)

            position_snapshots = real_broker.get_real_symbol_snapshots(list(self.positions.keys())) if self.positions else {}

            exited: List[Dict[str, Any]] = []
            for symbol in list(self.positions.keys()):
                snapshot = position_snapshots.get(symbol)
                if snapshot and self._should_exit_locked(self.positions[symbol], snapshot):
                    result = self._exit_position_locked(self.positions[symbol], snapshot)
                    if result:
                        exited.append(result)

            entered: List[Dict[str, Any]] = []
            if self.enabled and len(self.positions) < int(self.config["max_open_positions"]):
                ranked = sorted(
                    opportunities,
                    key=lambda item: (
                        float(item.get("apr_3d_percent", 0.0) or 0.0),
                        float(item.get("spread_percent", 0.0) or 0.0),
                    ),
                    reverse=True,
                )[: int(self.config["top_candidate_count"])]
                for opp in ranked:
                    symbol = opp.get("symbol")
                    if not symbol or symbol in self.positions:
                        continue
                    if len(self.positions) >= int(self.config["max_open_positions"]):
                        break
                    if self._should_enter_locked(opp):
                        result = self._enter_position_locked(opp)
                        if result:
                            entered.append(result)

            return {
                "enabled": self.enabled,
                "entered": entered,
                "exited": exited,
                "positions": list(self.positions.values()),
            }

    def _update_history_locked(self, opp_map: Dict[str, Dict[str, Any]]) -> None:
        window = max(1, int(self.config["persistence_scans"]))
        for symbol, opp in opp_map.items():
            queue = self.history.setdefault(symbol, deque(maxlen=window))
            queue.append(
                {
                    "spread_percent": float(opp.get("spread_percent", 0.0) or 0.0),
                    "opportunity_type": opp.get("opportunity_type"),
                    "timestamp": opp.get("timestamp") or datetime.now().isoformat(),
                }
            )

    def _should_enter_locked(self, opp: Dict[str, Any]) -> bool:
        spread_percent = float(opp.get("spread_percent", 0.0) or 0.0)
        if spread_percent < float(self.config["entry_threshold_percent"]):
            return False

        if not self._passes_liquidity_filter_locked(opp):
            return False

        net_spread = spread_percent - float(self.config["estimated_cost_percent"])
        if net_spread < float(self.config["min_net_spread_percent"]):
            return False

        symbol = opp.get("symbol")
        required_scans = max(1, int(self.config["persistence_scans"]))
        history = list(self.history.get(symbol, []))
        if len(history) < required_scans:
            return False

        expected_type = opp.get("opportunity_type")
        threshold = float(self.config["entry_threshold_percent"])
        recent = history[-required_scans:]
        return all(
            item.get("opportunity_type") == expected_type
            and float(item.get("spread_percent", 0.0) or 0.0) >= threshold
            for item in recent
        )

    def _current_spread_for_position(self, position: Dict[str, Any], snapshot: Dict[str, Any]) -> float:
        if position["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES":
            sell_price = float(snapshot.get("equity_bid", 0.0) or 0.0)
            buy_price = float(snapshot.get("futures_ask", 0.0) or 0.0)
        else:
            sell_price = float(snapshot.get("futures_bid", 0.0) or 0.0)
            buy_price = float(snapshot.get("equity_ask", 0.0) or 0.0)
        return sell_price - buy_price

    def _current_spread_percent_for_position(self, position: Dict[str, Any], snapshot: Dict[str, Any]) -> float:
        if position["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES":
            buy_price = float(snapshot.get("futures_ask", 0.0) or 0.0)
        else:
            buy_price = float(snapshot.get("equity_ask", 0.0) or 0.0)
        if buy_price <= 0:
            return 0.0
        return (self._current_spread_for_position(position, snapshot) / buy_price) * 100.0

    def _target_exit_spread_percent_locked(self, entry_spread_percent: float) -> float:
        estimated_cost = float(self.config["estimated_cost_percent"])
        minimum_profit = float(self.config["min_exit_profit_percent"])
        configured_target = float(self.config.get("exit_spread_target_percent", -0.1) or -0.1)
        no_loss_target = max(0.0, estimated_cost + minimum_profit - float(entry_spread_percent or 0.0))
        return max(configured_target, no_loss_target)

    def _combined_spread_capture_percent_locked(self, position: Dict[str, Any], snapshot: Dict[str, Any]) -> float:
        current_exit_spread_percent = self._current_spread_percent_for_position(position, snapshot)
        return float(position.get("entry_spread_percent", 0.0) or 0.0) + current_exit_spread_percent

    def _should_exit_locked(self, position: Dict[str, Any], snapshot: Dict[str, Any]) -> bool:
        current_spread = self._current_spread_for_position(position, snapshot)
        current_spread_percent = self._current_spread_percent_for_position(position, snapshot)
        combined_capture_percent = self._combined_spread_capture_percent_locked(position, snapshot)
        estimated_cost = float(self.config["estimated_cost_percent"])
        minimum_profit = float(self.config["min_exit_profit_percent"])
        net_capture_percent = combined_capture_percent - estimated_cost
        target_exit_spread_percent = float(
            position.get("exit_target_spread_percent", self._target_exit_spread_percent_locked(position.get("entry_spread_percent", 0.0)))
            or 0.0
        )

        position["last_seen_spread"] = current_spread
        position["last_seen_spread_percent"] = current_spread_percent
        position["combined_spread_capture_percent"] = combined_capture_percent
        position["net_spread_capture_percent"] = net_capture_percent
        position["exit_target_spread_percent"] = target_exit_spread_percent
        return current_spread_percent >= target_exit_spread_percent and net_capture_percent >= minimum_profit

    def _passes_liquidity_filter_locked(self, opp: Dict[str, Any]) -> bool:
        max_width_percent = float(self.config["max_bid_ask_width_percent"])
        if max_width_percent <= 0:
            return True

        eq_bid = float(opp.get("equity_bid", 0.0) or 0.0)
        eq_ask = float(opp.get("equity_ask", 0.0) or 0.0)
        fut_bid = float(opp.get("futures_bid", 0.0) or 0.0)
        fut_ask = float(opp.get("futures_ask", 0.0) or 0.0)
        if min(eq_bid, eq_ask, fut_bid, fut_ask) <= 0:
            return False

        eq_mid = (eq_bid + eq_ask) / 2.0
        fut_mid = (fut_bid + fut_ask) / 2.0
        if eq_mid <= 0 or fut_mid <= 0:
            return False

        eq_width = ((eq_ask - eq_bid) / eq_mid) * 100.0
        fut_width = ((fut_ask - fut_bid) / fut_mid) * 100.0
        return eq_width <= max_width_percent and fut_width <= max_width_percent

    def _resolve_quantity_locked(self, symbol: str, equity_price: float = 0.0, futures_price: float = 0.0) -> float:
        broker = real_broker.get_real_broker()
        if self.config["default_quantity_mode"] == "fixed":
            return float(self.config["fixed_quantity"])

        target_notional = float(getattr(broker, "default_order_notional", 0.0) or 0.0)
        if target_notional > 0 and hasattr(broker, "get_pair_quantity_for_notional"):
            quantity = float(
                broker.get_pair_quantity_for_notional(
                    symbol=symbol,
                    target_notional_quote=target_notional,
                    equity_price=float(equity_price or 0.0),
                    futures_price=float(futures_price or 0.0),
                )
                or 0.0
            )
            if quantity > 0:
                return quantity

        return float(getattr(broker, "get_default_quantity", lambda s: broker.lot_sizes.get(s, 0.0))(symbol) or broker.lot_sizes.get(symbol, 0.0) or self.config["fixed_quantity"])

    def _enter_position_locked(self, opp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        symbol = opp["symbol"]
        broker = real_broker.get_real_broker()

        if opp.get("can_place_order") is False:
            self._record_error_locked(symbol, "entry_blocked", opp.get("trade_blocked_reason", f"Entry blocked for {symbol}"))
            return None

        equity_price = float(opp.get("equity_ask") or 0.0)
        futures_price = float(opp.get("futures_bid") if opp["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES" else opp.get("futures_ask") or 0.0)
        requested_quantity = self._resolve_quantity_locked(symbol, equity_price, futures_price)
        minimum_quantity = float(getattr(broker, "get_minimum_entry_quantity", lambda *args: 0.0)(symbol, equity_price, futures_price) or 0.0)
        quantity = max(requested_quantity, minimum_quantity)

        if quantity <= 0:
            self._record_error_locked(symbol, "entry_failed", f"No valid entry quantity for {symbol}")
            return None

        try:
            if opp["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES":
                required_quote = max(equity_price * quantity, 0.0)
                broker._ensure_spot_balance(broker.quote_asset, required_quote, f"spot buy funding for {symbol} entry")
            elif opp["opportunity_type"] == "SELL_EQUITY_BUY_FUTURES":
                block_reason = broker._reverse_carry_entry_block_reason(symbol, quantity, equity_price, futures_price)
                if block_reason:
                    raise RuntimeError(block_reason)
        except Exception as exc:
            self._record_error_locked(symbol, "entry_failed", str(exc))
            return None

        result = real_broker.execute_real_arbitrage_pair(
            symbol=symbol,
            opportunity_type=opp["opportunity_type"],
            quantity=quantity,
            equity_price=equity_price,
            futures_price=futures_price,
            action="ENTRY",
        )
        if result.get("status") != "EXECUTED":
            self._record_error_locked(symbol, "entry_failed", result.get("message", "Entry failed"))
            return None

        entry_spread = float(opp.get("spread", 0.0) or 0.0)
        entry_spread_percent = float(opp.get("spread_percent", 0.0) or 0.0)
        exit_target = entry_spread * (1.0 - float(self.config["exit_shrink_ratio"]))
        exit_target_spread_percent = self._target_exit_spread_percent_locked(entry_spread_percent)
        position = {
            "symbol": symbol,
            "quantity": float(result.get("executed_quantity", quantity) or quantity),
            "opportunity_type": opp["opportunity_type"],
            "entry_spread": entry_spread,
            "entry_spread_percent": entry_spread_percent,
            "exit_target_spread": round(exit_target, 4),
            "exit_target_spread_percent": round(exit_target_spread_percent, 3),
            "apr_3d_percent": float(opp.get("apr_3d_percent", 0.0) or 0.0),
            "funding_3d_percent": float(opp.get("funding_3d_percent", 0.0) or 0.0),
            "entry_time": datetime.now().isoformat(),
            "entry_prices": {
                "equity": float(opp.get("equity_ask") if opp["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES" else opp.get("equity_bid") or 0.0),
                "futures": float(opp.get("futures_bid") if opp["opportunity_type"] == "BUY_EQUITY_SELL_FUTURES" else opp.get("futures_ask") or 0.0),
            },
            "last_seen_spread": float(opp.get("spread", 0.0) or 0.0),
            "last_seen_spread_percent": float(opp.get("spread_percent", 0.0) or 0.0),
            "combined_spread_capture_percent": float(opp.get("spread_percent", 0.0) or 0.0),
            "net_spread_capture_percent": float(opp.get("spread_percent", 0.0) or 0.0) - float(self.config["estimated_cost_percent"]),
            "entry_result": result,
        }
        self.positions[symbol] = position
        self._record_action_locked(symbol, "entered", f"Entered {symbol} at spread {position['entry_spread_percent']:.3f}% with exit target {position['exit_target_spread_percent']:.3f}% and 3D APR {position['apr_3d_percent']:.2f}%")
        return deepcopy(position)

    def _exit_position_locked(self, position: Dict[str, Any], snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        symbol = position["symbol"]
        opportunity_type = position["opportunity_type"]
        current_spread = self._current_spread_for_position(position, snapshot)
        current_spread_percent = self._current_spread_percent_for_position(position, snapshot)
        result = real_broker.execute_real_arbitrage_pair(
            symbol=symbol,
            opportunity_type=opportunity_type,
            quantity=float(position["quantity"]),
            equity_price=float(snapshot.get("equity_bid") if opportunity_type == "BUY_EQUITY_SELL_FUTURES" else snapshot.get("equity_ask") or 0.0),
            futures_price=float(snapshot.get("futures_ask") if opportunity_type == "BUY_EQUITY_SELL_FUTURES" else snapshot.get("futures_bid") or 0.0),
            action="EXIT",
        )
        if result.get("status") != "EXECUTED":
            self._record_error_locked(symbol, "exit_failed", result.get("message", "Exit failed"))
            return None

        closed = deepcopy(position)
        closed["exit_time"] = datetime.now().isoformat()
        closed["exit_spread"] = current_spread
        closed["exit_spread_percent"] = current_spread_percent
        closed["combined_spread_capture_percent"] = float(position.get("entry_spread_percent", 0.0) or 0.0) + current_spread_percent
        closed["net_spread_capture_percent"] = closed["combined_spread_capture_percent"] - float(self.config["estimated_cost_percent"])
        closed["exit_result"] = result
        self.positions.pop(symbol, None)
        self._record_action_locked(symbol, "exited", f"Exited {symbol} at spread {closed['exit_spread_percent']:.3f}% net {closed['net_spread_capture_percent']:.3f}%")
        return closed

    def _record_error_locked(self, symbol: str, code: str, message: str) -> None:
        self.last_errors.insert(
            0,
            {
                "symbol": symbol,
                "code": code,
                "message": message,
                "timestamp": datetime.now().isoformat(),
            },
        )
        del self.last_errors[10:]
        logger.error("AutoTrader %s %s: %s", symbol, code, message)

    def _record_action_locked(self, symbol: str, action: str, message: str) -> None:
        self.last_actions.insert(
            0,
            {
                "symbol": symbol,
                "action": action,
                "message": message,
                "timestamp": datetime.now().isoformat(),
            },
        )
        del self.last_actions[10:]
        logger.info("AutoTrader %s %s", symbol, message)

    def _snapshot_locked(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "config": deepcopy(self.config),
            "open_positions": len(self.positions),
            "positions": deepcopy(list(self.positions.values())),
            "last_errors": deepcopy(self.last_errors),
            "last_actions": deepcopy(self.last_actions),
        }

def run_credential_check() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")
    load_dotenv(env_path)

    print("=" * 60)
    print("Binance Credential Check")
    print("=" * 60)

    api_key = os.getenv("BINANCE_API_KEY") or os.getenv("API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY") or os.getenv("SECRET_KEY")
    use_testnet = str(os.getenv("BINANCE_USE_TESTNET", "true")).strip().lower() in {"1", "true", "yes", "on"}
    spot_base = os.getenv("BINANCE_SPOT_BASE_URL") or ("https://testnet.binance.vision" if use_testnet else "https://api.binance.com")
    futures_base = os.getenv("BINANCE_FUTURES_BASE_URL") or ("https://testnet.binancefuture.com" if use_testnet else "https://fapi.binance.com")
    tracked = os.getenv("BINANCE_BASE_SYMBOLS") or "BTCUSDT,ETHUSDT,SOLUSDT"

    print(f"Mode: {'TESTNET' if use_testnet else 'LIVE'}")
    print(f"Spot base URL: {spot_base}")
    print(f"Futures base URL: {futures_base}")
    print(f"Tracked symbols: {tracked}")
    print(f"python-dotenv installed: {'yes' if DOTENV_AVAILABLE else 'no'}")
    print(f"API key present: {'yes' if api_key else 'no'}")
    print(f"Secret key present: {'yes' if secret_key else 'no'}")

    if not DOTENV_AVAILABLE:
        print("\nInstall the missing dependency with:")
        print(r".venv\Scripts\python -m pip install -r requirements.txt")
        print("or")
        print(r".venv\Scripts\python -m pip install python-dotenv")
    elif not api_key or not secret_key:
        print("\nAdd these keys to .env before running live trades:")
        print("BINANCE_API_KEY=your_key")
        print("BINANCE_SECRET_KEY=your_secret")
        print("BINANCE_USE_TESTNET=true")
    else:
        print("\nCredentials found. Start the app and use /api/health or a manual trade to verify connectivity.")

USE_MOCK = False
UPDATE_INTERVAL_SECONDS = 3.0
BROKER_STREAM_INTERVAL_SECONDS = 2.0


auto_trader = AutoTrader()
active_connections: List[WebSocket] = []
background_task: Optional[asyncio.Task] = None
broker_connect_task: Optional[asyncio.Task] = None


class TradeSignal(BaseModel):
    symbol: str
    side: str
    instrument: str
    price: float
    quantity: float


class ArbitrageTradeRequest(BaseModel):
    symbol: str
    opportunity_type: str
    equity_price: float
    futures_price: float
    trade_notional_usdt: float
    action: str = "ENTRY"


class AutoTradingConfigUpdate(BaseModel):
    entry_threshold_percent: Optional[float] = None
    exit_shrink_ratio: Optional[float] = None
    max_open_positions: Optional[int] = None
    default_quantity_mode: Optional[str] = None
    fixed_quantity: Optional[float] = None
    persistence_scans: Optional[int] = None
    estimated_cost_percent: Optional[float] = None
    min_net_spread_percent: Optional[float] = None
    max_bid_ask_width_percent: Optional[float] = None
    top_candidate_count: Optional[int] = None
    min_exit_profit_percent: Optional[float] = None
    exit_spread_target_percent: Optional[float] = None


async def _run_blocking(func, *args, timeout: float = 12, **kwargs):
    if hasattr(asyncio, "to_thread"):
        return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout)

    loop = asyncio.get_running_loop()
    bound = partial(func, *args, **kwargs)
    return await asyncio.wait_for(loop.run_in_executor(None, bound), timeout=timeout)


def _get_active_symbol_count() -> int:
    broker = real_broker.get_cached_real_broker()
    if broker is None:
        return 0
    return len(getattr(broker, "symbols", []))


def _build_dashboard_payload(message_type: str, opportunities: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "type": message_type,
        "opportunities": opportunities,
        "symbols_above_threshold": [item.get("symbol") for item in opportunities if item.get("symbol")],
        "timestamp": datetime.now().isoformat(),
        "active_symbols": _get_active_symbol_count(),
        "auto_trading": auto_trader.get_status(),
        "broker": get_runtime_broker_settings(),
    }


async def _init_real_broker_connection():
    if not getattr(real_broker, "SUPPORTS_REALTIME_WEBSOCKET", True):
        print("Real broker WebSocket disabled for Binance build, using REST polling")
        return

    try:
        connected = await _run_blocking(real_broker.connect_real_broker_websocket, timeout=8)
        if connected:
            print("Real broker WebSocket connect initiated")
        else:
            print("Real broker WebSocket unavailable, using REST fallback")
    except asyncio.TimeoutError:
        print("Real broker WebSocket initialization timed out, using REST fallback")
    except Exception as exc:
        print(f"Real broker WebSocket initialization failed: {exc}")


async def periodic_updates():
    try:
        while True:
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)

            try:
                opportunities = await _run_blocking(real_broker.get_real_opportunities, timeout=12)
            except asyncio.TimeoutError:
                print("Skipping update cycle: opportunity fetch timed out")
                continue

            try:
                await _run_blocking(auto_trader.process_opportunities, opportunities, timeout=20)
            except asyncio.TimeoutError:
                print("Auto-trading cycle timed out")
            except Exception as exc:
                print(f"Auto-trading cycle failed: {exc}")

            payload = _build_dashboard_payload("update", opportunities)

            for connection in active_connections[:]:
                try:
                    await connection.send_json(payload)
                except Exception:
                    if connection in active_connections:
                        active_connections.remove(connection)
    except asyncio.CancelledError:
        print("Periodic updates task cancelled")
    except Exception as exc:
        print(f"Error in periodic updates: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task, broker_connect_task

    print("Starting arbitrage dashboard... (REAL broker mode)")

    broker_connect_task = asyncio.create_task(_init_real_broker_connection())
    background_task = asyncio.create_task(periodic_updates())

    yield

    print("Shutting down arbitrage dashboard...")

    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass
    background_task = None

    if broker_connect_task and not broker_connect_task.done():
        broker_connect_task.cancel()
        try:
            await broker_connect_task
        except asyncio.CancelledError:
            pass
    broker_connect_task = None

    real_broker.disconnect_real_broker_websocket()

    for connection in active_connections[:]:
        try:
            await connection.close()
        except Exception:
            pass
    active_connections.clear()


app = FastAPI(
    title="Arbitrage Trading Dashboard",
    description="Real-time arbitrage opportunities between Binance spot and perpetual futures",
    version="1.0.0",
    lifespan=lifespan,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html", {"use_mock": USE_MOCK})


@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)

    try:
        try:
            initial_opportunities = await _run_blocking(real_broker.get_real_opportunities, timeout=12)
        except asyncio.TimeoutError:
            initial_opportunities = []

        await websocket.send_json(_build_dashboard_payload("initial", initial_opportunities))

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat", "timestamp": datetime.now().isoformat()})
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
    except Exception as exc:
        print(f"Dashboard WebSocket error: {exc}")
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.websocket("/ws/broker")
async def websocket_broker(websocket: WebSocket):
    await websocket.accept()
    print("Real broker WebSocket connected")

    try:
        while True:
            try:
                market_data = await _run_blocking(real_broker.get_real_market_data, timeout=12)
                await websocket.send_json(market_data)
            except asyncio.TimeoutError:
                await websocket.send_json(
                    {
                        "type": "heartbeat",
                        "source": "broker-timeout",
                        "timestamp": datetime.now().isoformat(),
                    }
                )

            await asyncio.sleep(BROKER_STREAM_INTERVAL_SECONDS)
    except WebSocketDisconnect:
        print("Broker WebSocket disconnected")
    except Exception as exc:
        print(f"Broker WebSocket error: {exc}")


@app.post("/api/trade")
async def execute_trade(trade: TradeSignal):
    trade_id = str(uuid.uuid4())
    print(f"Executing trade: {trade}")

    try:
        broker = await _run_blocking(real_broker.get_real_broker, timeout=8)
        result = await _run_blocking(
            broker.execute_trade,
            symbol=trade.symbol,
            side=trade.side,
            instrument=trade.instrument,
            price=trade.price,
            quantity=trade.quantity,
            timeout=20,
        )
    except asyncio.TimeoutError:
        result = {"status": "ERROR", "message": "Trade execution timed out"}
    except Exception as exc:
        result = {"status": "ERROR", "message": str(exc)}

    details = result.get("details", {})
    if isinstance(details, dict):
        details.setdefault("symbol", trade.symbol)
        details.setdefault("side", trade.side)
        details.setdefault("instrument", trade.instrument)
        details.setdefault("price", trade.price)
        details.setdefault("quantity", trade.quantity)

    return JSONResponse(
        {
            "trade_id": result.get("trade_id", trade_id),
            "status": result.get("status", "ERROR"),
            "message": result.get("message", "Trade execution failed"),
            "timestamp": datetime.now().isoformat(),
            "details": details,
        }
    )


@app.post("/api/trade/pair")
async def execute_trade_pair(trade: ArbitrageTradeRequest):
    request_id = str(uuid.uuid4())

    try:
        broker = await _run_blocking(real_broker.get_real_broker, timeout=8)
        quantity = await _run_blocking(
            broker.get_pair_quantity_for_notional,
            symbol=trade.symbol,
            target_notional_quote=trade.trade_notional_usdt,
            equity_price=trade.equity_price,
            futures_price=trade.futures_price,
            timeout=8,
        )
        if quantity <= 0:
            result = {"status": "ERROR", "message": f"Unable to calculate trade quantity for {trade.symbol}"}
        else:
            result = await _run_blocking(
                broker.execute_arbitrage_pair,
                symbol=trade.symbol,
                opportunity_type=trade.opportunity_type,
                quantity=quantity,
                equity_price=trade.equity_price,
                futures_price=trade.futures_price,
                action=trade.action,
                timeout=30,
            )
            result.setdefault("executed_quantity", quantity)
    except asyncio.TimeoutError:
        result = {"status": "ERROR", "message": "Paired trade execution timed out"}
    except Exception as exc:
        result = {"status": "ERROR", "message": str(exc)}

    return JSONResponse(
        {
            "trade_id": request_id,
            "status": result.get("status", "ERROR"),
            "message": result.get("message", "Trade execution failed"),
            "timestamp": datetime.now().isoformat(),
            "trade_notional_usdt": trade.trade_notional_usdt,
            "executed_quantity": result.get("executed_quantity"),
            "leg_results": result.get("leg_results", []),
            "revert_results": result.get("revert_results", []),
        }
    )


@app.get("/api/opportunities")
async def get_opportunities():
    try:
        opportunities = await _run_blocking(real_broker.get_real_opportunities, timeout=12)
        return JSONResponse(
            {
                "success": True,
                "data": opportunities,
                "symbols_above_threshold": [item.get("symbol") for item in opportunities if item.get("symbol")],
                "timestamp": datetime.now().isoformat(),
                "count": len(opportunities),
                "active_symbols": _get_active_symbol_count(),
                "auto_trading": auto_trader.get_status(),
                "broker": get_runtime_broker_settings(),
                "metadata": {
                    "update_frequency": "1 second",
                    "data_feed": "real",
                    "trade_execution": "real",
                },
            }
        )
    except Exception as exc:
        return JSONResponse(
            {
                "success": False,
                "error": str(exc),
                "data": [],
                "timestamp": datetime.now().isoformat(),
                "auto_trading": auto_trader.get_status(),
            },
            status_code=500,
        )


@app.get("/api/health")
async def health_check():
    broker_status = "real-error"
    websocket_ready = False
    precheck: Dict[str, Any] = {}

    try:
        broker = await _run_blocking(real_broker.get_real_broker, timeout=5)
        websocket_ready = bool(getattr(broker, "connected", False))
        broker_status = "real-websocket" if websocket_ready else "real-rest-fallback"
        try:
            precheck = await _run_blocking(build_broker_precheck_report, broker, timeout=12)
        except asyncio.TimeoutError:
            precheck = {"status": "timeout", "message": "Credential precheck timed out"}
        except Exception as exc:
            precheck = {"status": "error", "message": str(exc)}
    except asyncio.TimeoutError:
        broker_status = "real-initializing"
        precheck = {"status": "initializing"}
    except Exception as exc:
        broker_status = "real-error"
        precheck = {"status": "error", "message": str(exc)}

    auto_state = auto_trader.get_status()
    return {
        "status": "healthy",
        "service": "arbitrage-dashboard",
        "timestamp": datetime.now().isoformat(),
        "broker": broker_status,
        "metrics": {
            "websocket_connections": len(active_connections),
            "background_task_running": background_task is not None and not background_task.done(),
            "broker_websocket_connected": websocket_ready,
            "auto_trading_enabled": auto_state["enabled"],
            "open_positions": auto_state["open_positions"],
        },
        "precheck": precheck,
    }


@app.get("/api/symbols")
async def get_symbols():
    try:
        broker = real_broker.get_cached_real_broker()
        if broker is None:
            broker = await _run_blocking(real_broker.get_real_broker, timeout=5)
        symbols = list(getattr(broker, "symbols", []))
    except Exception:
        symbols = []

    return {
        "symbols": symbols,
        "count": len(symbols),
        "categories": {
            "stocks": symbols,
        },
    }


@app.get("/api/status")
async def get_status():
    try:
        opportunities = await _run_blocking(real_broker.get_real_opportunities, timeout=12)
    except Exception:
        opportunities = []

    broker = real_broker.get_cached_real_broker()
    websocket_connected = bool(getattr(broker, "connected", False)) if broker else False

    return {
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "websocket_server": "running",
            "data_feed": "real",
            "arbitrage_engine": "active",
            "trade_execution": "real",
            "auto_trading": "enabled" if auto_trader.get_status()["enabled"] else "disabled",
        },
        "statistics": {
            "opportunities_generated": len(opportunities),
            "active_connections": len(active_connections),
            "tracked_symbols": _get_active_symbol_count(),
            "broker_websocket_connected": websocket_connected,
            "open_positions": auto_trader.get_status()["open_positions"],
        },
        "auto_trading": auto_trader.get_status(),
    }


@app.get("/api/config")
async def get_config():
    return {
        "arbitrage_threshold_percent": getattr(real_broker, "ARBITRAGE_THRESHOLD", 0.25),
        "update_interval_ms": int(UPDATE_INTERVAL_SECONDS * 1000),
        "mock_data_enabled": False,
        "trading_enabled": True,
        "futures_expiry": getattr(real_broker, "DEFAULT_EXPIRY", ""),
        "auto_trading": auto_trader.get_status()["config"],
    }


@app.post("/api/auto-trading/start")
async def start_auto_trading():
    status = await _run_blocking(auto_trader.start, timeout=5)
    return {"status": "started", "auto_trading": status, "timestamp": datetime.now().isoformat()}


@app.post("/api/auto-trading/stop")
async def stop_auto_trading():
    status = await _run_blocking(auto_trader.stop, timeout=5)
    return {"status": "stopped", "auto_trading": status, "timestamp": datetime.now().isoformat()}


@app.get("/api/auto-trading/status")
async def get_auto_trading_status():
    status = await _run_blocking(auto_trader.get_status, timeout=5)
    return {"success": True, "auto_trading": status, "timestamp": datetime.now().isoformat()}


@app.post("/api/auto-trading/config")
async def update_auto_trading_config(config_update: AutoTradingConfigUpdate):
    status = await _run_blocking(
        auto_trader.update_config,
        config_update.model_dump(exclude_none=True) if hasattr(config_update, "model_dump") else config_update.dict(exclude_none=True),
        timeout=5,
    )
    return {"success": True, "auto_trading": status, "timestamp": datetime.now().isoformat()}


@app.get("/api/broker/connect")
async def connect_broker_websocket():
    try:
        connected = await _run_blocking(real_broker.connect_real_broker_websocket, timeout=8)
    except asyncio.TimeoutError:
        return {"status": "failed", "message": "Connection attempt timed out"}
    except Exception as exc:
        return {"status": "failed", "message": f"Connection error: {exc}"}

    if connected:
        return {"status": "connected", "message": "Real broker WebSocket connect initiated"}
    return {"status": "failed", "message": "Failed to connect"}


@app.get("/api/broker/disconnect")
async def disconnect_broker_websocket():
    real_broker.disconnect_real_broker_websocket()
    return {"status": "disconnected", "message": "Real broker WebSocket disconnected"}

if __name__ == "__main__":
    import uvicorn

    if "--check-credentials" in sys.argv:
        run_credential_check()
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")

