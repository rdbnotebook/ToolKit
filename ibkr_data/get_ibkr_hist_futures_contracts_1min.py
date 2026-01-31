#!/usr/bin/env python3
"""
V3 Version: Retrieve historical 1-minute data for individual futures contracts with 24/6 coverage.

This script identifies front month contracts (expired and current) and saves each contract's 
data to individual files with full 24/6 trading hour coverage.

Key V3 Features:
- 24/6 data coverage (useRTH=False) for capturing all trading hours
- Walk-backward logic to fetch historical data as far back as available
- Intelligent data retrieval mechanism with fallbacks
- Special handling for index-based futures like VXM and currency futures like ZAR

The script uses an intelligent data retrieval mechanism with fallbacks:
1. For standard futures: First attempts to get 'TRADES' data, then falls back to
   'BID_ASK', 'MIDPOINT', and 'ADJUSTED_LAST' if needed.

Usage:
    # Back-fill mode - create new files or overwrite existing
    python get_hist_futures_contracts_1min.py --back-fill              # Process all futures securities
    python get_hist_futures_contracts_1min.py --back-fill --conid 123  # Process specific futures security
    python get_hist_futures_contracts_1min.py --back-fill --ticker ZT  # Process specific ticker
    python get_hist_futures_contracts_1min.py --back-fill --no-walk-backward # Disable walk-backward
    
    # Update mode - update existing contracts and fetch new ones
    python get_hist_futures_contracts_1min.py --update                 # Update all futures
    python get_hist_futures_contracts_1min.py --update --ticker ES     # Update specific ticker

Note: One of --back-fill or --update is REQUIRED
"""

import sys
import os
import json
import pandas as pd
import argparse
import logging
from datetime import datetime, timedelta, timezone
import time
from pathlib import Path
import collections
import signal
import re

# Add the parent directory to the path so we can import ib_insync directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import IB modules
from ib_insync import IB, Future, Contract, util

# Set up paths relative to project root
# Get the directory of this script
SCRIPT_DIR = Path(__file__).parent
# Get the project root (3 levels up from ibkr-fetch)
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
# Set up paths to bronze storage and logs
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "ibkr" / "futures_contracts"
BRONZE_DIR_BIDASK = PROJECT_ROOT / "data" / "bronze" / "ibkr" / "futures_contracts_bidask"
LOG_DIR = SCRIPT_DIR / "logs"
MAX_FETCH_DIR = SCRIPT_DIR / "max_fetch"
CACHE_DIR = MAX_FETCH_DIR / "cache"

# Ensure directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_BIDASK.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
MAX_FETCH_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / 'get_ibkr_hist_futures_contracts_1min.log')
    ]
)
logger = logging.getLogger(__name__)

# Output directory will be set based on --bid-ask flag in main()
OUTPUT_DIR = None
BIDASK_NO_DATA_TTL_DAYS = 7
TRADES_NO_DATA_TTL_DAYS = 7
UPDATE_CONTRACT_TIME_BUDGET_SEC = 300
DISABLE_CONTRACT_TIME_BUDGET = False
CONTRACT_DETAILS_CACHE: dict[str, list] = {}
CONNECTION_ERROR_CODES = {1100, 1101, 1102}

def _contract_key(contract):
    conid = getattr(contract, 'conId', None)
    if conid:
        return conid
    return getattr(contract, 'localSymbol', None)

def _contract_source(contract, active_keys, expired_keys):
    key = _contract_key(contract)
    if key in active_keys:
        return "active"
    if key in expired_keys:
        return "expired"
    return "unknown"

def _contract_expiry(contract):
    if not hasattr(contract, 'lastTradeDateOrContractMonth'):
        return None
    return contract.lastTradeDateOrContractMonth

def _normalize_expiry_digits(value) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 8:
        return digits[:8]
    if len(digits) >= 6:
        return digits[:6]
    return None


def _contract_time_budget_seconds(update_mode: bool, exchange: str) -> int | None:
    if not update_mode:
        return None
    if DISABLE_CONTRACT_TIME_BUDGET:
        return None
    if UPDATE_CONTRACT_TIME_BUDGET_SEC <= 0:
        return None
    multiplier = _exchange_wait_multiplier(exchange)
    return int(UPDATE_CONTRACT_TIME_BUDGET_SEC * multiplier)


def _attach_connection_error_handler(ib: IB) -> None:
    if getattr(ib, "_ab_connection_handler_attached", False):
        return

    def on_error(req_id, error_code, error_string, contract=None):
        if error_code in CONNECTION_ERROR_CODES:
            setattr(ib, "_ab_connection_lost", True)
            setattr(ib, "_ab_connection_lost_ts", time.time())
            setattr(ib, "_ab_connection_error", error_string)

    ib.errorEvent += on_error
    setattr(ib, "_ab_connection_handler_attached", True)


def _time_budget_exceeded(start_ts: float, budget_sec: int | None) -> bool:
    if budget_sec is None:
        return False
    return (time.time() - start_ts) > budget_sec


def _parse_expiry_value(value) -> datetime | None:
    digits = _normalize_expiry_digits(value)
    if not digits:
        return None
    try:
        if len(digits) == 8:
            return datetime.strptime(digits, "%Y%m%d")
        if len(digits) == 6:
            expiry_date = datetime.strptime(digits + "01", "%Y%m%d")
            next_month = expiry_date.replace(day=28) + timedelta(days=4)
            return next_month - timedelta(days=next_month.day)
    except ValueError:
        return None
    return None


def _parse_contract_expiry(contract):
    return _parse_expiry_value(_contract_expiry(contract))


def _contract_details_cache_key(contract: Contract) -> str:
    fields = [
        getattr(contract, "secType", ""),
        getattr(contract, "symbol", ""),
        getattr(contract, "exchange", ""),
        getattr(contract, "currency", ""),
        getattr(contract, "localSymbol", ""),
        getattr(contract, "tradingClass", ""),
        getattr(contract, "lastTradeDateOrContractMonth", ""),
        getattr(contract, "conId", ""),
    ]
    return "|".join(str(field or "") for field in fields)


def req_contract_details_safe(ib, contract: Contract) -> list:
    key = _contract_details_cache_key(contract)
    if key in CONTRACT_DETAILS_CACHE:
        return CONTRACT_DETAILS_CACHE[key]
    try:
        details = ib.reqContractDetails(contract)
    except KeyError as exc:
        logger.warning(f"reqContractDetails KeyError for {contract}: {exc}")
        return []
    CONTRACT_DETAILS_CACHE[key] = details
    return details


def _is_expired_on_or_before(contract, now_utc: datetime) -> bool:
    expiry_dt = _parse_contract_expiry(contract)
    if expiry_dt is None:
        return False
    return expiry_dt.date() <= now_utc.date()

def _expiry_far_future(contract, now_utc, threshold_days=120):
    expiry_dt = _parse_contract_expiry(contract)
    if expiry_dt is None:
        return False
    return (expiry_dt.date() - now_utc.date()).days >= threshold_days

def _select_root_contract(first_successful, active_contracts, expired_contracts):
    if first_successful is not None:
        return first_successful, "first_successful"
    if active_contracts:
        return active_contracts[0], "first_active"
    if expired_contracts:
        return expired_contracts[0], "first_expired"
    return None, "none"

def _write_futures_conid_artifact(rows, output_dir):
    if not rows:
        return
    output_path = Path(output_dir) / "futures_conid.csv"
    df = pd.DataFrame(rows)
    ordered_cols = [
        "ticker",
        "exchange",
        "currency",
        "mode",
        "contract_local_symbol",
        "contract_conid",
        "contract_expiry",
        "contract_exchange",
        "contract_trading_class",
        "contract_source",
        "fetch_status",
        "root_conid",
        "root_local_symbol",
        "root_expiry",
        "root_exchange",
        "root_trading_class",
        "root_source",
        "root_basis",
    ]
    cols = [c for c in ordered_cols if c in df.columns] + [c for c in df.columns if c not in ordered_cols]
    df = df[cols]
    df.to_csv(output_path, index=False)
    print(f"📄 Wrote futures conid artifact: {output_path}")


def _print_futures_summary(rows):
    if not rows:
        return
    counts = collections.Counter()
    for row in rows:
        status = row.get("fetch_status") or "unknown"
        counts[status] += 1
    print("\n" + "=" * 80)
    print("FUTURES SUMMARY")
    print("=" * 80)
    if counts:
        summary = ", ".join(f"{status}={count}" for status, count in counts.items())
        print(f"Status counts: {summary}")
    for row in rows:
        ticker = row.get("ticker", "")
        contract = row.get("contract_local_symbol") or ""
        status = row.get("fetch_status", "unknown")
        if contract:
            print(f"{ticker} {contract}: {status}")
        else:
            print(f"{ticker}: {status}")
    missing = [row.get("ticker") for row in rows if row.get("fetch_status") == "missing_file"]
    if missing:
        print(f"Missing files: {', '.join(sorted(m for m in missing if m))}")

# Special handling for historical trading class changes
# Format: {symbol: {'transition_date': datetime, 'old_class': string, 'new_class': string}}
HISTORICAL_TRADING_CLASS_MAPPING = {
    'SEK': {'transition_date': datetime(2022, 1, 1), 'old_class': 'SIR', 'new_class': 'SEK'},
    # Add others as needed if you discover new mappings
}

# Define the currency trading class mapping for special handling
CURRENCY_TRADING_CLASS = {
    # Standard currency futures where trading class != symbol
    'AUD': {'class': '6A', 'use_trading_class': True},
    'EUR': {'class': '6E', 'use_trading_class': True},
    'GBP': {'class': '6B', 'use_trading_class': True},
    'CAD': {'class': '6C', 'use_trading_class': True},
    'JPY': {'class': '6J', 'use_trading_class': True},
    'CHF': {'class': '6S', 'use_trading_class': True},
    'NZD': {'class': '6N', 'use_trading_class': True},
    'MXP': {'class': '6M', 'use_trading_class': True},
    'ZAR': {'class': '6Z', 'use_trading_class': True},
    'RUR': {'class': '6R', 'use_trading_class': True},
    'BRL': {'class': '6L', 'use_trading_class': True},
    'MXN': {'class': '6M', 'use_trading_class': True},
    
    # Currency futures where trading class == symbol
    'E7': {'class': 'E7', 'use_trading_class': True},
    'J7': {'class': 'J7', 'use_trading_class': True},
    'SEK': {'class': 'SEK', 'use_trading_class': True},
    'NOK': {'class': 'NOK', 'use_trading_class': False},
    'CNH': {'class': 'CNH', 'use_trading_class': False},
    'RP': {'class': 'RP', 'use_trading_class': False}
}

# Special handling for securities that are not directly futures but have futures derivatives
# For each security, specify:
# - secType: The actual security type of the base instrument
# - exchange: The exchange to use for the futures derivative
# - trading_class: The trading class to use for the futures derivative (if applicable)
# - derivative_exchange: The exchange to use for requesting futures derivatives
SPECIAL_DERIVATIVES_HANDLING = {
    'VXM': {
        'secType': 'IND',  # VXM is an index
        'exchange': 'CFE',  # Use CFE exchange instead of CBOE for futures
        'contract_info': "Mini VIX Futures",
        'fallback_exchanges': ['CFE', 'CBOE', 'SMART']
    }
}

# Request tracking system to comply with IBKR pacing guidelines
class RequestTracker:
    def __init__(self):
        # Track identical requests within 15 seconds
        self.last_request_time = {}
        
        # Track requests for the same contract within 2 seconds (max 6)
        self.contract_requests = collections.defaultdict(list)
        
        # Track total requests in 10-minute window (max 60)
        self.all_request_times = collections.deque(maxlen=60)
        # Track symbols we've already warned about to avoid log spam
        self.warned_symbols = set()
    
    def wait_if_needed(self, contract, request_type, bar_size, duration):
        """
        Check if we need to wait before making a request and wait the appropriate time.
        
        Args:
            contract: The contract object
            request_type: The type of request (e.g., 'historical')
            bar_size: Bar size for the request
            duration: Duration string for the request
            
        Returns:
            float: The time waited in seconds
        """
        now = time.time()
        
        # Ensure contract has a properly formatted localSymbol
        # For futures contracts, the standard format is SymbolMonthYear (e.g., ZTH5, ESM5)
        contract_symbol = contract.localSymbol if hasattr(contract, 'localSymbol') and contract.localSymbol else ""
        
        # Make sure we're using the properly formatted localSymbol (e.g., 'ZTH5' not 'ZT H5' or 'ZT JUN 23')
        if contract_symbol and ' ' in contract_symbol:
            if contract_symbol not in self.warned_symbols:
                print(
                    f"Warning: Contract symbol '{contract_symbol}' contains spaces. "
                    "This may cause issues with IBKR API."
                )
                self.warned_symbols.add(contract_symbol)
            # Try to clean up the symbol by removing spaces
            contract_symbol = contract_symbol.replace(' ', '')
            contract.localSymbol = contract_symbol
            if contract_symbol not in self.warned_symbols:
                print(f"Using cleaned contract symbol: '{contract_symbol}'")
                self.warned_symbols.add(contract_symbol)
        
        # Create a unique key for this exact request
        request_key = f"{contract.symbol}_{contract.secType}_{contract.exchange}_{contract.lastTradeDateOrContractMonth}_{request_type}_{bar_size}_{duration}"
        
        # Create a contract key (same contract, different parameters)
        contract_key = f"{contract.symbol}_{contract.secType}_{contract.exchange}_{contract.lastTradeDateOrContractMonth}"
        
        wait_time = 0
        wait_reason = None
        
        # Rule 1: Identical requests must be 6 seconds apart
        if request_key in self.last_request_time:
            last_time = self.last_request_time[request_key]
            time_since_last = now - last_time
            if time_since_last < 6:
                wait_needed = 6 - time_since_last
                wait_time = max(wait_time, wait_needed)
                wait_reason = "identical request"
        
        # Rule 2: No more than 6 requests for same contract within 2 seconds
        # Clean up old requests older than 2 seconds
        contract_times = self.contract_requests[contract_key]
        contract_times = [t for t in contract_times if now - t < 2]
        self.contract_requests[contract_key] = contract_times
        
        if len(contract_times) >= 5:  # Approaching the limit
            # Wait until we're under the threshold
            if contract_times:
                oldest_time = min(contract_times)
                time_to_clear = 2 - (now - oldest_time)
                if time_to_clear > 0:
                    wait_needed = time_to_clear + 0.1  # Add a small buffer
                    wait_time = max(wait_time, wait_needed)
                    wait_reason = "contract rate limit"
        
        # Rule 3: No more than 60 requests in any 10-minute period
        # Clean up requests older than 10 minutes
        all_times = [t for t in self.all_request_times if now - t < 600]
        self.all_request_times = collections.deque(all_times, maxlen=60)
        
        if len(self.all_request_times) >= 58:  # Approaching the limit
            # Wait until we're under the threshold
            if self.all_request_times:
                oldest_time = self.all_request_times[0]
                time_to_clear = 600 - (now - oldest_time)
                if time_to_clear > 0:
                    wait_needed = time_to_clear + 0.1  # Add a small buffer
                    wait_time = max(wait_time, wait_needed)
                    wait_reason = "global rate limit"
        
        # Wait if needed
        if wait_time > 0:
            print(f"Waiting {wait_time:.1f} seconds due to {wait_reason}...")
            time.sleep(wait_time)
        
        # Record this request
        now = time.time()  # Update time after waiting
        self.last_request_time[request_key] = now
        self.contract_requests[contract_key].append(now)
        self.all_request_times.append(now)
        
        return wait_time
        
    def start_request(self, identifier):
        """
        Mark the start of a request for the given identifier.
        Used in the updated historical data function.
        
        Args:
            identifier: A string identifier for the request
        """
        # We don't do much here, but this could be extended
        # to keep track of in-progress requests
        now = time.time()
        # Add this to the all_request_times to track rate limiting
        self.all_request_times.append(now)
        
        
def _to_naive(dt):
    if hasattr(dt, "to_pydatetime"):
        dt = dt.to_pydatetime()
    if isinstance(dt, datetime) and dt.tzinfo:
        return dt.replace(tzinfo=None)
    return dt


def _dt_to_iso(dt):
    dt = _to_naive(dt)
    if dt is None:
        return None
    return dt.replace(microsecond=0).isoformat()


def _parse_iso_dt(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _to_naive(parsed)


def _format_ib_end_datetime(dt: datetime) -> str:
    """Format endDateTime for IB (dash indicates UTC)."""
    if dt.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.replace(tzinfo=None).strftime("%Y%m%d-%H:%M:%S")


def _exchange_wait_multiplier(exchange: str) -> float:
    return 2.0 if exchange == "EUREX" else 1.0


def _compute_backoff(base_seconds: float, attempt: int, cap: float | None = None) -> float:
    wait = base_seconds * (2 ** attempt)
    if cap is not None:
        wait = min(wait, cap)
    return wait


def _contract_cache_key(contract: Contract) -> str:
    conid = getattr(contract, "conId", None)
    expiry = _contract_expiry(contract) or ""
    exchange = getattr(contract, "exchange", "") or ""
    local_symbol = getattr(contract, "localSymbol", "") or ""
    return "|".join([str(conid or ""), str(local_symbol), str(expiry), str(exchange)])


class BidAskCache:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, dict] = {}
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text()) or {}
            except json.JSONDecodeError:
                self.data = {}
        self._loaded = True

    def get_entry(self, key: str) -> dict:
        self.load()
        entry = self.data.get(key)
        if entry is None:
            entry = {
                "bid": {},
                "ask": {},
                "no_data_windows": [],
            }
            self.data[key] = entry
        else:
            entry.setdefault("bid", {})
            entry.setdefault("ask", {})
            entry.setdefault("no_data_windows", [])
        return entry

    def save(self) -> None:
        if not self._loaded:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        tmp_path.replace(self.path)


class TradesNoDataCache:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, dict] = {}
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text()) or {}
            except json.JSONDecodeError:
                self.data = {}
        self._loaded = True

    def get_entry(self, key: str) -> dict:
        self.load()
        entry = self.data.get(key)
        if entry is None:
            entry = {"no_data_windows": []}
            self.data[key] = entry
        else:
            entry.setdefault("no_data_windows", [])
        return entry

    def save(self) -> None:
        if not self._loaded:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        tmp_path.replace(self.path)


def _compute_hard_stop_date(end_date: datetime, years_back: int | None) -> datetime:
    hard_stop = datetime(2005, 1, 1)
    if years_back and years_back > 0:
        cap_date = end_date - timedelta(days=years_back * 365)
        if cap_date > hard_stop:
            hard_stop = cap_date
    return hard_stop


def _window_in_no_data_cache(entry: dict, start_dt: datetime, end_dt: datetime) -> bool:
    windows = entry.get("no_data_windows", [])
    if not windows:
        return False
    now = datetime.utcnow()
    keep = []
    hit = False
    for window in windows:
        cached_start = _parse_iso_dt(window.get("start"))
        cached_end = _parse_iso_dt(window.get("end"))
        expires_at = _parse_iso_dt(window.get("expires_at")) if window.get("expires_at") else None
        if expires_at and now > expires_at:
            continue
        if cached_start is None or cached_end is None:
            continue
        if start_dt >= cached_start and end_dt <= cached_end:
            hit = True
        keep.append(window)
    if keep != windows:
        entry["no_data_windows"] = keep
    return hit


def _cache_add_no_data_window(entry: dict, start_dt: datetime, end_dt: datetime, ttl_days: int | None = None) -> None:
    start_iso = _dt_to_iso(start_dt)
    end_iso = _dt_to_iso(end_dt)
    if not start_iso or not end_iso:
        return
    expires_iso = None
    if ttl_days and ttl_days > 0:
        expires_iso = _dt_to_iso(datetime.utcnow() + timedelta(days=ttl_days))
    for window in entry.get("no_data_windows", []):
        if window.get("start") == start_iso and window.get("end") == end_iso:
            if expires_iso:
                window["expires_at"] = expires_iso
            return
    new_window = {"start": start_iso, "end": end_iso}
    if expires_iso:
        new_window["expires_at"] = expires_iso
    entry.setdefault("no_data_windows", []).append(new_window)


def _probe_has_data(
    ib,
    contract,
    end_dt: datetime,
    side: str,
    timeout_multiplier: float,
    wait_multiplier: float,
    max_attempts: int = 3,
) -> bool:
    end_date_str = _format_ib_end_datetime(end_dt)
    retry_sleep = 2.0 * wait_multiplier
    for attempt in range(max_attempts):
        if attempt > 0:
            wait = _compute_backoff(2.0 * wait_multiplier, attempt - 1, cap=10.0 * wait_multiplier)
            print(f"Probe retry for {side}; waiting {wait:.1f} seconds...")
            time.sleep(wait)
        bars = fetch_historical_data_with_retry(
            ib,
            contract,
            end_date_str,
            "1 D",
            fallback_options=None,
            bid_ask=False,
            what_to_show_override=[side],
            max_retries=1,
            retry_sleep=retry_sleep,
            timeout_multiplier=timeout_multiplier,
            log_samples=False,
        )
        if bars:
            return True
    return False


def _find_earliest_side(
    ib,
    contract,
    side: str,
    end_dt: datetime,
    hard_stop: datetime,
    timeout_multiplier: float,
    wait_multiplier: float,
) -> datetime | None:
    if end_dt <= hard_stop:
        return None
    if not _probe_has_data(ib, contract, end_dt, side, timeout_multiplier, wait_multiplier):
        return None
    low = hard_stop
    high = end_dt
    while (high - low) > timedelta(days=7):
        mid = low + (high - low) / 2
        if mid <= low:
            break
        if _probe_has_data(ib, contract, mid, side, timeout_multiplier, wait_multiplier):
            high = mid
        else:
            low = mid
        time.sleep(0.5 * wait_multiplier)
    end_date_str = _format_ib_end_datetime(high)
    bars = fetch_historical_data_with_retry(
        ib,
        contract,
        end_date_str,
        "7 D",
        fallback_options=None,
        bid_ask=False,
        what_to_show_override=[side],
        max_retries=1,
        retry_sleep=2.0 * wait_multiplier,
        timeout_multiplier=timeout_multiplier,
        log_samples=False,
    )
    if not bars:
        return _to_naive(high)
    earliest = min(_to_naive(bar.date) for bar in bars)
    return earliest


def _retry_missing_side(
    ib,
    contract,
    side: str,
    end_date_str: str,
    duration_str: str,
    timeout_multiplier: float,
    wait_multiplier: float,
    deadline_ts: float | None = None,
    max_attempts: int = 4,
) -> list:
    base_wait = 10.0 * wait_multiplier
    for attempt in range(max_attempts):
        if deadline_ts is not None and time.time() > deadline_ts:
            return []
        wait = _compute_backoff(base_wait, attempt, cap=120.0 * wait_multiplier)
        print(f"Missing {side}; waiting {wait:.1f} seconds before retry...")
        time.sleep(wait)
        bars = fetch_historical_data_with_retry(
            ib,
            contract,
            end_date_str,
            duration_str,
            fallback_options=None,
            bid_ask=False,
            what_to_show_override=[side],
            max_retries=1,
            retry_sleep=1.0 * wait_multiplier,
            timeout_multiplier=timeout_multiplier,
            log_samples=False,
            deadline_ts=deadline_ts,
        )
        if bars:
            return bars
    # One short slice attempt as a last resort.
    if deadline_ts is not None and time.time() > deadline_ts:
        return []
    bars = fetch_historical_data_with_retry(
        ib,
        contract,
        end_date_str,
        "1 D",
        fallback_options=None,
        bid_ask=False,
        what_to_show_override=[side],
        max_retries=1,
        retry_sleep=1.0 * wait_multiplier,
        timeout_multiplier=timeout_multiplier,
        log_samples=False,
        deadline_ts=deadline_ts,
    )
    return bars or []


def _fetch_bidask_window(
    ib,
    contract,
    end_date_str: str,
    duration_str: str,
    timeout_multiplier: float,
    wait_multiplier: float,
    side_gap: float,
    deadline_ts: float | None = None,
    max_empty_retries: int = 3,
) -> tuple[str, list, list]:
    empty_attempts = 0
    while True:
        if deadline_ts is not None and time.time() > deadline_ts:
            return "budget_exceeded", [], []
        bars_bid = fetch_historical_data_with_retry(
            ib,
            contract,
            end_date_str,
            duration_str,
            fallback_options=None,
            bid_ask=False,
            what_to_show_override=["BID"],
            max_retries=1,
            retry_sleep=1.0 * wait_multiplier,
            timeout_multiplier=timeout_multiplier,
            deadline_ts=deadline_ts,
        )
        time.sleep(side_gap)
        bars_ask = fetch_historical_data_with_retry(
            ib,
            contract,
            end_date_str,
            duration_str,
            fallback_options=None,
            bid_ask=False,
            what_to_show_override=["ASK"],
            max_retries=1,
            retry_sleep=1.0 * wait_multiplier,
            timeout_multiplier=timeout_multiplier,
            deadline_ts=deadline_ts,
        )
        cnt_b = len(bars_bid) if bars_bid else 0
        cnt_a = len(bars_ask) if bars_ask else 0
        if cnt_b > 0 and cnt_a > 0:
            return "success", bars_bid, bars_ask
        if cnt_b == 0 and cnt_a == 0:
            empty_attempts += 1
            if empty_attempts <= max_empty_retries:
                wait = _compute_backoff(10.0 * wait_multiplier, empty_attempts - 1, cap=120.0 * wait_multiplier)
                print(f"Empty response (retry {empty_attempts}/{max_empty_retries}); waiting {wait:.1f} seconds...")
                time.sleep(wait)
                continue
            return "empty", [], []
        missing_side = "BID" if cnt_b == 0 else "ASK"
        print(f"Partial data; retrying missing {missing_side} with backoff...")
        missing_bars = _retry_missing_side(
            ib,
            contract,
            missing_side,
            end_date_str,
            duration_str,
            timeout_multiplier,
            wait_multiplier,
            deadline_ts=deadline_ts,
        )
        if missing_bars:
            if missing_side == "BID":
                bars_bid = missing_bars
            else:
                bars_ask = missing_bars
            return "success", bars_bid, bars_ask
        return "partial", bars_bid, bars_ask


# Function for IBKR contract formatting
def format_ibkr_contract_symbol(symbol, expiry_month, expiry_year):
    """
    Format a contract symbol according to IBKR standards.
    
    Args:
        symbol: The base symbol (e.g., 'ES', 'ZT')
        expiry_month: Month as int (1-12)
        expiry_year: Year as int
    
    Returns:
        str: Properly formatted contract symbol (e.g., 'ESM5', 'ZTH5')
    """
    # Standard month codes for futures
    month_codes = {
        1: 'F',  # January
        2: 'G',  # February
        3: 'H',  # March
        4: 'J',  # April
        5: 'K',  # May
        6: 'M',  # June
        7: 'N',  # July
        8: 'Q',  # August
        9: 'U',  # September
        10: 'V',  # October
        11: 'X',  # November
        12: 'Z'   # December
    }
    
    # Get the month code
    if expiry_month in month_codes:
        month_code = month_codes[expiry_month]
    else:
        raise ValueError(f"Invalid expiry month: {expiry_month}")
    
    # Get the last digit of the year
    year_digit = str(expiry_year)[-1]
    
    # Format according to IBKR standards
    return f"{symbol}{month_code}{year_digit}"

# Initialize global request tracker
request_tracker = RequestTracker()

def create_contract(security, expired=False):
    """
    Create an IBKR contract object from security information.
    
    Args:
        security: Dictionary containing security information
        expired: Boolean flag to indicate if we want expired contracts
    
    Returns:
        Contract object for use with IB API
    """
    try:
        ticker = security['symbol']
        exchange = security['exchange'] if 'exchange' in security and security['exchange'] else None
        
        # Create the appropriate contract type based on security type
        if security['secType'] == 'FUT':
            # For futures, create a generic contract to get all expirations
            contract = Future(symbol=ticker, exchange=exchange)
            
            # Handle special case for currency futures
            if ticker in CURRENCY_TRADING_CLASS:
                contract.tradingClass = CURRENCY_TRADING_CLASS[ticker]['class']
                print(f"Setting trading class to {CURRENCY_TRADING_CLASS[ticker]['class']} for {ticker}")
                
                # Force USD currency for currency futures
                if ticker in ['SEK', 'NOK', 'CHF', 'GBP', 'CAD', 'JPY', 'AUD', 'NZD', 'MXP', 'ZAR']:
                    contract.currency = 'USD'
            
            # Set to include expired contracts if needed
            contract.includeExpired = expired
            
            return contract
        else:
            print(f"❌ Unsupported security type: {security['secType']}")
            return None
    
    except Exception as e:
        print(f"❌ Error creating contract: {e}")
        return None

def connect_to_ibkr(host='127.0.0.1', port=7497, client_id=22, max_retries=3, prompt_user=True):
    """
    Connect directly to IBKR TWS with retry logic.
    
    Args:
        host: The hostname or IP address of the IBKR TWS host
        port: The port number of the IBKR TWS API
        client_id: The client ID to use for the connection
        max_retries: Maximum number of connection attempts
    
    Returns:
        IB: The IB connection object
    """
    print(f"Connecting to IBKR TWS at {host}:{port}...")
    logger.info(f"Connecting to IBKR TWS: host={host}, port={port}, client_id={client_id}")
    
    ib = IB()
    
    for retry in range(max_retries):
        try:
            ib.connect(host, port, clientId=client_id, readonly=True, timeout=30)
            print("✅ Connected to IBKR TWS")
            logger.info("Connected to IBKR TWS")
            _attach_connection_error_handler(ib)
            
            # Print API version and available accounts
            logger.info(f"API Version: {ib.client.serverVersion()}")
            accounts = ib.managedAccounts()
            logger.info(f"Available accounts: {accounts}")
            for account in accounts or []:
                try:
                    ib.reqAccountUpdates(False, account)
                except Exception:
                    logger.debug("Failed to disable account updates for %s", account)
            
            return ib
        
        except Exception as e:
            print(f"❌ Failed to connect to IBKR TWS (attempt {retry+1}/{max_retries}): {e}")
            logger.error(f"Failed to connect to IBKR TWS (attempt {retry+1}/{max_retries}): {e}")
            
            if retry < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                if not prompt_user:
                    print("Maximum retries reached. Connection attempt aborted (non-interactive mode).")
                    return None
                print("Maximum retries reached. Would you like to try again? (Y/N)")
                response = input().strip().upper()
                if response == 'Y':
                    # Reset retry counter and try again
                    return connect_to_ibkr(host, port, client_id, max_retries, prompt_user=prompt_user)
                print("Connection attempt aborted.")
                return None
    
    return None 

def format_contract_symbol(ticker, month, year):
    """
    Create a standard futures contract symbol in the format required by IBKR.
    
    Args:
        ticker: The ticker symbol (e.g., 'ZT')
        month: Month as integer (1-12)
        year: Year as integer (e.g., 2025)
    
    Returns:
        str: Formatted contract symbol (e.g., 'ZTM5')
    """
    # Standard futures month codes
    month_codes = {
        1: 'F',  # January
        2: 'G',  # February
        3: 'H',  # March
        4: 'J',  # April
        5: 'K',  # May
        6: 'M',  # June
        7: 'N',  # July
        8: 'Q',  # August
        9: 'U',  # September
        10: 'V',  # October
        11: 'X',  # November
        12: 'Z'   # December
    }
    
    # Get the month code
    month_code = month_codes.get(month)
    if not month_code:
        raise ValueError(f"Invalid month: {month}")
    
    # Get the last digit of the year
    year_digit = str(year)[-1]
    
    # Format the contract symbol (e.g., ZTM5)
    return f"{ticker}{month_code}{year_digit}"

def standardize_contract_symbol(contract):
    """
    Standardize contract symbol to ensure consistent formatting.
    
    Args:
        contract: The contract object to standardize
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure localSymbol is set and in proper format
        if hasattr(contract, 'localSymbol') and contract.localSymbol:
            # For futures, we want to make sure the format is consistent
            if hasattr(contract, 'secType') and contract.secType == 'FUT':
                # Special handling for currency futures where trading class might be different than symbol
                use_trading_class = False
                actual_symbol = contract.symbol
                
                # For currency futures, check if we should use the trading class instead of symbol
                if contract.symbol in CURRENCY_TRADING_CLASS and hasattr(contract, 'tradingClass'):
                    if CURRENCY_TRADING_CLASS[contract.symbol].get('use_trading_class', False):
                        use_trading_class = True
                        actual_symbol = contract.tradingClass
                
                # Clean up the local symbol (remove excess spaces)
                clean_local_symbol = contract.localSymbol.strip().replace('  ', ' ')

                # Preserve trading-class-prefixed local symbols (e.g., FMCHH6 for M1CN).
                trading_class = getattr(contract, 'tradingClass', '') or ''
                if trading_class:
                    trading_class = trading_class.strip()
                if trading_class and clean_local_symbol.startswith(trading_class) and trading_class != actual_symbol:
                    use_trading_class = True
                    actual_symbol = trading_class
                
                # Month name to code mapping (for verbose format like "ZT JUN 23")
                month_name_to_code = {
                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                }
                
                # Check if local symbol contains a month name (e.g., "ZT JUN 23")
                for month_name, month_code in month_name_to_code.items():
                    if month_name in clean_local_symbol.upper():
                        # Extract year digit (assume it's the last two characters)
                        parts = clean_local_symbol.upper().split()
                        if len(parts) >= 3:  # Format like "ZT JUN 23"
                            symbol_part = parts[0]
                            year_part = parts[2]
                            
                            # If year is 2 digits, use just the last digit
                            year_digit = year_part[-1] if len(year_part) > 0 else '0'
                            
                            # Create standardized symbol
                            standardized_symbol = f"{symbol_part}{month_code}{year_digit}"
                            print(f"Converted verbose contract symbol format: {clean_local_symbol} to {standardized_symbol}")
                            contract.localSymbol = standardized_symbol
                            return True
                
                # Extract symbol, month code, and year from localSymbol
                # Handle cases where trading class is used instead of symbol for local symbol
                symbol_to_check = actual_symbol if use_trading_class else contract.symbol
                
                # Check if the localSymbol starts with the symbol (or trading class)
                if clean_local_symbol.startswith(symbol_to_check):
                    # Extract the remaining part (should contain month code and year)
                    remaining = clean_local_symbol[len(symbol_to_check):].strip()
                    
                    # Month codes: F,G,H,J,K,M,N,Q,U,V,X,Z
                    month_codes = {
                        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
                    }
                    
                    if len(remaining) >= 2:
                        month_code = remaining[0].upper()
                        year_digit = remaining[1]
                        
                        # Check if month code is valid
                        if month_code in month_codes:
                            month = month_codes[month_code]
                            # Construct year from digit (assumes current century)
                            current_decade = (datetime.now().year // 10) * 10
                            year = current_decade + int(year_digit)
                            
                            # If the resulting year is too far in the future, it's likely previous decade
                            if year > datetime.now().year + 5:
                                year -= 10
                                
                            # Use the existing format_contract_symbol function for consistency
                            # Use the correct symbol based on whether we're using trading class or regular symbol
                            standardized_symbol = format_contract_symbol(symbol_to_check, month, year)
                            
                            if clean_local_symbol != standardized_symbol:
                                print(f"Standardized {clean_local_symbol} to {standardized_symbol}")
                                logger.info(f"Standardized {clean_local_symbol} to {standardized_symbol}")
                                contract.localSymbol = standardized_symbol
                else:
                    # If we can't easily parse the localSymbol directly, try to use lastTradeDateOrContractMonth
                    if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
                        try:
                            # Parse the date string (format may be YYYYMMDD or YYYYMM)
                            date_str = contract.lastTradeDateOrContractMonth
                            
                            if len(date_str) >= 6:  # At least YYYYMM format
                                year = int(date_str[0:4])
                                month = int(date_str[4:6])
                                
                                # Month codes mapping
                                month_to_code = {
                                    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                                    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                                }
                                
                                if month in month_to_code:
                                    month_code = month_to_code[month]
                                    year_digit = str(year)[-1]
                                    
                                    # Create standardized symbol
                                    symbol_to_use = actual_symbol if use_trading_class else contract.symbol
                                    standardized_symbol = f"{symbol_to_use}{month_code}{year_digit}"
                                    
                                    print(f"Generated standard contract symbol from expiry date: {standardized_symbol}")
                                    contract.localSymbol = standardized_symbol
                                    return True
                        except Exception as e:
                            print(f"Error generating standard contract symbol from expiry date: {e}")
                
                    # More complex case: the localSymbol doesn't start with the symbol or trading class
                    # This could be because of specific formatting used by IBKR
                    # Try to extract month code and year digit from anywhere in the localSymbol
                    
                    # First check if we can find a valid month code in the string
                    month_codes = {
                        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
                    }
                    
                    # Try to find a month code followed by a digit
                    month_year_pattern = re.compile(r'([FGHJKMNQUVXZ])(\d)')
                    match = month_year_pattern.search(clean_local_symbol)
                    
                    if match:
                        month_code = match.group(1)
                        year_digit = match.group(2)
                        
                        month = month_codes[month_code]
                        current_decade = (datetime.now().year // 10) * 10
                        year = current_decade + int(year_digit)
                        
                        if year > datetime.now().year + 5:
                            year -= 10
                            
                        standardized_symbol = format_contract_symbol(symbol_to_check, month, year)
                        
                        if clean_local_symbol != standardized_symbol:
                            print(f"Complex standardization: {clean_local_symbol} to {standardized_symbol}")
                            logger.info(f"Complex standardization: {clean_local_symbol} to {standardized_symbol}")
                            contract.localSymbol = standardized_symbol
        
        return True
    except Exception as e:
        print(f"Error standardizing contract symbol: {e}")
        logger.error(f"Error standardizing contract symbol: {e}")
        return False

def _parse_contract_expiry(contract):
    return _parse_expiry_value(_contract_expiry(contract))


def _filter_by_forward_window(contracts, max_forward_days):
    if not max_forward_days or max_forward_days <= 0:
        return contracts
    cutoff = datetime.now() + timedelta(days=max_forward_days)
    filtered = []
    for contract in contracts:
        expiry_date = _parse_contract_expiry(contract)
        if expiry_date is None:
            continue
        if expiry_date <= cutoff:
            filtered.append(contract)
    return filtered

def get_active_futures_contracts(
    ib,
    symbol,
    exchange,
    currency='USD',
    max_forward_days=None,
    local_symbol=None,
    conid: int | None = None,
):
    """
    Get active future contracts for a given symbol.
    
    Args:
        ib: IB connection object
        symbol: Futures symbol to look up
        exchange: Exchange to search on
        currency: Currency code
        
    Returns:
        list: List of contract objects
    """
    print(f"🔍 Getting active futures contracts for {symbol} on {exchange}")
    logger.info(f"Getting active futures contracts for {symbol} on {exchange}")
    
    try:
        # Check if this is a special handling case
        if symbol in SPECIAL_DERIVATIVES_HANDLING:
            special_info = SPECIAL_DERIVATIVES_HANDLING[symbol]
            print(f"⚠️ Applying special handling for {symbol} active contracts ({special_info['contract_info']})")
            logger.info(f"Applying special handling for {symbol} as {special_info['secType']} with derivatives")
            
            # Try different exchanges for special cases
            contracts = []
            exchanges_to_try = [exchange] + special_info.get('fallback_exchanges', [])
            
            # Remove duplicates while preserving order
            exchanges_to_try = list(dict.fromkeys(exchanges_to_try))
            
            for exch in exchanges_to_try:
                print(f"Trying to get {symbol} active futures contracts from {exch} exchange...")
                
                # For currency futures like ZAR, use the trading class immediately
                if special_info.get('secType') == 'CASH' and 'trading_class' in special_info:
                    # Create a Future contract with the specified trading class
                    futures_contract = Future(
                        symbol=symbol, 
                        exchange=exch,
                        currency='USD',  # Always use USD for currency futures
                        tradingClass=special_info['trading_class']
                    )
                    print(f"Using trading class {special_info['trading_class']} for {symbol} on {exch}")
                    contract_details = req_contract_details_safe(ib, futures_contract)
                    
                    if contract_details:
                        contracts = [details.contract for details in contract_details]
                        print(f"✅ Found {len(contracts)} active contracts for {symbol} using trading class {special_info['trading_class']} on {exch}")
                        break
                # For index futures like VXM
                elif special_info.get('secType') == 'IND':
                    futures_contract = Future(
                        symbol=symbol, 
                        exchange=exch,
                        currency=currency
                    )
                    contract_details = req_contract_details_safe(ib, futures_contract)
                    
                    if contract_details:
                        contracts = [details.contract for details in contract_details]
                        print(f"✅ Found {len(contracts)} active contracts for {symbol} on {exch}")
                        break
            
            if not contracts:
                # Try the portfolio approach - look for positions of this type in the current portfolio
                print(f"⚠️ No contracts found via direct queries. Checking current portfolio for active {symbol} futures positions...")
                
                positions = ib.positions()
                matching_positions = []
                
                # For ZAR or special cases with trading class
                if special_info.get('secType') == 'CASH' and 'trading_class' in special_info:
                    trading_class = special_info['trading_class']
                    for pos in positions:
                        # Check both symbol and trading class
                        if (hasattr(pos.contract, 'symbol') and pos.contract.symbol == symbol) or \
                           (hasattr(pos.contract, 'tradingClass') and pos.contract.tradingClass == trading_class):
                            print(f"Found matching position: {pos.contract.localSymbol}")
                            matching_positions.append(pos.contract)
                else:
                    # Standard symbol matching
                    for pos in positions:
                        if hasattr(pos.contract, 'symbol') and pos.contract.symbol == symbol:
                            print(f"Found matching position: {pos.contract.localSymbol}")
                            matching_positions.append(pos.contract)
                
                if matching_positions:
                    print(f"✅ Found {len(matching_positions)} active contracts for {symbol} in portfolio")
                    contracts = matching_positions
                else:
                    print(f"❌ Could not find any active futures contracts for {symbol} in portfolio or via direct queries")
                    return []
                
            # Sort contracts by expiration date
            contracts.sort(key=lambda x: x.lastTradeDateOrContractMonth if x.lastTradeDateOrContractMonth else '')
            
            # Standardize all contract symbols
            for contract in contracts:
                standardize_contract_symbol(contract)
                contract.includeExpired = True

            contracts = _filter_by_forward_window(contracts, max_forward_days)
            
            print(f"✅ Found {len(contracts)} active contracts for {symbol}")
            logger.info(f"Found {len(contracts)} active contracts for {symbol}")
            
            for i, contract in enumerate(contracts):
                print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
            
            return contracts
        
        # Standard handling for regular futures
        # Create a futures contract object with no expiry to get all active contracts
        if symbol in CURRENCY_TRADING_CLASS:
            currency = 'USD'
        futures_contract = Future(symbol=symbol, exchange=exchange, currency=currency)
        
        # Handle special case for currency futures
        if symbol in CURRENCY_TRADING_CLASS:
            futures_contract.tradingClass = CURRENCY_TRADING_CLASS[symbol]['class']
            print(f"Setting trading class to {CURRENCY_TRADING_CLASS[symbol]['class']} for {symbol}")
        
        # Request all active contracts
        active_contracts = req_contract_details_safe(ib, futures_contract)
        
        if not active_contracts:
            print(f"⚠️ No active contracts found for {symbol} on {exchange}")
            logger.warning(f"No active contracts found for {symbol} on {exchange}")
            
            # Try with generic 'SMART' exchange as a fallback
            if exchange != 'SMART':
                print(f"Trying with SMART exchange as fallback...")
                futures_contract = Future(symbol=symbol, exchange='SMART', currency=currency)
                
                # Apply trading class for currency futures even with SMART
                if symbol in CURRENCY_TRADING_CLASS:
                    futures_contract.tradingClass = CURRENCY_TRADING_CLASS[symbol]['class']
                    
                active_contracts = req_contract_details_safe(ib, futures_contract)
                
                if not active_contracts:
                    print(f"⚠️ No active contracts found for {symbol} on SMART either")
                    
                    # Last resort: check if we have any positions for this symbol
                    print(f"Checking current portfolio for active {symbol} futures positions...")
                    positions = ib.positions()
                    matching_positions = []
                    
                    # Check for positions with this symbol
                    for pos in positions:
                        if hasattr(pos.contract, 'symbol') and pos.contract.symbol == symbol:
                            print(f"Found matching position: {pos.contract.localSymbol}")
                            matching_positions.append(pos.contract)
                    
                    if matching_positions:
                        print(f"✅ Found {len(matching_positions)} active contracts for {symbol} in portfolio")
                        contracts = matching_positions
                        
                        # Sort contracts by expiration date
                        contracts.sort(key=lambda x: x.lastTradeDateOrContractMonth if x.lastTradeDateOrContractMonth else '')
                        
                        # Standardize all contract symbols
                        for contract in contracts:
                            standardize_contract_symbol(contract)
                            contract.includeExpired = True

                        contracts = _filter_by_forward_window(contracts, max_forward_days)
                        
                        print(f"✅ Found {len(contracts)} active contracts for {symbol}")
                        
                        for i, contract in enumerate(contracts):
                            print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                        
                        return contracts
                    
                    if not active_contracts and not conid:
                        return []

            if not active_contracts and conid:
                try:
                    con_contract = Contract(conId=int(conid))
                except Exception:
                    con_contract = None
                if con_contract is not None:
                    active_contracts = req_contract_details_safe(ib, con_contract)
                    if active_contracts:
                        print(f"✅ Found {len(active_contracts)} active contracts for {symbol} via conId {conid}")
            if not active_contracts:
                return []
        
        # Extract contract objects from details
        contracts = [details.contract for details in active_contracts]
        
        # Sort contracts by expiration date
        contracts.sort(key=lambda x: x.lastTradeDateOrContractMonth if x.lastTradeDateOrContractMonth else '')
        
        # Standardize all contract symbols
        for contract in contracts:
            standardize_contract_symbol(contract)
            contract.includeExpired = True

        contracts = _filter_by_forward_window(contracts, max_forward_days)
        
        print(f"✅ Found {len(contracts)} active contracts for {symbol} on {exchange}")
        logger.info(f"Found {len(contracts)} active contracts for {symbol} on {exchange}")
        
        for i, contract in enumerate(contracts):
            print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
        
        return contracts
    
    except Exception as e:
        print(f"❌ Error getting active futures contracts: {e}")
        logger.error(f"Error getting active futures contracts: {e}")
        return []

def get_expired_futures_contracts(
    ib,
    ticker,
    exchange,
    currency='USD',
    years_back=5,
    local_symbol=None,
    conid: int | None = None,
):
    """
    Get historical expired future contracts for a symbol.
    
    Args:
        ib: IB connection object
        ticker: The futures symbol to look up
        exchange: Exchange to search on
        currency: Currency code
        years_back: How many years to look back for expired contracts
    
    Returns:
        list: List of expired contract objects
    """
    if years_back <= 0:
        print(f"ℹ️ Skipping expired futures contracts for {ticker} (years_back={years_back})")
        logger.info("Skipping expired futures contracts (years_back <= 0)")
        return []
    print(f"🔍 Getting expired futures contracts for {ticker} on {exchange}, {years_back} years back")
    logger.info(f"Getting expired futures contracts for {ticker} on {exchange}, {years_back} years back")
    
    # Define our date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years_back)
    
    try:
        # Check if this is a special handling case
        if ticker in SPECIAL_DERIVATIVES_HANDLING:
            special_info = SPECIAL_DERIVATIVES_HANDLING[ticker]
            print(f"⚠️ Applying special handling for {ticker} expired contracts ({special_info['contract_info']})")
            logger.info(f"Applying special handling for {ticker} expired contracts as {special_info['secType']} with derivatives")
            
            # Try different exchanges for special cases
            all_contracts = []
            exchanges_to_try = [special_info.get('exchange')] + special_info.get('fallback_exchanges', [])
            
            # Remove duplicates while preserving order
            exchanges_to_try = list(dict.fromkeys(exchanges_to_try))
            
            for exch in exchanges_to_try:
                if exch == exchange:  # Skip if it's the same as the passed exchange that already failed
                    continue
                    
                print(f"Trying to get {ticker} expired futures contracts from {exch} exchange...")
                
                # For currency futures, use the trading class
                if special_info.get('secType') == 'CASH' and 'trading_class' in special_info:
                    # Create a Future contract with the specified trading class
                    future = Future(
                        symbol=ticker, 
                        exchange=exch,
                        currency='USD',  # Always use USD for currency futures
                        tradingClass=special_info['trading_class'],
                        includeExpired=True
                    )
                    contract_details = req_contract_details_safe(ib, future)
                    
                    if contract_details:
                        contracts = [details.contract for details in contract_details]
                        print(f"✅ Found {len(contracts)} expired contracts for {ticker} using trading class {special_info['trading_class']} on {exch}")
                        
                        # Filter contracts within our date range
                        filtered_contracts = []
                        for contract in contracts:
                            expiry_date = _parse_contract_expiry(contract)
                            if expiry_date is None:
                                continue
                            if start_date <= expiry_date <= end_date:
                                standardize_contract_symbol(contract)
                                filtered_contracts.append(contract)
                                    
                        if filtered_contracts:
                            all_contracts.extend(filtered_contracts)
                            print(f"✅ Added {len(filtered_contracts)} filtered contracts from {exch}")
                
                # For index futures
                elif special_info.get('secType') == 'IND':
                    future = Future(
                        symbol=ticker, 
                        exchange=exch,
                        currency='USD',
                        includeExpired=True
                    )
                    contract_details = req_contract_details_safe(ib, future)
                    
                    if contract_details:
                        contracts = [details.contract for details in contract_details]
                        print(f"✅ Found {len(contracts)} expired contracts for {ticker} on {exch}")
                        
                        # Filter contracts within our date range
                        filtered_contracts = []
                        for contract in contracts:
                            expiry_date = _parse_contract_expiry(contract)
                            if expiry_date is None:
                                continue
                            if start_date <= expiry_date <= end_date:
                                standardize_contract_symbol(contract)
                                filtered_contracts.append(contract)
                                    
                        if filtered_contracts:
                            all_contracts.extend(filtered_contracts)
                            print(f"✅ Added {len(filtered_contracts)} filtered contracts from {exch}")
            
            if all_contracts:
                # Sort contracts by expiry date
                all_contracts.sort(key=lambda c: c.lastTradeDateOrContractMonth)
                print(f"✅ Found total of {len(all_contracts)} expired contracts for {ticker} across all exchanges")
                
                # Print out all the contracts we found
                for i, contract in enumerate(all_contracts):
                    print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                
                return all_contracts
            
            # If we couldn't find any contracts through direct query, try generating them
            print(f"No expired contracts found through direct queries, falling back to generating contracts...")
            generated_contracts = generate_expired_contracts(
                ticker, 
                start_date, 
                end_date, 
                special_info.get('exchange', exchange),
                'USD'
            )
            
            if 'trading_class' in special_info:
                # Set the trading class for all generated contracts
                for contract in generated_contracts:
                    contract.tradingClass = special_info['trading_class']
                    
                    # Update the local symbol to use the trading class if needed
                    if hasattr(contract, 'localSymbol') and contract.localSymbol:
                        parts = re.search(r'([A-Z]+)([FGHJKMNQUVXZ])(\d)', contract.localSymbol)
                        if parts:
                            symbol_part = parts.group(1)
                            month_code = parts.group(2)
                            year_digit = parts.group(3)
                            
                            if symbol_part != special_info['trading_class']:
                                contract.localSymbol = f"{special_info['trading_class']}{month_code}{year_digit}"
                                print(f"Updated localSymbol from {symbol_part}{month_code}{year_digit} to {contract.localSymbol}")
            
            if generated_contracts:
                print(f"✅ Generated {len(generated_contracts)} expired contracts for {ticker}")
                
                # Print out all the contracts we generated
                for i, contract in enumerate(generated_contracts):
                    print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                
                return generated_contracts
            
            return []
            
        # First attempt - try to get expired contracts via the API
        if ticker in CURRENCY_TRADING_CLASS:
            currency = 'USD'
        future = Future(symbol=ticker, exchange=exchange, currency=currency, includeExpired=True)
        
        # Handle special case for currency futures
        is_currency_future = False
        if ticker in CURRENCY_TRADING_CLASS:
            future.tradingClass = CURRENCY_TRADING_CLASS[ticker]['class']
            is_currency_future = True
            
            # Force USD currency for standard currency futures
            if ticker in ['SEK', 'NOK', 'CHF', 'GBP', 'CAD', 'JPY', 'AUD', 'NZD', 'MXP', 'ZAR', 'EUR']:
                future.currency = 'USD'
                
        # Get expired contracts for the specified date range
        print(f"Requesting contract details for {ticker} on {exchange}...")
        contract_details = req_contract_details_safe(ib, future)
        print(f"Received {len(contract_details)} contract details")

        if (not contract_details) and conid:
            try:
                con_contract = Contract(conId=int(conid))
                con_contract.includeExpired = True
            except Exception:
                con_contract = None
            if con_contract is not None:
                contract_details = req_contract_details_safe(ib, con_contract)
                if contract_details:
                    print(f"✅ Found expired contract details for {ticker} via conId {conid}")
        
        if contract_details:
            # Filter contracts within our date range
            # Convert lastTradeDateOrContractMonth to datetime for comparison
            filtered_contracts = []
            for details in contract_details:
                contract = details.contract
                
                # Print contract info for debugging
                print(f"Processing contract: {contract.localSymbol}, expiry={contract.lastTradeDateOrContractMonth}")
                
                # Standardize the contract symbol first
                standardize_contract_symbol(contract)
                
                # Make sure the contract has a lastTradeDateOrContractMonth
                expiry_date = _parse_contract_expiry(contract)
                if expiry_date is None:
                    continue
                try:
                    if start_date <= expiry_date <= end_date:
                        # Process each contract
                        # For accuracy, qualify the contract
                        print(f"Qualifying contract: {contract.localSymbol}")
                        qualified_contract = ib.qualifyContracts(contract)
                        if qualified_contract:
                            # Standardize the contract symbol
                            standardize_contract_symbol(qualified_contract[0])
                            print(f"Added qualified contract: {qualified_contract[0].localSymbol} with expiry {qualified_contract[0].lastTradeDateOrContractMonth}")
                            filtered_contracts.append(qualified_contract[0])
                        else:
                            print(f"Failed to qualify contract: {contract.localSymbol}")
                            # Still try to use the unqualified contract
                            filtered_contracts.append(contract)
                    else:
                        print(f"Contract {contract.localSymbol} expiry {expiry_date} outside range {start_date} to {end_date}")
                except Exception as e:
                    print(f"Error processing contract {contract.localSymbol}: {e}")
                    logger.error(f"Error processing contract {contract.localSymbol}: {e}")
            
            # Sort contracts by expiry date
            filtered_contracts.sort(key=lambda c: c.lastTradeDateOrContractMonth)
            
            if filtered_contracts:
                print(f"✅ Found {len(filtered_contracts)} expired contracts for {ticker}")
                logger.info(f"Found {len(filtered_contracts)} expired contracts for {ticker}")
                
                # Print out all the contracts we found
                for i, contract in enumerate(filtered_contracts):
                    print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                
                return filtered_contracts
            else:
                print(f"⚠️ Found contracts, but none within the specified date range")
                logger.warning(f"Found contracts, but none within the specified date range")
        
        # If we're here, either no contracts were found or none were in our date range
        print(f"⚠️ No expired contracts found for {ticker} on {exchange} via direct query")
        logger.warning(f"No expired contracts found for {ticker} on {exchange} via direct query")
        
        # Try with different exchange as a fallback
        fallback_exchanges = ['SMART']
        if exchange != 'CME' and not is_currency_future:
            fallback_exchanges.append('CME')
        if exchange != 'GLOBEX' and not is_currency_future:
            fallback_exchanges.append('GLOBEX')
        
        for fallback_exchange in fallback_exchanges:
            print(f"Trying fallback exchange: {fallback_exchange}")
            logger.info(f"Trying fallback exchange: {fallback_exchange}")
            
            future = Future(symbol=ticker, exchange=fallback_exchange, includeExpired=True)
            
            # Handle special case for currency futures
            if ticker in CURRENCY_TRADING_CLASS:
                future.tradingClass = CURRENCY_TRADING_CLASS[ticker]['class']
                
                # Force USD currency for standard currency futures
                if ticker in ['SEK', 'NOK', 'CHF', 'GBP', 'CAD', 'JPY', 'AUD', 'NZD', 'MXP', 'ZAR', 'EUR']:
                    future.currency = 'USD'
            
            contract_details = req_contract_details_safe(ib, future)
            print(f"Received {len(contract_details)} contract details from fallback {fallback_exchange}")
            
            if contract_details:
                # Filter contracts within our date range
                filtered_contracts = []
                for details in contract_details:
                    contract = details.contract
                    
                    # Print contract info for debugging
                    print(f"Processing fallback contract: {contract.localSymbol}, expiry={contract.lastTradeDateOrContractMonth}")
                    
                    # Standardize the contract symbol first
                    standardize_contract_symbol(contract)
                    
                    # Make sure the contract has a lastTradeDateOrContractMonth
                    expiry_date = _parse_contract_expiry(contract)
                    if expiry_date is None:
                        continue
                    try:
                        if start_date <= expiry_date <= end_date:
                            # Process each contract
                            # For accuracy, qualify the contract
                            print(f"Qualifying fallback contract: {contract.localSymbol}")
                            qualified_contract = ib.qualifyContracts(contract)
                            if qualified_contract:
                                # Standardize the contract symbol
                                standardize_contract_symbol(qualified_contract[0])
                                print(f"Added qualified fallback contract: {qualified_contract[0].localSymbol}")
                                filtered_contracts.append(qualified_contract[0])
                            else:
                                print(f"Failed to qualify fallback contract: {contract.localSymbol}")
                                # Still try to use the unqualified contract
                                filtered_contracts.append(contract)
                        else:
                            print(f"Fallback contract {contract.localSymbol} expiry {expiry_date} outside range {start_date} to {end_date}")
                    except Exception as e:
                        print(f"Error processing fallback contract {contract.localSymbol}: {e}")
                        logger.error(f"Error processing fallback contract {contract.localSymbol}: {e}")
                
                # Sort contracts by expiry date
                filtered_contracts.sort(key=lambda c: c.lastTradeDateOrContractMonth)
                
                if filtered_contracts:
                    print(f"✅ Found {len(filtered_contracts)} expired contracts for {ticker} on fallback exchange {fallback_exchange}")
                    logger.info(f"Found {len(filtered_contracts)} expired contracts for {ticker} on fallback exchange {fallback_exchange}")
                    
                    # Print out all the contracts we found
                    for i, contract in enumerate(filtered_contracts):
                        print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                    
                    return filtered_contracts
        
        # Second fallback approach - generate contracts based on known patterns
        print(f"⚠️ No expired contracts found via direct API query. Generating contracts based on patterns...")
        logger.warning(f"No expired contracts found via direct API query. Generating contracts based on patterns...")
        
        generated_contracts = generate_expired_contracts(ticker, start_date, end_date, exchange)
        
        if generated_contracts:
            print(f"✅ Generated {len(generated_contracts)} expired contracts for {ticker}")
            logger.info(f"Generated {len(generated_contracts)} expired contracts for {ticker}")
            
            # Print out all the contracts we generated
            for i, contract in enumerate(generated_contracts):
                print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
            
            return generated_contracts
        
        # If all else fails, try with a longer date range
        if years_back < 10:
            print(f"⚠️ Trying with a longer date range: {years_back + 5} years")
            logger.warning(f"Trying with a longer date range: {years_back + 5} years")
            return get_expired_futures_contracts(
                ib,
                ticker,
                exchange,
                currency,
                years_back + 5,
                local_symbol,
                conid,
            )
        
        print(f"❌ Could not find any expired contracts for {ticker}")
        logger.error(f"Could not find any expired contracts for {ticker}")
        return []
        
    except Exception as e:
        print(f"❌ Error getting expired contracts for {ticker}: {e}")
        logger.error(f"Error getting expired contracts for {ticker}: {e}")
        return []

def generate_expired_contracts(symbol, start_date, end_date, exchange, currency='USD'):
    """
    Generate contract specifications for expired futures contracts.
    
    Args:
        symbol: The symbol of the futures contract (e.g., 'MES')
        start_date: The earliest date to generate contracts for
        end_date: The latest date to generate contracts for
        exchange: The exchange where the contract is traded
        currency: The currency of the contract
        
    Returns:
        list: List of contract specifications for expired contracts
    """
    # Common contract month codes
    # H=March, M=June, U=September, Z=December
    month_codes = {
        3: 'H',  # March
        6: 'M',  # June
        9: 'U',  # September
        12: 'Z'  # December
    }
    
    # Generate a range of contracts across the date range
    contracts = []
    
    # Start from the year before the requested start date
    # to ensure we have complete coverage
    start_year = start_date.year - 1
    end_year = end_date.year + 1  # Include an extra year for current contracts
    
    print(f"Generating contract specifications from {start_year} to {end_year}")

    # Define multipliers for currency futures - comprehensive list
    currency_multipliers = {
        'GBP': '62500',
        'CHF': '125000',
        'CAD': '100000',
        'NZD': '100000',
        'AUD': '100000',
        'JPY': '12500000',
        'EUR': '125000',
        'ZAR': '500000',
        'MXP': '500000',
        'MXN': '500000',
        'NOK': '2000000',
        'SEK': '2000000',
        'BRL': '100000',
        'RUR': '2500000',
        'CNH': '100000'
    }
    
    # Check if this is a special case
    use_special_trading_class = False
    trading_class = None
    
    # If this is a currency with special trading class handling
    if symbol in CURRENCY_TRADING_CLASS:
        trading_class = CURRENCY_TRADING_CLASS[symbol]['class']
        use_special_trading_class = CURRENCY_TRADING_CLASS[symbol].get('use_trading_class', False)
    
    # If this is in our special derivatives handling
    if symbol in SPECIAL_DERIVATIVES_HANDLING:
        special_info = SPECIAL_DERIVATIVES_HANDLING[symbol]
        if 'trading_class' in special_info:
            trading_class = special_info['trading_class']
            use_special_trading_class = True
            
        # If a specific exchange is defined, use it instead of the passed one
        if 'exchange' in special_info:
            exchange = special_info['exchange']
    
    for year in range(start_year, end_year + 1):
        for month in sorted(month_codes.keys()):  # March, June, Sept, Dec
            # Create contract with YYYYMM format
            lastTradeDateOrContractMonth = f"{year}{month:02d}"
            
            # For specific dates, create a datetime for comparison with transition dates
            contract_date = datetime(year, month, 15)  # Use middle of month for comparison
            
            # Skip future contracts - those will be handled by active contracts
            if contract_date > datetime.now():
                continue
                
            # Check if this is a currency future and needs USD
            is_currency_future = symbol in currency_multipliers or symbol in CURRENCY_TRADING_CLASS
            
            # Ensure USD currency for currency futures
            if is_currency_future:
                contract_currency = 'USD'
            else:
                contract_currency = currency
            
            # Create a contract specification - use the Futures contract directly without a localSymbol initially
            contract = Future(
                symbol=symbol,
                lastTradeDateOrContractMonth=lastTradeDateOrContractMonth,
                exchange=exchange,
                currency=contract_currency,
                includeExpired=True
            )
            
            # Add multiplier for currency futures
            if symbol in currency_multipliers:
                contract.multiplier = currency_multipliers[symbol]
                print(f"Setting multiplier {currency_multipliers[symbol]} for {symbol} {lastTradeDateOrContractMonth}")
            
            # Get year digit and month code for local symbol
            year_digit = str(year)[-1]  # Last digit of year
            month_code = month_codes[month]
            
            # Check if this symbol has historical trading class changes
            if symbol in HISTORICAL_TRADING_CLASS_MAPPING:
                transition_info = HISTORICAL_TRADING_CLASS_MAPPING[symbol]
                if contract_date < transition_info['transition_date']:
                    # Use old trading class for contracts before the transition date
                    trading_class = transition_info['old_class']
                else:
                    # Use new trading class for contracts after the transition date
                    trading_class = transition_info['new_class']
                
                contract.tradingClass = trading_class
                # Use appropriate local symbol based on trading class
                contract.localSymbol = f"{trading_class}{month_code}{year_digit}"
                print(f"Using historical trading class {trading_class} for {symbol} {lastTradeDateOrContractMonth}")
            
            # Handle special cases - VXM and ZAR
            elif symbol in SPECIAL_DERIVATIVES_HANDLING:
                special_info = SPECIAL_DERIVATIVES_HANDLING[symbol]
                
                # For currency futures like ZAR
                if special_info.get('secType') == 'CASH' and 'trading_class' in special_info:
                    trading_class = special_info['trading_class']
                    contract.tradingClass = trading_class
                    # VXM uses symbol in local symbol, ZAR uses 6Z
                    contract.localSymbol = f"{trading_class}{month_code}{year_digit}"
                    print(f"Using special trading class {trading_class} for {symbol} {lastTradeDateOrContractMonth}")
                
                # For index futures like VXM
                elif special_info.get('secType') == 'IND':
                    contract.tradingClass = symbol
                    contract.localSymbol = f"{symbol}{month_code}{year_digit}"
                    print(f"Using symbol as trading class for {symbol} {lastTradeDateOrContractMonth}")
            
            # Regular handling for other contracts
            elif symbol in CURRENCY_TRADING_CLASS:
                trading_class = CURRENCY_TRADING_CLASS[symbol]['class']
                contract.tradingClass = trading_class
                # Currency futures use trading class in local symbol
                if CURRENCY_TRADING_CLASS[symbol].get('use_trading_class', True):
                    contract.localSymbol = f"{trading_class}{month_code}{year_digit}"
                else:
                    # Some currencies use their symbol directly in local symbol
                    contract.localSymbol = f"{symbol}{month_code}{year_digit}"
                print(f"Using currency trading class {trading_class} for {symbol} {lastTradeDateOrContractMonth}")
            else:
                # Standard futures format for local symbol (e.g. ZTH5 for ZT March 2025)
                contract.localSymbol = f"{symbol}{month_code}{year_digit}"
                contract.tradingClass = symbol
            
            # Make sure the localSymbol doesn't have spaces and is correctly formatted
            if hasattr(contract, 'localSymbol'):
                # Ensure localSymbol format is correct (e.g., ZTH5 not "ZT   H5")
                contract.localSymbol = contract.localSymbol.strip()
                
                # Print debug info about the contract
                print(f"Generated contract: {contract.localSymbol} for {lastTradeDateOrContractMonth}")
                
            contracts.append(contract)
            
    print(f"Generated {len(contracts)} contract specifications")
    return contracts

def fetch_historical_data_with_retry(
    ib,
    contract,
    end_date_str,
    duration_str,
    fallback_options=None,
    bid_ask=False,
    what_to_show_override=None,
    max_retries: int = 3,
    retry_sleep: float = 10.0,
    timeout_multiplier: float = 1.0,
    log_samples: bool = True,
    meta: dict | None = None,
    deadline_ts: float | None = None,
):
    """
    Fetch historical data with retry logic.
    
    Args:
        ib: The IB connection object
        contract: The contract object
        end_date_str: End date string formatted for IBKR API
        duration_str: Duration string (e.g., "30 D")
        fallback_options: List of fallback data types to try if TRADES fails (e.g., ['BID_ASK', 'MIDPOINT'])
        bid_ask: If True, fetch BID_ASK data instead of TRADES
        max_retries: Max attempts per data type
        retry_sleep: Seconds to sleep between retries
        timeout_multiplier: Multiply request timeout (use >1 for slower exchanges)
        log_samples: Whether to print sample bars
        
    Returns:
        list: List of historical data bars or empty list if failed
        
    Raises:
        Exception: If a connection error occurs, it will be re-raised to be handled by the caller
    """
    bars = None
    
    # Determine which data type(s) to request
    if what_to_show_override is not None and len(what_to_show_override) > 0:
        # Explicit override provided by caller (e.g., ['BID'] or ['ASK'])
        what_to_show_options = list(what_to_show_override)
    else:
        what_to_show_options = []
        # Use BID_ASK if bid_ask flag is set, otherwise use TRADES
        # Note: BID_ASK historical bars are not supported for many futures on HMDS.
        # We keep BID_ASK first for compatibility, but callers requesting bid/ask
        # should prefer explicit BID/ASK requests (handled by caller when bid_ask=True).
        if bid_ask:
            what_to_show_options = ['BID_ASK']
        else:
            what_to_show_options = ['TRADES']
        
        # Add user-specified fallback options if provided
        if fallback_options:
            what_to_show_options.extend(fallback_options)
    
    # Improve logging to accurately reflect the request plan
    print(f"Using data types (in order): {what_to_show_options}")
    
    # Try each what_to_show option until one works
    for what_to_show in what_to_show_options:
        for retry_count in range(max_retries):
            if deadline_ts is not None and time.time() > deadline_ts:
                if meta is not None:
                    meta["last_error_kind"] = "budget_exceeded"
                return []
            bars = None
            last_error = None
            try:
                # Check if connected before making request
                if not ib.isConnected():
                    raise Exception("Not connected to IBKR")
                    
                # Use request tracker to handle pacing
                request_tracker.wait_if_needed(
                    contract=contract,
                    request_type='historical',
                    bar_size='1 min',
                    duration=duration_str
                )
                
                print(f"Requesting historical data with whatToShow={what_to_show}")

                exchange = str(getattr(contract, "exchange", "")).upper()
                req_timeout = 120 if exchange == "EUREX" else 60
                req_timeout = max(1, int(req_timeout * timeout_multiplier))

                # Request the historical data
                bars = ib.reqHistoricalData(
                    contract=contract,
                    endDateTime=end_date_str,
                    durationStr=duration_str,
                    barSizeSetting='1 min',
                    whatToShow=what_to_show,
                    useRTH=False,  # V3: Changed to False for 24/6 coverage
                    formatDate=2,  # Use formatDate=2 for timezone-aware timestamps
                    timeout=req_timeout,
                )
                
            except Exception as e:
                error_str = str(e)
                last_error = e
                # Check if this is a connection error
                if "Not connected" in error_str or not ib.isConnected():
                    print(f"❌ Connection lost during historical data request: {e}")
                    # Re-raise connection errors to be handled by the caller
                    raise
                # Check for specific data type errors that suggest we should try another data type
                elif (
                    "HMDS query returned no data" in error_str
                    or "No data of type" in error_str
                    or ("HMDS" in error_str and "162" in error_str)
                ):
                    print(f"❌ No data available for {what_to_show} data type: {e}")
                    bars = []
                    last_error = None
                    if meta is not None:
                        meta["last_error_kind"] = "hmds_no_data"
                        meta["last_error_message"] = error_str
                elif "Timeout" in error_str or "timed out" in error_str:
                    bars = []
                    last_error = None
                    if meta is not None:
                        meta["last_error_kind"] = "timeout"
                        meta["last_error_message"] = error_str
                else:
                    bars = None
                    if meta is not None:
                        meta["last_error_kind"] = "error"
                        meta["last_error_message"] = error_str

            if bars:
                print(f"✅ Retrieved {len(bars)} bars of data on attempt {retry_count + 1} using {what_to_show}")

                # Display a sample of the retrieved data
                if len(bars) > 0 and log_samples:
                    print("\n📊 SAMPLE OF RETRIEVED DATA:")
                    sample_size = min(3, len(bars))
                    for i in range(sample_size):
                        bar = bars[i]
                        print(
                            f"  {i+1}. Date: {bar.date}, Open: {bar.open}, High: {bar.high}, "
                            f"Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}"
                        )
                    print("  ...")
                    if len(bars) > sample_size:
                        bar = bars[-1]
                        print(
                            f"  {len(bars)}. Date: {bar.date}, Open: {bar.open}, High: {bar.high}, "
                            f"Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}"
                        )

                return bars  # Return immediately if we got data

            # No bars returned (empty list or error without data)
            if retry_count < max_retries - 1:
                if last_error is None:
                    print(
                        f"❌ No historical data retrieved with {what_to_show} "
                        f"(attempt {retry_count + 1}/{max_retries})"
                    )
                else:
                    print(
                        f"❌ Error requesting historical data with {what_to_show} "
                        f"(attempt {retry_count + 1}/{max_retries}): {last_error}"
                    )
                wait_time = _compute_backoff(retry_sleep, retry_count, cap=120.0)
                print(f"Waiting {wait_time:.1f} seconds before retrying...")
                time.sleep(wait_time)
                continue

            if last_error is None:
                print(f"❌ No historical data retrieved with {what_to_show} after {max_retries} attempts")
            else:
                print(f"❌ Error requesting historical data with {what_to_show} after {max_retries} attempts: {last_error}")
                logger.error(
                    f"Error requesting historical data with {what_to_show} after {max_retries} attempts: {last_error}"
                )
            print("Trying next data type if available...")
            break
    
    # If we get here, we've tried all what_to_show options and none worked
    print(f"❌ Failed to retrieve data with all available data types: {what_to_show_options}")
    if meta is not None and "last_error_kind" not in meta:
        meta["last_error_kind"] = "empty"
    return []

def get_historical_data_for_contract(
    ib,
    contract,
    ticker,
    walk_backward=True,
    fallback_options=None,
    update_mode=False,
    duration_override=None,
    bid_ask=False,
    client_id=None,
    max_lookback_override=None,
    years_back: int | None = None,
    bidask_cache: BidAskCache | None = None,
    trades_cache: TradesNoDataCache | None = None,
    prompt_user: bool = True,
):
    """
    Get historical data for a specific futures contract.
    
    Args:
        ib: The IB connection object
        contract: The contract object
        ticker: The ticker symbol (for logging)
        walk_backward: Whether to walk backward to fetch older data (default: True)
        fallback_options: List of fallback data types to try if TRADES fails
        update_mode: Whether we're in update mode (default: False)
        duration_override: Override the default duration (default: None)
        bid_ask: If True, fetch BID_ASK data instead of TRADES
        years_back: Hard cap in years for backfills
        bidask_cache: Optional bid/ask availability cache
        trades_cache: Optional TRADES no-data cache
        
    Returns:
        pd.DataFrame: DataFrame containing historical data or None if error
    """
    max_reconnection_attempts = 3
    reconnection_attempt = 0
    contract_start_ts = time.time()
    budget_sec: int | None = None
    deadline_ts: float | None = None
    
    while reconnection_attempt <= max_reconnection_attempts:
        try:
            # Check if we're still connected to IBKR
            force_reconnect = bool(getattr(ib, "_ab_connection_lost", False))
            if force_reconnect:
                lost_msg = getattr(ib, "_ab_connection_error", "")
                print(f"⚠️ Detected IBKR connectivity warning (1100): {lost_msg}")
                lost_ts = getattr(ib, "_ab_connection_lost_ts", None)
                if lost_ts is not None:
                    wait_for = max(0.0, 5.0 - (time.time() - lost_ts))
                    if wait_for > 0:
                        print(f"Waiting {wait_for:.1f}s before reconnecting...")
                        time.sleep(wait_for)
                setattr(ib, "_ab_connection_lost", False)
                try:
                    ib.disconnect()
                except Exception:
                    pass
            if force_reconnect or not ib.isConnected():
                print(f"❌ Lost connection to IBKR while processing {contract.localSymbol}")
                reconnection_attempt += 1
                
                if reconnection_attempt <= max_reconnection_attempts:
                    print(f"Attempting to reconnect (attempt {reconnection_attempt}/{max_reconnection_attempts})...")
                    # Get connection parameters from the current ib object or use defaults
                    host = getattr(ib, 'host', '127.0.0.1')
                    port = getattr(ib, 'port', 7497)
                    # Prefer explicit client_id passed from CLI; fall back to current IB clientId, then default
                    effective_client_id = client_id if client_id is not None else getattr(ib, 'clientId', 22)
                    
                    try:
                        ib.disconnect()
                    except:
                        pass
                    
                    try:
                        ib.connect(host, port, clientId=effective_client_id, readonly=True, timeout=30)
                        if ib.isConnected():
                            print("✅ Successfully reconnected to IBKR")
                            # Reset attempt counter on successful reconnection
                            reconnection_attempt = 0
                        else:
                            print(f"❌ Failed to reconnect on attempt {reconnection_attempt}")
                            print("Waiting 10 seconds before next attempt...")
                            time.sleep(10)
                            continue
                    except Exception as e:
                        print(f"❌ Error during reconnection attempt: {e}")
                        print("Waiting 10 seconds before next attempt...")
                        time.sleep(10)
                        continue
                else:
                    # Maximum reconnection attempts reached
                    if not prompt_user:
                        print(f"Skipping {contract.localSymbol} due to connection issues (non-interactive mode)")
                        return None
                    prompt_retry = input("Reached maximum reconnection attempts. Try again? (Y/N): ")
                    if prompt_retry.upper() == 'Y':
                        reconnection_attempt = 0
                        continue
                    print(f"Skipping {contract.localSymbol} due to connection issues")
                    return None
            
            # Ensure contract symbol is properly formatted
            standardize_contract_symbol(contract)
            
            # Ensure the contract is properly set up
            contract.includeExpired = True
            
            # Log contract info for debugging
            contract_info = f"{contract.localSymbol} - {contract.lastTradeDateOrContractMonth}"
            print(f"Getting historical data for {contract_info}...")
            logger.info(f"Getting historical data for {contract_info}")
            
            # Special handling for certain symbols
            is_special_contract = ticker in SPECIAL_DERIVATIVES_HANDLING
            is_zar_contract = ticker == 'ZAR'
            
            if is_special_contract:
                print(f"⚠️ Using special historical data handling for {ticker}")
                if is_zar_contract:
                    print(f"⚠️ ZAR data requires special treatment. Will try multiple data types and shorter durations.")
            
            # Qualify the contract through IBKR
            try:
                print(f"Qualifying contract: {contract.localSymbol}...")
                qualified_contracts = ib.qualifyContracts(contract)
                
                if not qualified_contracts:
                    print(f"❌ Failed to qualify contract: {contract.localSymbol}")
                    logger.error(f"Failed to qualify contract: {contract.localSymbol}")
                    return None
                    
                contract = qualified_contracts[0]
                print(f"✅ Successfully qualified contract: {contract.localSymbol}")
                
                # Run standardize again after qualification to ensure proper format
                standardize_contract_symbol(contract)
            except Exception as e:
                error_str = str(e)
                logger.error(f"Error during contract qualification: {e}")
                
                # Check if this is a connection error
                if "Not connected" in error_str:
                    print(f"❌ Connection lost during contract qualification: {e}")
                    # Don't increment reconnection_attempt here, let the outer loop handle it
                    # Break out of this try/except and let the while loop detect the disconnection
                    if not ib.isConnected():
                        continue
                else:
                    print(f"❌ Error during contract qualification: {e}")
                    return None
            
            # Create the data directory if it doesn't exist
            data_dir = Path("data")
            if not data_dir.exists():
                data_dir.mkdir(parents=True)
                print(f"Created data directory: {data_dir}")
            
            # Construct the filename for this contract
            file_name = f"{ticker}_{contract.localSymbol}_1min.csv"
            file_path = data_dir / file_name
            
            # Note if we already have data for this contract (but continue anyway to overwrite)
            if file_path.exists():
                print(f"Data file exists for {contract.localSymbol}: {file_path}, will overwrite with new data")
                logger.info(f"Data file exists for {contract.localSymbol}: {file_path}, will overwrite with new data")
            
            # Determine if the contract is active or expired
            is_expired = False
            expiry_date = None
            expiry_date = _parse_contract_expiry(contract)
            if expiry_date:
                is_expired = expiry_date < datetime.now()
            
            # Set up parameters for historical data requests
            print(f"Contract status: {'Expired' if is_expired else 'Active'}")
            
            # For both active and expired contracts, we'll use multiple requests to get full history
            # Start with an appropriate end date
            if is_expired and expiry_date:
                # For expired contracts, start from near the expiration date
                end_date = min(expiry_date + timedelta(days=5), datetime.now())
                print(f"Using initial end date near expiration: {end_date.strftime('%Y%m%d')}")
            else:
                # For active contracts or if we couldn't determine expiry, start from current date
                end_date = datetime.now()
            
            # Start the multi-request process to get all historical data
            # If bid_ask mode, collect BID and ASK separately for merging
            all_bars = []
            all_bars_bid = [] if bid_ask else None
            all_bars_ask = [] if bid_ask else None
            # Limit update-mode runs to a single window to keep refreshes fast.
            max_lookback = max_lookback_override if max_lookback_override is not None else (1 if update_mode else 18)
            lookback_count = 0
            consecutive_empty_periods = 0  # Track consecutive periods with no data
            consecutive_partial_periods = 0  # Track consecutive periods with only one side
            empty_response_retries = 0  # Track retries for non-bid/ask windows
            
            # Adjust duration settings
            duration_days = 3 if bid_ask else 7  # Shorter windows for bid/ask
            if is_zar_contract and duration_days > 5:
                duration_days = 5  # Shorter duration for ZAR contracts
                
            if duration_override:
                # Use the provided duration override (for update mode)
                duration_str = duration_override
                print(f"Using duration override: {duration_str}")
            else:
                # Standard duration logic
                if is_zar_contract:
                    print(f"⚠️ Using shorter duration ({duration_days} days) for ZAR contract")
                duration_str = f"{duration_days} D"

            exchange = str(getattr(contract, "exchange", "")).upper()
            wait_multiplier = _exchange_wait_multiplier(exchange)
            timeout_multiplier = 1.0
            budget_sec = _contract_time_budget_seconds(update_mode, exchange)
            if budget_sec:
                deadline_ts = contract_start_ts + budget_sec
            hard_stop_date = _compute_hard_stop_date(end_date, years_back)
            min_required_date = hard_stop_date
            cache_entry = None
            trades_cache_entry = None

            if bid_ask:
                if bidask_cache:
                    cache_entry = bidask_cache.get_entry(_contract_cache_key(contract))
                    cache_entry["exchange"] = exchange
                else:
                    cache_entry = {"bid": {}, "ask": {}, "no_data_windows": []}

                if not update_mode:
                    bid_earliest = _parse_iso_dt(cache_entry.get("bid", {}).get("earliest"))
                    ask_earliest = _parse_iso_dt(cache_entry.get("ask", {}).get("earliest"))
                    if bid_earliest is not None and bid_earliest < hard_stop_date:
                        bid_earliest = hard_stop_date
                    if ask_earliest is not None and ask_earliest < hard_stop_date:
                        ask_earliest = hard_stop_date
                    if bid_earliest is None:
                        bid_earliest = _find_earliest_side(
                            ib,
                            contract,
                            "BID",
                            end_date,
                            hard_stop_date,
                            timeout_multiplier,
                            wait_multiplier,
                        )
                    if ask_earliest is None:
                        ask_earliest = _find_earliest_side(
                            ib,
                            contract,
                            "ASK",
                            end_date,
                            hard_stop_date,
                            timeout_multiplier,
                            wait_multiplier,
                        )
                    if bid_earliest is None or ask_earliest is None:
                        print(f"❌ No bid/ask data available for {contract.localSymbol}")
                        return None
                    cache_entry.setdefault("bid", {})["earliest"] = _dt_to_iso(bid_earliest)
                    cache_entry.setdefault("ask", {})["earliest"] = _dt_to_iso(ask_earliest)
                    cache_entry.setdefault("bid", {})["last_checked"] = _dt_to_iso(end_date)
                    cache_entry.setdefault("ask", {})["last_checked"] = _dt_to_iso(end_date)
                    if bidask_cache:
                        bidask_cache.save()
                    min_required_date = max(bid_earliest, ask_earliest)
                    print(f"Earliest bid/ask boundary for {contract.localSymbol}: {min_required_date}")
            elif not bid_ask and trades_cache:
                trades_cache_entry = trades_cache.get_entry(_contract_cache_key(contract))

            min_end_date = min_required_date + timedelta(days=duration_days)
            
            # Continue fetching data until we have collected all available history or reached max lookback
            while lookback_count < max_lookback:
                if _time_budget_exceeded(contract_start_ts, budget_sec):
                    print(f"⏱️ Time budget exceeded for {contract.localSymbol}; stopping.")
                    break
                # Check connection before each request
                if not ib.isConnected():
                    print("Connection lost. Breaking out of data retrieval loop.")
                    break

                if end_date < min_end_date:
                    print("Reached earliest allowed date; stopping backfill.")
                    break
                    
                lookback_count += 1
                
                # Format the end date for this request
                # Use explicit UTC timezone to avoid IBKR warnings
                # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
                end_date_str = _format_ib_end_datetime(end_date)
                window_start = end_date - timedelta(days=duration_days)
                
                # Duration already set above based on override or standard logic
                
                print(f"Request {lookback_count}/{max_lookback}: endDateTime={end_date_str}, duration={duration_str}")
                
                try:
                    # Fetch data for this time period
                    if bid_ask:
                        if cache_entry and _window_in_no_data_cache(cache_entry, window_start, end_date):
                            print("Skipping cached no-data window.")
                            end_date = end_date - timedelta(days=duration_days)
                            continue
                        print("Requesting historical BID and ASK bars separately (futures)")
                        side_gap = 1.5 * wait_multiplier
                        status, bars_bid, bars_ask = _fetch_bidask_window(
                            ib,
                            contract,
                            end_date_str,
                            duration_str,
                            timeout_multiplier,
                            wait_multiplier,
                            side_gap,
                            deadline_ts=deadline_ts,
                        )
                        if status == "budget_exceeded":
                            print(f"⏱️ Time budget exceeded for {contract.localSymbol}; stopping.")
                            break
                        if status == "empty":
                            consecutive_empty_periods += 1
                            consecutive_partial_periods = 0
                            if cache_entry is not None:
                                _cache_add_no_data_window(
                                    cache_entry,
                                    window_start,
                                    end_date,
                                    ttl_days=BIDASK_NO_DATA_TTL_DAYS,
                                )
                                if bidask_cache:
                                    bidask_cache.save()
                            print(
                                f"Empty window after retries. Consecutive empty periods: "
                                f"{consecutive_empty_periods}/3"
                            )
                            if consecutive_empty_periods >= 3:
                                print("Stopping: 3 consecutive periods with no data after retries")
                                break
                            end_date = end_date - timedelta(days=duration_days)
                            continue
                        if status == "partial":
                            consecutive_partial_periods += 1
                            consecutive_empty_periods = 0
                            if cache_entry is not None:
                                _cache_add_no_data_window(
                                    cache_entry,
                                    window_start,
                                    end_date,
                                    ttl_days=BIDASK_NO_DATA_TTL_DAYS,
                                )
                                if bidask_cache:
                                    bidask_cache.save()
                            print(
                                f"Partial window after retries. Consecutive partial periods: "
                                f"{consecutive_partial_periods}/3"
                            )
                            if consecutive_partial_periods >= 3:
                                print("Stopping: too many partial bid/ask windows")
                                break
                            end_date = end_date - timedelta(days=duration_days)
                            continue
                    else:
                        if trades_cache_entry and _window_in_no_data_cache(trades_cache_entry, window_start, end_date):
                            print("Skipping cached no-data window.")
                            end_date = end_date - timedelta(days=duration_days)
                            continue
                        request_meta: dict = {}
                        bars = fetch_historical_data_with_retry(
                            ib,
                            contract,
                            end_date_str,
                            duration_str,
                            fallback_options,
                            bid_ask,
                            retry_sleep=10.0 * wait_multiplier,
                            timeout_multiplier=timeout_multiplier,
                            meta=request_meta,
                            deadline_ts=deadline_ts,
                        )
                        
                        if not bars or len(bars) == 0:
                            error_kind = request_meta.get("last_error_kind")
                            if error_kind == "budget_exceeded":
                                print(f"⏱️ Time budget exceeded for {contract.localSymbol}; stopping.")
                                break
                            empty_response_retries += 1
                            print(f"Empty response (retry {empty_response_retries}/3 for period {consecutive_empty_periods + 1})")
                            
                            if empty_response_retries < 3:
                                wait_time = _compute_backoff(
                                    10.0 * wait_multiplier,
                                    empty_response_retries - 1,
                                    cap=120.0 * wait_multiplier,
                                )
                                print(f"Retrying same period after {wait_time:.1f} seconds...")
                                time.sleep(wait_time)
                                lookback_count -= 1  # Decrement to retry the same period
                                continue
                            else:
                                # After 3 retries, mark this period as empty and move on
                                consecutive_empty_periods += 1
                                empty_response_retries = 0  # Reset retry counter for next period
                                if trades_cache_entry is not None:
                                    ttl_days = None
                                    if error_kind in {"hmds_no_data", "timeout", "budget_exceeded"}:
                                        ttl_days = TRADES_NO_DATA_TTL_DAYS
                                    _cache_add_no_data_window(
                                        trades_cache_entry,
                                        window_start,
                                        end_date,
                                        ttl_days=ttl_days,
                                    )
                                    if trades_cache:
                                        trades_cache.save()
                                print(f"Period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/3")
                                
                                if consecutive_empty_periods >= 3:
                                    print(f"Stopping: 3 consecutive periods with no data after retries")
                                    break
                                else:
                                    print(f"Moving to next period despite empty data...")
                                    # Move the end date back and continue
                                    days_to_subtract = duration_days
                                    end_date = end_date - timedelta(days=days_to_subtract)
                                    continue
                    
                    # Successfully got data - reset consecutive empty counter
                    consecutive_empty_periods = 0
                    consecutive_partial_periods = 0
                    empty_response_retries = 0
                    
                    # Add these bars to our collection
                    if bid_ask:
                        if bars_bid:
                            all_bars_bid.extend(bars_bid)
                        if bars_ask:
                            all_bars_ask.extend(bars_ask)
                        count_bid = len(all_bars_bid) if all_bars_bid is not None else 0
                        count_ask = len(all_bars_ask) if all_bars_ask is not None else 0
                        print(f"Total BID bars: {count_bid}, ASK bars: {count_ask}")
                    else:
                        all_bars.extend(bars)
                        print(f"Total bars collected so far: {len(all_bars)}")
                    
                    # Move the end date back by the duration for the next request
                    # No overlap to minimize duplicates
                    days_to_subtract = duration_days  # 7 days (no overlap)
                    end_date = end_date - timedelta(days=days_to_subtract)
                    
                    # Pause between requests to respect rate limits
                    if lookback_count < max_lookback:
                        wait_time = 2.0 * wait_multiplier
                        print(f"Waiting {wait_time:.1f} seconds before next request...")
                        time.sleep(wait_time)
                    
                    # If we got fewer bars than expected, we may have reached the beginning of data
                    # For 10-day chunks with 1-minute data, expect around 10*24*60 = 14,400 bars at maximum
                    expected_bars = duration_days * 24 * 60  # days of minute data
                    # Determine current batch size for heuristic
                    current_batch_len = 0
                    if bid_ask:
                        current_batch_len = (len(bars_bid) if bars_bid else 0) + (len(bars_ask) if bars_ask else 0)
                    else:
                        current_batch_len = len(bars)
                    if current_batch_len < expected_bars * 0.5:  # Less than half of expected minute data
                        print(f"Received fewer bars than expected ({current_batch_len} < {expected_bars*0.5}), may be near beginning of data")
                        # We could break here, but let's try one more request to be sure
                        if current_batch_len < 200:  # Very few bars, probably near the beginning
                            print("Very few bars received, likely reached beginning of data")
                            # Let's do one final request with a longer duration to catch any remaining data
                            if lookback_count < max_lookback:
                                lookback_count += 1
                                # Use explicit UTC timezone to avoid IBKR warnings
                                end_date_str = _format_ib_end_datetime(end_date)
                                # Double the duration for the final sweep (14 days for standard, 10 for ZAR)
                                duration_str = f"{duration_days * 2} D"
                                print(f"Final sweep request {lookback_count}/{max_lookback}: endDateTime={end_date_str}, duration={duration_str}")
                                
                                # Pause before the final request
                                wait_time = 3.0 * wait_multiplier
                                print(f"Waiting {wait_time:.1f} seconds before final request...")
                                time.sleep(wait_time)
                                
                                # Check connection again before final sweep
                                if not ib.isConnected():
                                    print("Connection lost before final sweep. Breaking out of data retrieval.")
                                    break
                                
                                # Make the final request
                                if bid_ask:
                                    final_bid = fetch_historical_data_with_retry(
                                        ib, contract, end_date_str, duration_str,
                                        fallback_options=None, bid_ask=False, what_to_show_override=['BID'],
                                        deadline_ts=deadline_ts,
                                    )
                                    time.sleep(1.5 * wait_multiplier)
                                    final_ask = fetch_historical_data_with_retry(
                                        ib, contract, end_date_str, duration_str,
                                        fallback_options=None, bid_ask=False, what_to_show_override=['ASK'],
                                        deadline_ts=deadline_ts,
                                    )
                                    if final_bid:
                                        all_bars_bid.extend(final_bid)
                                    if final_ask:
                                        all_bars_ask.extend(final_ask)
                                    total = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                                    print(f"Added final sweep; Total BID+ASK bars collected: {total}")
                                else:
                                    final_meta: dict = {}
                                    final_bars = fetch_historical_data_with_retry(
                                        ib,
                                        contract,
                                        end_date_str,
                                        duration_str,
                                        fallback_options,
                                        bid_ask,
                                        retry_sleep=10.0 * wait_multiplier,
                                        timeout_multiplier=timeout_multiplier,
                                        meta=final_meta,
                                        deadline_ts=deadline_ts,
                                    )
                                    if final_bars and len(final_bars) > 0:
                                        all_bars.extend(final_bars)
                                        print(f"Added {len(final_bars)} additional bars from final sweep")
                                        print(f"Total bars collected: {len(all_bars)}")
                            break
                except Exception as e:
                    error_str = str(e)
                    if "Not connected" in error_str:
                        print(f"❌ Connection lost during data retrieval: {e}")
                        # Don't break out of the while loop yet, let the outer loop handle reconnection
                        break
                    else:
                        print(f"❌ Error during data retrieval: {e}")
                        logger.error(f"Error during data retrieval: {e}")
                        # For non-connection errors, continue with next lookback period
                        continue
            
            # If we lost connection, go back to the reconnection loop
            if not ib.isConnected():
                continue
            
            # V3: Add walk-backward logic to fetch older data
            completed_initial_lookback = lookback_count >= max_lookback
            # walk_backward is now a parameter passed to this function
            walk_backward_consecutive_empty_periods = 0  # Track consecutive periods with no data
            walk_backward_empty_retries = 0  # Track retries for current empty period
            
            # Disable walk-backward in update mode
            have_any_data = False
            if bid_ask:
                have_any_data = ((all_bars_bid and len(all_bars_bid) > 0) or (all_bars_ask and len(all_bars_ask) > 0))
            else:
                have_any_data = len(all_bars) > 0
            if completed_initial_lookback and walk_backward and not update_mode and have_any_data:
                print("\n🔄 V3: Starting walk-backward phase to fetch older data...")
                logger.info("Starting walk-backward phase")
                
                walk_stop_date = min_required_date
                walk_backward_consecutive_partial_periods = 0
                while end_date > walk_stop_date:
                    if not ib.isConnected():
                        print("Connection lost during walk-backward.")
                        break

                    end_date = end_date - timedelta(days=duration_days)
                    if end_date <= walk_stop_date:
                        print(f"Reached hard stop date ({walk_stop_date.strftime('%Y-%m-%d')})")
                        break

                    end_date_str = _format_ib_end_datetime(end_date)
                    print(f"Walk-backward request: endDateTime={end_date_str}, duration={duration_str}")

                    try:
                        if bid_ask:
                            side_gap = 1.5 * wait_multiplier
                            status, bars_bid, bars_ask = _fetch_bidask_window(
                                ib,
                                contract,
                                end_date_str,
                                duration_str,
                                timeout_multiplier,
                                wait_multiplier,
                                side_gap,
                                deadline_ts=deadline_ts,
                            )
                            if status == "budget_exceeded":
                                print(f"⏱️ Time budget exceeded for {contract.localSymbol}; stopping.")
                                break
                            if status == "empty":
                                walk_backward_consecutive_empty_periods += 1
                                walk_backward_consecutive_partial_periods = 0
                                if cache_entry is not None:
                                    window_start = end_date - timedelta(days=duration_days)
                                    _cache_add_no_data_window(
                                        cache_entry,
                                        window_start,
                                        end_date,
                                        ttl_days=BIDASK_NO_DATA_TTL_DAYS,
                                    )
                                    if bidask_cache:
                                        bidask_cache.save()
                                print(
                                    "Walk-backward empty window. "
                                    f"Consecutive empty periods: {walk_backward_consecutive_empty_periods}/3"
                                )
                                if walk_backward_consecutive_empty_periods >= 3:
                                    print("Stopping walk-backward: 3 consecutive periods with no data after retries")
                                    break
                                continue
                            if status == "partial":
                                walk_backward_consecutive_partial_periods += 1
                                walk_backward_consecutive_empty_periods = 0
                                print(
                                    "Walk-backward partial window. "
                                    f"Consecutive partial periods: {walk_backward_consecutive_partial_periods}/3"
                                )
                                if walk_backward_consecutive_partial_periods >= 3:
                                    print("Stopping walk-backward: too many partial bid/ask windows")
                                    break
                                continue
                        else:
                            window_start = end_date - timedelta(days=duration_days)
                            if trades_cache_entry and _window_in_no_data_cache(trades_cache_entry, window_start, end_date):
                                print("Skipping cached no-data window (walk-backward).")
                                continue
                            walk_meta: dict = {}
                            bars = fetch_historical_data_with_retry(
                                ib,
                                contract,
                                end_date_str,
                                duration_str,
                                fallback_options,
                                bid_ask,
                                retry_sleep=10.0 * wait_multiplier,
                                timeout_multiplier=timeout_multiplier,
                                meta=walk_meta,
                                deadline_ts=deadline_ts,
                            )

                            if not bars or len(bars) == 0:
                                error_kind = walk_meta.get("last_error_kind")
                                if error_kind == "budget_exceeded":
                                    print(f"⏱️ Time budget exceeded for {contract.localSymbol}; stopping.")
                                    break
                                walk_backward_empty_retries += 1
                                print(
                                    f"Empty response during walk-backward (retry {walk_backward_empty_retries}/3 "
                                    f"for period {walk_backward_consecutive_empty_periods + 1})"
                                )

                                if walk_backward_empty_retries < 3:
                                    wait_time = _compute_backoff(
                                        10.0 * wait_multiplier,
                                        walk_backward_empty_retries - 1,
                                        cap=120.0 * wait_multiplier,
                                    )
                                    print(f"Retrying same walk-backward period after {wait_time:.1f} seconds...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    walk_backward_consecutive_empty_periods += 1
                                    walk_backward_empty_retries = 0
                                    if trades_cache_entry is not None:
                                        ttl_days = None
                                        if error_kind in {"hmds_no_data", "timeout", "budget_exceeded"}:
                                            ttl_days = TRADES_NO_DATA_TTL_DAYS
                                        _cache_add_no_data_window(
                                            trades_cache_entry,
                                            window_start,
                                            end_date,
                                            ttl_days=ttl_days,
                                        )
                                        if trades_cache:
                                            trades_cache.save()
                                    print(
                                        "Walk-backward period failed after 3 retries. "
                                        f"Consecutive empty periods: {walk_backward_consecutive_empty_periods}/3"
                                    )

                                    if walk_backward_consecutive_empty_periods >= 3:
                                        print("Stopping walk-backward: 3 consecutive periods with no data after retries")
                                        break
                                    else:
                                        print("Moving to next walk-backward period despite empty data...")
                                        continue

                        # Successfully got data - reset counters
                        walk_backward_consecutive_empty_periods = 0
                        walk_backward_consecutive_partial_periods = 0
                        walk_backward_empty_retries = 0

                        # Add these bars to our collection
                        if bid_ask:
                            if bars_bid:
                                all_bars_bid.extend(bars_bid)
                            if bars_ask:
                                all_bars_ask.extend(bars_ask)
                            total = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                            print(f"Walk-backward: Added BID/ASK bars. Total collected (BID+ASK): {total}")
                        else:
                            all_bars.extend(bars)
                            print(f"Walk-backward: Added {len(bars)} bars. Total: {len(all_bars)}")

                        # Check if we've gone far enough back
                        if bid_ask:
                            dates = []
                            if bars_bid:
                                dates.extend([bar.date for bar in bars_bid])
                            if bars_ask:
                                dates.extend([bar.date for bar in bars_ask])
                            earliest_date = min(dates) if dates else None
                        else:
                            earliest_date = min(bar.date for bar in bars)
                        earliest_date = _to_naive(earliest_date)

                        if earliest_date is not None and earliest_date <= walk_stop_date:
                            print(f"Reached data from {earliest_date}, stopping walk-backward")
                            break

                        # Add a pause between successful walk-backward chunks
                        wait_time = 2.0 * wait_multiplier
                        print(f"Waiting {wait_time:.1f} seconds before next walk-backward request...")
                        time.sleep(wait_time)

                    except Exception as e:
                        error_str = str(e)
                        if "Not connected" in error_str or not ib.isConnected():
                            print(f"❌ Connection lost during walk-backward: {e}")
                            break
                        else:
                            if "can't compare offset-naive and offset-aware" in str(e):
                                print(f"⚠️ Timezone comparison issue (continuing): {e}")
                            else:
                                print(f"❌ Error during walk-backward: {e}")
                                print("Too many errors during walk-backward, stopping")
                                break
                
                if bid_ask:
                    total = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                    print(f"Walk-backward complete. Total BID+ASK bars collected: {total}")
                else:
                    print(f"Walk-backward complete. Total bars collected: {len(all_bars)}")
            
            # Check if we got any data
            if bid_ask:
                total_collected = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                if total_collected == 0:
                    print(f"❌ No historical bid/ask data retrieved for {contract.localSymbol}")
                    return None
                print(f"✅ Retrieved total of {total_collected} BID/ASK bars (combined counts)")
                
                # Build DataFrames for BID and ASK and merge
                df_bid = util.df(all_bars_bid) if all_bars_bid else pd.DataFrame(columns=['date'])
                df_ask = util.df(all_bars_ask) if all_bars_ask else pd.DataFrame(columns=['date'])
                
                # Drop duplicates and sort
                if not df_bid.empty:
                    df_bid = df_bid.drop_duplicates(subset=['date']).sort_values('date')
                if not df_ask.empty:
                    df_ask = df_ask.drop_duplicates(subset=['date']).sort_values('date')
                
                # Rename columns
                rename_map = {
                    'open': 'bid_open', 'high': 'bid_high', 'low': 'bid_low', 'close': 'bid_close', 'volume': 'bid_volume'
                }
                df_bid = df_bid.rename(columns=rename_map)
                rename_map_ask = {
                    'open': 'ask_open', 'high': 'ask_high', 'low': 'ask_low', 'close': 'ask_close', 'volume': 'ask_volume'
                }
                df_ask = df_ask.rename(columns=rename_map_ask)
                
                # Merge on timestamp
                df = pd.merge(df_bid, df_ask, on='date', how='outer').sort_values('date').reset_index(drop=True)
                
                # Add metadata columns
                df['symbol'] = ticker
                df['local_symbol'] = contract.localSymbol
                df['expiry'] = contract.lastTradeDateOrContractMonth
                df['exchange'] = contract.exchange
                df['datetime'] = df['date']
                # Enforce both-sides-only rows to avoid single-sided periods
                before_len = len(df)
                if 'bid_close' in df.columns and 'ask_close' in df.columns:
                    df = df.dropna(subset=['bid_close', 'ask_close'])
                    dropped = before_len - len(df)
                    if dropped > 0:
                        print(f"Dropped {dropped} single-sided rows to enforce both-sides-only output.")
                
                # Save the data to CSV
                print(f"📝 Saving {len(df)} rows of 1-minute BID/ASK data for {contract.localSymbol}")
                output_dir = Path(OUTPUT_DIR) / ticker
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / f"{contract.localSymbol}.csv"
                df.to_csv(str(output_file), index=False)
                print(f"✅ Saved {len(df)} rows for {contract.localSymbol} to {output_file}")
                
                # Display the first 5 rows of data with prominent formatting
                print("\n" + "=" * 80)
                print(f"📊 FIRST 5 ROWS OF BID/ASK DATA FOR {contract.localSymbol}".center(80))
                print("=" * 80)
                print(df.head().to_string())
                print("=" * 80)
                
                # Display the date range of the data
                if len(df) > 0:
                    start_date = df['datetime'].min()
                    end_date = df['datetime'].max()
                    print(f"\nData range: {start_date} to {end_date}")
                    print(f"Total days covered: {(end_date - start_date).days + 1}")
                
                return df
            else:
                if not all_bars or len(all_bars) == 0:
                    print(f"❌ No historical data retrieved for {contract.localSymbol}")
                    return None
                
                print(f"✅ Retrieved total of {len(all_bars)} bars of historical data")
                
                # Create a DataFrame from all the bars
                df = util.df(all_bars)
                
                # Remove any duplicate entries that might occur due to overlapping requests
                initial_rows = len(df)
                df = df.drop_duplicates(subset=['date'])
                if len(df) < initial_rows:
                    print(f"Removed {initial_rows - len(df)} duplicate entries")
                
                # Sort the data chronologically
                df = df.sort_values('date')
                
                # Add contract information to the DataFrame
                df['symbol'] = ticker
                df['local_symbol'] = contract.localSymbol
                df['expiry'] = contract.lastTradeDateOrContractMonth
                df['exchange'] = contract.exchange
                # With formatDate=2, 'date' column already contains timezone-aware timestamps
                # Create a duplicate 'datetime' column for consistency
                df['datetime'] = df['date']
                
                # Save the data to CSV
                print(f"📝 Saving {len(df)} rows of 1-minute data for {contract.localSymbol}")
                
                # Create the output directory if needed
                output_dir = Path(OUTPUT_DIR) / ticker
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Format the output filename based on the contract's standardized localSymbol
                output_file = output_dir / f"{contract.localSymbol}.csv"
                
                # Save to CSV
                df.to_csv(str(output_file), index=False)
                print(f"✅ Saved {len(df)} rows for {contract.localSymbol} to {output_file}")
                
                # Display the first 5 rows of data with prominent formatting
                print("\n" + "=" * 80)
                print(f"📊 FIRST 5 ROWS OF DATA FOR {contract.localSymbol}".center(80))
                print("=" * 80)
                print(df.head().to_string())
                print("=" * 80)
                
                # Display the date range of the data
                if len(df) > 0:
                    start_date = df['datetime'].min()
                    end_date = df['datetime'].max()
                    print(f"\nData range: {start_date} to {end_date}")
                    print(f"Total days covered: {(end_date - start_date).days + 1}")
                
                # If we got here, we've successfully processed this contract
                return df
            
        except Exception as e:
            # Check if this is a connection error
            error_str = str(e)
            if "Not connected" in error_str or not ib.isConnected():
                print(f"❌ Connection error detected: {e}")
                # Let the while loop handle reconnection
                continue
            else:
                print(f"❌ Error getting historical data: {e}")
                logger.error(f"Error getting historical data: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return None
    
    # If we've exhausted all reconnection attempts and the user declined to retry
    print(f"❌ Failed to process {contract.localSymbol} after multiple reconnection attempts")
    return None

def validate_currency_future(contract, ticker):
    """
    Validate and fix a currency future contract.
    
    Args:
        contract: The contract object to validate
        ticker: The ticker symbol (e.g., 'GBP', 'CHF')
        
    Returns:
        Contract: The validated contract
    """
    # Currency trading class mapping
    currency_trading_class = {
        'GBP': {'class': '6B', 'multiplier': '62500'},
        'CHF': {'class': '6S', 'multiplier': '125000'},
        'CAD': {'class': '6C', 'multiplier': '100000'},
        'JPY': {'class': '6J', 'multiplier': '12500000'},
        'AUD': {'class': '6A', 'multiplier': '100000'},
        'NZD': {'class': '6N', 'multiplier': '100000'},
        'MXP': {'class': '6M', 'multiplier': '500000'},
        'ZAR': {'class': '6Z', 'multiplier': '500000'},
        'NOK': {'class': 'NOK', 'multiplier': '2000000'},
        'SEK': {'class': 'SEK', 'multiplier': '2000000'},
        'EUR': {'class': '6E', 'multiplier': '125000'}
    }
    
    # If ticker is a known currency future
    if ticker in currency_trading_class:
        print(f"Validating {ticker} currency future contract...")
        
        # Always force USD currency for these futures
        contract.currency = 'USD'
        print(f"  Setting currency to USD")
        
        # Set the correct trading class
        trading_class = currency_trading_class[ticker]['class']
        contract.tradingClass = trading_class
        print(f"  Setting trading class to {trading_class}")
        
        # Set the correct multiplier
        multiplier = currency_trading_class[ticker]['multiplier']
        contract.multiplier = multiplier
        print(f"  Setting multiplier to {multiplier}")
        
        # Ensure symbol is correct (GBP not B6)
        contract.symbol = ticker
        print(f"  Setting symbol to {ticker}")
        
        # If we have a lastTradeDateOrContractMonth and local symbol is missing or incorrect
        if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
            # Extract month and year to make local symbol
            if len(contract.lastTradeDateOrContractMonth) >= 6:
                # Get the year and month from the contract month string
                year = contract.lastTradeDateOrContractMonth[0:4]  # Full year (e.g., 2025)
                month = int(contract.lastTradeDateOrContractMonth[4:6])  # Month as number (e.g., 3)
                
                # Convert month to code (H=3, M=6, U=9, Z=12)
                month_codes = {3: 'H', 6: 'M', 9: 'U', 12: 'Z'}
                if month in month_codes:
                    month_code = month_codes[month]
                    # Get last digit of year
                    year_digit = str(year)[-1]
                    
                    # Construct local symbol (e.g., 6BM5 for GBP June 2025)
                    local_symbol = f"{trading_class}{month_code}{year_digit}"
                    
                    # Only update if different from current
                    if not hasattr(contract, 'localSymbol') or contract.localSymbol != local_symbol:
                        contract.localSymbol = local_symbol
                        print(f"  Corrected local symbol to {local_symbol}")
    
    # Always force includeExpired for futures
    contract.includeExpired = True
    
    return contract

def save_data(df, ticker, contract_symbol):
    """
    Save historical data for a single futures contract to a CSV file.
    Creates a subdirectory for each ticker and saves individual contract data.
    
    Args:
        df: DataFrame containing historical data
        ticker: The ticker symbol (e.g., ZT)
        contract_symbol: The contract symbol (e.g., ZTM4)
    
    Returns:
        str: Path to the saved file
    """
    if df is None or df.empty:
        logger.warning(f"No data to save for {ticker}/{contract_symbol}")
        return None
    
    # Ensure data is properly sorted by datetime
    df = df.sort_values('datetime')
    
    # Remove any duplicate timestamps
    initial_count = len(df)
    df = df.drop_duplicates(subset=['datetime'])
    if len(df) < initial_count:
        logger.info(f"Removed {initial_count - len(df)} duplicate timestamps for {ticker}/{contract_symbol}")
    
    # Ensure the data is in chronological order
    df = df.reset_index(drop=True)
    
    # Create ticker subdirectory
    ticker_dir = OUTPUT_DIR / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    
    # Create output filename based on the localSymbol
    output_file = ticker_dir / f"{contract_symbol}.csv"
    
    # Save to CSV
    df.to_csv(str(output_file), index=False)
    
    print(f"✅ Saved {len(df)} bars for {ticker}/{contract_symbol} to {output_file}")
    logger.info(f"Saved {len(df)} bars for {ticker}/{contract_symbol} to {output_file}, time range: {df['datetime'].min()} to {df['datetime'].max()}")
    
    return output_file 

def load_securities(csv_file='securities_daily_update.csv'):
    """
    Load securities from the CSV file.
    
    Args:
        csv_file: Path to the CSV file
    
    Returns:
        pd.DataFrame: DataFrame containing securities information
    """
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} securities from {csv_file}")
        return df
    except Exception as e:
        logger.error(f"Error loading securities from {csv_file}: {e}")
        print(f"❌ Error loading securities from {csv_file}: {e}")
        return None

def calculate_update_duration(days_to_update):
    """
    Calculate the appropriate duration string for IBKR API based on days to update.
    
    Args:
        days_to_update: Number of days from last data to now
        
    Returns:
        str: Duration string for IBKR API (e.g., '7 D', '1 M', '1 Y')
    """
    if days_to_update <= 1:
        return '1 D'
    elif days_to_update <= 7:
        return f'{days_to_update} D'
    elif days_to_update <= 30:
        return '1 M'
    elif days_to_update <= 90:
        return '3 M'
    elif days_to_update <= 180:
        return '6 M'
    elif days_to_update <= 365:
        return '1 Y'
    else:
        # For anything more than 1 year, use year-based duration
        years = min(5, (days_to_update // 365) + 1)
        return f'{years} Y'

def expected_last_trading_date(now_utc, expiry_dt: datetime | None = None):
    day = now_utc.date()
    weekday = day.weekday()
    if weekday == 5:
        expected = day - timedelta(days=1)
    elif weekday == 6:
        expected = day - timedelta(days=2)
    else:
        expected = day
    if expiry_dt is not None:
        expiry_date = expiry_dt.date()
        if expiry_date < expected:
            expected = expiry_date
    return expected

def warn_if_stale(symbol, latest_dt, expected_date):
    if latest_dt is None or pd.isna(latest_dt):
        return
    latest_date = latest_dt.date()
    if latest_date < expected_date:
        if (expected_date - latest_date).days <= 1:
            return
        msg = (
            f"⚠️ {symbol} latest bar {latest_date} behind expected "
            f"{expected_date} (weekday check)"
        )
        print(msg)
        logger.warning(msg)

def update_contract_data(new_df, ticker, contract_symbol, expiry_dt: datetime | None = None):
    """
    Update an existing futures contract CSV file with new data.
    
    Args:
        new_df: DataFrame containing new historical data
        ticker: The futures ticker (e.g., 'ZT')
        contract_symbol: The contract symbol (e.g., 'ZTH5')
    
    Returns:
        str: Path to the updated file, or None if no update was needed
    """
    if new_df is None or new_df.empty:
        logger.warning(f"No new data to update for {contract_symbol}")
        return None
    
    output_file = Path(OUTPUT_DIR) / ticker / f"{contract_symbol}.csv"
    
    if not output_file.exists():
        # In update mode for futures, we create the file if it doesn't exist (new contract)
        print(f"Creating new file for {contract_symbol}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(str(output_file), index=False)
        print(f"✅ Created new file with {len(new_df)} rows for {contract_symbol}")
        return str(output_file)
    
    try:
        # Read existing data
        existing_df = pd.read_csv(str(output_file))
        
        # Ensure datetime column is present
        if 'datetime' in existing_df.columns:
            existing_df['datetime'] = pd.to_datetime(existing_df['datetime'])
            date_col = 'datetime'
        elif 'date' in existing_df.columns:
            existing_df['date'] = pd.to_datetime(existing_df['date'])
            date_col = 'date'
        else:
            print(f"❌ No date column found in existing file for {contract_symbol}")
            return None
        
        # Get the latest date in existing data
        latest_date = existing_df[date_col].max()
        
        # Ensure new data has same date column
        if date_col in new_df.columns:
            new_df[date_col] = pd.to_datetime(new_df[date_col])
        else:
            print(f"❌ Date column mismatch for {contract_symbol}")
            return None
        
        # Filter new data to only include rows after the latest date
        new_data = new_df[new_df[date_col] > latest_date]

        if new_data.empty:
            expected_date = expected_last_trading_date(datetime.now(timezone.utc), expiry_dt=expiry_dt)
            warn_if_stale(contract_symbol, latest_date, expected_date)
            print(f"✅ No new data available for {contract_symbol}. File is already up to date.")
            return None
        
        # Combine existing and new data
        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        
        # Sort by date and remove duplicates
        combined_df = combined_df.sort_values(date_col)
        combined_df = combined_df.drop_duplicates(subset=[date_col])
        
        # Save updated file
        combined_df.to_csv(str(output_file), index=False)
        
        print(f"✅ Updated {contract_symbol} with {len(new_data)} new bars")
        print(f"   Date range: {new_data[date_col].min()} to {new_data[date_col].max()}")
        logger.info(f"Updated {contract_symbol} with {len(new_data)} new bars")
        
        return str(output_file)
    
    except Exception as e:
        print(f"❌ Error updating data for {contract_symbol}: {e}")
        logger.error(f"Error updating data for {contract_symbol}: {e}")
        return None

def scan_existing_contracts(ticker):
    """
    Scan the output directory for existing contract files for a given ticker.
    
    Args:
        ticker: The futures ticker (e.g., 'ZT')
    
    Returns:
        set: Set of contract symbols found (e.g., {'ZTH5', 'ZTM5', ...})
    """
    output_dir = Path(OUTPUT_DIR) / ticker
    if not output_dir.exists():
        return set()
    
    existing_contracts = set()
    for csv_file in output_dir.glob("*.csv"):
        # Extract contract symbol from filename (without .csv extension)
        contract_symbol = csv_file.stem
        existing_contracts.add(contract_symbol)
    
    return existing_contracts

def check_for_new_contracts(active_contracts, existing_contracts):
    """
    Identify new contracts that need to be fetched.
    
    Args:
        active_contracts: List of active Contract objects from IBKR
        existing_contracts: Set of contract symbols already on disk
    
    Returns:
        list: List of Contract objects that are new (not in existing_contracts)
    """
    new_contracts = []
    
    for contract in active_contracts:
        # Standardize the contract symbol first
        standardize_contract_symbol(contract)
        
        if contract.localSymbol not in existing_contracts:
            new_contracts.append(contract)
            print(f"  📌 New contract found: {contract.localSymbol}")
    
    return new_contracts

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Retrieve historical data for individual futures contracts.')
    
    parser.add_argument('--conid', type=int, default=None,
                        help='ConId of a specific futures security to process')
    
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='IBKR TWS hostname (default: 127.0.0.1)')
    
    parser.add_argument('--port', type=int, default=7497,
                        help='IBKR TWS port (default: 7497)')
    
    parser.add_argument('--client-id', type=int, default=22,
                        help='Client ID for IBKR connection (default: 22)')
    
    parser.add_argument('--input-file', type=str, default='securities_daily_update.csv',
                        help='Path to the securities CSV file (default: securities_daily_update.csv)')
    
    parser.add_argument('--duration', type=str, default='1 Y',
                        help='Duration of historical data (default: 1 Y)')
    
    parser.add_argument('--bar-size', type=str, default='1 min',
                        help='Size of bars (default: 1 min)')
    
    parser.add_argument('--ticker', type=str, nargs='+', default=None,
                        help='Process one or more tickers (e.g., ZT or ZT ZN ES)')
    
    parser.add_argument('--years-back', type=int, default=5,
                        help='Number of years to look back for expired futures contracts (default: 5)')

    parser.add_argument('--max-forward-days', type=int, default=0,
                        help='Limit active contracts to expiring within N days (0 disables)')
    parser.add_argument('--active-limit', type=int, default=0,
                        help='Limit number of active contracts processed (0 disables)')
    parser.add_argument('--max-lookback', type=int, default=None,
                        help='Limit initial lookback chunks (default: 18; update mode forces 1)')
    parser.add_argument('--skip-new-contracts', action='store_true',
                        help='Skip fetching full history for newly discovered contracts in update mode')
    parser.add_argument('--update-duration', type=str, default=None,
                        help='Override update duration for existing contracts (e.g., \"7 D\")')
    
    # V3: Add walk-backward options
    parser.add_argument('--walk-backward', action='store_true', default=True,
                        help='Walk backward to fetch older historical data after initial fetch (default: enabled)')
    parser.add_argument('--no-walk-backward', dest='walk_backward', action='store_false',
                        help='Disable walk-backward phase')
    
    # Add fallback options for data types
    parser.add_argument('--fallback', action='append', 
                        choices=['BID_ASK', 'BID', 'ASK', 'MIDPOINT', 'ADJUSTED_LAST'],
                        help='Add fallback data types if TRADES is not available. Can be specified multiple times. '
                             'Example: --fallback BID_ASK --fallback MIDPOINT')
    
    # Add bid-ask flag
    parser.add_argument('--bid-ask', action='store_true',
                        help='Fetch BID_ASK data instead of TRADES and save to futures_contracts_bidask folder')

    parser.add_argument(
        '--no-prompt',
        action='store_true',
        help='Disable interactive prompts; skip to next contract on aborts',
    )
    parser.add_argument(
        '--no-max-seconds',
        action='store_true',
        help='Disable per-contract max-seconds timeout guard',
    )
    
    # Mode selection - make these mutually exclusive and required
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--back-fill', action='store_true',
                        help='Back-fill mode: create new files or overwrite existing ones with full historical data')
    mode_group.add_argument('--update', action='store_true',
                        help='Update mode: update existing contracts and fetch new contracts if available')
    
    return parser.parse_args() 

def main():
    """Main entry point for the script."""
    # Parse command line arguments
    args = parse_args()

    global DISABLE_CONTRACT_TIME_BUDGET
    DISABLE_CONTRACT_TIME_BUDGET = args.no_max_seconds
    
    # Set the OUTPUT_DIR based on the --bid-ask flag
    global OUTPUT_DIR
    if args.bid_ask:
        OUTPUT_DIR = BRONZE_DIR_BIDASK
        print(f"Using BID_ASK data mode - saving to {OUTPUT_DIR}")
    else:
        OUTPUT_DIR = BRONZE_DIR
        print(f"Using TRADES data mode - saving to {OUTPUT_DIR}")

    if not args.bid_ask and not args.fallback:
        args.fallback = ["MIDPOINT"]

    bidask_cache = None
    trades_cache = None
    if args.bid_ask:
        cache_path = CACHE_DIR / f"bidask_availability_{args.client_id}.json"
        bidask_cache = BidAskCache(cache_path)
        bidask_cache.load()
    else:
        cache_path = CACHE_DIR / f"trades_no_data_{args.client_id}.json"
        trades_cache = TradesNoDataCache(cache_path)
        trades_cache.load()
    
    # Log mode of operation
    if args.update:
        print("\n" + "=" * 80)
        print("UPDATE MODE ACTIVATED")
        print("Will update existing contracts and fetch new contracts")
        if args.bid_ask:
            print("Fetching BID_ASK data")
        print("=" * 80 + "\n")
        logger.info("Script running in UPDATE mode")
    elif args.back_fill:
        print("\n" + "=" * 80)
        print("BACK-FILL MODE - Creating/Overwriting Contract Files")
        print(f"Walk-backward: {args.walk_backward}")
        if args.bid_ask:
            print("Fetching BID_ASK data")
        print("=" * 80 + "\n")
        logger.info(f"Script running in BACK-FILL mode")
    
    # Load securities from CSV
    securities = load_securities(args.input_file)
    if securities is None:
        return
    
    # Connect to IBKR with retry logic
    ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id, prompt_user=not args.no_prompt)
    if not ib:
        print("Failed to connect to IBKR")
        return

    conid_rows = []

    try:
        # If conid is specified, filter the securities
        if args.conid is not None:
            securities = securities[securities['IBKR_Conid'] == args.conid]
            if len(securities) == 0:
                print(f"❌ No security found with conid {args.conid}")
                return
                
        # If ticker(s) specified, filter by ticker(s)
        if args.ticker is not None:
            # args.ticker is now a list of tickers
            tickers = args.ticker
            print(f"\nFiltering for specified ticker(s): {', '.join(tickers)}")
            securities = securities[securities['FR_Ticker'].isin(tickers)]
            if len(securities) == 0:
                print(f"❌ No securities found for ticker(s): {', '.join(tickers)}")
                return
            # Check which tickers were found and which were not
            found_tickers = securities['FR_Ticker'].unique().tolist()
            not_found = [t for t in tickers if t not in found_tickers]
            if not_found:
                print(f"⚠️ Warning: The following ticker(s) were not found: {', '.join(not_found)}")
            print(f"✅ Found {len(securities)} securities for ticker(s): {', '.join(found_tickers)}")
        
        # Filter to futures securities only
        securities = securities[securities['SecurityType'] == 'futures']
        
        print(f"Processing {len(securities)} futures securities...")
        
        # Process each security one at a time
        for idx, security in securities.iterrows():
            retry_count = 0
            max_disconnection_retries = 3
            
            while retry_count <= max_disconnection_retries:
                try:
                    # Check if we're still connected to IBKR
                    if not ib.isConnected():
                        print("Lost connection to IBKR. Attempting to reconnect...")
                        ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id, prompt_user=not args.no_prompt)
                        if not ib:
                            print("Failed to reconnect to IBKR")
                            retry_count += 1
                            if retry_count <= max_disconnection_retries:
                                print(f"Retrying in 10 seconds (attempt {retry_count}/{max_disconnection_retries})...")
                                time.sleep(10)
                                continue
                            else:
                                if args.no_prompt:
                                    return
                                prompt_retry = input("Reached maximum reconnection attempts. Try again? (Y/N): ")
                                if prompt_retry.upper() == 'Y':
                                    retry_count = 0
                                    continue
                                return
                    
                    # We are connected, process this security
                    ticker = security['FR_Ticker'] if pd.notna(security['FR_Ticker']) else None
                    exchange = security['IBKR_exchange'] if pd.notna(security['IBKR_exchange']) else None
                    currency = security['ibkr_currency'] if pd.notna(security['ibkr_currency']) else 'USD'
                    conid = security['IBKR_Conid'] if pd.notna(security['IBKR_Conid']) else None
                    
                    if pd.isna(ticker) or ticker is None:
                        print(f"❌ Missing ticker for security ID {security.get('Security_ID', 'Unknown')}")
                        break

                    # Check if this symbol needs special handling - use the correct exchange from the start
                    if ticker in SPECIAL_DERIVATIVES_HANDLING:
                        special_info = SPECIAL_DERIVATIVES_HANDLING[ticker]
                        if 'exchange' in special_info:
                            # Use the exchange specified in the special handling configuration
                            original_exchange = exchange
                            exchange = special_info['exchange']
                            print(f"⚠️ Overriding exchange from {original_exchange} to {exchange} for {ticker} based on special handling")

                    if ticker in CURRENCY_TRADING_CLASS:
                        currency = 'USD'
                    
                    if pd.isna(exchange) or exchange is None:
                        print(f"❌ Missing exchange for {ticker}")
                        # If exchange is missing, try common exchanges for futures as fallbacks
                        fallback_exchanges = ['GLOBEX', 'CBOT', 'NYMEX', 'COMEX']
                        print(f"Will try fallback exchanges: {', '.join(fallback_exchanges)}")
                        
                        # Try with the first fallback
                        exchange = fallback_exchanges[0]
                        print(f"Using fallback exchange {exchange} for {ticker}")
                    
                    print(f"\n{'=' * 80}")
                    print(f"PROCESSING {ticker} FUTURES ON {exchange}".center(80))
                    print(f"{'=' * 80}")
                    
                    # Get active contracts for this security
                    print("\nRetrieving active futures contracts...")
                    active_contracts = get_active_futures_contracts(
                        ib,
                        ticker,
                        exchange,
                        currency,
                        max_forward_days=args.max_forward_days if args.max_forward_days else None,
                        local_symbol=None,
                        conid=conid,
                    )
                    
                    # Standardize active contract symbols
                    for contract in active_contracts:
                        standardize_contract_symbol(contract)

                    if args.active_limit and active_contracts:
                        active_contracts = active_contracts[: args.active_limit]
                        
                    # Display list of active contracts
                    if active_contracts:
                        print(f"\n✅ Found {len(active_contracts)} active contracts for {ticker}:")
                        for i, contract in enumerate(active_contracts):
                            print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                    else:
                        print(f"\n❌ No active contracts found for {ticker}")
                        print(f"Attempting to find expired contracts only...")
                    
                    # Get expired contracts for this security
                    print("\nRetrieving expired futures contracts...")
                    expired_contracts = get_expired_futures_contracts(
                        ib,
                        ticker,
                        exchange,
                        currency,
                        years_back=args.years_back,
                        local_symbol=None,
                        conid=conid,
                    )
                    
                    # Standardize expired contract symbols
                    for contract in expired_contracts:
                        standardize_contract_symbol(contract)
                    
                    # Display list of expired contracts
                    if expired_contracts:
                        print(f"\n✅ Found {len(expired_contracts)} expired contracts for {ticker}:")
                        for i, contract in enumerate(expired_contracts):
                            print(f"  {i+1}. {contract.localSymbol}: Expiry={contract.lastTradeDateOrContractMonth}")
                    else:
                        print(f"\n❌ No expired contracts found for {ticker}")
                    
                    if not expired_contracts and not active_contracts:
                        print(f"❌ No active or expired contracts found for {ticker}")
                        break
                    
                    # Combine active and expired contracts
                    all_contracts = []
                    if active_contracts:
                        all_contracts.extend(active_contracts)
                        print(f"Found {len(active_contracts)} active contracts")
                    
                    if expired_contracts:
                        all_contracts.extend(expired_contracts)
                        print(f"Found {len(expired_contracts)} expired contracts")
                    
                    print(f"Total of {len(all_contracts)} contracts to process")

                    active_keys = {_contract_key(c) for c in active_contracts} if active_contracts else set()
                    expired_keys = {_contract_key(c) for c in expired_contracts} if expired_contracts else set()
                    ticker_rows = []
                    first_successful_contract = None
                    mode_label = "update" if args.update else "back_fill"
                    
                    # Create the data directory structure
                    output_dir = Path(OUTPUT_DIR) / ticker
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Handle update mode differently
                    if args.update:
                        print("\n📋 Update Mode: Checking for new contracts and updates...")
                        
                        # Scan existing contracts
                        existing_contracts = scan_existing_contracts(ticker)
                        print(f"Found {len(existing_contracts)} existing contract files")
                        
                        # Check for new contracts
                        if args.skip_new_contracts:
                            new_contracts = []
                            print("\n⚠️ Skipping new contract fetch (skip_new_contracts enabled)")
                        else:
                            new_contracts = check_for_new_contracts(active_contracts, existing_contracts)
                            if new_contracts:
                                print(f"\n🆕 Found {len(new_contracts)} new contracts to fetch")
                        
                        # Process new contracts with update window (no full backfill in update mode)
                        for contract in new_contracts:
                            try:
                                standardize_contract_symbol(contract)
                                now_utc = datetime.now(timezone.utc)
                                expiry_dt = _parse_contract_expiry(contract)
                                if _is_expired_on_or_before(contract, now_utc):
                                    expiry_label = expiry_dt.date().isoformat() if expiry_dt else "unknown"
                                    print(f"Skipping expired contract {contract.localSymbol} (expiry {expiry_label})")
                                    fetch_status = "expired_skip"
                                    ticker_rows.append({
                                        "ticker": ticker,
                                        "exchange": exchange,
                                        "currency": currency,
                                        "mode": mode_label,
                                        "contract_local_symbol": contract.localSymbol,
                                        "contract_conid": getattr(contract, 'conId', None),
                                        "contract_expiry": _contract_expiry(contract),
                                        "contract_exchange": getattr(contract, 'exchange', None),
                                        "contract_trading_class": getattr(contract, 'tradingClass', None),
                                        "contract_source": _contract_source(contract, active_keys, expired_keys),
                                        "fetch_status": fetch_status,
                                    })
                                    continue
                                if _expiry_far_future(contract, now_utc):
                                    logger.debug(
                                        "Skipping %s; contract expiry is far in the future (likely not trading yet).",
                                        contract.localSymbol,
                                    )
                                    fetch_status = "skipped_far_future"
                                    ticker_rows.append({
                                        "ticker": ticker,
                                        "exchange": exchange,
                                        "currency": currency,
                                        "mode": mode_label,
                                        "contract_local_symbol": contract.localSymbol,
                                        "contract_conid": getattr(contract, 'conId', None),
                                        "contract_expiry": _contract_expiry(contract),
                                        "contract_exchange": getattr(contract, 'exchange', None),
                                        "contract_trading_class": getattr(contract, 'tradingClass', None),
                                        "contract_source": _contract_source(contract, active_keys, expired_keys),
                                        "fetch_status": fetch_status,
                                    })
                                    continue

                                duration = args.update_duration or "2 D"
                                print(
                                    f"\nFetching update window for new contract: "
                                    f"{contract.localSymbol} (duration={duration})"
                                )

                                # Get update-window data for new contract
                                contract_df = get_historical_data_for_contract(
                                    ib,
                                    contract,
                                    ticker,
                                    walk_backward=False,
                                    fallback_options=args.fallback,
                                    update_mode=True,
                                    duration_override=duration,
                                    bid_ask=args.bid_ask,
                                    client_id=args.client_id,
                                    max_lookback_override=args.max_lookback,
                                    years_back=args.years_back,
                                    bidask_cache=bidask_cache,
                                    trades_cache=trades_cache,
                                    prompt_user=not args.no_prompt,
                                )
                                
                                if contract_df is not None and not contract_df.empty:
                                    output_file = output_dir / f"{contract.localSymbol}.csv"
                                    contract_df.to_csv(str(output_file), index=False)
                                    print(f"✅ Saved {len(contract_df)} rows for new contract {contract.localSymbol}")
                                    if first_successful_contract is None:
                                        first_successful_contract = contract
                                    fetch_status = "success"
                                else:
                                    print(
                                        f"⚠️ No data returned for update window on "
                                        f"{contract.localSymbol}; contract may not be trading yet."
                                    )
                                    fetch_status = "no_data"
                                
                                ticker_rows.append({
                                    "ticker": ticker,
                                    "exchange": exchange,
                                    "currency": currency,
                                    "mode": mode_label,
                                    "contract_local_symbol": contract.localSymbol,
                                    "contract_conid": getattr(contract, 'conId', None),
                                    "contract_expiry": _contract_expiry(contract),
                                    "contract_exchange": getattr(contract, 'exchange', None),
                                    "contract_trading_class": getattr(contract, 'tradingClass', None),
                                    "contract_source": _contract_source(contract, active_keys, expired_keys),
                                    "fetch_status": fetch_status,
                                })
                                
                                time.sleep(2)  # Pause between contracts
                                
                            except Exception as e:
                                print(f"❌ Error processing new contract {contract.localSymbol}: {e}")
                                logger.error(f"Error processing new contract {contract.localSymbol}: {e}")
                        
                        # Update existing contracts
                        print(f"\n🔄 Updating existing contracts...")
                        contracts_to_update = [c for c in all_contracts if c.localSymbol in existing_contracts]
                        now_utc = datetime.now(timezone.utc)
                        
                        for contract in contracts_to_update:
                            try:
                                standardize_contract_symbol(contract)
                                output_file = output_dir / f"{contract.localSymbol}.csv"

                                expiry_dt = _parse_contract_expiry(contract)
                                if _is_expired_on_or_before(contract, now_utc):
                                    expiry_label = expiry_dt.date().isoformat() if expiry_dt else "unknown"
                                    print(f"Skipping expired contract {contract.localSymbol} (expiry {expiry_label})")
                                    ticker_rows.append({
                                        "ticker": ticker,
                                        "exchange": exchange,
                                        "currency": currency,
                                        "mode": mode_label,
                                        "contract_local_symbol": contract.localSymbol,
                                        "contract_conid": getattr(contract, 'conId', None),
                                        "contract_expiry": _contract_expiry(contract),
                                        "contract_exchange": getattr(contract, 'exchange', None),
                                        "contract_trading_class": getattr(contract, 'tradingClass', None),
                                        "contract_source": _contract_source(contract, active_keys, expired_keys),
                                        "fetch_status": "expired_skip",
                                    })
                                    continue
                                
                                # Read existing data to get last date
                                existing_df = pd.read_csv(str(output_file))
                                date_col = 'datetime' if 'datetime' in existing_df.columns else 'date'
                                existing_df[date_col] = pd.to_datetime(existing_df[date_col], utc=True, errors='coerce')
                                last_date = existing_df[date_col].max()
                                if pd.isna(last_date):
                                    print(f"❌ No valid timestamps found in {output_file}; skipping update")
                                    continue
                                
                                # Calculate duration needed
                                days_to_update = (datetime.now(timezone.utc) - last_date).days
                                
                                if days_to_update <= 0:
                                    print(f"✅ {contract.localSymbol} is already up to date")
                                    continue
                                
                                duration = args.update_duration or calculate_update_duration(days_to_update)
                                print(f"\nUpdating {contract.localSymbol}: {days_to_update} days, duration={duration}")
                                
                                # Get update data
                                contract_df = get_historical_data_for_contract(
                                    ib,
                                    contract,
                                    ticker,
                                    walk_backward=False,  # No walk-backward for updates
                                    fallback_options=args.fallback,
                                    update_mode=True,
                                    duration_override=duration,
                                    bid_ask=args.bid_ask,
                                    client_id=args.client_id,
                                    max_lookback_override=args.max_lookback,
                                    years_back=args.years_back,
                                    bidask_cache=bidask_cache,
                                    trades_cache=trades_cache,
                                    prompt_user=not args.no_prompt,
                                )
                                
                                if contract_df is not None and not contract_df.empty:
                                    # Use update_contract_data to merge
                                    update_contract_data(contract_df, ticker, contract.localSymbol, expiry_dt=expiry_dt)
                                    if first_successful_contract is None:
                                        first_successful_contract = contract
                                    fetch_status = "success"
                                else:
                                    expected_date = expected_last_trading_date(
                                        datetime.now(timezone.utc),
                                        expiry_dt=expiry_dt,
                                    )
                                    warn_if_stale(contract.localSymbol, last_date, expected_date)
                                    print(f"No new data for {contract.localSymbol}")
                                    fetch_status = "no_new_data"
                                
                                ticker_rows.append({
                                    "ticker": ticker,
                                    "exchange": exchange,
                                    "currency": currency,
                                    "mode": mode_label,
                                    "contract_local_symbol": contract.localSymbol,
                                    "contract_conid": getattr(contract, 'conId', None),
                                    "contract_expiry": _contract_expiry(contract),
                                    "contract_exchange": getattr(contract, 'exchange', None),
                                    "contract_trading_class": getattr(contract, 'tradingClass', None),
                                    "contract_source": _contract_source(contract, active_keys, expired_keys),
                                    "fetch_status": fetch_status,
                                })
                                
                                time.sleep(2)  # Pause between contracts
                                
                            except Exception as e:
                                print(f"❌ Error updating contract {contract.localSymbol}: {e}")
                                logger.error(f"Error updating contract {contract.localSymbol}: {e}")
                        
                        print(f"\n✅ Update complete for {ticker}")
                        
                    else:  # Back-fill mode
                        # Process each contract to get historical data
                        successful_contracts = 0
                        for i, contract in enumerate(all_contracts):
                            try:
                                # Ensure contract symbol is standardized before processing
                                standardize_contract_symbol(contract)
                                print(f"\nProcessing contract [{i+1}/{len(all_contracts)}]: {contract.localSymbol}")
                                
                                # Format the output filename based on the contract's localSymbol
                                output_file = output_dir / f"{contract.localSymbol}.csv"
                                
                                # Note if file already exists (but will overwrite)
                                if output_file.exists() and output_file.stat().st_size > 0:
                                    print(f"File for {contract.localSymbol} already exists, will overwrite with new data")
                                
                                # Get historical data for this contract - using our function
                                contract_df = get_historical_data_for_contract(
                                    ib,
                                    contract,
                                    ticker,
                                    args.walk_backward,
                                    args.fallback,
                                    bid_ask=args.bid_ask,
                                    client_id=args.client_id,
                                    max_lookback_override=args.max_lookback,
                                    years_back=args.years_back,
                                    bidask_cache=bidask_cache,
                                    trades_cache=trades_cache,
                                    prompt_user=not args.no_prompt,
                                )
                                
                                if contract_df is not None and not contract_df.empty:
                                    # Save to CSV (using standardized symbol)
                                    output_file = output_dir / f"{contract.localSymbol}.csv"
                                    contract_df.to_csv(str(output_file), index=False)
                                    print(f"✅ Saved {len(contract_df)} rows for {contract.localSymbol} to {output_file}")
                                    successful_contracts += 1
                                    if first_successful_contract is None:
                                        first_successful_contract = contract
                                    fetch_status = "success"
                                    
                                    # Display first 5 rows
                                    print("\nFirst 5 rows of data:")
                                    print(contract_df.head())
                                else:
                                    print(f"❌ Failed to retrieve data for {contract.localSymbol}")
                                    fetch_status = "failed"
                                
                                ticker_rows.append({
                                    "ticker": ticker,
                                    "exchange": exchange,
                                    "currency": currency,
                                    "mode": mode_label,
                                    "contract_local_symbol": contract.localSymbol,
                                    "contract_conid": getattr(contract, 'conId', None),
                                    "contract_expiry": _contract_expiry(contract),
                                    "contract_exchange": getattr(contract, 'exchange', None),
                                    "contract_trading_class": getattr(contract, 'tradingClass', None),
                                    "contract_source": _contract_source(contract, active_keys, expired_keys),
                                    "fetch_status": fetch_status,
                                })
                                
                                # Add a pause between contracts
                                if i < len(all_contracts) - 1:
                                    print("Waiting 2 seconds before next contract...")
                                    time.sleep(2)
                            
                            except Exception as e:
                                print(f"❌ Error processing contract {contract.localSymbol}: {e}")
                                logger.error(f"Error processing contract {contract.localSymbol}: {e}")
                                continue
                        
                        print(f"\nCompleted processing {ticker}: {successful_contracts}/{len(all_contracts)} contracts successfully retrieved")

                    if ticker_rows:
                        root_contract, root_basis = _select_root_contract(
                            first_successful_contract,
                            active_contracts,
                            expired_contracts,
                        )
                        if root_contract is not None:
                            root_conid = getattr(root_contract, 'conId', None)
                            root_local_symbol = getattr(root_contract, 'localSymbol', None)
                            root_expiry = _contract_expiry(root_contract)
                            root_exchange = getattr(root_contract, 'exchange', None)
                            root_trading_class = getattr(root_contract, 'tradingClass', None)
                            root_source = _contract_source(root_contract, active_keys, expired_keys)
                        else:
                            root_conid = None
                            root_local_symbol = None
                            root_expiry = None
                            root_exchange = None
                            root_trading_class = None
                            root_source = None

                        for row in ticker_rows:
                            row["root_conid"] = root_conid
                            row["root_local_symbol"] = root_local_symbol
                            row["root_expiry"] = root_expiry
                            row["root_exchange"] = root_exchange
                            row["root_trading_class"] = root_trading_class
                            row["root_source"] = root_source
                            row["root_basis"] = root_basis

                        conid_rows.extend(ticker_rows)

                    # Break out of the retry loop for this security
                    break
                    
                except Exception as e:
                    print(f"❌ Error processing security {ticker}: {e}")
                    logger.error(f"Error processing security {ticker}: {e}")
                    retry_count += 1
                    if retry_count <= max_disconnection_retries:
                        print(f"Retrying in 10 seconds (attempt {retry_count}/{max_disconnection_retries})...")
                        time.sleep(10)
                    else:
                        print("Maximum retries reached for this security, moving to next...")
                        break
        
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE".center(80))
        print("=" * 80)
        _print_futures_summary(conid_rows)
        
    except Exception as e:
        print(f"❌ Error in main processing: {e}")
        logger.error(f"Error in main processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        # Disconnect from IBKR
        if conid_rows:
            _write_futures_conid_artifact(conid_rows, MAX_FETCH_DIR)
        print("\nDisconnecting from IBKR")
        ib.disconnect()

if __name__ == "__main__":
    main() 
