#!/usr/bin/env python3
"""
get_ibkr_options.py
Self-contained script for fetching historical options data from IBKR.

This script fetches options data at multiple time granularities (1min, 5min, 15min)
and can aggregate to EOD. Data is saved in CSV format for compatibility and ease of use.

Usage:
    python get_ibkr_options.py --back-fill              # Full historical fetch
    python get_ibkr_options.py --update                 # Update existing data
    python get_ibkr_options.py --back-fill --symbol SPY # Single symbol
    python get_ibkr_options.py --update --tier T1       # Update Tier 1 symbols only
"""

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import yaml
from ib_insync import IB, Stock, Option, util, Contract

# Set up paths relative to project root
# Get the directory of this script
SCRIPT_DIR = Path(__file__).parent
# Get the project root (3 levels up from ibkr-fetch)
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
# Set up path to bronze storage
BRONZE_OPTIONS_DIR = PROJECT_ROOT / "data" / "bronze" / "ibkr" / "options"

# ============================================================================
# CONFIGURATION LOADING
# ============================================================================

# Global variables that will be set after loading config
TIER1_SYMBOLS = None
TIER2_SYMBOLS = None
TIER3_SYMBOLS = None
BAR_CONFIGS = None
DEFAULT_HOST = None
DEFAULT_PORT = None
DEFAULT_CLIENT_ID = None
OUTPUT_BASE_DIR = None
PROGRESS_FILE = 'options_fetch_progress.json'
LOG_DIR = SCRIPT_DIR / 'logs'
RISK_FREE_RATE = None
MIN_IV = None
MAX_IV = None

def load_option_styles(style_file='GetOptions/option_styles.yaml'):
    """Load option style configuration (American vs European)."""
    try:
        style_path = Path(style_file)
        if not style_path.exists() and not style_path.is_absolute():
            # Allow running from repo root: resolve relative to this script directory.
            style_path = (SCRIPT_DIR / style_path).resolve()
        with open(style_path, 'r') as f:
            styles = yaml.safe_load(f)
        return styles
    except FileNotFoundError:
        # Try alternate path
        try:
            alt = SCRIPT_DIR / 'option_styles.yaml'
            with open(alt, 'r') as f:
                styles = yaml.safe_load(f)
            return styles
        except FileNotFoundError:
            print(f"WARNING: Option styles file {style_file} not found")
            print("Defaulting to American style for all options")
            return {
                'american_style': [],
                'european_style': [],
                'default_style': 'american'
            }

def get_option_style(symbol, styles_config):
    """Determine if an option is American or European style."""
    if symbol in styles_config.get('american_style', []):
        return 'american'
    elif symbol in styles_config.get('european_style', []):
        return 'european'
    else:
        return styles_config.get('default_style', 'american')

def load_config(config_file='GetOptions/options_intraday.yaml'):
    """Load configuration from YAML file and set global variables."""
    global TIER1_SYMBOLS, TIER2_SYMBOLS, TIER3_SYMBOLS, BAR_CONFIGS
    global DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CLIENT_ID
    global OUTPUT_BASE_DIR, RISK_FREE_RATE, MIN_IV, MAX_IV
    
    config_path = Path(config_file)
    attempted = []
    if config_path.exists():
        attempted.append(str(config_path))
    else:
        attempted.append(str(config_path))
        # Allow running from repo root: resolve relative to this script directory.
        if not config_path.is_absolute():
            script_relative = (SCRIPT_DIR / config_path).resolve()
            attempted.append(str(script_relative))
            if script_relative.exists():
                config_path = script_relative
        # Backward-compat: allow a bare options_intraday.yaml adjacent to script.
        if not config_path.exists():
            alt = (SCRIPT_DIR / "options_intraday.yaml").resolve()
            attempted.append(str(alt))
            if alt.exists():
                config_path = alt

    if not config_path.exists():
        print(f"ERROR: Configuration file {config_file} not found")
        print("Searched (in order):")
        for p in attempted:
            print(f"  - {p}")
        sys.exit(1)

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Extract configuration values
    TIER1_SYMBOLS = config['symbols']['T1']
    TIER2_SYMBOLS = config['symbols']['T2']
    TIER3_SYMBOLS = config['symbols'].get('T3', [])  # T3 is optional for backward compatibility
    
    # Build bar configurations from YAML
    BAR_CONFIGS = {}
    for tier, tier_config in config['settings']['bar_configs'].items():
        BAR_CONFIGS[tier] = {
            'bar_sizes': tier_config['bar_sizes'],
            'max_days': {},
            'batch_size': {},
            'sleep_between': {}
        }
        
        # Set max days, batch sizes, and sleep times based on bar settings
        for bar_size in tier_config['bar_sizes']:
            bar_settings = config['settings']['bar_settings'].get(bar_size, {})
            BAR_CONFIGS[tier]['max_days'][bar_size] = bar_settings.get('max_history_days', 365)
            
            # Determine batch sizes based on bar granularity
            if bar_size == '1 min':
                BAR_CONFIGS[tier]['batch_size'][bar_size] = 2
                BAR_CONFIGS[tier]['sleep_between'][bar_size] = 20
            elif bar_size == '5 mins':
                BAR_CONFIGS[tier]['batch_size'][bar_size] = 3
                BAR_CONFIGS[tier]['sleep_between'][bar_size] = 15
            else:  # 15 mins
                BAR_CONFIGS[tier]['batch_size'][bar_size] = 5
                BAR_CONFIGS[tier]['sleep_between'][bar_size] = 10
    
    # IBKR TWS settings from YAML (fallback to legacy ib_gateway key)
    ib_cfg = config.get('ib_tws') or config.get('ib_gateway') or {}
    DEFAULT_HOST = ib_cfg.get('host', '127.0.0.1')
    DEFAULT_PORT = ib_cfg.get('port', 7497)
    DEFAULT_CLIENT_ID = ib_cfg.get('client_id', 30)
    
    # Output settings from YAML - override with bronze layer if not specified
    yaml_output = config.get('output', {}).get('base_path')
    if yaml_output:
        OUTPUT_BASE_DIR = Path(yaml_output)
    else:
        OUTPUT_BASE_DIR = BRONZE_OPTIONS_DIR
        OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Greeks calculation settings from YAML
    RISK_FREE_RATE = config['settings']['greeks'].get('risk_free_rate', 0.05)
    
    # Quality filters from YAML
    MIN_IV = config['quality']['min_iv']
    MAX_IV = config['quality']['max_iv']
    
    return config

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(debug=False):
    """Setup logging configuration."""
    LOG_DIR.mkdir(exist_ok=True)
    
    level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_DIR / 'get_ibkr_options.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ============================================================================
# PROGRESS TRACKING
# ============================================================================

class ProgressTracker:
    """Track progress for resumable fetching."""
    
    def __init__(self, progress_file=PROGRESS_FILE, force_refresh=False):
        self.progress_file = progress_file
        self.completed = {} if force_refresh else self.load_progress()
        
    def load_progress(self):
        """Load progress from file."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load progress file: {e}")
        return {}
    
    def save_progress(self):
        """Save progress to file."""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.completed, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def mark_completed(self, key):
        """Mark a task as completed."""
        self.completed[key] = datetime.now().isoformat()
        self.save_progress()
    
    def is_completed(self, key):
        """Check if a task is completed."""
        return key in self.completed
    
    def clear(self):
        """Clear all progress."""
        self.completed = {}
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)

# ============================================================================
# CONTRACT SELECTION & CHAIN ENUMERATION
# ============================================================================

def get_third_friday(year: int, month: int) -> datetime:
    """Get the third Friday of a given month (typical options expiry)."""
    first_day = datetime(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    if days_until_friday == 0 and first_day.weekday() != 4:
        days_until_friday = 7
    first_friday = first_day + timedelta(days=days_until_friday)
    third_friday = first_friday + timedelta(days=14)
    return third_friday

def find_monthly_expiry(target_days: int = 30) -> str:
    """Find the next monthly expiry date approximately target_days out."""
    today = datetime.now()
    target_date = today + timedelta(days=target_days)
    
    # Find the third Friday of the target month
    expiry = get_third_friday(target_date.year, target_date.month)
    
    # If it's too close, go to next month
    if (expiry - today).days < 20:
        if target_date.month == 12:
            expiry = get_third_friday(target_date.year + 1, 1)
        else:
            expiry = get_third_friday(target_date.year, target_date.month + 1)
    
    return expiry.strftime('%Y%m%d')

def enumerate_option_chain(ib: IB, symbol: str, dte_min: int = 5, dte_max: int = 45,
                          moneyness_range: Tuple[float, float] = (0.90, 1.10),
                          max_strikes_per_expiry: int = 10) -> List[Contract]:
    """
    Enumerate full option chain for a symbol.
    
    Args:
        ib: IB connection
        symbol: Stock symbol
        dte_min: Minimum days to expiry
        dte_max: Maximum days to expiry
        moneyness_range: (min, max) moneyness ratio for strikes
        max_strikes_per_expiry: Maximum strikes to fetch per expiry
    
    Returns:
        List of qualified option contracts
    """
    try:
        # Get the underlying stock
        stock = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(stock)
        
        # Get current price for moneyness filtering
        ticker = ib.reqMktData(stock, '', False, False)
        ib.sleep(3)  # Wait longer for price
        
        # Try multiple price fields
        spot_price = None
        if ticker.last and not math.isnan(ticker.last):
            spot_price = ticker.last
        elif ticker.close and not math.isnan(ticker.close):
            spot_price = ticker.close
        elif ticker.bid and ticker.ask:
            if not math.isnan(ticker.bid) and not math.isnan(ticker.ask):
                spot_price = (ticker.bid + ticker.ask) / 2
        
        ib.cancelMktData(stock)
        
        if not spot_price:
            # Try historical data as fallback
            bars = ib.reqHistoricalData(
                stock, '', '1 D', '1 day', 'TRADES', True, 1, False, timeout=10
            )
            if bars:
                df = util.df(bars)
                spot_price = df['close'].iloc[-1]
            else:
                logger.error(f"Cannot get spot price for {symbol}")
                return []
        
        logger.info(f"Spot price for {symbol}: {spot_price}")
        
        # Get option chain parameters
        chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            logger.error(f"No option chains found for {symbol}")
            return []
        
        # Find SMART exchange chain
        chain = None
        for c in chains:
            if c.exchange == 'SMART':
                chain = c
                break
        
        if not chain:
            chain = chains[0]  # Fallback to first available
            
        # Filter expiries by DTE
        today = datetime.now().date()
        valid_expiries = []
        
        for exp_str in chain.expirations:
            exp_date = datetime.strptime(exp_str, '%Y%m%d').date()
            dte = (exp_date - today).days
            if dte_min <= dte <= dte_max:
                valid_expiries.append(exp_str)
        
        logger.info(f"Found {len(valid_expiries)} valid expiries for {symbol}")
        
        # Filter strikes by moneyness - focus on near ATM for liquidity
        min_strike = spot_price * max(0.90, moneyness_range[0])  # At least 90% moneyness
        max_strike = spot_price * min(1.10, moneyness_range[1])  # At most 110% moneyness
        
        valid_strikes = []
        for strike in chain.strikes:
            if min_strike <= strike <= max_strike:
                valid_strikes.append(strike)
        
        # Sort by distance from ATM and take closest strikes
        valid_strikes.sort(key=lambda x: abs(x - spot_price))
        
        # Limit strikes per expiry - take closest to ATM
        if len(valid_strikes) > max_strikes_per_expiry:
            valid_strikes = valid_strikes[:max_strikes_per_expiry]
        
        # Re-sort by strike value for consistent ordering
        valid_strikes.sort()
        
        logger.info(f"Selected {len(valid_strikes)} strikes for {symbol}")
        
        # Build option contracts
        contracts = []
        for expiry in valid_expiries:
            for strike in valid_strikes:
                for right in ['C', 'P']:
                    option = Option(
                        symbol, expiry, strike, right, 'SMART',
                        tradingClass=chain.tradingClass,
                        multiplier=str(chain.multiplier) if chain.multiplier else '100'
                    )
                    contracts.append(option)
        
        # Qualify contracts in batches
        qualified = []
        batch_size = 50  # IB limit
        
        for i in range(0, len(contracts), batch_size):
            batch = contracts[i:i+batch_size]
            try:
                qualified_batch = ib.qualifyContracts(*batch)
                qualified.extend(qualified_batch)
                logger.info(f"Qualified {len(qualified_batch)} contracts in batch {i//batch_size + 1}")
            except Exception as e:
                logger.warning(f"Failed to qualify batch {i//batch_size + 1}: {e}")
            
            time.sleep(0.5)  # Rate limiting
        
        logger.info(f"Total qualified contracts for {symbol}: {len(qualified)}")
        return qualified
        
    except Exception as e:
        logger.error(f"Error enumerating chain for {symbol}: {e}")
        return []

def pick_atm_call(ib: IB, symbol: str, expiry: str = None) -> Optional[Contract]:
    """
    Pick the at-the-money call option for a symbol.
    [DEPRECATED - Use enumerate_option_chain instead]
    
    Args:
        ib: IB connection
        symbol: Stock symbol
        expiry: Expiry date (YYYYMMDD format), defaults to ~30 days out
    
    Returns:
        ATM call option contract or None
    """
    try:
        # Get the underlying stock
        stock = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(stock)
        
        # Get current price
        ticker = ib.reqMktData(stock, '', False, False)
        ib.sleep(2)  # Wait for price
        
        if not ticker.last:
            logger.warning(f"No price data for {symbol}")
            return None
        
        spot_price = ticker.last
        ib.cancelMktData(stock)
        
        # Use default expiry if not provided
        if not expiry:
            expiry = find_monthly_expiry()
        
        # Find the nearest strike
        strikes = range(int(spot_price - 10), int(spot_price + 11), 1)
        best_strike = min(strikes, key=lambda x: abs(x - spot_price))
        
        # Create the option contract
        option = Option(symbol, expiry, best_strike, 'C', 'SMART')
        contracts = ib.qualifyContracts(option)
        
        if contracts:
            return contracts[0]
        else:
            logger.warning(f"No ATM call found for {symbol} at strike {best_strike}")
            return None
            
    except Exception as e:
        logger.error(f"Error picking ATM call for {symbol}: {e}")
        return None

# ============================================================================
# GREEKS AND IV CALCULATION (EUROPEAN BSM WITH CARRY)
# ============================================================================

def calculate_iv_bsm(price: float, S: float, K: float, T: float, r: float, 
                     q: float = 0.0, option_type: str = 'C') -> Optional[float]:
    """
    Calculate implied volatility using European Black-Scholes-Merton model with dividends.
    
    Args:
        price: Option price (mid price)
        S: Spot price at the time of the bar
        K: Strike price
        T: Time to expiry (years, ACT/365)
        r: Risk-free rate
        q: Dividend yield (continuous)
        option_type: 'C' for call, 'P' for put
    
    Returns:
        Implied volatility or None if cannot be calculated
    """
    from scipy.stats import norm
    from scipy.optimize import brentq
    
    # Validate inputs
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    
    # Minimum time to avoid division errors
    T = max(T, 1/(365*24*60))  # At least 1 minute
    
    def bs_price(vol):
        """Calculate theoretical option price."""
        if vol <= 0:
            return 1e10  # Return large number for invalid vol
        
        try:
            d1 = (math.log(S/K) + (r - q + 0.5*vol**2)*T) / (vol*math.sqrt(T))
            d2 = d1 - vol*math.sqrt(T)
            
            if option_type == 'C':
                return S*math.exp(-q*T)*norm.cdf(d1) - K*math.exp(-r*T)*norm.cdf(d2)
            else:
                return K*math.exp(-r*T)*norm.cdf(-d2) - S*math.exp(-q*T)*norm.cdf(-d1)
        except:
            return 1e10
    
    try:
        # Use Brent's method to find IV
        # Start with wider bounds for extreme cases
        iv = brentq(lambda x: bs_price(x) - price, 0.001, 5.0, maxiter=100)
        
        # Validate result
        if MIN_IV <= iv <= MAX_IV:
            return iv
        else:
            return None
            
    except Exception:
        # If Brent fails, try a few fixed points
        for test_vol in [0.2, 0.3, 0.5, 1.0]:
            if abs(bs_price(test_vol) - price) < 0.01:
                return test_vol if MIN_IV <= test_vol <= MAX_IV else None
        return None

def calculate_bsm_greeks(S: float, K: float, T: float, r: float, q: float, 
                         vol: float, option_type: str = 'C') -> Dict[str, float]:
    """
    Calculate option Greeks using European BSM model with dividends.
    
    Args:
        S: Spot price at the time of the bar
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        q: Dividend yield
        vol: Implied volatility
        option_type: 'C' for call, 'P' for put
    
    Returns:
        Dictionary with delta, gamma, theta, vega, rho
    """
    from scipy.stats import norm
    
    # Handle edge cases
    if T <= 0 or vol <= 0 or S <= 0 or K <= 0:
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0, 'rho': 0.0}
    
    # Minimum time to avoid division errors
    T = max(T, 1/(365*24*60))
    
    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S/K) + (r - q + 0.5*vol**2)*T) / (vol*sqrt_T)
        d2 = d1 - vol*sqrt_T
        
        exp_qT = math.exp(-q*T)
        exp_rT = math.exp(-r*T)
        
        greeks = {}
        
        # Delta
        if option_type == 'C':
            greeks['delta'] = exp_qT * norm.cdf(d1)
        else:
            greeks['delta'] = -exp_qT * norm.cdf(-d1)
        
        # Gamma (same for calls and puts)
        greeks['gamma'] = exp_qT * norm.pdf(d1) / (S * vol * sqrt_T)
        
        # Theta (annualized, then converted to daily)
        if option_type == 'C':
            greeks['theta'] = (-S * exp_qT * norm.pdf(d1) * vol / (2 * sqrt_T)
                              - r * K * exp_rT * norm.cdf(d2)
                              + q * S * exp_qT * norm.cdf(d1))
        else:
            greeks['theta'] = (-S * exp_qT * norm.pdf(d1) * vol / (2 * sqrt_T)
                              + r * K * exp_rT * norm.cdf(-d2)
                              - q * S * exp_qT * norm.cdf(-d1))
        
        greeks['theta'] /= 365  # Convert to per day
        
        # Vega (per 1% change in vol)
        greeks['vega'] = S * exp_qT * norm.pdf(d1) * sqrt_T / 100
        
        # Rho (per 1% change in rate)
        if option_type == 'C':
            greeks['rho'] = K * T * exp_rT * norm.cdf(d2) / 100
        else:
            greeks['rho'] = -K * T * exp_rT * norm.cdf(-d2) / 100
        
        return greeks
        
    except Exception as e:
        logger.warning(f"Error calculating Greeks: {e}")
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0, 'rho': 0.0}

def calculate_iv_black_scholes(S, K, T, r, price, option_type='C', style='american'):
    """
    [DEPRECATED - Use calculate_iv_bsm instead]
    Calculate implied volatility using Black-Scholes model.
    """
    # Redirect to new function for backward compatibility
    return calculate_iv_bsm(price, S, K, T, r, 0.0, option_type)

def calculate_greeks(S, K, T, r, vol, option_type='C'):
    """
    [DEPRECATED - Use calculate_bsm_greeks instead]
    Calculate option Greeks.
    """
    # Redirect to new function for backward compatibility
    return calculate_bsm_greeks(S, K, T, r, 0.0, vol, option_type)

# ============================================================================
# IB CONNECTION MANAGEMENT
# ============================================================================

def connect_to_ibkr(host=DEFAULT_HOST, port=DEFAULT_PORT, client_id=DEFAULT_CLIENT_ID, max_retries=5):
    """Connect to IBKR TWS with retry logic."""
    ib = IB()
    
    for attempt in range(max_retries):
        try:
            current_id = client_id + attempt
            print(f"Attempting to connect to IBKR TWS at {host}:{port} with client_id {current_id}...")
            ib.connect(host, port, clientId=current_id, readonly=True, timeout=20)
            print(f"✅ Connected successfully with client_id: {current_id}")
            logger.info(f"Connected to IBKR TWS with client_id: {current_id}")
            return ib
        except Exception as e:
            error_msg = str(e)
            if "already in use" in error_msg.lower() and attempt < max_retries - 1:
                print(f"Client ID {current_id} is already in use. Trying next ID...")
                time.sleep(1)
            else:
                print(f"❌ Failed to connect: {e}")
                logger.error(f"Failed to connect: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
    
    return None

def reconnect_if_needed(ib, host, port, client_id):
    """Check connection and reconnect if needed."""
    if not ib or not ib.isConnected():
        print("Connection lost. Attempting to reconnect...")
        return connect_to_ibkr(host, port, client_id)
    return ib

# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_historical_data(ib, contract, duration, bar_size, what_to_show='MIDPOINT'):
    """
    Fetch historical data for a contract.
    
    Args:
        ib: IB connection
        contract: Option contract
        duration: Duration string (e.g., '30 D')
        bar_size: Bar size (e.g., '1 min')
        what_to_show: Data type (MIDPOINT, BID_ASK, etc.)
    
    Returns:
        DataFrame with historical data or None
    """
    try:
        end_datetime = ''  # Use current time
        
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_datetime,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=False,  # Include extended hours
            formatDate=1,
            keepUpToDate=False,
            timeout=60
        )
        
        if bars:
            df = util.df(bars)
            df['symbol'] = contract.symbol
            df['expiry'] = contract.lastTradeDateOrContractMonth
            df['strike'] = contract.strike
            df['right'] = contract.right
            return df
        
    except Exception as e:
        logger.error(f"Error fetching data for {contract.localSymbol}: {e}")
    
    return None

def fetch_option_bars_frd(ib: IB, contract: Contract, bar_size: str, 
                         start_date: datetime, end_date: datetime,
                         underlying_bars: pd.DataFrame = None) -> pd.DataFrame:
    """
    Fetch option bars with BID, ASK, and TRADES separately for FRD format.
    
    Args:
        ib: IB connection
        contract: Option contract
        bar_size: Bar size ('1 min', '5 mins', '15 mins', '1 hour', '1 day')
        start_date: Start date for data
        end_date: End date for data
        underlying_bars: Pre-fetched underlying bars for S_t alignment
    
    Returns:
        DataFrame with FRD-formatted option data
    """
    try:
        # Determine duration based on bar size and date range
        if bar_size in ['1 min', '5 mins', '15 mins']:
            # Use 1-day batches for intraday
            duration = '1 D'
        elif bar_size == '1 hour':
            days = (end_date - start_date).days
            duration = f'{min(days, 30)} D'
        else:  # 1 day
            days = (end_date - start_date).days
            duration = f'{min(days, 365)} D'
        
        # Format end datetime
        end_dt_str = end_date.strftime('%Y%m%d 16:00:00')
        
        # Fetch BID bars
        bid_bars = ib.reqHistoricalData(
            contract, endDateTime=end_dt_str, durationStr=duration,
            barSizeSetting=bar_size, whatToShow='BID',
            useRTH=True, formatDate=1, keepUpToDate=False, timeout=30
        )
        
        # Fetch ASK bars
        ask_bars = ib.reqHistoricalData(
            contract, endDateTime=end_dt_str, durationStr=duration,
            barSizeSetting=bar_size, whatToShow='ASK',
            useRTH=True, formatDate=1, keepUpToDate=False, timeout=30
        )
        
        # Fetch TRADES bars for volume
        trade_bars = ib.reqHistoricalData(
            contract, endDateTime=end_dt_str, durationStr=duration,
            barSizeSetting=bar_size, whatToShow='TRADES',
            useRTH=True, formatDate=1, keepUpToDate=False, timeout=30
        )
        
        # Convert to DataFrames
        df_bid = util.df(bid_bars) if bid_bars else pd.DataFrame()
        df_ask = util.df(ask_bars) if ask_bars else pd.DataFrame()
        df_trade = util.df(trade_bars) if trade_bars else pd.DataFrame()
        
        if df_bid.empty and df_ask.empty and df_trade.empty:
            logger.warning(f"No data for {contract.localSymbol}")
            return pd.DataFrame()
        
        # Merge DataFrames
        df = pd.DataFrame()
        
        # Use trade data as base if available
        if not df_trade.empty:
            df = df_trade[['date', 'volume']].copy()
        elif not df_bid.empty:
            df = df_bid[['date']].copy()
            df['volume'] = 0
        else:
            df = df_ask[['date']].copy()
            df['volume'] = 0
        
        # Add bid/ask data
        if not df_bid.empty:
            df = df.merge(df_bid[['date', 'close']].rename(columns={'close': 'bid'}), 
                         on='date', how='left')
        else:
            df['bid'] = 0
            
        if not df_ask.empty:
            df = df.merge(df_ask[['date', 'close']].rename(columns={'close': 'ask'}), 
                         on='date', how='left')
        else:
            df['ask'] = 0
        
        # Calculate mid price
        df['mid'] = (df['bid'] + df['ask']) / 2
        df.loc[df['bid'] <= 0, 'mid'] = df.loc[df['bid'] <= 0, 'ask']
        df.loc[df['ask'] <= 0, 'mid'] = df.loc[df['ask'] <= 0, 'bid']
        
        # Add contract details
        df['symbol'] = contract.symbol
        df['strike'] = contract.strike
        df['expiry'] = contract.lastTradeDateOrContractMonth
        df['type'] = contract.right.lower()
        
        # Calculate time to expiry and moneyness
        df['date'] = pd.to_datetime(df['date'])
        expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d')
        df['dte'] = (expiry_date - df['date']).dt.days
        
        # Add underlying price if available
        if underlying_bars is not None and not underlying_bars.empty:
            underlying_bars['date'] = pd.to_datetime(underlying_bars['date'])
            df = df.merge(underlying_bars[['date', 'close']].rename(columns={'close': 'underlying_price'}),
                         on='date', how='left')
            df['underlying_price'].fillna(method='ffill', inplace=True)
            df['moneyness'] = df['underlying_price'] / contract.strike
        else:
            df['underlying_price'] = 0
            df['moneyness'] = 0
        
        # Calculate IV and Greeks
        df['iv'] = 0.0
        df['delta'] = 0.0
        df['gamma'] = 0.0
        df['theta'] = 0.0
        df['vega'] = 0.0
        df['rho'] = 0.0
        
        for idx, row in df.iterrows():
            if row['mid'] > 0 and row['underlying_price'] > 0 and row['dte'] > 0:
                T = row['dte'] / 365.0
                S = row['underlying_price']
                
                # Calculate IV
                iv = calculate_iv_bsm(
                    row['mid'], S, contract.strike, T, 
                    RISK_FREE_RATE, 0.0, contract.right
                )
                
                if iv:
                    df.at[idx, 'iv'] = iv
                    
                    # Calculate Greeks
                    greeks = calculate_bsm_greeks(
                        S, contract.strike, T, RISK_FREE_RATE, 
                        0.0, iv, contract.right
                    )
                    
                    df.at[idx, 'delta'] = greeks['delta']
                    df.at[idx, 'gamma'] = greeks['gamma']
                    df.at[idx, 'theta'] = greeks['theta']
                    df.at[idx, 'vega'] = greeks['vega']
                    df.at[idx, 'rho'] = greeks['rho']
        
        # Add additional fields
        df['oi'] = 0  # Will be populated from live data in future
        df['conId'] = contract.conId
        df['tradingClass'] = contract.tradingClass if hasattr(contract, 'tradingClass') else ''
        df['multiplier'] = contract.multiplier if hasattr(contract, 'multiplier') else '100'
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching FRD bars for {contract.localSymbol}: {e}")
        return pd.DataFrame()

def fetch_underlying_bars(ib: IB, symbol: str, bar_size: str, 
                         start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch underlying stock bars for a given timeframe.
    
    Args:
        ib: IB connection
        symbol: Stock symbol
        bar_size: Bar size
        start_date: Start date
        end_date: End date
    
    Returns:
        DataFrame with underlying price data
    """
    try:
        stock = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(stock)
        
        # Determine duration
        if bar_size in ['1 min', '5 mins', '15 mins']:
            duration = '1 D'
        elif bar_size == '1 hour':
            days = (end_date - start_date).days
            duration = f'{min(days, 30)} D'
        else:
            days = (end_date - start_date).days
            duration = f'{min(days, 365)} D'
        
        end_dt_str = end_date.strftime('%Y%m%d 16:00:00')
        
        bars = ib.reqHistoricalData(
            stock, endDateTime=end_dt_str, durationStr=duration,
            barSizeSetting=bar_size, whatToShow='TRADES',
            useRTH=True, formatDate=1, keepUpToDate=False, timeout=30
        )
        
        if bars:
            return util.df(bars)
        
    except Exception as e:
        logger.error(f"Error fetching underlying bars for {symbol}: {e}")
    
    return pd.DataFrame()

def save_frd_csv(df: pd.DataFrame, output_path: Path, bar_size: str, 
                 symbol: str, date: datetime = None, contract: Contract = None) -> bool:
    """
    Save DataFrame in FRD-compliant CSV format.
    
    Args:
        df: DataFrame with option data
        output_path: Base output directory
        bar_size: Bar size for directory structure
        symbol: Stock symbol
        date: Date for intraday files
        contract: Option contract for filename
    
    Returns:
        True if saved successfully
    """
    try:
        # Map bar sizes to directory names
        bar_size_map = {
            '1 min': '1min',
            '5 mins': '5min',
            '15 mins': '15min',
            '1 hour': '1hr',
            '1 day': 'EOD'
        }
        
        bar_dir = bar_size_map.get(bar_size, bar_size.replace(' ', ''))
        
        # Create directory structure
        if bar_size == '1 day':
            # EOD: data/bronze/ibkr/options/EOD/{symbol}/{date}.csv
            dir_path = output_path / bar_dir / symbol
            dir_path.mkdir(parents=True, exist_ok=True)
            
            date_str = date.strftime('%Y%m%d') if date else datetime.now().strftime('%Y%m%d')
            file_path = dir_path / f"{date_str}.csv"
        else:
            # Intraday: data/bronze/ibkr/options/{timeframe}/{symbol}/{date}/{expiry}_{strike}_{type}.csv
            date_str = date.strftime('%Y%m%d') if date else datetime.now().strftime('%Y%m%d')
            dir_path = output_path / bar_dir / symbol / date_str
            dir_path.mkdir(parents=True, exist_ok=True)
            
            if contract:
                filename = f"{contract.lastTradeDateOrContractMonth}_{int(contract.strike)}_{contract.right.lower()}.csv"
            else:
                filename = "chain.csv"
            
            file_path = dir_path / filename
        
        # Create vendor schema DataFrame (16 columns - compatible with build_continuous_options.py)
        vendor_df = pd.DataFrame()
        
        # Column 0: quote_date
        if 'date' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['date']):
                vendor_df['quote_date'] = df['date'].dt.strftime('%Y-%m-%d')
            else:
                vendor_df['quote_date'] = df['date']
        else:
            vendor_df['quote_date'] = date.strftime('%Y-%m-%d') if date else datetime.now().strftime('%Y-%m-%d')
        
        # Column 1: strike
        vendor_df['strike'] = df['strike'] if 'strike' in df.columns else 0
        
        # Column 2: expiry
        if 'expiry' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['expiry']):
                vendor_df['expiry'] = df['expiry'].dt.strftime('%Y-%m-%d')
            else:
                vendor_df['expiry'] = df['expiry']
        else:
            vendor_df['expiry'] = ''
        
        # Column 3: cp (call/put)
        if 'type' in df.columns:
            vendor_df['cp'] = df['type'].str.lower() if hasattr(df['type'], 'str') else df['type']
        elif contract:
            vendor_df['cp'] = contract.right.lower()
        else:
            vendor_df['cp'] = ''
        
        # Column 4: last (use mid as proxy)
        vendor_df['last'] = df['mid'] if 'mid' in df.columns else (df['bid'] + df['ask']) / 2 if 'bid' in df.columns and 'ask' in df.columns else 0
        
        # Column 5-6: bid/ask
        vendor_df['bid'] = df['bid'] if 'bid' in df.columns else 0
        vendor_df['ask'] = df['ask'] if 'ask' in df.columns else 0
        
        # Column 7-8: iv_bid/iv_ask (create spread around main IV)
        if 'iv' in df.columns:
            # Create slight spread around main IV for bid/ask IVs
            vendor_df['iv_bid'] = df['iv'] * 0.98  # 2% lower for bid
            vendor_df['iv_ask'] = df['iv'] * 1.02  # 2% higher for ask
        else:
            vendor_df['iv_bid'] = 0
            vendor_df['iv_ask'] = 0
        
        # Column 9-10: open_interest and volume
        vendor_df['open_interest'] = df['oi'].astype(int) if 'oi' in df.columns else 0
        vendor_df['volume'] = df['volume'].astype(int) if 'volume' in df.columns else 0
        
        # Column 11-15: Greeks
        vendor_df['delta'] = df['delta'] if 'delta' in df.columns else 0
        vendor_df['gamma'] = df['gamma'] if 'gamma' in df.columns else 0
        vendor_df['vega'] = df['vega'] if 'vega' in df.columns else 0
        vendor_df['theta'] = df['theta'] if 'theta' in df.columns else 0
        vendor_df['rho'] = df['rho'] if 'rho' in df.columns else 0
        
        # Save without header for vendor format compatibility
        vendor_df.to_csv(file_path, index=False, header=False, float_format='%.6f')
        
        logger.info(f"Saved {len(vendor_df)} rows to {file_path} (vendor schema)")
        return True
        
    except Exception as e:
        logger.error(f"Error saving FRD CSV: {e}")
        return False

def process_option_chain(ib: IB, symbol: str, bar_size: str, 
                        start_date: datetime, end_date: datetime,
                        output_dir: Path, update_mode: bool = False,
                        dte_min: int = 5, dte_max: int = 45,
                        moneyness_range: Tuple[float, float] = (0.90, 1.10)) -> bool:
    """
    Process full option chain for a symbol in FRD format.
    
    Args:
        ib: IB connection
        symbol: Stock symbol
        bar_size: Bar size ('1 min', '5 mins', '15 mins', '1 hour', '1 day')
        start_date: Start date for historical data
        end_date: End date for historical data
        output_dir: Output directory
        update_mode: If True, only fetch new data
        dte_min: Minimum days to expiry
        dte_max: Maximum days to expiry
        moneyness_range: (min, max) moneyness ratio
    
    Returns:
        True if successful
    """
    try:
        logger.info(f"Processing {symbol} chain for {bar_size} from {start_date} to {end_date}")
        
        # Enumerate option chain
        contracts = enumerate_option_chain(ib, symbol, dte_min, dte_max, moneyness_range)
        
        if not contracts:
            logger.warning(f"No contracts found for {symbol}")
            return False
        
        logger.info(f"Found {len(contracts)} contracts for {symbol}")
        
        # Process in daily batches for intraday
        if bar_size in ['1 min', '5 mins', '15 mins']:
            current_date = start_date
            
            while current_date <= end_date:
                logger.info(f"Processing {symbol} for {current_date.strftime('%Y-%m-%d')}")
                
                # Fetch underlying bars for this day
                underlying_bars = fetch_underlying_bars(
                    ib, symbol, bar_size, current_date, current_date
                )
                
                # Process contracts in batches
                batch_size = 5  # Adjust based on bar size
                for i in range(0, len(contracts), batch_size):
                    batch = contracts[i:i+batch_size]
                    
                    for contract in batch:
                        # Check if already exists in update mode
                        if update_mode:
                            bar_dir = {'1 min': '1min', '5 mins': '5min', '15 mins': '15min'}[bar_size]
                            date_str = current_date.strftime('%Y%m%d')
                            check_path = output_dir / bar_dir / symbol / date_str / \
                                       f"{contract.lastTradeDateOrContractMonth}_{int(contract.strike)}_{contract.right.lower()}.csv"
                            
                            if check_path.exists():
                                logger.debug(f"Skipping existing file: {check_path}")
                                continue
                        
                        # Fetch option bars
                        df = fetch_option_bars_frd(
                            ib, contract, bar_size, current_date, current_date, underlying_bars
                        )
                        
                        if not df.empty:
                            save_frd_csv(df, output_dir, bar_size, symbol, current_date, contract)
                        
                        time.sleep(0.5)  # Rate limiting
                    
                    time.sleep(2)  # Pause between batches
                
                current_date += timedelta(days=1)
                
        else:
            # For hourly and daily, fetch full range at once
            underlying_bars = fetch_underlying_bars(ib, symbol, bar_size, start_date, end_date)
            
            if bar_size == '1 day':
                # For EOD, create single file with full chain per date
                all_data = []
                
                for contract in contracts:
                    df = fetch_option_bars_frd(
                        ib, contract, bar_size, start_date, end_date, underlying_bars
                    )
                    
                    if not df.empty:
                        all_data.append(df)
                    
                    time.sleep(0.5)
                
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # Group by date and save each date's chain
                    for date, date_df in combined_df.groupby(combined_df['date'].dt.date):
                        save_frd_csv(date_df, output_dir, bar_size, symbol, date)
            else:
                # For 1 hour, save per contract
                for contract in contracts:
                    df = fetch_option_bars_frd(
                        ib, contract, bar_size, start_date, end_date, underlying_bars
                    )
                    
                    if not df.empty:
                        # Split by date for hourly data
                        for date, date_df in df.groupby(df['date'].dt.date):
                            save_frd_csv(date_df, output_dir, bar_size, symbol, date, contract)
                    
                    time.sleep(0.5)
        
        logger.info(f"✅ Completed processing {symbol} chain for {bar_size}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing chain for {symbol}: {e}")
        return False

def process_symbol(ib, symbol, bar_size, duration_days, output_dir, update_mode=False, option_style='american'):
    """
    [DEPRECATED - Use process_option_chain instead]
    Process a single symbol for a specific bar size.
    
    Args:
        ib: IB connection
        symbol: Stock symbol
        bar_size: Bar size (e.g., '1 min')
        duration_days: Number of days to fetch
        output_dir: Output directory
        update_mode: If True, only fetch new data
        option_style: 'american' or 'european'
    
    Returns:
        True if successful, False otherwise
    """
    # Redirect to new function
    end_date = datetime.now()
    start_date = end_date - timedelta(days=duration_days)
    
    return process_option_chain(
        ib, symbol, bar_size, start_date, end_date, 
        Path(output_dir), update_mode
    )


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_tier_chains(ib, tier_name, symbols, bar_configs, progress_tracker, 
                       update_mode=False, output_dir=OUTPUT_BASE_DIR, host=DEFAULT_HOST, 
                       port=DEFAULT_PORT, client_id=DEFAULT_CLIENT_ID, styles_config=None,
                       days_back=30):
    """
    Process all symbols in a tier with full option chains.
    
    Args:
        ib: IB connection
        tier_name: 'T1', 'T2', or 'T3'
        symbols: List of symbols
        bar_configs: Configuration for bar sizes
        progress_tracker: ProgressTracker instance
        update_mode: If True, only fetch new data
        output_dir: Output directory
        host, port, client_id: Connection parameters for reconnection
        styles_config: Option styles configuration (not used in BSM)
        days_back: Number of days to fetch historical data
    """
    config = bar_configs[tier_name]
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    for bar_size in config['bar_sizes']:
        print(f"\n{'='*60}")
        print(f"Processing {tier_name} symbols - {bar_size} (FRD Format)")
        print(f"{'='*60}")
        
        # Get batch settings from config (from YAML bar_settings)
        # Use defaults based on bar size
        if bar_size == '1 min':
            batch_size = 3
            sleep_time = 20
        elif bar_size == '5 mins':
            batch_size = 5
            sleep_time = 15
        elif bar_size == '15 mins':
            batch_size = 7
            sleep_time = 10
        elif bar_size == '1 hour':
            batch_size = 5
            sleep_time = 10
        else:  # 1 day
            batch_size = 10
            sleep_time = 5
        
        for symbol in symbols:
            progress_key = f"{tier_name}_{symbol}_{bar_size}_{start_date.strftime('%Y%m%d')}"
            
            # Check if already completed
            if not update_mode and progress_tracker.is_completed(progress_key):
                print(f"{symbol} {bar_size} already completed, skipping...")
                continue
            
            print(f"\nProcessing {symbol} option chain for {bar_size}")
            
            # Check connection
            ib = reconnect_if_needed(ib, host, port, client_id)
            if not ib:
                logger.error("Failed to maintain connection")
                return
            
            # Process full option chain
            success = process_option_chain(
                ib, symbol, bar_size, start_date, end_date,
                Path(output_dir), update_mode
            )
            
            if success:
                print(f"✅ Successfully processed {symbol} chain")
                if not update_mode:
                    progress_tracker.mark_completed(progress_key)
            else:
                print(f"❌ Failed to process {symbol} chain")
            
            # Sleep between symbols
            print(f"Sleeping {sleep_time} seconds before next symbol...")
            time.sleep(sleep_time)
    
    # Process EOD if configured
    if config.get('eod', False):
        print(f"\n{'='*60}")
        print(f"Processing {tier_name} symbols - EOD (Daily)")
        print(f"{'='*60}")
        
        for symbol in symbols:
            progress_key = f"{tier_name}_{symbol}_EOD_{start_date.strftime('%Y%m%d')}"
            
            if not update_mode and progress_tracker.is_completed(progress_key):
                print(f"{symbol} EOD already completed, skipping...")
                continue
            
            print(f"\nProcessing {symbol} EOD chain")
            
            ib = reconnect_if_needed(ib, host, port, client_id)
            if not ib:
                logger.error("Failed to maintain connection")
                return
            
            success = process_option_chain(
                ib, symbol, '1 day', start_date, end_date,
                Path(output_dir), update_mode
            )
            
            if success:
                print(f"✅ Successfully processed {symbol} EOD")
                if not update_mode:
                    progress_tracker.mark_completed(progress_key)
            
            time.sleep(5)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch historical options data from IBKR in FRD format'
    )
    
    # Required mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--back-fill', action='store_true',
                           help='Fetch full historical data (creates/overwrites files)')
    mode_group.add_argument('--update', action='store_true',
                           help='Update existing data with new bars')
    mode_group.add_argument('--eod-chain', action='store_true',
                           help='Fetch EOD option chains only')
    mode_group.add_argument('--daily-snapshot', action='store_true',
                           help='Daily forward capture mode (for cron jobs)')
    
    # Optional arguments
    parser.add_argument('--config', type=str, default='config/options_intraday.yaml',
                       help='Path to configuration YAML file')
    parser.add_argument('--symbol', type=str,
                       help='Process single symbol only')
    parser.add_argument('--tier', choices=['T1', 'T2', 'T3', 'ALL'],
                       default='ALL', help='Which tier to process')
    parser.add_argument('--bar-size', choices=['1 min', '5 mins', '15 mins', '1 hour', '1 day', 'ALL'],
                       help='Process specific bar size only')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to fetch (default: 30)')
    parser.add_argument('--all-timeframes', action='store_true',
                       help='Process all configured timeframes for the symbol/tier')
    parser.add_argument('--force', action='store_true',
                       help='Force refresh, ignore previous progress')
    
    # Connection arguments (defaults will be set from config)
    parser.add_argument('--host', type=str,
                       help='IBKR TWS host (overrides config)')
    parser.add_argument('--port', type=int,
                       help='IBKR TWS port (overrides config)')
    parser.add_argument('--client-id', type=int,
                       help='Starting client ID (overrides config)')
    
    # Debug
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    # Consolidation
    parser.add_argument('--skip-consolidation', action='store_true',
                       help='Skip automatic consolidation to continuous chains')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Load option styles configuration
    styles_config = load_option_styles()
    
    # Setup logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Override config values with command line args if provided
    host = args.host if args.host else DEFAULT_HOST
    port = args.port if args.port else DEFAULT_PORT
    client_id = args.client_id if args.client_id else DEFAULT_CLIENT_ID
    
    print("\n" + "="*60)
    print("IBKR Options Data Fetcher")
    print("="*60)
    print(f"Mode: {'BACK-FILL' if args.back_fill else 'UPDATE'}")
    print(f"Config: {args.config}")
    print(f"Tier: {args.tier}")
    if args.symbol:
        print(f"Symbol: {args.symbol}")
    if args.bar_size:
        print(f"Bar Size: {args.bar_size}")
    print(f"Connection: {host}:{port} (client_id: {client_id})")
    print("="*60 + "\n")
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(force_refresh=args.force)
    
    # Connect to IBKR
    ib = connect_to_ibkr(host, port, client_id)
    if not ib:
        print("Failed to connect to IBKR TWS")
        sys.exit(1)
    
    try:
        # Determine symbols to process
        if args.symbol:
            # Single symbol - determine its tier
            if args.symbol in TIER1_SYMBOLS:
                symbols = [args.symbol]
                tiers = [('T1', symbols)]
            elif args.symbol in TIER2_SYMBOLS:
                symbols = [args.symbol]
                tiers = [('T2', symbols)]
            elif args.symbol in TIER3_SYMBOLS:
                symbols = [args.symbol]
                tiers = [('T3', symbols)]
            else:
                print(f"Warning: {args.symbol} not in configured symbols, treating as T1")
                symbols = [args.symbol]
                tiers = [('T1', symbols)]
        else:
            # Process by tier
            tiers = []
            if args.tier in ['T1', 'ALL']:
                tiers.append(('T1', TIER1_SYMBOLS))
            if args.tier in ['T2', 'ALL']:
                tiers.append(('T2', TIER2_SYMBOLS))
            if args.tier in ['T3', 'ALL']:
                tiers.append(('T3', TIER3_SYMBOLS))
        
        
        # Process each tier with full chains
        for tier_name, tier_symbols in tiers:
            # Override bar configs if specific bar size requested
            tier_config = BAR_CONFIGS[tier_name].copy()
            
            if args.bar_size and args.bar_size != 'ALL':
                # Process only the specified bar size
                if args.bar_size == '1 day':
                    tier_config['bar_sizes'] = []
                    tier_config['eod'] = True
                else:
                    tier_config['bar_sizes'] = [args.bar_size]
                    tier_config['eod'] = False
            elif args.eod_chain:
                # EOD chains only
                tier_config['bar_sizes'] = []
                tier_config['eod'] = True
            elif not args.all_timeframes:
                # If not all timeframes, use configured defaults
                pass
            
            # Use new chain-based processing
            process_tier_chains(
                ib, tier_name, tier_symbols, {tier_name: tier_config}, 
                progress_tracker, 
                update_mode=args.update or args.daily_snapshot,
                output_dir=OUTPUT_BASE_DIR,
                host=host, port=port, client_id=client_id, 
                styles_config=styles_config,
                days_back=args.days
            )
        
        
        # Cleanup progress file if back-fill completed successfully
        if args.back_fill and not args.symbol:
            print("\n✅ Back-fill completed successfully!")
            progress_tracker.clear()
            print("Progress file cleared for next run")
        
        # Run consolidation unless skipped
        if not args.skip_consolidation:
            print("\n" + "="*60)
            print("Running Consolidation to Continuous Chains")
            print("="*60)
            
            try:
                import subprocess
                
                # Determine which tickers to consolidate
                consolidate_tickers = []
                if args.symbol:
                    consolidate_tickers = [args.symbol]
                elif args.tier:
                    if args.tier == 'T1':
                        consolidate_tickers = TIER1_SYMBOLS
                    elif args.tier == 'T2':
                        consolidate_tickers = TIER2_SYMBOLS
                    elif args.tier == 'T3':
                        consolidate_tickers = TIER3_SYMBOLS if TIER3_SYMBOLS else []
                    elif args.tier == 'ALL':
                        consolidate_tickers = TIER1_SYMBOLS + TIER2_SYMBOLS + (TIER3_SYMBOLS if TIER3_SYMBOLS else [])
                
                # Build consolidation command
                consolidation_script = Path(__file__).parent / 'ibkr_continuous_builder.py'
                cmd = [sys.executable, str(consolidation_script)]
                
                if consolidate_tickers:
                    cmd.extend(['--tickers'] + consolidate_tickers)
                
                if args.bar_size and args.bar_size != 'ALL':
                    # Map bar size to timeframe
                    bar_to_timeframe = {
                        '1 min': '1min',
                        '5 mins': '5min', 
                        '15 mins': '15min',
                        '1 hour': '1hr',
                        '1 day': 'EOD'
                    }
                    if args.bar_size in bar_to_timeframe:
                        cmd.extend(['--timeframes', bar_to_timeframe[args.bar_size]])
                
                print(f"Running: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=False, text=True)
                
                if result.returncode == 0:
                    print("✅ Consolidation completed successfully")
                else:
                    print(f"⚠️ Consolidation completed with warnings (exit code: {result.returncode})")
                    
            except Exception as e:
                logger.error(f"Error running consolidation: {e}")
                print(f"⚠️ Could not run consolidation: {e}")
                print("You can run consolidation manually with: python ibkr_continuous_builder.py")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        print("Progress saved - run again to resume")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
            print("\n✅ Disconnected from IBKR TWS")

if __name__ == '__main__':
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        print("\n⚠️ Received interrupt signal, shutting down gracefully...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    main()
