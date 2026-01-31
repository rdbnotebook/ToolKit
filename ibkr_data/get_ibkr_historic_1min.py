#!/usr/bin/env python3
"""
V3 Version: Retrieve historical 1-minute data with extended hours and walk-backward capability.

This script fetches 1-minute historical data for all non-futures securities with:
- Extended hours data where available (useRTH=False)
- Walk-backward logic to fetch data as far back as possible (e.g., SPY back to 2004)
- Intelligent data type selection based on security type

Key V3 Features:
- Extended hours coverage for all security types
- Walk-backward to fetch historical data beyond initial 5-year window
- Adaptive data fetching based on market type
- Two modes: back-fill (create/overwrite) and update (append only)

Usage:
    # Back-fill mode - create new files or overwrite existing
    python get_historic_1min.py --back-fill                      # Process all securities
    python get_historic_1min.py --back-fill --conid 123          # Process specific security
    python get_historic_1min.py --back-fill --no-walk-backward   # Disable walk-backward
    
    # Update mode - append new data to existing files
    python get_historic_1min.py --update                         # Update all securities
    python get_historic_1min.py --update --conid 123             # Update specific security

Note: One of --back-fill or --update is REQUIRED
"""

import sys
import os
import pandas as pd
import argparse
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
import time
import json
import asyncio
import math

# Add the parent directory to the path so we can import ib_insync directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import IB modules
from ib_insync import IB, Stock, Forex, Index, Crypto, util, Contract, RequestError

# Set up paths relative to project root
from pathlib import Path

# Get the directory of this script
SCRIPT_DIR = Path(__file__).parent
# Get the project root (3 levels up from ibkr-fetch)
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
# Set up paths to bronze storage and logs
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "ibkr" / "historic_data"
BRONZE_DIR_BIDASK = PROJECT_ROOT / "data" / "bronze" / "ibkr" / "historic_data_bidask"
LOG_DIR = SCRIPT_DIR / "logs"
MAX_FETCH_DIR = SCRIPT_DIR / "max_fetch"

# Ensure directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_BIDASK.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
MAX_FETCH_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / 'get_ibkr_historic_1min.log')
    ]
)
logger = logging.getLogger(__name__)

# Output directory will be set based on --bid-ask flag in main()
OUTPUT_DIR = None

DEFAULT_MAX_BIDASK_TIMEOUTS = 3
DEFAULT_BIDASK_CHUNK_DAYS = 3
DEFAULT_BIDASK_MAX_SECONDS = 600
CONNECTION_ERROR_CODES = {1100, 1101, 1102}

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


def _consume_connection_lost_flag(ib) -> bool:
    if not ib:
        return False
    if not getattr(ib, "_ab_connection_lost", False):
        return False
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
    return True

def _write_nonfutures_conid_artifact(rows, output_dir):
    if not rows:
        return
    output_path = Path(output_dir) / "nonfutures_conid.csv"
    df = pd.DataFrame(rows)
    ordered_cols = [
        "ticker",
        "security_type",
        "mode",
        "contract_conid",
        "contract_local_symbol",
        "contract_exchange",
        "contract_primary_exchange",
        "contract_trading_class",
        "contract_currency",
        "fetch_status",
        "data_source",
    ]
    cols = [c for c in ordered_cols if c in df.columns] + [c for c in df.columns if c not in ordered_cols]
    df = df[cols]
    df.to_csv(output_path, index=False)
    print(f"📄 Wrote non-futures conid artifact: {output_path}")


def _print_nonfutures_summary(rows):
    if not rows:
        return
    counts = Counter()
    for row in rows:
        status = row.get("fetch_status") or "unknown"
        counts[status] += 1
    print("\n" + "=" * 80)
    print("NON-FUTURES SUMMARY")
    print("=" * 80)
    if counts:
        summary = ", ".join(f"{status}={count}" for status, count in counts.items())
        print(f"Status counts: {summary}")
    for row in rows:
        ticker = row.get("ticker", "")
        status = row.get("fetch_status", "unknown")
        source = row.get("data_source")
        if source:
            print(f"{ticker}: {status} ({source})")
        else:
            print(f"{ticker}: {status}")
    missing = [row.get("ticker") for row in rows if row.get("fetch_status") == "missing_file"]
    if missing:
        print(f"Missing files: {', '.join(sorted(m for m in missing if m))}")

def attempt_reconnect(host, port, client_id, max_retries=3, prompt_user=True):
    """
    Attempt to reconnect to IBKR TWS after connection loss.
    
    Args:
        host: The hostname or IP address of the IBKR TWS
        port: The port number of the IBKR TWS
        client_id: The client ID to use for the connection
        max_retries: Maximum number of reconnection attempts before prompting user
    
    Returns:
        IB or None: The IB connection object if reconnected, None otherwise
    """
    print("\n" + "!" * 80)
    print("CONNECTION TO IBKR TWS LOST - ATTEMPTING RECONNECTION")
    print("(This might be due to the scheduled IBKR nightly reset around 11:45pm CT)")
    print("!" * 80 + "\n")
    
    logger.warning("Connection to IBKR TWS lost - attempting reconnection")
    
    while True:
        # Try reconnecting max_retries times
        for retry in range(1, max_retries + 1):
            print(f"Reconnection attempt {retry}/{max_retries}...")
            logger.info(f"Reconnection attempt {retry}/{max_retries}")
            
            try:
                ib = IB()
                ib.connect(host, port, clientId=client_id, readonly=True, timeout=30)
                _attach_connection_error_handler(ib)
                print("\n✅ Successfully reconnected to IBKR TWS")
                logger.info("Successfully reconnected to IBKR TWS")
                return ib
            except Exception as e:
                print(f"❌ Reconnection attempt {retry} failed: {e}")
                logger.error(f"Reconnection attempt {retry} failed: {e}")
                # Wait before next retry (increasing backoff)
                wait_time = 10 * retry
                print(f"Waiting {wait_time} seconds before next attempt...")
                time.sleep(wait_time)
        
        # If we reach here, all retry attempts failed
        print("\n" + "=" * 80)
        print("UNABLE TO RECONNECT AUTOMATICALLY")
        print("=" * 80)
        
        if not prompt_user:
            print("Reconnection aborted (non-interactive mode).")
            logger.info("Reconnection aborted (non-interactive mode)")
            return None

        # Prompt user for next action
        while True:
            try:
                choice = input("\nDo you want to try reconnecting again? (Y/N): ").strip().upper()
                if choice in ['Y', 'N']:
                    break
                print("Invalid choice. Please enter Y or N.")
            except KeyboardInterrupt:
                print("\nProcess interrupted by user.")
                return None

        if choice == 'N':
            print("Reconnection cancelled by user. Exiting.")
            logger.info("Reconnection cancelled by user")
            return None
        
        # If user chooses Y, we'll loop back and try again

def connect_to_ibkr(host='127.0.0.1', port=7497, client_id=10):
    """
    Connect directly to IBKR TWS.
    
    Args:
        host: The hostname or IP address of the IBKR TWS
        port: The port number of the IBKR TWS
        client_id: The client ID to use for the connection
    
    Returns:
        IB: The IB connection object
    """
    print(f"Connecting to IBKR TWS at {host}:{port}...")
    logger.info(f"Connecting to IBKR TWS: host={host}, port={port}, client_id={client_id}")
    
    ib = IB()
    
    try:
        ib.connect(host, port, clientId=client_id, readonly=True, timeout=30)
        _attach_connection_error_handler(ib)
        print("✅ Connected to IBKR TWS")
        logger.info("Connected to IBKR TWS")
        
        # Print API version and available accounts
        logger.info(f"API Version: {ib.client.serverVersion()}")
        accounts = ib.managedAccounts()
        logger.info(f"Available accounts: {accounts}")
        
        return ib
    
    except Exception as e:
        print(f"❌ Failed to connect to IBKR TWS: {e}")
        logger.error(f"Failed to connect to IBKR TWS: {e}")
        return None

def create_contract(security):
    """
    Create an IBKR contract object based on the security type.
    
    Args:
        security: A pandas Series with security information
    
    Returns:
        Contract: The IBKR contract object
    """
    try:
        sec_type = security['IBKR_instrument_type'] if pd.notna(security['IBKR_instrument_type']) else None
        symbol = security['FR_Ticker'] if pd.notna(security['FR_Ticker']) else None
        exchange = security['IBKR_exchange'] if pd.notna(security['IBKR_exchange']) else 'SMART'
        currency = security['ibkr_currency'] if pd.notna(security['ibkr_currency']) else 'USD'
        con_id = security['IBKR_Conid'] if pd.notna(security['IBKR_Conid']) else None
        local_symbol = security.get('IBKR_details_local_symbol', None)
        
        # If we have a conid, use it to create the contract (most reliable way)
        if con_id and pd.notna(con_id):
            # Convert to int if it's a float
            if isinstance(con_id, float):
                con_id = int(con_id)
            contract = Contract()
            contract.conId = con_id
            contract.exchange = exchange
            return contract

        # No conid, try to create contract based on security type
        if pd.isna(symbol) or symbol is None:
            logger.warning(f"Missing symbol for security ID {security['Security_ID']}")
            return None

        # Fix sec_type for FOREX if needed
        security_type = security.get('SecurityType', '')
        if security_type == 'fx' or (pd.isna(sec_type) and '.' in symbol):
            sec_type = 'CASH'

        if sec_type == 'STK':
            contract = Stock(symbol, exchange, currency)
        elif sec_type == 'CASH' or sec_type == 'FOREX':
            # For forex, handle symbol in format BASE.QUOTE
            if '.' in symbol:
                base_currency, quote_currency = symbol.split('.')
                contract = Forex(base_currency, quote_currency)
                contract.exchange = exchange
            elif local_symbol and '.' in local_symbol:
                # Try with local_symbol if available
                base_currency, quote_currency = local_symbol.split('.')
                contract = Forex(base_currency, quote_currency)
                contract.exchange = exchange
            else:
                logger.warning(f"Invalid forex symbol format for {symbol}, should be BASE.QUOTE")
                
                # If we can't parse it but we know it's forex, create a generic contract
                if security_type == 'fx' and con_id:
                    contract = Contract()
                    # Convert to int if it's a float
                    if isinstance(con_id, float):
                        con_id = int(con_id)
                    contract.conId = con_id
                    contract.secType = 'CASH'
                    contract.exchange = exchange
                    return contract
                return None
        elif sec_type == 'IND':
            contract = Index(symbol, exchange, currency)
        elif sec_type == 'CRYPTO':
            if '.' in symbol:
                base_currency, quote_currency = symbol.split('.')
                contract = Crypto(base_currency, quote_currency, exchange)
            else:
                contract = Crypto(symbol, currency, exchange)
        else:
            logger.warning(f"Unsupported security type: {sec_type} for {symbol}")
            return None

        return contract
    
    except Exception as e:
        logger.error(f"Error creating contract for {security.get('FR_Ticker', 'Unknown')}: {e}")
        return None

def get_historical_data(
    ib,
    contract,
    ticker,
    end_date=None,
    duration='5 Y',
    bar_size='1 min',
    host=None,
    port=None,
    client_id=None,
    walk_backward=True,
    update_mode=False,
    bid_ask=False,
    daily_index_fallback=False,
    index_midpoint_fallback=False,
    allow_5min_fallback=True,
    max_timeouts=None,
    max_seconds=None,
    disable_max_seconds=False,
    prompt_user=True,
):
    """
    Get historical 1-minute data for a specific contract for the past 5 years.
    
    Args:
        ib: The IB connection object
        contract: The contract object
        ticker: The ticker symbol (for logging)
        end_date: The end date for the data (defaults to now)
        duration: The duration string (e.g., '5 Y', '1 Y', '6 M')
        bar_size: The bar size (e.g., '1 min', '5 mins', '1 hour')
        host: The hostname or IP address of the IBKR TWS (for reconnection)
        port: The port number of the IBKR TWS (for reconnection)
        client_id: The client ID to use for the connection (for reconnection)
        walk_backward: If True, continue fetching older data beyond initial duration (V3 feature)
        update_mode: If True, in update mode (default: False)
        bid_ask: If True, fetch BID_ASK data instead of TRADES/MIDPOINT/AGGTRADES
        daily_index_fallback: If True, allow fallback to daily bars for indices
        index_midpoint_fallback: If True, allow MIDPOINT fallback for indices
        allow_5min_fallback: If False, do not fall back to 5-minute bars
        max_timeouts: Abort after this many timeout errors (None uses defaults)
        max_seconds: Abort after this many seconds spent per symbol (None uses defaults)
        disable_max_seconds: If True, disable per-symbol time budget aborts
        prompt_user: If False, avoid interactive prompts during reconnection handling
    
    Returns:
        pd.DataFrame: DataFrame containing historical data
    """
    # Create a variable to hold the connection that can be updated
    # This is a workaround for the nonlocal issue
    connection = {'ib': ib}
    timeout_failures = 0
    if max_timeouts is None:
        max_timeouts = DEFAULT_MAX_BIDASK_TIMEOUTS if bid_ask else DEFAULT_MAX_BIDASK_TIMEOUTS + 2
    if disable_max_seconds:
        max_seconds = None
    elif max_seconds is None and bid_ask:
        max_seconds = DEFAULT_BIDASK_MAX_SECONDS
    symbol_start_time = time.time()

    def _time_budget_exceeded() -> bool:
        if max_seconds is None:
            return False
        elapsed = time.time() - symbol_start_time
        if elapsed >= max_seconds:
            msg = (
                f"⏱️ Aborting {ticker} after {elapsed:.1f}s "
                f"(limit {max_seconds}s)"
            )
            print(msg)
            logger.warning(msg)
            return True
        return False

    if connection['ib'] is not None:
        _attach_connection_error_handler(connection['ib'])

    def _ensure_connection() -> bool:
        ib_obj = connection['ib']
        if _consume_connection_lost_flag(ib_obj) or not ib_obj or not ib_obj.isConnected():
            reconnect_host = host or getattr(ib_obj, 'host', '127.0.0.1') if ib_obj else host
            reconnect_port = port or getattr(ib_obj, 'port', 7497) if ib_obj else port
            reconnect_client_id = client_id if client_id is not None else getattr(ib_obj, 'clientId', 22) if ib_obj else 22
            if reconnect_host is None or reconnect_port is None:
                return False
            new_ib = attempt_reconnect(reconnect_host, reconnect_port, reconnect_client_id, prompt_user=prompt_user)
            if not new_ib:
                return False
            connection['ib'] = new_ib
        return True
    
    try:
        # Format end_datetime properly for IBKR API - avoid timezone issues
        if end_date is None:
            # Use UTC time when emitting dash-formatted timestamps (IB treats dash as UTC).
            end_date_obj = datetime.now(timezone.utc)
        else:
            end_date_obj = end_date
            if end_date_obj.tzinfo is not None:
                end_date_obj = end_date_obj.astimezone(timezone.utc)
        end_date_obj = end_date_obj.replace(tzinfo=None)
        # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
        end_datetime = end_date_obj.strftime("%Y%m%d-%H:%M:%S")
        
        # For FOREX pairs, try a different approach if contract is of type CASH
        is_forex = contract.secType == 'CASH'
        is_index = contract.secType == 'IND'
        is_crypto = contract.secType == 'CRYPTO'
        
        # Set appropriate whatToShow based on security type and bid_ask flag
        # For bid_ask=True, we will request BID and ASK separately and merge.
        if bid_ask:
            what_to_show = None
        elif is_forex:
            what_to_show = 'MIDPOINT'
        elif is_crypto:
            what_to_show = 'AGGTRADES'  # Use AGGTRADES for crypto instead of TRADES
        else:
            what_to_show = 'TRADES'
        index_midpoint_fallback_used = False
            
        # V3: Use extended hours for all security types to capture maximum data
        use_rth = False  # Changed to False in V3 for extended hours coverage
        
        print(f"Requesting {bar_size} bars for {ticker} with duration {duration}...")
        logger.info(f"Requesting historical data for {ticker} with bar size {bar_size}, duration {duration}")
        logger.info(f"Using end date/time: {end_datetime}")
        if bid_ask:
            logger.info("Using separate BID and ASK requests (merge later)")
        else:
            logger.info(f"Using whatToShow: {what_to_show}")
        
        # Request historical data - IBKR has limits on how much data can be retrieved at once
        # We'll need to handle this with pagination for long durations
        all_bars = []
        all_bars_bid = [] if bid_ask else None
        all_bars_ask = [] if bid_ask else None
        daily_fallback_used = False

        def _try_daily_index_fallback() -> bool:
            nonlocal daily_fallback_used, all_bars, bar_size
            if not (is_index and daily_index_fallback and not bid_ask):
                return False
            print(f"No intraday data for index {ticker}. Trying daily bars...")
            logger.info(f"No intraday data for index {ticker}; attempting daily bars")
            if not _ensure_connection():
                return False
            try:
                daily_bars = connection['ib'].reqHistoricalData(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=duration,
                    barSizeSetting='1 day',
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=2,
                )
            except Exception as e:
                print(f"Error retrieving daily data for {ticker}: {e}")
                logger.warning(f"Error retrieving daily data for {ticker}: {e}")
                return False
            if daily_bars and len(daily_bars) > 0:
                print(f"Retrieved {len(daily_bars)} daily bars for {ticker}")
                all_bars = daily_bars
                bar_size = '1 day'
                daily_fallback_used = True
                return True
            print(f"No daily data available for {ticker}.")
            logger.warning(f"No daily data available for {ticker}")
            return False
        
        # For forex, we'll only use MIDPOINT (already set above)
        # No fallback attempts - if MIDPOINT doesn't work, we'll try 5-minute bars
        
        # Try to get 1-minute data first
        if bar_size == '1 min':
            has_1min = False
            if not _ensure_connection():
                return None
            # Test if 1-minute data is available
            try:
                if bid_ask:
                    print("Testing availability of 1-minute BID and ASK bars...")
                    test_bars_bid = connection['ib'].reqHistoricalData(
                        contract,
                        endDateTime=end_datetime,
                        durationStr='1 D',
                        barSizeSetting=bar_size,
                        whatToShow='BID',
                        useRTH=use_rth,
                        formatDate=2
                    )
                    test_bars_ask = connection['ib'].reqHistoricalData(
                        contract,
                        endDateTime=end_datetime,
                        durationStr='1 D',
                        barSizeSetting=bar_size,
                        whatToShow='ASK',
                        useRTH=use_rth,
                        formatDate=2
                    )
                    has_1min = (test_bars_bid and len(test_bars_bid) > 0) or (test_bars_ask and len(test_bars_ask) > 0)
                    if has_1min:
                        print("Found 1-minute BID/ASK data. Proceeding with full request.")
                    else:
                        print(f"No 1-minute BID/ASK data available for {ticker}.")
                else:
                    test_bars = connection['ib'].reqHistoricalData(
                        contract,
                        endDateTime=end_datetime,
                        durationStr='1 D',  # Just 1 day to test
                        barSizeSetting=bar_size,
                        whatToShow=what_to_show,
                        useRTH=use_rth,
                        formatDate=2  # Use formatDate=2 for timezone-aware timestamps
                    )
                    
                    if test_bars and len(test_bars) > 0:
                        print(f"Found 1-minute data using {what_to_show}. Proceeding with full request.")
                    else:
                        if is_index and index_midpoint_fallback and what_to_show != 'MIDPOINT':
                            print(f"No 1-minute TRADES data for index {ticker}. Trying MIDPOINT...")
                            try:
                                midpoint_bars = connection['ib'].reqHistoricalData(
                                    contract,
                                    endDateTime=end_datetime,
                                    durationStr='1 D',
                                    barSizeSetting=bar_size,
                                    whatToShow='MIDPOINT',
                                    useRTH=use_rth,
                                    formatDate=2
                                )
                                if midpoint_bars and len(midpoint_bars) > 0:
                                    print(f"Found 1-minute data using MIDPOINT for index {ticker}.")
                                    what_to_show = 'MIDPOINT'
                                    index_midpoint_fallback_used = True
                                    test_bars = midpoint_bars
                                else:
                                    print(f"No 1-minute MIDPOINT data available for index {ticker}.")
                                    test_bars = None
                            except Exception as e:
                                print(f"Error testing MIDPOINT data for index {ticker}: {e}")
                                test_bars = None
                        else:
                            print(f"No 1-minute data available for {ticker}.")
                            test_bars = None
            except Exception as e:
                print(f"Error testing 1-minute data: {e}")
                test_bars = None
                # Check for connection issues
                error_msg = str(e)
                connection_lost = any(err in error_msg.lower() for err in 
                                      ["connection refused", "not connected", "peer closed connection", 
                                       "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                
                if connection_lost and host and port and client_id:
                    print("Detected possible connection loss during data testing.")
                    # Try to reconnect
                    new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                    if new_ib:
                        # Reconnection successful, update the connection
                        connection['ib'] = new_ib
                    else:
                        # User chose to abort
                        print("Reconnection aborted. Exiting.")
                        return None
            
            # If no 1-minute data, try 5-minute bars
            if (bid_ask and not has_1min) or (not bid_ask and (not test_bars or len(test_bars) == 0)):
                has_5min = False
                if not allow_5min_fallback:
                    if _try_daily_index_fallback():
                        # Daily fallback succeeded; skip 5-minute fallback.
                        pass
                    else:
                        print(
                            f"No 1-minute data available for {ticker} and "
                            "5-minute fallback is disabled. Stopping data retrieval."
                        )
                        logger.warning(
                            f"No 1-minute data available for {ticker}; 5-minute fallback disabled"
                        )
                        return None
                if not daily_fallback_used:
                    print(f"No 1-minute data available for {ticker}. Trying with 5-minute bars...")
                    bar_size = '5 mins'
                
                # Try with 5-minute bars
                if not _ensure_connection():
                    return None
                try:
                    if bid_ask:
                        test_bars_bid = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=end_datetime,
                            durationStr='1 D',
                            barSizeSetting=bar_size,
                            whatToShow='BID',
                            useRTH=use_rth,
                            formatDate=2
                        )
                        test_bars_ask = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=end_datetime,
                            durationStr='1 D',
                            barSizeSetting=bar_size,
                            whatToShow='ASK',
                            useRTH=use_rth,
                            formatDate=2
                        )
                        has_5min = (test_bars_bid and len(test_bars_bid) > 0) or (test_bars_ask and len(test_bars_ask) > 0)
                        if has_5min:
                            print(f"Found 5-minute BID/ASK data. Proceeding with 5-minute bars.")
                        else:
                            print(f"No 5-minute BID/ASK data available for {ticker}.")
                    else:
                        test_bars = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=end_datetime,
                            durationStr='1 D',
                            barSizeSetting=bar_size,
                            whatToShow=what_to_show,  # Keep the same whatToShow
                            useRTH=use_rth,
                            formatDate=2  # Use formatDate=2 for timezone-aware timestamps
                        )
                        
                        if test_bars and len(test_bars) > 0:
                            print(f"Found 5-minute data. Proceeding with 5-minute bars.")
                        else:
                            print(f"No 5-minute data available for {ticker}.")
                except Exception as e:
                    print(f"Error testing 5-minute data: {e}")
                    # Check for connection issues
                    error_msg = str(e)
                    connection_lost = any(err in error_msg.lower() for err in 
                                          ["connection refused", "not connected", "peer closed connection", 
                                           "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                    
                    if connection_lost and host and port and client_id:
                        print("Detected possible connection loss during bar size testing.")
                        # Try to reconnect
                        new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                        if new_ib:
                            # Reconnection successful, update the connection and continue
                            connection['ib'] = new_ib
                        else:
                            # User chose to abort
                            print("Reconnection aborted. Exiting.")
                            return None
                
                # If no 5-minute data either, stop here (unless index daily fallback is enabled)
                if (not daily_fallback_used) and (
                    (bid_ask and not has_5min) or (not bid_ask and (not test_bars or len(test_bars) == 0))
                ):
                    if _try_daily_index_fallback():
                        # Daily fallback succeeded; skip 5-minute fallback.
                        pass
                    else:
                        print(f"No 5-minute data available for {ticker}. Stopping data retrieval.")
                        logger.warning(f"No historical data available for {ticker} at requested granularity")
                        return None
        
        # For 1-minute data, use smaller time chunks to avoid timeout/cancellation
        if bar_size == '1 min':
            # Use shorter chunk sizes for bid/ask and crypto to avoid timeouts
            if bid_ask:
                chunk_days = DEFAULT_BIDASK_CHUNK_DAYS
            elif is_crypto:
                chunk_days = 3
            else:
                chunk_days = 7
            chunk_delta = timedelta(days=chunk_days)
            
            # Parse duration to estimate start date
            duration_value = int(duration.split()[0])
            duration_unit = duration.split()[1].upper()
            
            if duration_unit == 'Y':
                lookback_days = duration_value * 365
            elif duration_unit == 'M':
                lookback_days = duration_value * 30
            elif duration_unit == 'W':
                lookback_days = duration_value * 7
            else:  # D
                lookback_days = duration_value
            
            # Calculate approximate start date - make sure it's a naive datetime like end_date_obj
            start_date_obj = end_date_obj.replace(tzinfo=None) - timedelta(days=lookback_days)
            
            print(f"Retrieving data from {start_date_obj.date()} to {end_date_obj.date()} in {chunk_days}-day chunks")
            
            # Start from the end date and work backwards in chunks
            current_end = end_date_obj.replace(tzinfo=None)  # Ensure it's a naive datetime
            chunk_count = 0
            # Set the max chunks dynamically based on requested duration and chunk size
            max_chunks = max(10, math.ceil(lookback_days / chunk_days))
            max_retries = 3  # Maximum number of retries per chunk
            consecutive_empty_periods = 0  # Track consecutive periods with no data
            empty_response_retries = 0  # Track retries for current empty period
            max_consecutive_empty_periods = 3  # Stop after 3 consecutive periods fail completely
            
            # Helper to (re)fetch a single side when the other side succeeded
            def _refetch_missing_side(side_label, end_dt, days, bar_size_setting, use_rth_flag):
                """Attempt to retrieve missing BID or ASK bars for a given window using progressively smaller slices.
                Returns list of bars (may be empty)."""
                results = []
                # First attempt: same full window
                try_durations = []
                if days >= 1:
                    try_durations.append(f"{days} D")
                if days >= 3:
                    try_durations.append("3 D")
                # Small pacing between sequential requests
                def _req(end_str, dur):
                    return connection['ib'].reqHistoricalData(
                        contract,
                        endDateTime=end_str,
                        durationStr=dur,
                        barSizeSetting=bar_size_setting,
                        whatToShow=side_label,
                        useRTH=use_rth_flag,
                        formatDate=2,
                        timeout=150
                    )
                # Try full-size then 3D
                base_wait = 2.0
                for attempt, dur in enumerate(try_durations):
                    end_str = end_dt.strftime("%Y%m%d-23:59:59")
                    wait = min(base_wait * (2 ** attempt), 30.0)
                    try:
                        time.sleep(wait)
                        bars_side = _req(end_str, dur)
                        if bars_side:
                            results.extend(bars_side)
                            # If we got something with a bigger window, no need to sub-slice further
                            return results
                    except Exception as _:
                        # ignore and continue to finer slices
                        pass
                # Final attempt: 1-day slices across the window
                base_slice_wait = 1.0
                for i in range(0, days):
                    sub_end = (end_dt - timedelta(days=i))
                    end_str = sub_end.strftime("%Y%m%d-23:59:59")
                    try:
                        wait = min(base_slice_wait * (2 ** min(i, 3)), 8.0)
                        time.sleep(wait)
                        bars_side = _req(end_str, "1 D")
                        if bars_side:
                            results.extend(bars_side)
                    except Exception:
                        continue
                return results

            while current_end > start_date_obj and chunk_count < max_chunks and consecutive_empty_periods < max_consecutive_empty_periods:
                if _time_budget_exceeded():
                    return None
                if not _ensure_connection():
                    return None
                # Only increment chunk count if not retrying
                if empty_response_retries == 0:
                    chunk_count += 1
                    
                    # Format the current end date for the request without timezone
                    # Use explicit UTC timezone to avoid IBKR warnings
                    # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
                    current_end_str = current_end.strftime("%Y%m%d-%H:%M:%S")
                    
                    # For the first chunk, use the exact end time; for subsequent chunks, use end of day
                    if chunk_count == 1:
                        request_end = current_end_str
                    else:
                        # Use explicit UTC timezone to avoid IBKR warnings
                        # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
                        request_end = current_end.strftime("%Y%m%d-23:59:59")
                    
                    print(f"Requesting chunk {chunk_count}: data ending at {current_end.date()}...")
                else:
                    # Retrying same period - request_end stays the same
                    print(f"Retrying chunk {chunk_count} (attempt {empty_response_retries + 1}/3)...")
                
                # Flag to track if this chunk succeeded
                chunk_succeeded = False
                
                try:
                    chunk_duration = f"{chunk_days} D"
                    
                    # Set a timeout for historical data request (increased default)
                    timeout_seconds = 150  # 2.5 minutes per request
                    
                    # Make the request(s)
                    if bid_ask:
                        # Request BID and ASK separately
                        bars_bid = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=request_end,
                            durationStr=chunk_duration,
                            barSizeSetting=bar_size,
                            whatToShow='BID',
                            useRTH=use_rth,
                            formatDate=2,  # Use formatDate=2 for timezone-aware timestamps
                            timeout=timeout_seconds
                        )
                        # Small pacing between side requests
                        time.sleep(0.75)
                        bars_ask = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=request_end,
                            durationStr=chunk_duration,
                            barSizeSetting=bar_size,
                            whatToShow='ASK',
                            useRTH=use_rth,
                            formatDate=2,  # Use formatDate=2 for timezone-aware timestamps
                            timeout=timeout_seconds
                        )
                        # If one side is missing but the other succeeded, retry the missing side with smaller slices
                        cnt_b0 = len(bars_bid) if bars_bid else 0
                        cnt_a0 = len(bars_ask) if bars_ask else 0
                        if (cnt_b0 == 0 and cnt_a0 > 0) or (cnt_a0 == 0 and cnt_b0 > 0):
                            print("Partial success detected; retrying missing side with smaller slices...")
                            if cnt_b0 == 0 and cnt_a0 > 0:
                                refill = _refetch_missing_side(
                                    'BID',
                                    current_end if chunk_count > 1 else pd.to_datetime(current_end_str, format="%Y%m%d-%H:%M:%S"),
                                    chunk_days,
                                    bar_size,
                                    use_rth,
                                )
                                if refill:
                                    bars_bid = (bars_bid or []) + refill
                            if cnt_a0 == 0 and cnt_b0 > 0:
                                refill = _refetch_missing_side(
                                    'ASK',
                                    current_end if chunk_count > 1 else pd.to_datetime(current_end_str, format="%Y%m%d-%H:%M:%S"),
                                    chunk_days,
                                    bar_size,
                                    use_rth,
                                )
                                if refill:
                                    bars_ask = (bars_ask or []) + refill
                        cnt_b = len(bars_bid) if bars_bid else 0
                        cnt_a = len(bars_ask) if bars_ask else 0
                        if cnt_b > 0 and cnt_a > 0:
                            print(f"Retrieved {cnt_b} BID and {cnt_a} ASK bars for chunk {chunk_count}")
                            # Show a sample
                            temp_df = util.df(bars_bid)
                            print("\nSample of received data (first 5 rows):")
                            print(temp_df.head()[['date', 'open', 'high', 'low', 'close', 'volume']])
                            all_bars_bid.extend(bars_bid)
                            all_bars_ask.extend(bars_ask)
                            chunk_succeeded = True
                            # Reset counters on success
                            consecutive_empty_periods = 0
                            empty_response_retries = 0
                        elif cnt_b == 0 and cnt_a == 0:
                            # No data returned
                            empty_response_retries += 1
                            print(f"Empty response (retry {empty_response_retries}/3 for period {consecutive_empty_periods + 1})")
                            
                            if empty_response_retries < 3:
                                # Retry the same period with increasing wait times
                                wait_times = [10, 20, 30]  # 10s, 20s, 30s
                                wait_time = wait_times[empty_response_retries - 1]
                                print(f"Retrying same period after {wait_time} seconds...")
                                time.sleep(wait_time)
                                continue  # Retry the same period
                            else:
                                # After 3 retries, mark this period as empty and move on
                                consecutive_empty_periods += 1
                                empty_response_retries = 0  # Reset retry counter for next period
                                print(f"Period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/3")
                                
                                if consecutive_empty_periods >= 3:
                                    if is_index and index_midpoint_fallback and not index_midpoint_fallback_used and what_to_show != 'MIDPOINT':
                                        print(f"No additional TRADES data for index {ticker}. Switching to MIDPOINT and retrying...")
                                        logger.info(f"[{ticker}] switching to MIDPOINT after consecutive empty TRADES periods")
                                        what_to_show = 'MIDPOINT'
                                        index_midpoint_fallback_used = True
                                        consecutive_empty_periods = 0
                                        empty_response_retries = 0
                                        continue
                                    print(f"Stopping: 3 consecutive periods with no data after retries")
                                    break
                                else:
                                    print(f"Moving to next period despite empty data...")
                                    # Move back in time for next chunk
                                    current_end = current_end - chunk_delta
                                    continue
                        else:
                            # Partial data only - treat as gap (discard this window to avoid single-sided bars)
                            print("Partial data only for this period; discarding to avoid single-sided bars.")
                            consecutive_empty_periods += 1
                            empty_response_retries = 0
                            if consecutive_empty_periods >= 3:
                                print(f"Stopping: 3 consecutive periods with failures")
                                break
                            current_end = current_end - chunk_delta
                            continue
                    else:
                        bars = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=request_end,
                            durationStr=chunk_duration,
                            barSizeSetting=bar_size,
                            whatToShow=what_to_show,
                            useRTH=use_rth,
                            formatDate=2,  # Use formatDate=2 for timezone-aware timestamps
                            timeout=timeout_seconds
                        )
                        
                        if bars and len(bars) > 0:
                            print(f"Retrieved {len(bars)} bars for chunk {chunk_count}")
                            
                            # Show sample of the received data (first 5 rows)
                            temp_df = util.df(bars)
                            print("\nSample of received data (first 5 rows):")
                            print(temp_df.head()[['date', 'open', 'high', 'low', 'close', 'volume']])
                            
                            all_bars.extend(bars)
                            chunk_succeeded = True
                            
                            # Reset counters on success
                            consecutive_empty_periods = 0
                            empty_response_retries = 0
                        else:
                            # No data returned
                            empty_response_retries += 1
                            print(f"Empty response (retry {empty_response_retries}/3 for period {consecutive_empty_periods + 1})")
                            
                            if empty_response_retries < 3:
                                # Retry the same period with increasing wait times
                                wait_times = [10, 20, 30]  # 10s, 20s, 30s
                                wait_time = wait_times[empty_response_retries - 1]
                                print(f"Retrying same period after {wait_time} seconds...")
                                time.sleep(wait_time)
                                continue  # Retry the same period
                            else:
                                # After 3 retries, mark this period as empty and move on
                                consecutive_empty_periods += 1
                                empty_response_retries = 0  # Reset retry counter for next period
                                print(f"Period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/3")
                                
                                if consecutive_empty_periods >= 3:
                                    print(f"Stopping: 3 consecutive periods with no data after retries")
                                    break
                                else:
                                    print(f"Moving to next period despite empty data...")
                                    # Move back in time for next chunk
                                    current_end = current_end - chunk_delta
                                    continue
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error retrieving chunk {chunk_count}: {error_msg}")
                    logger.error(f"Error retrieving data for {ticker} chunk {chunk_count}: {e}")
                    
                    # Check for connection-related errors
                    connection_lost = any(err in error_msg.lower() for err in 
                                        ["connection refused", "not connected", "peer closed connection", 
                                         "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                    
                    if connection_lost and host and port and client_id:
                        print("Detected possible connection loss to IBKR TWS.")
                        # Try to reconnect
                        new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                        if new_ib:
                            # Reconnection successful, update the connection
                            connection['ib'] = new_ib
                            # Don't increment retry counter, just retry
                            continue
                        else:
                            # User chose to abort
                            print("Reconnection aborted. Exiting.")
                            return None
                    
                    # Handle specific error types
                    if "API historical data query cancelled" in error_msg or "pacing violation" in error_msg.lower():
                        empty_response_retries += 1
                        print(f"API cancelled/pacing issue (retry {empty_response_retries}/3)")
                        
                        if empty_response_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[empty_response_retries - 1]
                            print(f"Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            empty_response_retries = 0
                            print(f"Period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/3")
                            
                            if consecutive_empty_periods >= 3:
                                print(f"Stopping: 3 consecutive periods with failures")
                                break
                            else:
                                current_end = current_end - chunk_delta
                                continue
                    elif "TimeoutError" in error_msg or "timeout" in error_msg.lower():
                        timeout_failures += 1
                        if timeout_failures >= max_timeouts:
                            logger.warning(
                                f"Aborting {ticker} after {timeout_failures} timeouts"
                            )
                            return None
                        empty_response_retries += 1
                        print(f"Timeout (retry {empty_response_retries}/3)")
                        
                        if empty_response_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[empty_response_retries - 1]
                            print(f"Retrying after {wait_time} seconds...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            empty_response_retries = 0
                            print(f"Period failed after timeouts. Consecutive empty periods: {consecutive_empty_periods}/3")
                            
                            if consecutive_empty_periods >= 3:
                                break
                            else:
                                current_end = current_end - chunk_delta
                                continue
                    else:
                        # Other errors
                        empty_response_retries += 1
                        print(f"Error (retry {empty_response_retries}/3)")
                        
                        if empty_response_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[empty_response_retries - 1]
                            print(f"Retrying after {wait_time} seconds...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            empty_response_retries = 0
                            print(f"Period failed. Consecutive empty periods: {consecutive_empty_periods}/3")
                            
                            if consecutive_empty_periods >= 3:
                                break
                            else:
                                current_end = current_end - chunk_delta
                                continue
                
                # If we got here and succeeded, move to next chunk
                if chunk_succeeded:
                    # Move back in time for the next chunk based on the data we just retrieved
                    if bid_ask:
                        # Determine the earliest timestamp from the current chunk's BID/ASK bars
                        dates = []
                        try:
                            if 'bars_bid' in locals() and bars_bid:
                                dates.append(min(b.date for b in bars_bid))
                            if 'bars_ask' in locals() and bars_ask:
                                dates.append(min(a.date for a in bars_ask))
                        except Exception:
                            dates = dates  # keep whatever we have
                        if dates:
                            earliest_date = pd.to_datetime(min(dates)).replace(tzinfo=None)
                            current_end = earliest_date - timedelta(days=1)
                        else:
                            # Fallback: move back by the chunk duration
                            current_end = current_end - chunk_delta
                    elif bars and len(bars) > 0:
                        # Get the earliest date in the bars
                        earliest_date = pd.to_datetime(min(bar.date for bar in bars)).replace(tzinfo=None)
                        # Set the next end date to one day before the earliest date
                        current_end = earliest_date - timedelta(days=1)
                    else:
                        # Shouldn't get here if chunk_succeeded is True, but just in case
                        current_end = current_end - chunk_delta
                    
                    # Add a pause between successful chunks to avoid overwhelming the API
                    time.sleep(2)
            
            # After the loop, check why we stopped
            if consecutive_empty_periods >= max_consecutive_empty_periods:
                print(f"⚠️ Stopping data retrieval for {ticker} after {max_consecutive_empty_periods} consecutive failed periods")
                logger.warning(f"Stopped data retrieval for {ticker} after {max_consecutive_empty_periods} consecutive failed periods")
                
                # If we failed to get 1-minute data after multiple attempts and this is an index,
                # try falling back to daily data as a last resort
                if is_index and not bid_ask and not all_bars and daily_index_fallback:
                    print(f"No 1-minute data available for index {ticker}. Falling back to daily data as a last resort.")
                    try:
                        if not _ensure_connection():
                            return None
                        daily_bars = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=end_datetime,
                            durationStr=duration,
                            barSizeSetting='1 day',
                            whatToShow=what_to_show,
                            useRTH=use_rth,
                            formatDate=2  # Use formatDate=2 for timezone-aware timestamps
                        )
                        
                        if daily_bars and len(daily_bars) > 0:
                            print(f"Successfully retrieved {len(daily_bars)} daily bars for {ticker}")
                            all_bars = daily_bars
                    except Exception as e:
                        error_msg = str(e)
                        print(f"Error retrieving daily data: {error_msg}")
                        
                        # Check for connection-related errors
                        connection_lost = any(err in error_msg.lower() for err in 
                                            ["connection refused", "not connected", "peer closed connection", 
                                             "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                        
                        if connection_lost and host and port and client_id:
                            print("Detected possible connection loss to IBKR TWS.")
                            # Try to reconnect
                            new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                            if new_ib:
                                # Reconnection successful, update the connection and try again
                                connection['ib'] = new_ib
                                try:
                                    daily_bars = connection['ib'].reqHistoricalData(
                                        contract,
                                        endDateTime=end_datetime,
                                        durationStr=duration,
                                        barSizeSetting='1 day',
                                        whatToShow=what_to_show,
                                        useRTH=use_rth,
                                        formatDate=2  # Use formatDate=2 for timezone-aware timestamps
                                    )
                                    
                                    if daily_bars and len(daily_bars) > 0:
                                        print(f"Successfully retrieved {len(daily_bars)} daily bars for {ticker}")
                                        all_bars = daily_bars
                                except Exception as e2:
                                    print(f"Error retrieving daily data after reconnection: {e2}")
                            else:
                                print("Reconnection aborted. Exiting.")
                                return None
            
        # V3: Walk-backward phase - continue fetching older data if enabled
        # Disable walk-backward in update mode
        if walk_backward and not update_mode and (
            (not bid_ask and all_bars and len(all_bars) > 0) or
            (bid_ask and ((all_bars_bid and len(all_bars_bid) > 0) or (all_bars_ask and len(all_bars_ask) > 0)))
        ) and bar_size == '1 min':
            print("\n🔄 V3: Starting walk-backward phase to fetch older data...")
            logger.info(f"Starting walk-backward phase for {ticker}")
            
            # Get the earliest date from existing data
            if bid_ask:
                candidate_dates = []
                if all_bars_bid and len(all_bars_bid) > 0:
                    candidate_dates.append(min(b.date for b in all_bars_bid))
                if all_bars_ask and len(all_bars_ask) > 0:
                    candidate_dates.append(min(a.date for a in all_bars_ask))
                earliest_date = pd.to_datetime(min(candidate_dates)).replace(tzinfo=None)
            else:
                earliest_bar = min(all_bars, key=lambda x: x.date)
                earliest_date = pd.to_datetime(earliest_bar.date).replace(tzinfo=None)
            
            print(f"Earliest data point: {earliest_date}")
            print(f"Will continue fetching backwards from this point...")
            
            # Set a hard stop date at January 1, 2005
            hard_stop_date = datetime(2005, 1, 1)
            print(f"Hard stop date: {hard_stop_date.date()} (will not fetch data before this date)")
            
            # Walk backward in chunks
            walk_chunk_count = 0
            consecutive_empty_periods = 0
            max_consecutive_empty_periods = 3  # Stop after 3 consecutive periods fail (each with 3 retries)
            walk_backward_empty_retries = 0  # Retry counter for current period
            walk_days = chunk_days
            walk_delta = timedelta(days=walk_days)
            
            current_end = earliest_date - timedelta(days=1)  # Start from day before earliest data
            
            while current_end > hard_stop_date and consecutive_empty_periods < max_consecutive_empty_periods:
                if _time_budget_exceeded():
                    return None
                if not _ensure_connection():
                    return None
                walk_chunk_count += 1
                
                # Only format date and print message if not retrying
                if walk_backward_empty_retries == 0:
                    # Format the end date for the request
                    # Use explicit UTC timezone to avoid IBKR warnings
                    # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
                    walk_end_str = current_end.strftime("%Y%m%d-23:59:59")
                    print(f"\nWalk-backward chunk {walk_chunk_count}: Requesting data ending at {current_end.date()}...")
                else:
                    # Retrying same period
                    walk_chunk_count -= 1  # Keep chunk count consistent
                    print(f"\nRetrying walk-backward chunk {walk_chunk_count} (attempt {walk_backward_empty_retries + 1}/3)...")
                
                try:
                    walk_duration = f"{walk_days} D"
                    
                    if bid_ask:
                        bars_bid = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=walk_end_str,
                            durationStr=walk_duration,
                            barSizeSetting=bar_size,
                            whatToShow='BID',
                            useRTH=use_rth,
                            formatDate=2,
                            timeout=90
                        )
                        time.sleep(0.75)
                        bars_ask = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=walk_end_str,
                            durationStr=walk_duration,
                            barSizeSetting=bar_size,
                            whatToShow='ASK',
                            useRTH=use_rth,
                            formatDate=2,
                            timeout=90
                        )
                        # Retry missing side if partial
                        cnt_b0 = len(bars_bid) if bars_bid else 0
                        cnt_a0 = len(bars_ask) if bars_ask else 0
                        if (cnt_b0 == 0 and cnt_a0 > 0) or (cnt_a0 == 0 and cnt_b0 > 0):
                            print("Partial success detected in walk-back; retrying missing side with smaller slices...")
                            days = walk_days
                            end_dt = current_end  # walk-back current_end corresponds to walk_end day
                            if cnt_b0 == 0 and cnt_a0 > 0:
                                refill = _refetch_missing_side('BID', end_dt, days, bar_size, use_rth)
                                if refill:
                                    bars_bid = (bars_bid or []) + refill
                            if cnt_a0 == 0 and cnt_b0 > 0:
                                refill = _refetch_missing_side('ASK', end_dt, days, bar_size, use_rth)
                                if refill:
                                    bars_ask = (bars_ask or []) + refill
                        cnt_b = len(bars_bid) if bars_bid else 0
                        cnt_a = len(bars_ask) if bars_ask else 0
                        if cnt_b > 0 and cnt_a > 0:
                            print(f"Retrieved {cnt_b} BID and {cnt_a} ASK bars in walk-backward chunk {walk_chunk_count}")
                            all_bars_bid = bars_bid + all_bars_bid
                            all_bars_ask = bars_ask + all_bars_ask
                            consecutive_empty_periods = 0
                            walk_backward_empty_retries = 0
                            total = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                            print(f"Total BID+ASK bars collected so far: {total}")
                            current_end = current_end - walk_delta
                            if current_end.year <= 2004:
                                print(
                                    f"\n⛔ Stopping walk-backward: Next request would be in year "
                                    f"{current_end.year} (stopping at 2004 boundary)"
                                )
                                break
                        elif cnt_b == 0 and cnt_a == 0:
                            # No data returned - implement retry logic
                            walk_backward_empty_retries += 1
                            print(f"Empty response in walk-backward (retry {walk_backward_empty_retries}/3 for period {consecutive_empty_periods + 1})")
                            
                            if walk_backward_empty_retries < 3:
                                # Retry the same period with increasing wait times
                                wait_times = [10, 20, 30]  # 10s, 20s, 30s
                                wait_time = wait_times[walk_backward_empty_retries - 1]
                                print(f"Retrying same walk-backward period after {wait_time} seconds...")
                                time.sleep(wait_time)
                                continue  # Retry the same period
                            # After 3 retries, mark this period as empty and move on
                            consecutive_empty_periods += 1
                            walk_backward_empty_retries = 0  # Reset retry counter for next period
                            print(f"Walk-backward period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                            
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                if is_index and index_midpoint_fallback and not index_midpoint_fallback_used and what_to_show != 'MIDPOINT':
                                    print(f"No additional TRADES data for index {ticker} in walk-backward. Switching to MIDPOINT and retrying...")
                                    logger.info(f"[{ticker}] switching to MIDPOINT for walk-backward after consecutive empty TRADES periods")
                                    what_to_show = 'MIDPOINT'
                                    index_midpoint_fallback_used = True
                                    consecutive_empty_periods = 0
                                    walk_backward_empty_retries = 0
                                    continue
                                print(f"Stopping walk-backward: {max_consecutive_empty_periods} consecutive periods with no data after retries")
                                break
                            else:
                                # Move back by fixed 7 days to try an earlier period
                                current_end = current_end - walk_delta
                                
                                # Check if we've hit 2004 or earlier - immediate stop
                                if current_end.year <= 2004:
                                    print(f"\n⛔ Stopping walk-backward: Next request would be in year {current_end.year} (stopping at 2004 boundary)")
                                    break
                        else:
                            # Partial data only - discard to avoid single-sided rows
                            print("Partial data only in walk-backward; discarding this period to avoid single-sided bars.")
                            consecutive_empty_periods += 1
                            walk_backward_empty_retries = 0
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                print(f"Stopping walk-backward: {max_consecutive_empty_periods} consecutive periods with failures")
                                break
                            current_end = current_end - walk_delta
                            if current_end.year <= 2004:
                                print(f"\n⛔ Stopping walk-backward: Next request would be in year {current_end.year} (stopping at 2004 boundary)")
                                break
                    else:
                        bars = connection['ib'].reqHistoricalData(
                            contract,
                            endDateTime=walk_end_str,
                            durationStr=walk_duration,
                            barSizeSetting=bar_size,
                            whatToShow=what_to_show,
                            useRTH=use_rth,
                            formatDate=2,  # Use formatDate=2 for timezone-aware timestamps
                            timeout=20
                        )
                        
                        if bars and len(bars) > 0:
                            print(f"Retrieved {len(bars)} bars in walk-backward chunk {walk_chunk_count}")
                            
                            # Add to the beginning of all_bars (older data first)
                            all_bars = bars + all_bars
                            
                            # Reset counters on success
                            consecutive_empty_periods = 0
                            walk_backward_empty_retries = 0
                            
                            # Show progress
                            total_bars = len(all_bars)
                            print(f"Total bars collected so far: {total_bars}")
                            
                            # Move back by fixed window for next chunk (prevents oscillation)
                            current_end = current_end - walk_delta
                            
                            # Check if we've hit 2004 or earlier - immediate stop
                            if current_end.year <= 2004:
                                print(f"\n⛔ Stopping walk-backward: Next request would be in year {current_end.year} (stopping at 2004 boundary)")
                                break
                        else:
                            # No data returned - implement retry logic
                            walk_backward_empty_retries += 1
                            print(f"Empty response in walk-backward (retry {walk_backward_empty_retries}/3 for period {consecutive_empty_periods + 1})")
                            
                            if walk_backward_empty_retries < 3:
                                # Retry the same period with increasing wait times
                                wait_times = [10, 20, 30]  # 10s, 20s, 30s
                                wait_time = wait_times[walk_backward_empty_retries - 1]
                                print(f"Retrying same walk-backward period after {wait_time} seconds...")
                                time.sleep(wait_time)
                                continue  # Retry the same period
                            else:
                                # After 3 retries, mark this period as empty and move on
                                consecutive_empty_periods += 1
                                walk_backward_empty_retries = 0  # Reset retry counter for next period
                            print(f"Walk-backward period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                            
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                print(f"Stopping walk-backward: {max_consecutive_empty_periods} consecutive periods with no data after retries")
                                break
                            else:
                                # Move back by fixed window to try an earlier period
                                current_end = current_end - walk_delta
                                
                                # Check if we've hit 2004 or earlier - immediate stop
                                if current_end.year <= 2004:
                                    print(f"\n⛔ Stopping walk-backward: Next request would be in year {current_end.year} (stopping at 2004 boundary)")
                                    break
                    
                    # Pause between chunks to avoid overwhelming the API
                    time.sleep(2)
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error in walk-backward chunk {walk_chunk_count}: {error_msg}")
                    
                    # Check for connection issues
                    connection_lost = any(err in error_msg.lower() for err in 
                                         ["connection refused", "not connected", "peer closed connection", 
                                          "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                    
                    if connection_lost and host and port and client_id:
                        print("Detected connection loss during walk-backward phase.")
                        new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                        if new_ib:
                            connection['ib'] = new_ib
                            # Don't increment retry counter, just retry
                            continue
                        else:
                            print("Reconnection aborted. Stopping walk-backward.")
                            break
                    
                    # Handle specific error types with retry logic
                    elif "API historical data query cancelled" in error_msg or "pacing violation" in error_msg.lower():
                        walk_backward_empty_retries += 1
                        print(f"API cancelled/pacing issue in walk-backward (retry {walk_backward_empty_retries}/3)")
                        
                        if walk_backward_empty_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[walk_backward_empty_retries - 1]
                            print(f"Waiting {wait_time} seconds before walk-backward retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            walk_backward_empty_retries = 0
                            print(f"Walk-backward period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                            
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                print(f"Stopping walk-backward: {max_consecutive_empty_periods} consecutive periods with failures")
                                break
                            else:
                                current_end = current_end - walk_delta
                                if current_end.year <= 2004:
                                    print(f"\n⛔ Stopping walk-backward: Reached year {current_end.year}")
                                    break
                    
                    elif "TimeoutError" in error_msg or "timeout" in error_msg.lower():
                        timeout_failures += 1
                        if timeout_failures >= max_timeouts:
                            logger.warning(
                                f"Aborting {ticker} after {timeout_failures} timeouts"
                            )
                            return None
                        walk_backward_empty_retries += 1
                        print(f"Timeout in walk-backward (retry {walk_backward_empty_retries}/3)")
                        
                        if walk_backward_empty_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[walk_backward_empty_retries - 1]
                            print(f"Retrying walk-backward after {wait_time} seconds...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            walk_backward_empty_retries = 0
                            print(f"Walk-backward period failed after timeouts. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                            
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                break
                            else:
                                current_end = current_end - walk_delta
                                if current_end.year <= 2004:
                                    print(f"\n⛔ Stopping walk-backward: Reached year {current_end.year}")
                                    break
                    
                    else:
                        # Other errors - apply retry logic
                        walk_backward_empty_retries += 1
                        print(f"Error in walk-backward (retry {walk_backward_empty_retries}/3)")
                        
                        if walk_backward_empty_retries < 3:
                            wait_times = [10, 20, 30]
                            wait_time = wait_times[walk_backward_empty_retries - 1]
                            print(f"Retrying walk-backward after {wait_time} seconds...")
                            time.sleep(wait_time)
                            continue
                        else:
                            consecutive_empty_periods += 1
                            walk_backward_empty_retries = 0
                            print(f"Walk-backward period failed. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                            
                            if consecutive_empty_periods >= max_consecutive_empty_periods:
                                break
                            else:
                                # Move back by fixed 7 days even on error (maintains consistent spacing)
                                current_end = current_end - walk_delta
                                
                                # Check if we've hit 2004 or earlier - immediate stop
                                if current_end.year <= 2004:
                                    print(f"\n⛔ Stopping walk-backward: Reached year {current_end.year} (stopping at 2004 boundary)")
                                    break
            
            # Check final stop conditions
            if consecutive_empty_periods >= max_consecutive_empty_periods:
                print(f"\n✅ V3: Walk-backward complete. Stopped after {max_consecutive_empty_periods} consecutive failed periods (each with 3 retries).")
            elif current_end <= hard_stop_date or current_end.year <= 2004:
                print(f"\n✅ V3: Walk-backward complete. Reached stop boundary (2005-01-01 or year 2004).")
            
            if bid_ask:
                total = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
                print(f"Walk-backward added {walk_chunk_count} chunks. Total BID+ASK bars: {total}")
                logger.info(f"Walk-backward complete for {ticker}. Added {walk_chunk_count} chunks. Total BID+ASK bars: {total}")
            else:
                print(f"Walk-backward added {walk_chunk_count} chunks. Total bars: {len(all_bars)}")
                logger.info(f"Walk-backward complete for {ticker}. Added {walk_chunk_count} chunks. Total bars: {len(all_bars)}")
        
        # Rest of the function processing and return part
        if bid_ask:
            total_collected = (len(all_bars_bid) if all_bars_bid else 0) + (len(all_bars_ask) if all_bars_ask else 0)
            if total_collected == 0:
                print(f"⚠️ No BID/ASK data found for {ticker}")
                logger.warning(f"No BID/ASK data found for {ticker}")
                return None
            df_bid = util.df(all_bars_bid) if all_bars_bid else pd.DataFrame(columns=['date'])
            df_ask = util.df(all_bars_ask) if all_bars_ask else pd.DataFrame(columns=['date'])
            if not df_bid.empty:
                df_bid = df_bid.drop_duplicates(subset=['date']).sort_values('date')
            if not df_ask.empty:
                df_ask = df_ask.drop_duplicates(subset=['date']).sort_values('date')
            df_bid = df_bid.rename(columns={'open': 'bid_open', 'high': 'bid_high', 'low': 'bid_low', 'close': 'bid_close', 'volume': 'bid_volume'})
            df_ask = df_ask.rename(columns={'open': 'ask_open', 'high': 'ask_high', 'low': 'ask_low', 'close': 'ask_close', 'volume': 'ask_volume'})
            df = pd.merge(df_bid, df_ask, on='date', how='outer').sort_values('date').reset_index(drop=True)
            df['symbol'] = ticker
            df['datetime'] = df['date']
            df = df.sort_values('datetime').drop_duplicates(subset=['datetime'])
            # Drop any rows where only one side is present to avoid single-sided periods
            before_len = len(df)
            if 'bid_close' in df.columns and 'ask_close' in df.columns:
                df = df.dropna(subset=['bid_close', 'ask_close'])
                dropped = before_len - len(df)
                if dropped > 0:
                    print(f"Dropped {dropped} single-sided rows to enforce both-sides-only output.")
            print(f"Retrieved {len(df)} {bar_size} BID/ASK rows for {ticker} from {df['datetime'].min()} to {df['datetime'].max()}")
            logger.info(f"Retrieved {len(df)} {bar_size} BID/ASK rows for {ticker} from {df['datetime'].min()} to {df['datetime'].max()}")
            print("\nComplete dataset sample (first 5 rows):")
            sample_cols = [c for c in ['datetime','bid_close','ask_close','bid_volume','ask_volume'] if c in df.columns]
            print(df.head()[sample_cols])
            return df
        else:
            if not all_bars or len(all_bars) == 0:
                print(f"⚠️ No data found for {ticker}")
                logger.warning(f"No data found for {ticker}")
                return None
            
            # Convert to DataFrame
            df = util.df(all_bars)
            
            # Add ticker information
            df['symbol'] = ticker
            
            # Convert date to datetime and ensure UTC timezone
            # With formatDate=2, 'date' column already contains timezone-aware timestamps
            # Create a duplicate 'datetime' column for consistency
            df['datetime'] = df['date']
            
            # Sort by datetime and remove duplicates
            df = df.sort_values('datetime')
            df = df.drop_duplicates(subset=['datetime'])
            
            print(f"Retrieved {len(df)} {bar_size} bars for {ticker} from {df['datetime'].min()} to {df['datetime'].max()}")
            logger.info(f"Retrieved {len(df)} {bar_size} bars for {ticker} from {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Show the first 5 rows of the complete dataset
            print("\nComplete dataset sample (first 5 rows):")
            print(df.head()[['datetime', 'open', 'high', 'low', 'close', 'volume']])
            
            return df
    
    except Exception as e:
        print(f"❌ Error retrieving historical data for {ticker}: {e}")
        logger.error(f"Error retrieving historical data for {ticker}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Check for connection-related errors in the overall function
        error_msg = str(e)
        connection_lost = any(err in error_msg.lower() for err in 
                             ["connection refused", "not connected", "peer closed connection", 
                              "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
        
        if connection_lost and host and port and client_id:
            print("Detected possible connection loss to IBKR TWS.")
            # Try to reconnect
            new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
            if new_ib:
                # Reconnection successful, but we'll return None to let the main function handle the retry logic
                print("Successfully reconnected, but need to restart data retrieval for this security.")
                return None
            else:
                print("Reconnection aborted. Exiting.")
                return None
        
        return None

def get_market_snapshot(ib, contract, ticker, host=None, port=None, client_id=None, prompt_user=True):
    """
    Get market data snapshot for a contract (similar to stream_security.py)
    
    Args:
        ib: The IB connection object
        contract: The contract object
        ticker: The ticker symbol (for logging)
        host: The hostname or IP address of the IBKR TWS (for reconnection)
        port: The port number of the IBKR TWS (for reconnection)
        client_id: The client ID to use for the connection (for reconnection)
        
    Returns:
        pd.DataFrame: DataFrame containing market data or None if not available
    """
    # Create a variable to hold the connection that can be updated
    connection = {'ib': ib}
    
    try:
        print(f"Attempting to get market data snapshot for {ticker}...")
        
        # Define the fields we want to request
        fields = ["31", "84", "86", "85", "87", "88", "89", "70", "71", "82"]
        # 31=Last, 84=Bid, 86=Ask, 70=Open, 71=Close, 82=Volume
        
        # For indices, use a different approach based on stream_security.py
        if contract.secType == 'IND':
            # First make a preflight request to initialize the market data
            # This is how stream_security.py handles it
            print(f"Making preflight request for index {ticker}...")
            
            try:
                # Create an IB ticker
                # Indices only allow a limited set of generic tick IDs; request a basic snapshot instead.
                market_ticker = connection['ib'].reqMktData(contract, '', snapshot=True)
                
                # Wait for data to arrive
                start_time = time.time()
                timeout = 15  # seconds (longer timeout for indices)
                
                while time.time() - start_time < timeout:
                    connection['ib'].sleep(1)  # Allow more time between checks for indices
                    # Check if we have data
                    if market_ticker.last or market_ticker.close:
                        print(f"Received market data for index {ticker}")
                        break
                
                # Wait just a bit longer to ensure all data has arrived
                connection['ib'].sleep(2)
                
                # Create a dataframe with the market data - focus on last price for indices
                data = {
                    'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    'open': [market_ticker.open],
                    'high': [market_ticker.high],
                    'low': [market_ticker.low], 
                    'close': [market_ticker.close or market_ticker.last],  # Use close or last
                    'volume': [market_ticker.volume],
                    'symbol': [ticker]
                }
                
                # For most indices, bid/ask are not available, but we still include them
                data['bid'] = [market_ticker.bid]
                data['ask'] = [market_ticker.ask]
                
                # Snapshot requests auto-complete; no cancel required.
                
                # Check if we have usable data (last or close)
                if not pd.notna(data['close'][0]):
                    print(f"No usable market data received for index {ticker}")
                    return None
                    
                df = pd.DataFrame(data)
                print(f"Created dataframe with market data for index: {df.iloc[0].to_dict()}")
                
                # Display the snapshot data
                print("\nMarket snapshot data sample:")
                print(df[['timestamp', 'open', 'close', 'volume', 'bid', 'ask', 'symbol']])
                
                return df
            
            except Exception as e:
                error_msg = str(e)
                print(f"Error getting market data for index {ticker}: {e}")
                
                # Check for connection-related errors
                connection_lost = any(err in error_msg.lower() for err in 
                                     ["connection refused", "not connected", "peer closed connection", 
                                      "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                
                if connection_lost and host and port and client_id:
                    print("Detected possible connection loss to IBKR TWS.")
                    # Try to reconnect
                    new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                    if new_ib:
                        # Reconnection successful, update the connection and try again
                        connection['ib'] = new_ib
                        try:
                            # Try again with the new connection
                            return get_market_snapshot(new_ib, contract, ticker, host, port, client_id, prompt_user=prompt_user)
                        except Exception as e2:
                            print(f"Error getting market data after reconnection: {e2}")
                            return None
                    else:
                        print("Reconnection aborted. Exiting.")
                        return None
                return None
        else:
            # Original approach for non-indices
            try:
                # Create an IB ticker
                market_ticker = connection['ib'].reqMktData(contract, ','.join(fields))
                
                # Wait for data to arrive
                start_time = time.time()
                timeout = 10  # seconds
                
                while time.time() - start_time < timeout:
                    connection['ib'].sleep(0.5)
                    # Check if we have data
                    if market_ticker.last or market_ticker.bid or market_ticker.ask:
                        print(f"Received market data for {ticker}")
                        break
                
                # Cancel the market data request
                connection['ib'].cancelMktData(market_ticker)
                
                # Check if we have data
                if not (market_ticker.last or market_ticker.bid or market_ticker.ask):
                    print(f"No market data received for {ticker}")
                    return None
                
                # Create a dataframe with the market data
                data = {
                    'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    'open': [market_ticker.open],
                    'close': [market_ticker.close or market_ticker.last],  # Use close or last
                    'volume': [market_ticker.volume],
                    'bid': [market_ticker.bid],
                    'ask': [market_ticker.ask],
                    'symbol': [ticker]
                }
                
                df = pd.DataFrame(data)
                print(f"Created dataframe with market data: {df.iloc[0].to_dict()}")
                
                # Display the snapshot data
                print("\nMarket snapshot data sample:")
                print(df[['timestamp', 'open', 'close', 'volume', 'bid', 'ask', 'symbol']])
                
                return df
            
            except Exception as e:
                error_msg = str(e)
                print(f"Error getting market data for {ticker}: {e}")
                
                # Check for connection-related errors
                connection_lost = any(err in error_msg.lower() for err in 
                                     ["connection refused", "not connected", "peer closed connection", 
                                      "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
                
                if connection_lost and host and port and client_id:
                    print("Detected possible connection loss to IBKR TWS.")
                    # Try to reconnect
                    new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
                    if new_ib:
                        # Reconnection successful, update the connection and try again
                        connection['ib'] = new_ib
                        try:
                            # Try again with the new connection
                            return get_market_snapshot(new_ib, contract, ticker, host, port, client_id, prompt_user=prompt_user)
                        except Exception as e2:
                            print(f"Error getting market data after reconnection: {e2}")
                            return None
                    else:
                        print("Reconnection aborted. Exiting.")
                        return None
                return None
    
    except Exception as e:
        print(f"❌ Error getting market data for {ticker}: {e}")
        logger.error(f"Error getting market data for {ticker}: {e}")
        
        # Check for connection-related errors
        error_msg = str(e)
        connection_lost = any(err in error_msg.lower() for err in 
                             ["connection refused", "not connected", "peer closed connection", 
                              "socket.gaierror", "ib_insync", "broken pipe", "connection reset"])
        
        if connection_lost and host and port and client_id:
            print("Detected possible connection loss to IBKR TWS.")
            # Try to reconnect
            new_ib = attempt_reconnect(host, port, client_id, prompt_user=prompt_user)
            if new_ib:
                # Reconnection successful, update the connection and try again
                try:
                    # Try again with the new connection
                    return get_market_snapshot(new_ib, contract, ticker, host, port, client_id, prompt_user=prompt_user)
                except Exception as e2:
                    print(f"Error getting market data after reconnection: {e2}")
                    return None
            else:
                print("Reconnection aborted. Exiting.")
                return None
        
        return None

def save_data(df, ticker):
    """
    Save historical data to a CSV file.
    
    Args:
        df: DataFrame containing historical data
        ticker: The ticker symbol
    
    Returns:
        str: Path to the saved file
    """
    if df is None or df.empty:
        logger.warning(f"No data to save for {ticker}")
        return None
    
    # Display sample of data to be saved
    print(f"\nSample of data to be saved for {ticker} (first 5 rows):")
    columns_to_show = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'bid_close', 'ask_close'] 
    columns_to_show = [col for col in columns_to_show if col in df.columns]
    print(df.head()[columns_to_show])
    
    # Create output filename
    output_file = OUTPUT_DIR / f"{ticker}.csv"
    
    # Save to CSV
    df.to_csv(str(output_file), index=False)
    
    print(f"✅ Saved {len(df)} bars for {ticker} to {output_file}")
    logger.info(f"Saved {len(df)} bars for {ticker} to {output_file}")
    
    return output_file

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

def update_data(new_df, ticker):
    """
    Update the existing CSV file with new data.
    
    Args:
        new_df: DataFrame containing new historical data
        ticker: The ticker symbol
    
    Returns:
        str: Path to the updated file, or None if no update was needed
    """
    if new_df is None or new_df.empty:
        logger.warning(f"No new data to update for {ticker}")
        return None
    
    output_file = OUTPUT_DIR / f"{ticker}.csv"
    
    # Check if file exists - this should not happen in update mode as we check earlier
    if not output_file.exists():
        print(f"❌ Error: File for {ticker} doesn't exist. Cannot update non-existent file.")
        return None
    
    try:
        # Read existing data
        existing_df = pd.read_csv(str(output_file))
        
        # Ensure datetime column is present in existing data
        if 'datetime' not in existing_df.columns:
            if 'date' in existing_df.columns:
                existing_df['datetime'] = pd.to_datetime(existing_df['date'], utc=True, errors='coerce')
            else:
                print(f"❌ No datetime or date column found in existing file for {ticker}")
                return None
        else:
            # Convert to datetime if it's not already
            existing_df['datetime'] = pd.to_datetime(existing_df['datetime'], utc=True, errors='coerce')
        
        # Get the latest date in the existing data
        latest_date = existing_df['datetime'].max()
        
        # Filter new data to only include rows after the latest date in existing data
        new_df['datetime'] = pd.to_datetime(new_df['datetime'], utc=True, errors='coerce')
        new_data = new_df[new_df['datetime'] > latest_date]
        
        # If there's no new data to add, return
        if new_data.empty:
            print(f"✅ No new data available for {ticker}. File is already up to date.")
            return None
        
        # Combine existing and new data
        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        
        # Sort by datetime and remove duplicates
        combined_df = combined_df.sort_values('datetime')
        combined_df = combined_df.drop_duplicates(subset=['datetime'])
        
        # Save the updated file
        combined_df.to_csv(str(output_file), index=False)
        
        print(f"✅ Updated {ticker} with {len(new_data)} new bars from {new_data['datetime'].min()} to {new_data['datetime'].max()}")
        logger.info(f"Updated {ticker} with {len(new_data)} new bars from {new_data['datetime'].min()} to {new_data['datetime'].max()}")
        
        return output_file
    
    except Exception as e:
        print(f"❌ Error updating data for {ticker}: {e}")
        logger.error(f"Error updating data for {ticker}: {e}")
        return None

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

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Retrieve historical 1-minute data for securities.')
    
    parser.add_argument('--conid', type=int, default=None,
                        help='ConId of a specific security to process')
    
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='IBKR TWS hostname (default: 127.0.0.1)')
    
    parser.add_argument('--port', type=int, default=7497,
                        help='IBKR TWS port (default: 7497)')
    
    parser.add_argument('--client-id', type=int, default=10,
                        help='Client ID for IBKR connection (default: 10)')
    
    parser.add_argument('--input-file', type=str, default='securities_daily_update.csv',
                        help='Path to the securities CSV file (default: securities_daily_update.csv)')
    
    parser.add_argument('--duration', type=str, default='5 Y',
                        help='Duration of historical data (default: 5 Y)')
    
    parser.add_argument('--bar-size', type=str, default='1 min',
                        help='Size of bars (default: 1 min)')
    
    parser.add_argument('--start-from', type=str, default=None,
                        help='Start processing from this ticker symbol (for resuming interrupted runs)')
    
    # V3: Add walk-backward arguments
    parser.add_argument('--walk-backward', action='store_true', default=True,
                        help='Enable walk-backward to fetch data older than the initial lookback period (default: enabled)')
    
    parser.add_argument('--no-walk-backward', action='store_false', dest='walk_backward',
                        help='Disable walk-backward functionality')
    
    # Add bid-ask flag
    parser.add_argument('--bid-ask', action='store_true',
                        help='Fetch BID_ASK data instead of TRADES/MIDPOINT/AGGTRADES and save to historic_data_bidask folder')

    parser.add_argument(
        '--max-update-days',
        type=int,
        default=None,
        help='Hard cap on update window in days (applies only in --update mode)',
    )

    parser.add_argument(
        '--create-missing-update',
        action='store_true',
        help='In --update mode, create a new file using the update window when no file exists',
    )

    parser.add_argument(
        '--no-prompt',
        action='store_true',
        help='Disable interactive prompts; skip to next security on aborts',
    )
    
    # Optional: allow falling back to daily bars for indices if minute data is unavailable
    parser.add_argument('--daily-index-fallback', action='store_true',
                        help='If set, allow fallback to daily TRADES for indices when 1-minute bars are unavailable')

    parser.add_argument(
        '--no-5min-fallback',
        action='store_true',
        help='Disable fallback to 5-minute bars when 1-minute data is unavailable',
    )
    parser.add_argument(
        '--no-max-seconds',
        action='store_true',
        help='Disable per-symbol max-seconds timeout guard',
    )

    parser.add_argument(
        '--index-midpoint-fallback',
        action='store_true',
        help='Allow MIDPOINT fallback for indices when 1-minute TRADES are unavailable',
    )
    
    # Mode selection - make these mutually exclusive and required
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--back-fill', action='store_true',
                        help='Back-fill mode: create new files or overwrite existing ones with full historical data')
    mode_group.add_argument('--update', action='store_true',
                        help='Update mode: append new data to existing files (fails if file does not exist)')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Set the OUTPUT_DIR based on the --bid-ask flag
    global OUTPUT_DIR
    if args.bid_ask:
        OUTPUT_DIR = BRONZE_DIR_BIDASK
        print(f"Using BID_ASK data mode - saving to {OUTPUT_DIR}")
    else:
        OUTPUT_DIR = BRONZE_DIR
        print(f"Using default data mode (TRADES/MIDPOINT/AGGTRADES) - saving to {OUTPUT_DIR}")
    
    # Log mode of operation
    if args.update:
        print("\n" + "=" * 80)
        print("UPDATE MODE ACTIVATED")
        print("Will only update existing files with new data")
        if args.bid_ask:
            print("Fetching BID_ASK data")
        print("=" * 80 + "\n")
        logger.info("Script running in UPDATE mode")
    elif args.back_fill:
        print("\n" + "=" * 80)
        print("BACK-FILL MODE - Creating/Overwriting Files")
        print(f"Duration: {args.duration}, Walk-backward: {args.walk_backward}")
        if args.bid_ask:
            print("Fetching BID_ASK data")
        print("=" * 80 + "\n")
        logger.info(f"Script running in BACK-FILL mode with duration {args.duration}")
    
    # Load securities from CSV
    securities = load_securities(args.input_file)
    if securities is None:
        print("Failed to load securities")
        return
    
    # Connect to IBKR
    ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id)
    if not ib:
        print("Failed to connect to IBKR")
        return

    conid_rows = []

    try:
        # If conid is specified, filter the securities
        if args.conid is not None:
            # Filter to the specific security by ConId
            filtered_securities = securities[securities['IBKR_Conid'] == args.conid]
            if len(filtered_securities) == 0:
                print(f"❌ No security found with conid {args.conid}")
                return
            
            # Verify this is not a futures security
            if filtered_securities.iloc[0]['SecurityType'] == 'futures':
                print(f"❌ ConId {args.conid} is a futures security. This script only processes non-futures securities.")
                print("For futures, please use the get_hist_cont_futures_1min.py script instead.")
                return

            if args.bid_ask and filtered_securities.iloc[0]['SecurityType'] == 'index':
                ticker = filtered_securities.iloc[0]['FR_Ticker']
                print(f"⚠️ Skipping index {ticker} for --bid-ask (no bid/ask data)")
                return
                
            securities = filtered_securities
        else:
            # Filter out futures securities
            securities = securities[securities['SecurityType'] != 'futures']

        if args.bid_ask:
            index_mask = securities['SecurityType'] == 'index'
            if index_mask.any():
                skipped = securities.loc[index_mask, 'FR_Ticker'].dropna().astype(str).tolist()
                if skipped:
                    print(f"⚠️ Skipping index tickers for --bid-ask (no bid/ask data): {', '.join(skipped)}")
                securities = securities.loc[~index_mask]
        
        print(f"Processing {len(securities)} non-futures securities...")
        
        # Check if we need to start from a specific ticker
        start_from_found = args.start_from is None
        if args.start_from:
            print(f"Will start processing from ticker: {args.start_from}")
        
        # Process each security
        for idx, security in securities.iterrows():
            # Verify if we're still connected before processing each security
            force_reconnect = _consume_connection_lost_flag(ib)
            if force_reconnect or not ib or not ib.isConnected():
                print("\nConnection to IBKR lost. Attempting to reconnect...")
                ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id)
                if not ib:
                    print("Failed to reconnect to IBKR. Exiting.")
                    return
                
            ticker = security['FR_Ticker']
            conid = security['IBKR_Conid'] if pd.notna(security['IBKR_Conid']) else None
            sec_type = security['SecurityType']
            
            # Skip until we reach the start-from ticker
            if not start_from_found:
                if ticker == args.start_from:
                    start_from_found = True
                    print(f"\n{'=' * 80}")
                    print(f"RESUMING PROCESSING FROM {ticker}")
                    print(f"{'=' * 80}")
                else:
                    print(f"Skipping {ticker} (waiting to reach {args.start_from})")
                    continue
            
            print(f"\n{'-' * 80}")
            print(f"Processing {ticker} (ConId: {conid}, Type: {sec_type})")
            print(f"{'-' * 80}")
            
            # Create contract
            contract = create_contract(security)
            if contract is None:
                print(f"❌ Failed to create contract for {ticker}")
                continue
            
            # Get contract details to make sure we have a valid contract
            try:
                # Verify connection again before making the API call
                force_reconnect = _consume_connection_lost_flag(ib)
                if force_reconnect or not ib.isConnected():
                    print("Lost connection before getting contract details. Reconnecting...")
                    ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id)
                    if not ib:
                        print("Failed to reconnect. Skipping this security.")
                        continue
                        
                details = ib.reqContractDetails(contract)
                if details:
                    contract = details[0].contract
                    qualified_contract = contract
                    print(f"Using qualified contract: {contract}")
                else:
                    print(f"❌ Failed to get contract details for {ticker}")
                    continue
            except Exception as e:
                print(f"❌ Error getting contract details for {ticker}: {e}")
                # Check if it's a connection error and try to reconnect
                if "Not connected" in str(e):
                    print("Lost connection. Attempting to reconnect...")
                    ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id)
                    if not ib:
                        print("Failed to reconnect. Skipping this security.")
                        continue
                    # Try one more time with the new connection
                    try:
                        details = ib.reqContractDetails(contract)
                        if details:
                            contract = details[0].contract
                            qualified_contract = contract
                            print(f"Using qualified contract: {contract}")
                        else:
                            print(f"❌ Failed to get contract details for {ticker}")
                            continue
                    except Exception as e2:
                        print(f"❌ Error getting contract details on retry: {e2}")
                        continue
                else:
                    continue
            
            # Handle update mode - check if file exists and calculate duration
            duration = args.duration  # Default duration
            missing_file = False
            
            if args.update:
                # In update mode, file must exist
                output_file = OUTPUT_DIR / f"{ticker}.csv"
                if not output_file.exists():
                    if args.create_missing_update:
                        missing_file = True
                        days_to_update = args.max_update_days if args.max_update_days else 2
                        duration = calculate_update_duration(days_to_update)
                        print(
                            f"⚠️ Missing file for {ticker}; will create new file "
                            f"from update window ({days_to_update} days, duration {duration})"
                        )
                    else:
                        print(f"❌ Error: No existing file found for {ticker}. Cannot update non-existent file.")
                        print(f"    Use without --update flag to create initial data file.")
                        conid_rows.append({
                            "ticker": ticker,
                            "security_type": sec_type,
                            "mode": "update",
                            "contract_conid": getattr(qualified_contract, 'conId', None),
                            "contract_local_symbol": getattr(qualified_contract, 'localSymbol', None),
                            "contract_exchange": getattr(qualified_contract, 'exchange', None),
                            "contract_primary_exchange": getattr(qualified_contract, 'primaryExchange', None),
                            "contract_trading_class": getattr(qualified_contract, 'tradingClass', None),
                            "contract_currency": getattr(qualified_contract, 'currency', None),
                            "fetch_status": "missing_file",
                            "data_source": "historical",
                        })
                        continue
                
                try:
                    if not missing_file:
                        # Read existing data to get the last date
                        existing_df = pd.read_csv(str(output_file))
                        if existing_df.empty:
                            if args.create_missing_update:
                                missing_file = True
                                days_to_update = args.max_update_days if args.max_update_days else 2
                                duration = calculate_update_duration(days_to_update)
                                print(
                                    f"⚠️ No rows found in {ticker} file; will create new file "
                                    f"from update window ({days_to_update} days, duration {duration})"
                                )
                            else:
                                print(f"❌ Error: No rows found in {ticker} file. Cannot determine update range.")
                                continue
                        if 'datetime' in existing_df.columns:
                            existing_df['datetime'] = pd.to_datetime(existing_df['datetime'], utc=True, errors='coerce')
                            last_date = existing_df['datetime'].max()
                        elif 'date' in existing_df.columns:
                            existing_df['date'] = pd.to_datetime(existing_df['date'], utc=True, errors='coerce')
                            last_date = existing_df['date'].max()
                        else:
                            if args.create_missing_update:
                                missing_file = True
                                days_to_update = args.max_update_days if args.max_update_days else 2
                                duration = calculate_update_duration(days_to_update)
                                print(
                                    f"⚠️ No date column found in {ticker} file; will create new file "
                                    f"from update window ({days_to_update} days, duration {duration})"
                                )
                            else:
                                print(f"❌ Error: No date column found in {ticker} file. Cannot determine update range.")
                                continue
                        if not missing_file and pd.isna(last_date):
                            if args.create_missing_update:
                                missing_file = True
                                days_to_update = args.max_update_days if args.max_update_days else 2
                                duration = calculate_update_duration(days_to_update)
                                print(
                                    f"⚠️ No valid timestamps found in {ticker} file; will create new file "
                                    f"from update window ({days_to_update} days, duration {duration})"
                                )
                            else:
                                print(f"❌ Error: No valid timestamps found in {ticker} file. Cannot determine update range.")
                                continue
                        
                        if not missing_file:
                            # Calculate days from last date to now
                            days_to_update = (datetime.now(timezone.utc) - last_date).days
                            if args.max_update_days is not None and args.max_update_days > 0:
                                if days_to_update > args.max_update_days:
                                    print(
                                        f"🔧 Capping update window for {ticker}: {days_to_update} -> {args.max_update_days} days"
                                    )
                                days_to_update = min(days_to_update, args.max_update_days)
                            
                            if days_to_update <= 0:
                                print(f"✅ {ticker} is already up to date (last data: {last_date})")
                                continue
                            
                            # Calculate appropriate duration for update
                            duration = calculate_update_duration(days_to_update)
                            
                            print(f"Last data for {ticker}: {last_date.strftime('%Y-%m-%d %H:%M:%S')}")
                            print(f"Days to update: {days_to_update}, Duration: {duration}")
                
                except Exception as e:
                    print(f"❌ Error reading existing file for {ticker}: {e}")
                    continue
            
            # First, try getting historical 1-minute data directly - this is the primary goal
            snapshot_data = None
            
            # For indices, get the market snapshot first as a backup in case historical data fails
            is_index = sec_type == 'index'
            if is_index and not args.update:  # Skip snapshot in update mode
                print(f"Attempting to get market snapshot data as backup for index {ticker}...")
                snapshot_data = get_market_snapshot(
                    ib, 
                    contract, 
                    ticker,
                    host=args.host,
                    port=args.port,
                    client_id=args.client_id,
                    prompt_user=not args.no_prompt,
                )
                if snapshot_data is not None and not snapshot_data.empty and pd.notna(snapshot_data['close']).any():
                    print(f"Successfully retrieved market snapshot data for index {ticker} as backup")
            
            # Verify connection before requesting historical data
            force_reconnect = _consume_connection_lost_flag(ib)
            if force_reconnect or not ib.isConnected():
                print("Lost connection before requesting historical data. Attempting to reconnect...")
                ib = connect_to_ibkr(host=args.host, port=args.port, client_id=args.client_id)
                if not ib:
                    print("Failed to reconnect. Skipping this security.")
                    continue
                    
            # Always try to get historical 1-minute data regardless of security type
            print(f"Attempting to get historical 1-minute data for {ticker}...")
            data_source = "historical"
            df = get_historical_data(
                ib=ib,
                contract=contract,
                ticker=ticker,
                duration=duration,  # Use calculated duration
                bar_size=args.bar_size,
                host=args.host,
                port=args.port,
                client_id=args.client_id,
                walk_backward=args.walk_backward,  # V3: Pass walk-backward flag
                update_mode=args.update,  # Pass update mode flag
                bid_ask=args.bid_ask,  # Pass bid-ask flag
                daily_index_fallback=args.daily_index_fallback,
                index_midpoint_fallback=args.index_midpoint_fallback,
                allow_5min_fallback=not args.no_5min_fallback,
                disable_max_seconds=args.no_max_seconds,
                prompt_user=not args.no_prompt,
            )
            
            # Handle reconnection case - if get_historical_data returns None due to user abort
            if df is None:
                print(f"⚠️ Data retrieval for {ticker} was aborted")
                conid_rows.append({
                    "ticker": ticker,
                    "security_type": sec_type,
                    "mode": "update" if args.update else "back_fill",
                    "contract_conid": getattr(qualified_contract, 'conId', None),
                    "contract_local_symbol": getattr(qualified_contract, 'localSymbol', None),
                    "contract_exchange": getattr(qualified_contract, 'exchange', None),
                    "contract_primary_exchange": getattr(qualified_contract, 'primaryExchange', None),
                    "contract_trading_class": getattr(qualified_contract, 'tradingClass', None),
                    "contract_currency": getattr(qualified_contract, 'currency', None),
                    "fetch_status": "aborted",
                    "data_source": data_source,
                })
                if args.no_prompt:
                    print(f"Skipping {ticker} and continuing with next security (non-interactive mode)...")
                    continue
                # Ask user if they want to continue with the next security or exit
                choice = input("\nDo you want to continue with the next security? (Y/N): ").strip().upper()
                if choice != 'Y':
                    print("Process aborted by user.")
                    return
                print(f"Skipping {ticker} and continuing with next security...")
                continue
            
            # If historical data retrieval failed, use the snapshot as a fallback for indices
            if (df is None or df.empty) and snapshot_data is not None and not snapshot_data.empty:
                print(f"Using market snapshot data as fallback for {ticker}")
                df = snapshot_data
                data_source = "snapshot"
            # For non-indices, try getting a market snapshot as a last resort
            elif (df is None or df.empty) and not is_index:
                print(f"No historical data available for {ticker}. Trying to get market data snapshot...")
                df = get_market_snapshot(
                    ib, 
                    contract, 
                    ticker,
                    host=args.host,
                    port=args.port,
                    client_id=args.client_id,
                    prompt_user=not args.no_prompt,
                )
                if df is not None and not df.empty:
                    data_source = "snapshot"
            
            # Save data
            if df is not None and not df.empty:
                fetch_status = "success"
                if args.update and not missing_file:
                    # Use update_data function in update mode
                    update_data(df, ticker)
                elif args.update and missing_file:
                    fetch_status = "created"
                    save_data(df, ticker)
                else:
                    # Use save_data function in normal mode
                    save_data(df, ticker)
            else:
                print(f"❌ No data retrieved for {ticker}")
                fetch_status = "failed"

            conid_rows.append({
                "ticker": ticker,
                "security_type": sec_type,
                "mode": "update" if args.update else "back_fill",
                "contract_conid": getattr(qualified_contract, 'conId', None),
                "contract_local_symbol": getattr(qualified_contract, 'localSymbol', None),
                "contract_exchange": getattr(qualified_contract, 'exchange', None),
                "contract_primary_exchange": getattr(qualified_contract, 'primaryExchange', None),
                "contract_trading_class": getattr(qualified_contract, 'tradingClass', None),
                "contract_currency": getattr(qualified_contract, 'currency', None),
                "fetch_status": fetch_status,
                "data_source": data_source,
            })
            
            # Sleep to avoid overwhelming the API
            time.sleep(1)
        
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        _print_nonfutures_summary(conid_rows)
        
    finally:
        # Disconnect
        if conid_rows:
            _write_nonfutures_conid_artifact(conid_rows, MAX_FETCH_DIR)
        if ib and ib.isConnected():
            print("\nDisconnecting from IBKR")
            ib.disconnect()

if __name__ == "__main__":
    main() 
