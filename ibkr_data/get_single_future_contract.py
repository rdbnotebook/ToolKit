#!/usr/bin/env python3
"""
Get Historical 1-Minute Data for a Single Futures Contract

This script fetches historical 1-minute data for a specific futures contract
(e.g., ZTH25, ESM24) from Interactive Brokers.

Supports both FRD and IBKR contract naming conventions:
- FRD format: H25 (month + 2-digit year)
- IBKR format: ZTH5 (ticker + month + 1-digit year)

Author: Assistant
Date: 2024
"""

import os
import sys
import time
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import signal

# Import IB modules
from ib_insync import IB, Future, util, Contract

# Ensure the logs directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, 'get_hist_future_single_contract.log'))
    ]
)
logger = logging.getLogger(__name__)

# Ensure the output directory exists
OUTPUT_DIR = "historic_future_data_contracts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Valid futures month codes
VALID_MONTH_CODES = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

# Month code to month number mapping
MONTH_CODE_TO_NUM = {
    'F': 1,  'G': 2,  'H': 3,  'J': 4,  'K': 5,  'M': 6,
    'N': 7,  'Q': 8,  'U': 9,  'V': 10, 'X': 11, 'Z': 12
}

# Global variable for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    global shutdown_requested
    print("\n⚠️ Interrupt received. Completing current operation and shutting down...")
    shutdown_requested = True
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Reset to default for force quit

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def parse_contract_spec(ticker: str, contract_spec: str) -> Tuple[str, int, str, str]:
    """
    Parse contract specification in either FRD or IBKR format
    
    Args:
        ticker: Base symbol (e.g., 'ZT')
        contract_spec: Contract in various formats:
            - FRD: 'H25' (month + 2-digit year)
            - IBKR: 'ZTH5' (ticker + month + 1-digit year)
    
    Returns:
        tuple: (month_code, year_4digit, ibkr_format, frd_format)
        Example: ('H', 2025, 'ZTH5', 'H25')
    """
    original_spec = contract_spec
    contract_spec = contract_spec.upper()
    ticker = ticker.upper()
    
    # Remove ticker if present at start
    if contract_spec.startswith(ticker):
        contract_spec = contract_spec[len(ticker):]
    
    # Validate length
    if len(contract_spec) < 2 or len(contract_spec) > 3:
        raise ValueError(f"Invalid contract format: {original_spec}. Expected format like H25 or ZTH5")
    
    # Extract month code and year
    month_code = contract_spec[0]
    year_part = contract_spec[1:]
    
    # Validate month code
    if month_code not in VALID_MONTH_CODES:
        raise ValueError(f"Invalid month code: {month_code}. Valid codes are: {', '.join(VALID_MONTH_CODES)}")
    
    # Convert year to 4-digit
    try:
        if len(year_part) == 1:  # IBKR format: single digit year
            year_digit = int(year_part)
            # Assume 202X for 0-9, 203X for future expansion if needed
            if year_digit <= 9:
                year = 2020 + year_digit
            else:
                year = 2010 + year_digit
        elif len(year_part) == 2:  # FRD format: two digit year
            year_num = int(year_part)
            # Assume 20XX for years 00-99
            year = 2000 + year_num
        else:
            raise ValueError(f"Invalid year format: {year_part}")
    except ValueError:
        raise ValueError(f"Invalid year in contract: {original_spec}")
    
    # Create standardized formats
    ibkr_year = year % 10
    ibkr_format = f"{ticker}{month_code}{ibkr_year}"
    frd_format = f"{month_code}{year % 100:02d}"
    
    logger.info(f"Parsed contract: {original_spec} -> Month: {month_code}, Year: {year}, IBKR: {ibkr_format}, FRD: {frd_format}")
    
    return month_code, year, ibkr_format, frd_format

def get_contract_expiry(month_code: str, year: int) -> str:
    """
    Get the contract expiry date in YYYYMM format
    
    Args:
        month_code: Single letter month code
        year: 4-digit year
        
    Returns:
        str: Contract month in YYYYMM format
    """
    month_num = MONTH_CODE_TO_NUM[month_code]
    return f"{year}{month_num:02d}"

def build_futures_contract(ticker: str, month_code: str, year: int, exchange: Optional[str] = None) -> Future:
    """
    Build IBKR Future contract object
    
    Args:
        ticker: Base symbol (e.g., 'ZT')
        month_code: Single letter month code
        year: 4-digit year
        exchange: Optional exchange override
    
    Returns:
        Future: IBKR Future contract object
    """
    contract_month = get_contract_expiry(month_code, year)
    
    # Create the Future contract
    contract = Future(
        symbol=ticker,
        lastTradeDateOrContractMonth=contract_month,
        exchange=exchange if exchange else '',  # Empty string for SMART routing
        currency='USD'
    )
    
    logger.info(f"Built contract: {ticker} {contract_month} on {exchange if exchange else 'SMART'}")
    return contract

def attempt_reconnect(host: str, port: int, client_id: int, max_retries: int = 3) -> Optional[IB]:
    """
    Attempt to reconnect to IBKR Gateway
    
    Args:
        host: Gateway hostname
        port: Gateway port
        client_id: Client ID
        max_retries: Maximum reconnection attempts
        
    Returns:
        IB connection object or None if failed
    """
    for attempt in range(1, max_retries + 1):
        print(f"\n🔄 Reconnection attempt {attempt}/{max_retries}...")
        try:
            ib = IB()
            ib.connect(host, port, clientId=client_id, timeout=20)
            if ib.isConnected():
                print("✅ Reconnected successfully!")
                return ib
        except Exception as e:
            print(f"❌ Reconnection attempt {attempt} failed: {e}")
            if attempt < max_retries:
                wait_time = 10 * attempt
                print(f"Waiting {wait_time} seconds before next attempt...")
                time.sleep(wait_time)
    
    return None

def fetch_historical_data_with_retry(
    ib: IB,
    contract: Contract,
    end_date_str: str,
    duration_str: str,
    use_fallback: bool = False,
    use_rth: bool = False
) -> Optional[List]:
    """
    Fetch historical data with optional MEDIAN fallback and retry logic
    
    Args:
        ib: IB connection
        contract: Future contract object
        end_date_str: End date string
        duration_str: Duration string (e.g., "7 D")
        use_fallback: If True, try MEDIAN if TRADES fails
        use_rth: If True, use regular trading hours only
    
    Returns:
        List of bars or None
    """
    # Always try TRADES first (best for futures)
    what_to_show_options = ['TRADES']
    
    # Add MEDIAN as fallback if requested
    if use_fallback:
        what_to_show_options.append('MEDIAN')
    
    for what_to_show in what_to_show_options:
        print(f"Attempting to fetch data with {what_to_show}...")
        
        max_retries = 3
        for retry in range(max_retries):
            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_date_str,
                    durationStr=duration_str,
                    barSizeSetting='1 min',
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=1,
                    timeout=60
                )
                
                if bars and len(bars) > 0:
                    print(f"✅ Retrieved {len(bars)} bars using {what_to_show}")
                    return bars
                else:
                    print(f"No data returned with {what_to_show}")
                    
            except Exception as e:
                error_msg = str(e)
                print(f"Attempt {retry + 1}/{max_retries} failed with {what_to_show}: {error_msg}")
                
                if retry < max_retries - 1:
                    # Wait before retry with exponential backoff
                    wait_times = [10, 20, 30]
                    wait_time = wait_times[retry]
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                elif what_to_show == what_to_show_options[-1]:
                    # Last option failed
                    logger.error(f"All attempts failed for {contract.symbol}")
                    raise
    
    return None

def fetch_contract_data(
    ib: IB,
    contract: Future,
    duration: str = "2 Y",
    end_date: Optional[datetime] = None,
    use_fallback: bool = False,
    use_rth: bool = False,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 10
) -> Optional[pd.DataFrame]:
    """
    Fetch historical data for a single futures contract
    
    Args:
        ib: IB connection
        contract: Future contract object
        duration: How far back to fetch
        end_date: End date for data (default: now)
        use_fallback: Enable MEDIAN fallback
        use_rth: Use regular trading hours only
        host: Gateway hostname (for reconnection)
        port: Gateway port (for reconnection)
        client_id: Client ID (for reconnection)
    
    Returns:
        DataFrame with historical data or None
    """
    global shutdown_requested
    
    if end_date is None:
        end_date = datetime.now()
    
    # Parse duration to calculate start date
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
    
    start_date = end_date - timedelta(days=lookback_days)
    
    print(f"\n📊 Fetching data from {start_date.date()} to {end_date.date()}")
    print(f"Contract: {contract.symbol} {contract.lastTradeDateOrContractMonth}")
    print(f"Using RTH only: {use_rth} (24/6 data: {not use_rth})")
    if use_fallback:
        print("MEDIAN fallback enabled")
    
    # Collect all bars
    all_bars = []
    current_end = end_date
    chunk_count = 0
    max_chunks = 200  # Safety limit
    consecutive_empty_periods = 0
    max_consecutive_empty_periods = 3
    empty_response_retries = 0
    
    # Fetch data in 7-day chunks
    while current_end > start_date and chunk_count < max_chunks and consecutive_empty_periods < max_consecutive_empty_periods:
        if shutdown_requested:
            print("Shutdown requested. Stopping data collection...")
            break
        
        # Only increment chunk count if not retrying
        if empty_response_retries == 0:
            chunk_count += 1
            # Use explicit UTC timezone to avoid IBKR warnings
            # Format: YYYYMMDD-HH:MM:SS (dash indicates UTC time)
            end_date_str = current_end.strftime("%Y%m%d-%H:%M:%S")
            print(f"\nChunk {chunk_count}: Requesting data ending at {current_end.date()}")
        else:
            # Retrying same period
            print(f"Retrying chunk {chunk_count} (attempt {empty_response_retries + 1}/3)...")
        
        try:
            # Check connection
            if not ib.isConnected():
                print("Connection lost. Attempting to reconnect...")
                ib = attempt_reconnect(host, port, client_id)
                if not ib:
                    print("Failed to reconnect. Exiting.")
                    return None
            
            # Fetch data for this chunk
            bars = fetch_historical_data_with_retry(
                ib, contract, end_date_str, "7 D", use_fallback, use_rth
            )
            
            if bars and len(bars) > 0:
                all_bars.extend(bars)
                print(f"Total bars collected: {len(all_bars)}")
                
                # Reset counters on success
                consecutive_empty_periods = 0
                empty_response_retries = 0
                
                # Move to next chunk (7 days earlier)
                current_end = current_end - timedelta(days=7)
                
                # Wait between successful requests
                time.sleep(2)
            else:
                # No data returned
                empty_response_retries += 1
                
                if empty_response_retries < 3:
                    # Retry the same period
                    wait_times = [10, 20, 30]
                    wait_time = wait_times[empty_response_retries - 1]
                    print(f"No data. Retrying after {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    # After 3 retries, mark this period as empty
                    consecutive_empty_periods += 1
                    empty_response_retries = 0
                    print(f"Period failed after 3 retries. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                    
                    if consecutive_empty_periods >= max_consecutive_empty_periods:
                        print(f"Stopping: {max_consecutive_empty_periods} consecutive periods with no data")
                        break
                    else:
                        # Move to next chunk
                        current_end = current_end - timedelta(days=7)
                        continue
                        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error in chunk {chunk_count}: {error_msg}")
            
            # Check for connection issues
            if any(err in error_msg.lower() for err in ['not connected', 'connection', 'socket']):
                print("Connection issue detected. Attempting to reconnect...")
                ib = attempt_reconnect(host, port, client_id)
                if not ib:
                    print("Failed to reconnect. Exiting.")
                    return None
                continue
            else:
                # Non-connection error - apply retry logic
                empty_response_retries += 1
                
                if empty_response_retries < 3:
                    wait_times = [10, 20, 30]
                    wait_time = wait_times[empty_response_retries - 1]
                    print(f"Error. Retrying after {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    consecutive_empty_periods += 1
                    empty_response_retries = 0
                    print(f"Period failed. Consecutive empty periods: {consecutive_empty_periods}/{max_consecutive_empty_periods}")
                    
                    if consecutive_empty_periods >= max_consecutive_empty_periods:
                        break
                    else:
                        current_end = current_end - timedelta(days=7)
                        continue
    
    # Process collected data
    if not all_bars:
        print("❌ No data collected")
        return None
    
    print(f"\n✅ Data collection complete. Total bars: {len(all_bars)}")
    
    # Convert to DataFrame
    df = util.df(all_bars)
    
    # Add contract information
    df['symbol'] = contract.symbol
    df['contract'] = contract.localSymbol if hasattr(contract, 'localSymbol') else contract.symbol
    
    # Convert date to datetime and ensure UTC timezone
    # IBKR returns dates in UTC when formatDate=1 is used
    df['datetime'] = pd.to_datetime(df['date'], utc=True)
    # Store as timezone-naive UTC (removes timezone info but keeps UTC time)
    df['datetime'] = df['datetime'].dt.tz_localize(None)
    
    # Sort and remove duplicates
    df = df.sort_values('datetime')
    df = df.drop_duplicates(subset=['datetime'])
    
    # Select and reorder columns
    columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'contract']
    df = df[columns]
    
    print(f"Final dataset: {len(df)} unique bars")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    
    return df

def save_data(df: pd.DataFrame, ticker: str, contract_spec: str, output_dir: str = OUTPUT_DIR):
    """
    Save DataFrame to CSV file
    
    Args:
        df: DataFrame with historical data
        ticker: Base ticker symbol
        contract_spec: Contract specification (for filename)
        output_dir: Output directory
    """
    # Create filename (use FRD format for consistency)
    _, _, _, frd_format = parse_contract_spec(ticker, contract_spec)
    filename = f"{ticker}_{frd_format}_1min.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV
    df.to_csv(filepath, index=False)
    print(f"\n💾 Data saved to: {filepath}")
    logger.info(f"Saved {len(df)} bars to {filepath}")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Fetch historical 1-minute data for a single futures contract',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # FRD format (month + 2-digit year)
  python %(prog)s --ticker ZT --contract H25
  
  # IBKR format (ticker + month + 1-digit year)
  python %(prog)s --ticker ZT --contract ZTH5
  
  # With MEDIAN fallback
  python %(prog)s --ticker ES --contract M24 --fallback
  
  # Custom duration
  python %(prog)s --ticker NQ --contract Z25 --duration "6 M"
        """
    )
    
    parser.add_argument('--ticker', type=str, required=True,
                        help='Base futures ticker (e.g., ZT, ES, NQ)')
    
    parser.add_argument('--contract', type=str, required=True,
                        help='Contract specification (e.g., H25 or ZTH5)')
    
    parser.add_argument('--exchange', type=str, default=None,
                        help='Exchange (optional, uses SMART routing if not specified)')
    
    parser.add_argument('--duration', type=str, default='2 Y',
                        help='How far back to fetch (default: 2 Y)')
    
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date for data in YYYYMMDD format (default: today)')
    
    parser.add_argument('--fallback', action='store_true',
                        help='Enable MEDIAN fallback if TRADES fails')
    
    parser.add_argument('--output-dir', type=str, default=OUTPUT_DIR,
                        help=f'Output directory (default: {OUTPUT_DIR})')
    
    parser.add_argument('--use-rth', action='store_true',
                        help='Use regular trading hours only (default: False for 24/6 data)')
    
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='IBKR Gateway hostname (default: 127.0.0.1)')
    
    parser.add_argument('--port', type=int, default=4002,
                        help='IBKR Gateway port (default: 4002)')
    
    parser.add_argument('--client-id', type=int, default=10,
                        help='Client ID for IBKR connection (default: 10)')
    
    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_args()
    
    # Parse contract specification
    try:
        month_code, year, ibkr_format, frd_format = parse_contract_spec(args.ticker, args.contract)
        print(f"\n📋 Contract Details:")
        print(f"  Ticker: {args.ticker}")
        print(f"  Month: {month_code} ({MONTH_CODE_TO_NUM[month_code]:02d})")
        print(f"  Year: {year}")
        print(f"  IBKR Format: {ibkr_format}")
        print(f"  FRD Format: {frd_format}")
    except ValueError as e:
        print(f"❌ Error: {e}")
        return 1
    
    # Parse end date if provided
    end_date = None
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y%m%d")
        except ValueError:
            print(f"❌ Invalid end date format: {args.end_date}. Use YYYYMMDD")
            return 1
    
    # Create output directory if needed
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Connect to IBKR
    print(f"\n🔌 Connecting to IBKR Gateway at {args.host}:{args.port}")
    ib = IB()
    
    try:
        ib.connect(args.host, args.port, clientId=args.client_id, timeout=20)
        print("✅ Connected to IBKR Gateway")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        logger.error(f"Connection failed: {e}")
        return 1
    
    try:
        # Build contract
        contract = build_futures_contract(args.ticker, month_code, year, args.exchange)
        
        # Qualify the contract to get full details
        contracts = ib.qualifyContracts(contract)
        if not contracts:
            print(f"❌ Could not qualify contract {ibkr_format}")
            return 1
        
        contract = contracts[0]
        print(f"✅ Contract qualified: {contract.localSymbol} on {contract.exchange}")
        
        # Fetch historical data
        df = fetch_contract_data(
            ib, contract, args.duration, end_date,
            args.fallback, args.use_rth,
            args.host, args.port, args.client_id
        )
        
        if df is not None and len(df) > 0:
            # Save data
            save_data(df, args.ticker, args.contract, args.output_dir)
            print("\n✅ Script completed successfully")
            return 0
        else:
            print("\n❌ No data retrieved")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Script error: {e}")
        return 1
        
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("🔌 Disconnected from IBKR Gateway")

if __name__ == "__main__":
    sys.exit(main())