#!/usr/bin/env python3
"""
Script to get full JSON details for all potential matching securities based on a ticker

This script:
1. Takes a ticker as a command-line argument
2. Searches for all matching contracts in IBKR via TWS API
3. Gets detailed contract information for each match
4. Saves all data to a CSV file named {ticker}_jsons.csv in the ./jsons folder
5. Includes a final 'JSON' column with the complete raw JSON data
6. Orders columns with sections at the end before JSON

Usage:
    python get_ticker_jsons.py --ticker TICKER [--securities-csv <path>] [--fut-exchange <EXCH>] [--fut-currency <CCY>] [--no-futures-root]
"""

import os
import sys
import logging
import argparse
import json
import pandas as pd
import time
from datetime import datetime, date
from dotenv import load_dotenv

# Import IB modules
from ib_insync import IB, Contract, Stock, Forex, Index, Crypto, util

# Ensure the jsons directory exists
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))
DEFAULT_SECURITIES_CSV = os.path.join(REPO_ROOT, "apps", "fetch", "ibkr-fetch", "securities_daily_update.csv")
JSONS_DIR = "jsons"
os.makedirs(JSONS_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def connect_to_ibkr(host='127.0.0.1', port=4002, client_id=12345):
    """
    Connect to IBKR TWS or Gateway.
    
    Args:
        host: The hostname or IP address of the TWS/Gateway
        port: The port number of the TWS/Gateway
        client_id: The client ID to use for the connection
    
    Returns:
        IB: The IB connection object or None if connection failed
    """
    logger.info(f"Connecting to IBKR: host={host}, port={port}, client_id={client_id}")
    
    ib = IB()
    
    try:
        ib.connect(host, port, clientId=client_id, readonly=True, timeout=30)
        logger.info("✅ Connected to IBKR")
        
        # Print API version and available accounts
        logger.info(f"API Version: {ib.client.serverVersion()}")
        accounts = ib.managedAccounts()
        logger.info(f"Available accounts: {accounts}")
        
        return ib
    
    except Exception as e:
        logger.error(f"❌ Failed to connect to IBKR: {e}")
        print("\nTo start the IBKR Gateway or TWS:")
        print("1. Launch TWS or IB Gateway application")
        print("2. Enable API connections in settings")
        print("3. Make sure the specified port is correct")
        print("4. Run this script again\n")
        return None

def search_contract(ib, ticker):
    """
    Search for contracts matching a ticker
    
    Args:
        ib: The IB connection object
        ticker: Ticker symbol to search for
        
    Returns:
        list: List of dictionaries containing contract information
    """
    logger.info(f"Searching for contracts matching ticker: {ticker}")
    
    # Create a simple contract to search with
    contract = Contract()
    contract.symbol = ticker
    
    try:
        # Use the searchContractDetails method to find matching contracts
        matches = ib.reqMatchingSymbols(ticker)
        
        if not matches:
            logger.warning(f"No contracts found for ticker: {ticker}")
            return []
        
        logger.info(f"Found {len(matches)} matching contracts")
        return matches
    
    except Exception as e:
        logger.error(f"Error searching for contracts: {e}")
        return []

def get_contract_details(ib, contract):
    """
    Get contract details for a specific contract
    
    Args:
        ib: The IB connection object
        contract: Contract object to get details for
        
    Returns:
        dict: Dictionary containing contract details
    """
    try:
        # Get contract details from IB
        details = ib.reqContractDetails(contract)
        
        if not details:
            logger.warning(f"No details found for contract: {contract.symbol}")
            return None
        
        # Return the first details object
        return details[0]
    
    except Exception as e:
        logger.error(f"Error getting contract details: {e}")
        return None


def _add_details_fields(contract_info, details):
    details_dict = None
    if details:
        details_dict = util.dataclassAsDict(details)
        for key, value in details_dict.items():
            if key == 'contract':
                contract_obj = util.dataclassAsDict(value)
                for contract_key, contract_value in contract_obj.items():
                    detail_key = f"details_contract_{contract_key}"
                    try:
                        if isinstance(contract_value, (datetime, date)):
                            contract_info[detail_key] = contract_value.isoformat()
                        else:
                            contract_info[detail_key] = contract_value
                    except Exception:
                        contract_info[detail_key] = str(contract_value)
            else:
                detail_key = f"details_{key}"
                try:
                    if isinstance(value, (datetime, date)):
                        contract_info[detail_key] = value.isoformat()
                    else:
                        contract_info[detail_key] = value
                except Exception:
                    contract_info[detail_key] = str(value)
        if hasattr(details.contract, 'conId'):
            contract_info['conid'] = details.contract.conId
    return details_dict


def _parse_expiry(raw):
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        digits = digits[:8]
        try:
            return datetime.strptime(digits, "%Y%m%d")
        except Exception:
            return None
    if len(digits) >= 6:
        digits = digits[:6]
        try:
            return datetime.strptime(digits, "%Y%m")
        except Exception:
            return None
    return None


def _lookup_futures_spec(ticker, securities_csv, fut_exchange=None, fut_currency=None):
    exchange = fut_exchange
    currency = fut_currency
    if securities_csv and os.path.exists(securities_csv):
        try:
            df = pd.read_csv(securities_csv)
            df['FR_Ticker'] = df['FR_Ticker'].astype(str).str.strip()
            df['SecurityType'] = df['SecurityType'].astype(str).str.lower().str.strip()
            row = df[(df['FR_Ticker'] == ticker) & (df['SecurityType'] == 'futures')]
            if not row.empty:
                exchange = exchange or str(row.iloc[0].get('IBKR_exchange') or "").strip()
                currency = currency or str(row.iloc[0].get('ibkr_currency') or "").strip()
        except Exception as exc:
            logger.warning("Failed to read securities CSV %s: %s", securities_csv, exc)
    exchange = exchange or None
    currency = currency or None
    return exchange, currency


def _build_futures_root_summary(ib, ticker, exchange, currency):
    if not exchange:
        return None
    contract = Contract()
    contract.symbol = ticker
    contract.secType = "FUT"
    contract.exchange = exchange
    if currency:
        contract.currency = currency
    details_list = ib.reqContractDetails(contract)
    if not details_list:
        return None
    parsed = []
    for details in details_list:
        expiry = _parse_expiry(getattr(details.contract, "lastTradeDateOrContractMonth", None))
        parsed.append((expiry, details))
    parsed.sort(key=lambda item: item[0] or datetime.max)
    rep_details = parsed[0][1]
    expiries = [item[0] for item in parsed if item[0] is not None]
    exp_min = min(expiries).date().isoformat() if expiries else None
    exp_max = max(expiries).date().isoformat() if expiries else None
    exchanges = sorted({getattr(d.contract, "exchange", "") for _, d in parsed if getattr(d.contract, "exchange", "")})
    trading_classes = sorted({getattr(d.contract, "tradingClass", "") for _, d in parsed if getattr(d.contract, "tradingClass", "")})
    contract_info = {
        "ticker": ticker,
        "symbol": ticker,
        "secType": "FUT_ROOT",
        "exchange": exchange,
        "currency": currency,
        "description": f"{ticker} FUT root",
        "fut_contract_count": len(details_list),
        "fut_expiry_min": exp_min,
        "fut_expiry_max": exp_max,
        "fut_exchanges": ",".join(exchanges),
        "fut_trading_classes": ",".join(trading_classes),
    }
    details_dict = _add_details_fields(contract_info, rep_details)
    sections = [{
        "secType": "FUT",
        "exchange": exchange,
        "currency": currency or "",
    }]
    contract_info["sections"] = json.dumps(sections)
    for i, section in enumerate(sections):
        section_prefix = f'section_{i+1}_'
        for key, value in section.items():
            contract_info[f"{section_prefix}{key}"] = value
    combined_json = {
        "search": {
            "symbol": ticker,
            "secType": "FUT",
            "exchange": exchange,
            "currency": currency or "",
            "summary_only": True,
            "contract_count": len(details_list),
        },
        "details": details_dict,
    }
    contract_info["JSON"] = json.dumps(json.loads(json.dumps(combined_json, default=str)))
    return contract_info

def get_ticker_jsons(ib, ticker, securities_csv=DEFAULT_SECURITIES_CSV, fut_exchange=None,
                     fut_currency=None, include_futures_root=True):
    """
    Get full JSON details for all securities matching a ticker
    
    Args:
        ib: The IB connection object
        ticker: Ticker symbol to search for
        
    Returns:
        list: List of dictionaries containing contract information
    """
    # Search for contracts matching the ticker
    search_results = search_contract(ib, ticker)
    
    if not search_results:
        if include_futures_root:
            exchange, currency = _lookup_futures_spec(
                ticker, securities_csv, fut_exchange=fut_exchange, fut_currency=fut_currency
            )
            fut_root = _build_futures_root_summary(ib, ticker, exchange, currency)
            if fut_root:
                return [fut_root]
        return []
    
    # Process search results
    contracts = []
    for match in search_results:
        # ContractDescription has a contract field that contains the actual Contract
        contract_desc = match
        match_contract = contract_desc.contract
        
        # Store the original contract object
        contract_info = {
            'ticker': ticker,
            'symbol': match_contract.symbol,
            'secType': match_contract.secType,
            'exchange': match_contract.exchange,
            'currency': match_contract.currency,
            'description': str(match_contract.symbol)  # Use symbol as description
        }
        
        # Add derivative/option data if available
        if hasattr(contract_desc, 'derivativeSecTypes'):
            contract_info['derivativeSecTypes'] = ','.join(contract_desc.derivativeSecTypes)
        
        # Create a contract object for detailed query
        contract = Contract()
        contract.symbol = match_contract.symbol
        contract.secType = match_contract.secType
        contract.exchange = match_contract.exchange
        contract.currency = match_contract.currency
        
        # Get detailed contract information
        details = get_contract_details(ib, contract)
        
        details_dict = None
        if details:
            details_dict = _add_details_fields(contract_info, details)
        
        # Break down sections for compatibility with original script
        # In TWS API, we don't have direct sections, so we'll create a synthetic version
        sections = []
        
        # Add a section for the main contract type
        section = {
            'secType': match_contract.secType,
            'exchange': match_contract.exchange,
            'currency': match_contract.currency
        }
        sections.append(section)
        
        # Store sections as JSON
        contract_info['sections'] = json.dumps(sections)
        
        # Process sections
        for i, section in enumerate(sections):
            section_prefix = f'section_{i+1}_'
            for key, value in section.items():
                column_name = f"{section_prefix}{key}"
                contract_info[column_name] = value
        
        # Create the combined JSON string
        combined_json = {
            "search": {
                "symbol": match_contract.symbol,
                "secType": match_contract.secType,
                "exchange": match_contract.exchange,
                "currency": match_contract.currency,
                "derivativeSecTypes": contract_desc.derivativeSecTypes if hasattr(contract_desc, 'derivativeSecTypes') else []
            },
            "details": details_dict
        }
        
        # Handle non-serializable objects in combined_json
        combined_json_serializable = json.loads(json.dumps(combined_json, default=str))
        contract_info["JSON"] = json.dumps(combined_json_serializable)
        
        contracts.append(contract_info)
        
        # Pause briefly to avoid overwhelming the API
        time.sleep(0.2)
    
    if include_futures_root:
        exchange, currency = _lookup_futures_spec(
            ticker, securities_csv, fut_exchange=fut_exchange, fut_currency=fut_currency
        )
        fut_root = _build_futures_root_summary(ib, ticker, exchange, currency)
        if fut_root:
            contracts.append(fut_root)
    
    return contracts

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Get full JSON details for contracts matching a ticker")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to search for")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="TWS/Gateway host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=4002, help="TWS/Gateway port (default: 4002, 7497 for TWS)")
    parser.add_argument("--client-id", type=int, default=12345, help="Client ID for TWS/Gateway connection (default: 12345)")
    parser.add_argument("--securities-csv", type=str, default=DEFAULT_SECURITIES_CSV,
                        help="CSV with futures metadata for root lookup")
    parser.add_argument("--fut-exchange", type=str, default=None,
                        help="Override futures exchange for root lookup")
    parser.add_argument("--fut-currency", type=str, default=None,
                        help="Override futures currency for root lookup")
    parser.add_argument("--no-futures-root", action="store_true",
                        help="Disable FUT root summary lookup")
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Connect to IBKR
    ib = connect_to_ibkr(args.host, args.port, args.client_id)
    
    if ib is None:
        return 1
    
    try:
        # Get contract JSONs
        ticker = args.ticker
        contracts = get_ticker_jsons(
            ib,
            ticker,
            securities_csv=args.securities_csv,
            fut_exchange=args.fut_exchange,
            fut_currency=args.fut_currency,
            include_futures_root=not args.no_futures_root,
        )
        
        if not contracts:
            logger.warning(f"No contract information found for ticker: {ticker}")
            return 1
        
        # Process contract information for display
        for i, contract in enumerate(contracts):
            print(f"\nContract {i+1}:")
            print(f"Ticker: {contract.get('ticker')}")
            print(f"ConID: {contract.get('conid', 'N/A')}")
            print(f"Symbol: {contract.get('symbol', 'N/A')}")
            
            # Display security type if available
            print(f"Type: {contract.get('secType', 'N/A')}")
            
            # Display exchange
            print(f"Exchange: {contract.get('exchange', 'N/A')}")
            
            # Display description
            print(f"Description: {contract.get('description', 'N/A')}")
            
            # Display company name if available
            if 'details_longName' in contract:
                print(f"Company Name: {contract.get('details_longName')}")
            
            # Display currency
            print(f"Currency: {contract.get('currency', 'N/A')}")
            
            # Display sections information if available
            section_keys = [key for key in contract.keys() if key.startswith('section_')]
            if section_keys:
                print("\nSections:")
                for key in sorted(section_keys):
                    if 'secType' in key:
                        section_num = key.split('_')[1]
                        sec_type = contract.get(key)
                        exchange = contract.get(f'section_{section_num}_exchange', 'N/A')
                        print(f"  - {sec_type} on {exchange}")
            
            print("=" * 40)
        
        # Create a DataFrame and flatten nested structures
        df = pd.DataFrame(contracts)
        
        # Reorder columns to move sections and JSON to the end
        # First, identify column categories
        section_cols = [col for col in df.columns if col == 'sections' or col.startswith('section_')]
        json_col = ['JSON'] if 'JSON' in df.columns else []
        
        # Get non-section, non-JSON columns
        other_cols = [col for col in df.columns if col not in section_cols and col not in json_col]
        
        # Reorder columns: other_cols, section_cols, json_col
        ordered_cols = other_cols + section_cols + json_col
        
        # Make sure all columns in ordered_cols actually exist in df
        valid_ordered_cols = [col for col in ordered_cols if col in df.columns]
        df = df[valid_ordered_cols]
        
        # Save to CSV with the ticker name in the jsons folder
        output_file = os.path.join(JSONS_DIR, f"{ticker.lower()}_jsons.csv")
        df.to_csv(output_file, index=False)
        
        # Display information about the output
        num_columns = len(df.columns)
        logger.info(f"Contract data saved to {output_file} with {num_columns} columns")
        
        # List just a few key columns to avoid overwhelming output
        first_cols = list(df.columns[:min(5, len(df.columns))])
        has_sections = any(col == 'sections' or col.startswith('section_') for col in df.columns)
        has_json = 'JSON' in df.columns
        
        key_cols_msg = f"First columns: {', '.join(first_cols)}"
        if has_sections:
            key_cols_msg += ", [section columns]"
        if has_json:
            key_cols_msg += ", JSON"
        
        logger.info(key_cols_msg)
        logger.info(f"Total columns: {num_columns}")
        
    finally:
        # Disconnect from IBKR
        if ib:
            ib.disconnect()
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 
