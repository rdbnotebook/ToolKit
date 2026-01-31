from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import argparse
import threading
import time

class IBKRContractDetails(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.contract_details = []

    def contractDetails(self, reqId, contractDetails):
        """Callback for contract details response."""
        contract = contractDetails.contract
        self.contract_details.append({
            "conId": contract.conId,
            "symbol": contract.symbol,
            "secType": contract.secType,
            "lastTradeDateOrContractMonth": contract.lastTradeDateOrContractMonth,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "localSymbol": contract.localSymbol,
            "tradingClass": contract.tradingClass,
            "multiplier": contract.multiplier,
            "minTick": contractDetails.minTick,
            "validExchanges": contractDetails.validExchanges
        })

    def contractDetailsEnd(self, reqId):
        """Callback indicating end of contract details for this request."""
        print(f"\nReceived {len(self.contract_details)} contract(s):")
        for details in self.contract_details:
            print("\nContract Details:")
            for key, value in details.items():
                print(f"  {key}: {value}")
        self.disconnect()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=None):
        """Callback for error handling."""
        print(f"Error: reqId={reqId}, code={errorCode}, message={errorString}")
        if errorCode in [200, 1100]:  # Invalid contract or connectivity issue
            self.disconnect()

def main(symbol, sec_type, exchange, currency):
    # Initialize the app
    app = IBKRContractDetails()
    
    # Connect to TWS or IB Gateway (default: localhost, port 7497 for TWS paper trading)
    app.connect("127.0.0.1", 7497, clientId=123)
    
    # Start a separate thread to handle API messages
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    # Wait briefly to ensure connection
    time.sleep(1)
    
    # Define the contract
    contract = Contract()
    contract.symbol = symbol.upper()
    contract.secType = sec_type.upper()
    contract.exchange = exchange.upper()
    contract.currency = currency.upper()
    
    # Request contract details
    req_id = 1
    app.reqContractDetails(req_id, contract)
    
    # Keep the main thread alive until disconnected
    while app.isConnected():
        time.sleep(1)

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Fetch IBKR contract details")
    parser.add_argument("--symbol", default="KRW", help="Contract symbol (e.g., KRW)")
    parser.add_argument("--sec-type", default="FUT", help="Security type (e.g., FUT)")
    parser.add_argument("--exchange", default="CME", help="Exchange (e.g., CME)")
    parser.add_argument("--currency", default="USD", help="Currency (e.g., USD)")
    args = parser.parse_args()
    
    # Run the main function with provided arguments
    main(args.symbol, args.sec_type, args.exchange, args.currency)