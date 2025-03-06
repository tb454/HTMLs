from flask import Flask, jsonify
import yfinance as yf

app = Flask(__name__)

# Mapping from commodity names to Yahoo Finance ticker symbols.
COMMODITY_TICKERS = {
    "Copper": "HG=F",                  # COMEX Copper Futures
    "Busheling Steel": "STEEL",         # Placeholder; adjust as needed
    "Brass": "BRASS",                   # Placeholder
    "Aluminum": "ALI=F",                # Example for aluminum futures
    "Electric Motors": "EMOT",          # Placeholder; may need simulation
    "Insulated Copper Wire": "ICW",     # Placeholder
    "Lead": "LEAD=F",                   # COMEX Lead Futures (if available)
    "HMS Steel #2": "HMS",              # Placeholder
    "Cu Radiator (50Cu/50Brass)": "CRAD", # Placeholder
    "Carbide": "CARB",                  # Placeholder
    "Stainless Steel": "STL"            # Placeholder
}

@app.route('/api/market-data')
def market_data():
    result = {}
    for commodity, ticker in COMMODITY_TICKERS.items():
        try:
            # Use yfinance to fetch ticker info.
            data = yf.Ticker(ticker)
            # Attempt to get the current market price (note: this might be delayed)
            price = data.info.get("regularMarketPrice", None)
            if price is None:
                # Fallback: get the last closing price from historical data
                hist = data.history(period="1d")
                price = hist['Close'].iloc[-1] if not hist.empty else None
            result[commodity] = price
        except Exception as e:
            result[commodity] = None
    return jsonify({"marketPrices": result})

if __name__ == '__main__':
    app.run(debug=True)
