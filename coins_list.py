import requests

url = "https://api.binance.com/api/v3/exchangeInfo"
data = requests.get(url).json()

symbols = [
    s["symbol"]
    for s in data["symbols"]
    if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["isSpotTradingAllowed"]
]

print(symbols)