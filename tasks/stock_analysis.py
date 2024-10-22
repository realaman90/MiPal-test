# filename: stock_analysis.py
import yfinance as yf
from datetime import datetime, timedelta

# Define the ticker symbols
tickers = ["SLX", "MT"]

# Get today's date and the date 6 months ago
today = datetime.today().date()
six_months_ago = today - timedelta(days=182)  # About 6 months ago

# Fetch the historical data for SLX and MT
data_slx = yf.download("SLX", start=six_months_ago.strftime('%Y-%m-%d'), end=today.strftime('%Y-%m-%d'))
data_mt = yf.download("MT", period="1d")

# Get the current prices
current_price_slx = data_slx['Close'][-1]
current_price_mt = data_mt['Close'][0]

# Get the price 6 months ago
price_6_months_ago_slx = data_slx['Close'][0]

# Calculate the percentage change for SLX
percentage_change_slx = ((current_price_slx - price_6_months_ago_slx) / price_6_months_ago_slx) * 100

print(f"Current price of SLX: ${current_price_slx:.2f}")
print(f"Current price of MT: ${current_price_mt:.2f}")
print(f"SLX performance over the past 6 months: {percentage_change_slx:.2f}%")