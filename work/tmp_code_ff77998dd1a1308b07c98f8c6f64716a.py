import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

# Fetch more historical data for better time series analysis
stock_data = yf.Ticker('X').history(period="1y")

# For simplicity, keeping one price column like closing price
df = stock_data['Close'].dropna()

# Fit ARIMA model (p, d, q parameters might need tuning)
model = ARIMA(df, order=(1, 1, 1))
model_fit = model.fit()

# Forecast for next 7 days
forecast = model_fit.forecast(steps=7)
print("Forecasted Prices for the next 7 days:", forecast)

# Plot to visualize
plt.figure(figsize=(10, 5))
plt.plot(df, label='Observed')
plt.plot(pd.Series(forecast, index=pd.date_range(df.index[-1], periods=8)[1:]), label='Forecast')
plt.title('United States Steel Corporation (X) Forecast')
plt.legend()
plt.show()