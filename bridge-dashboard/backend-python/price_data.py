import pandas as pd
import random
from datetime import datetime, timedelta

# Generate 1 year of daily scrap price data (synthetic)
dates = [datetime.today() - timedelta(days=i) for i in range(365)]
prices = [300 + random.uniform(-50, 50) for _ in range(365)]  # Scrap prices fluctuate around $300/ton

# Save data to CSV
df = pd.DataFrame({'Date': dates, 'Close': prices})
df.to_csv('data/scrap_prices.csv', index=False)

print("✅ Scrap price dataset generated and saved to 'data/scrap_prices.csv'")
