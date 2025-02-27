import backtrader as bt
import pandas as pd

# Custom Scrap Metal Hedging Strategy
class ScrapHedgeStrategy(bt.Strategy):
    params = (('hedge_price', 280), ('sell_price', 320))

    def __init__(self):
        self.order = None

    def next(self):
        if not self.position:  # No open trade
            if self.data.close[0] < self.params.hedge_price:
                self.order = self.buy(size=10)  # Hedge (buy futures contract)
        else:
            if self.data.close[0] > self.params.sell_price:
                self.order = self.sell(size=10)  # Sell to close hedge

# Load Scrap Metal Price Data
data = bt.feeds.GenericCSVData(
    dataname='../data/scrap_prices.csv',
    dtformat='%Y-%m-%d',
    timeframe=bt.TimeFrame.Days,
    compression=1,
    openinterest=-1
)

# Initialize Backtrader Engine
cerebro = bt.Cerebro()
cerebro.addstrategy(ScrapHedgeStrategy)
cerebro.adddata(data)
cerebro.run()
cerebro.plot()
