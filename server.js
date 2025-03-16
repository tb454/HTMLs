// server.js
const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

// Use JSON parser middleware
app.use(express.json());

// Simulated Market Data
let marketData = {
  Copper: 9500,
  Brass: 7500,
  Aluminum: 2200,
  Steel: 500
};

// Simulated Inventory Data
let inventoryData = {
  Copper: { wip: 15000, finished: 25000 },
  Aluminum: { wip: 12000, finished: 18000 },
  Brass: { wip: 10000, finished: 15000 },
  Steel: { wip: 8000, finished: 12000 }
};

// GET endpoint for market data
app.get('/api/market', (req, res) => {
  res.json(marketData);
});

// GET endpoint for inventory data
app.get('/api/inventory', (req, res) => {
  res.json(inventoryData);
});

// POST endpoint to simulate a trade
app.post('/api/trade', (req, res) => {
  const { material, contracts, tradePrice } = req.body;
  // For simulation: Update finished goods inventory
  if (inventoryData[material]) {
    // Assume each contract reduces finished inventory by 20,000 lbs.
    const reduction = contracts * 20000;
    inventoryData[material].finished = Math.max(inventoryData[material].finished - reduction, 0);
    res.json({
      success: true,
      message: `Trade executed for ${contracts} contract(s) of ${material}.`,
      updatedInventory: inventoryData[material]
    });
  } else {
    res.status(400).json({ success: false, message: 'Invalid material' });
  }
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
