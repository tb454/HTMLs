// server.js
require('dotenv').config();
const express = require('express');
const axios = require('axios'); // use for real API calls
const app = express();
const PORT = process.env.PORT || 3000;

const marketApiKey = process.env.MARKET_API_KEY;
const inventoryApiKey = process.env.INVENTORY_API_KEY;
const port = process.env.PORT || 3000;

// Middleware to parse JSON requests
app.use(express.json());

// ===== Market Data Endpoint =====
app.get('/api/market', async (req, res) => {
  try {
    // Replace the following with a real API call using your API key.
    // Example: const response = await axios.get('https://api.example.com/market', { params: { apiKey: process.env.MARKET_API_KEY } });
    const marketData = {
      copper: 9500,    // these values should come from your external market data provider
      brass: 7500,
      aluminum: 2200,
      // add other commodities as needed...
    };
    res.json(marketData);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ===== Inventory Data Endpoint =====
app.get('/api/inventory', async (req, res) => {
  try {
    // Here, integrate your actual inventory system or database.
    // For demo purposes, we simulate inventory data:
    const inventoryData = {
      totalWIP: 150000,
      totalFinished: 250000,
      total: 400000,
      details: [
        { commodity: "Copper", wip: 5000, finished: 10000, total: 15000, avgCost: 0.80, totalPurchaseValue: 15000 * 0.80 },
        { commodity: "Brass", wip: 3000, finished: 7000, total: 10000, avgCost: 0.70, totalPurchaseValue: 10000 * 0.70 },
        // add additional commodity objects as needed...
      ]
    };
    res.json(inventoryData);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ===== Trade Execution Endpoint =====
app.post('/api/trades', (req, res) => {
  try {
    const { material, quantity, tradePrice, expiration } = req.body;
    // Here you would:
    // - Process the trade (e.g., update inventory, record the trade in a DB, etc.)
    // - For demo, we simply return a success message.
    res.json({ success: true, message: 'Trade executed successfully.' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
