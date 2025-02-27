// server.js
const express = require('express');
const path = require('path');
const { PythonShell } = require('python-shell'); // Added PythonShell for running Python scripts
const app = express();
const port = 4000;

// Serve static files from the "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// Simulated Market Data API Endpoint
app.get('/api/trade-data', (req, res) => {
  const tradeData = {
    timestamp: new Date().toISOString(),
    marketPrices: {
      Copper: 9500 + (Math.random() * 200 - 100),       // Simulated Copper price
      Aluminum: 2200 + (Math.random() * 100 - 50)       // Simulated Aluminum price
      // You can add additional commodities here...
    }
  };
  res.json(tradeData);
});

// API to Generate Scrap Prices
app.get('/generate-prices', (req, res) => {
    PythonShell.run('./backend-python/price_data.py', null, function (err) {
        if (err) res.status(500).send("Error running script");
        res.send("✅ Scrap prices generated successfully!");
    });
});

// API to Run Hedge Simulator
app.get('/run-hedge', (req, res) => {
    PythonShell.run('./backend-python/hedge_sim.py', null, function (err) {
        if (err) res.status(500).send("Error running hedge simulation");
        res.send("✅ Hedge simulation completed!");
    });
});

// Optional: Root route to serve index.html explicitly
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start Server
app.listen(port, () => {
  console.log(`🔥 Server running on http://localhost:${port}`);
});
