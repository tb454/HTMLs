// server.js
const { PythonShell } = require('python-shell');
const express = require('express');
const path = require('path');
const app = express();

const port = 4000; // You can change to 3002 if needed

// Serve static files from the "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// Simulated Market Data API
app.get('/api/trade-data', (req, res) => {
  res.json({
    timestamp: new Date().toISOString(),
    marketPrices: {
      Copper: 9500 + (Math.random() * 200 - 100),
      Aluminum: 2200 + (Math.random() * 100 - 50),
    },
  });
});

// API to Generate Scrap Prices
app.get('/generate-prices', (req, res) => {
    PythonShell.run('./backend-python/price_data.py', null, function (err) {
        if (err) {
            res.status(500).send("Error running script: " + err.message);
        } else {
            res.send("✅ Scrap prices generated successfully!");
        }
    });
});

// API to Run Hedge Simulator
app.get('/run-hedge', (req, res) => {
    PythonShell.run('./backend-python/hedge_sim.py', null, function (err) {
        if (err) {
            res.status(500).send("Error running hedge simulation: " + err.message);
        } else {
            res.send("✅ Hedge simulation completed!");
        }
    });
});

// Root route to serve index.html explicitly
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start the server
app.listen(port, () => {
  console.log(`🔥 Server running on http://localhost:${port}`);
});
