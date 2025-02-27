// server.js
const express = require('express');
const path = require('path');
const app = express();
const port = 3002;

// Serve static files from the "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// Simulated Market Data API Endpoint
app.get('/api/trade-data', (req, res) => {
  const tradeData = {
    timestamp: new Date().toISOString(),
    marketPrices: {
      Copper: 9500 + (Math.random() * 200 - 100),       // Simulated Copper price
      Aluminum: 2200 + (Math.random() * 100 - 50)         // Simulated Aluminum price
      // You can add additional commodities here...
    }
  };
  res.json(tradeData);
});

// Optional: Root route to serve index.html explicitly
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
