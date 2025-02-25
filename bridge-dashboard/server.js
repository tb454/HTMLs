// server.js
const express = require('express');
const path = require('path');
const app = express();
const port = process.env.PORT || 3000;

// Serve static files from the "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// Example API endpoint to simulate trade data
app.get('/api/trade-data', (req, res) => {
  // Simulated data - in a real system, this would be dynamic
  const tradeData = {
    timestamp: new Date().toISOString(),
    marketPrices: {
      Copper: 9500 + Math.random() * 200 - 100,
      Aluminum: 2200 + Math.random() * 100 - 50
      // Add more commodities as needed
    }
  };
  res.json(tradeData);
});

// Example API endpoint for active contracts (you can simulate further endpoints similarly)
app.get('/api/active-contracts', (req, res) => {
  const contracts = [
    { ticker: 'COPR25', material: 'Copper', salePrice: 9500, expiration: '03/15/25' },
    { ticker: 'ALUM25', material: 'Aluminum', salePrice: 2200, expiration: '03/15/25' }
  ];
  res.json(contracts);
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
