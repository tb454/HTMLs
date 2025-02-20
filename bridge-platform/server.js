// server.js
const express = require('express');
const app = express();
const port = 4000; // Using 4000 since 3000 is in use

// Simulated market data (you can update these values as needed)
const marketData = {
  Copper: { price: 9500, lastUpdated: new Date() },
  Brass: { price: 7500, lastUpdated: new Date() },
  Steel: { price: 500, lastUpdated: new Date() },
  Aluminum: { price: 2200, lastUpdated: new Date() }
};

// Simulated active contracts data
let activeContracts = [
  { material: 'Copper #1', lastPrice: 9600, change: '+0.05', changePercent: '+0.52%', expiration: '03/25/2025' },
  { material: 'Brass #2', lastPrice: 7500, change: '-0.02', changePercent: '-0.26%', expiration: '04/25/2025' },
  { material: 'Steel #5', lastPrice: 500, change: '+0.10', changePercent: '+0.80%', expiration: '06/25/2025' },
  { material: 'Aluminum #3', lastPrice: 2200, change: '-0.01', changePercent: '-0.45%', expiration: '09/25/2025' }
];

// Serve static files (your wireframe) from the public folder
app.use(express.static('public'));

// API endpoint to fetch market data
app.get('/api/market-data', (req, res) => {
  res.json(marketData);
});

// API endpoint to fetch active contracts
app.get('/api/active-contracts', (req, res) => {
  res.json(activeContracts);
});

// Start the server
app.listen(port, () => {
  console.log(`BRidge backend running at http://localhost:${port}`);
});
