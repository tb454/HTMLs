// server.js
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const app = express();
const logger = require('./logger');
const { fetchExternalData } = require('./apiClient');
const port = process.env.PORT || 3000;

// Load environment variables
require('dotenv').config();

// Security Middleware
app.use(helmet());
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
});
app.use(limiter);

// Use JSON parser middleware
app.use(express.json());

// (Optional) Serve static assets from the "bridge-dashboard" folder if needed
// Remove or adjust this if your static assets are located elsewhere
app.use(express.static(path.join(__dirname, 'bridge-dashboard')));

// Serve the complete dashboard as the home page
app.get('/', (req, res, next) => {
  // Use the file from the repository root since combined-layout.html is located there
  const filePath = path.join(__dirname, "combined-layout.html");
  console.log("Resolved file path:", filePath);
  res.sendFile(filePath, (err) => {
    if (err) {
      console.error("Error sending file:", err);
      next(err);
    }
  });
});

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
app.get('/api/market', (req, res, next) => {
  try {
    res.json(marketData);
  } catch (err) {
    next(err);
  }
});

// GET endpoint for inventory data
app.get('/api/inventory', (req, res, next) => {
  try {
    res.json(inventoryData);
  } catch (err) {
    next(err);
  }
});

// POST endpoint to simulate a trade
app.post('/api/trade', async (req, res, next) => {
  try {
    const { material, contracts, tradePrice } = req.body;
    if (!material || !contracts) {
      const err = new Error('Missing required trade information.');
      err.status = 400;
      throw err;
    }
    if (!inventoryData[material]) {
      const err = new Error('Invalid material');
      err.status = 400;
      throw err;
    }
    const reduction = contracts * 20000;
    inventoryData[material].finished = Math.max(inventoryData[material].finished - reduction, 0);
    res.json({
      success: true,
      message: `Trade executed for ${contracts} contract(s) of ${material}.`,
      updatedInventory: inventoryData[material]
    });
  } catch (error) {
    next(error);
  }
});

// Catch-all for non-existent routes (404 Not Found)
app.use((req, res, next) => {
  const err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// Global error-handling middleware
app.use((err, req, res, next) => {
  console.error(err);
  res.status(err.status || 500).json({
    success: false,
    error: {
      message: err.message,
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
    }
  });
});
// Example endpoint that uses the configured Axios instance
app.get('/api/external', async (req, res, next) => {
  try {
    const data = await fetchExternalData('https://api.example.com/data');
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
