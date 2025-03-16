// server.js
const express = require('express');
const helmet = require('helmet'); // Helps secure your app by setting various HTTP headers
const rateLimit = require('express-rate-limit'); // Middleware for rate limiting
const app = express();
const port = process.env.PORT || 3000;

// Load environment variables
require('dotenv').config();

// Security Middleware
app.use(helmet());

// Apply rate limiting to all requests (customize as needed)
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minute window
  max: 100, // limit each IP to 100 requests per windowMs
});
app.use(limiter);

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
    
    // Basic validation for required fields
    if (!material || !contracts) {
      const err = new Error('Missing required trade information.');
      err.status = 400;
      throw err;
    }
    
    // For simulation: Update finished goods inventory
    if (!inventoryData[material]) {
      const err = new Error('Invalid material');
      err.status = 400;
      throw err;
    }
    
    // Assume each contract reduces finished inventory by 20,000 lbs.
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
  // Log the error (consider using a logging library like Winston in production)
  console.error(err);
  
  // Set the status code and send a JSON error response
  res.status(err.status || 500).json({
    success: false,
    error: {
      message: err.message,
      // Only include the stack trace in development mode for security
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
    }
  });
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});

app.use(express.static('public'));
