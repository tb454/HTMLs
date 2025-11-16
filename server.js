// server.js
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const { body, validationResult } = require('express-validator');
const app = express();
const logger = require('./logger'); // Ensure you have logger.js that sets up Winston
const compression = require('compression');
const { fetchExternalData } = require('./apiClient');
const axiosRetry = require('axios-retry').default;
const port = process.env.PORT || 3000;

// Load environment variables
require('dotenv').config();

// --- Basic Authentication Middleware ---
const basicAuth = (req, res, next) => {
  const auth = { username: 'betaUser', password: 'betaPass' };
  const authHeader = req.headers.authorization || '';
  const b64auth = authHeader.split(' ')[1] || '';
  const [login, password] = Buffer.from(b64auth, 'base64').toString().split(':');
  if (login && password && login === auth.username && password === auth.password) {
    return next();
  }
  res.set('WWW-Authenticate', 'Basic realm="Beta Access"');
  return res.status(401).send('Authentication required.');
};

// Apply basic auth to all routes (for beta deployment)
app.use(basicAuth);

// --- Security and Performance Middleware ---
app.use(helmet());
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
});
app.use(limiter);
app.use(express.json());
app.use(compression());

// Serve static assets from the "bridge-dashboard" folder with caching for 1 day
app.use(express.static(path.join(__dirname, 'bridge-dashboard'), { maxAge: '1d' }));

// --- Routes ---

// Home route: Serve the complete dashboard as the home page
app.get('/', (req, res, next) => {
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

// POST endpoint to simulate a trade with input validation
app.post('/api/trade', [
  body('material').isString().withMessage('Material must be a string'),
  body('contracts').isInt({ gt: 0 }).withMessage('Contracts must be a positive integer'),
  // Optional: Validate tradePrice if needed
  // body('tradePrice').isFloat({ gt: 0 }).withMessage('Trade price must be a positive number')
], async (req, res, next) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }
    const { material, contracts, tradePrice } = req.body;
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

// Example endpoint that uses the configured Axios instance
app.get('/api/external', async (req, res, next) => {
  try {
    const data = await fetchExternalData('https://api.example.com/data');
    res.json(data);
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
  logger.error(err.message);
  res.status(err.status || 500).json({
    success: false,
    error: {
      message: err.message,
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
    }
  });
});

// --- Conditional Server Start ---
// Only call app.listen() if this file is run directly
if (require.main === module) {
  const server = app.listen(port, () => {
    logger.info(`Server listening on port ${port}`);
  });

  // Graceful shutdown
  const shutdown = () => {
    logger.info('Received shutdown signal, closing server...');
    server.close(() => {
      logger.info('HTTP server closed.');
      process.exit(0);
    });
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

module.exports = app;
