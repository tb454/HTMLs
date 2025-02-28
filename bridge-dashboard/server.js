const express = require('express');
const app = express();
const port = process.env.PORT || 4000;

// Serve static files from the current directory (adjust as needed)
app.use(express.static(__dirname));

// Sample API endpoint for trade data
app.get('/api/trade-data', (req, res) => {
  res.json({
    marketPrices: {
      Copper: 9500,
      'Busheling Steel': 700,
      Brass: 7500,
      Aluminum: 2200,
      'Electric Motors': 900,
      'Insulated Copper Wire': 3600,
      Lead: 1500,
      'HMS Steel #2': 500,
      'Cu Radiator (50Cu/50Brass)': 5600,
      Carbide: 20000,
      'Stainless Steel': 1600
    }
  });
});

// Start server with error handling
app.listen(port, (err) => {
  if (err) {
    console.error('Error starting server:', err);
    process.exit(1);
  }
  console.log(`Server is listening on port ${port}`);
});

app.get('/generate-prices', (req, res) => {
  PythonShell.run('./backend-python/price_data.py', null, function (err) {
      if (err) res.status(500).send("Error running script: " + err.message);
      else res.send("✅ Scrap prices generated successfully!");
  });
});

app.get('/', (req, res) => {
  res.send("✅ Server is running, but no index.html found. Try /generate-prices");
});
