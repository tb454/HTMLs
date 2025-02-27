// server.js
const express = require('express');
const path = require('path');
const app = express();
const port = 3000; // We'll use port 3000

// Serve static files from the "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// Optional: Define a route for the root URL explicitly
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

