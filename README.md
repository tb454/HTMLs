# HTMLs
# Bridge Dashboard

## Overview
The Bridge Dashboard is a Node.js-based application that simulates market and inventory data for various commodities. It offers REST API endpoints for market data, inventory, and trade simulation. The application features a responsive front-end dashboard, dynamic charts, and real-time updates. This project is intended for beta testing on a closed network before integrating live API data.

## Features
- **Simulated API Endpoints:**  
  - `/api/market` – Returns simulated market data.
  - `/api/inventory` – Returns simulated inventory data.
  - `/api/trade` – Simulates executing a trade with input validation.
- **Dashboard UI:**  
  Responsive front-end built with HTML, CSS (Bootstrap), and JavaScript.
- **Security and Performance:**  
  Utilizes Helmet for security, compression for performance, and caching headers for static assets.
- **Error Handling & Logging:**  
  Integrated Winston for logging and graceful error handling.
- **Basic Authentication:**  
  Protects beta access using simple Basic Authentication.

## Setup Instructions

### Prerequisites
- [Node.js](https://nodejs.org/) (version 14 or later)
- npm (comes with Node.js)
- Git

### Installation
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/tb454/HTMLs.git
   cd HTMLs
