# Bridge Dashboard

## Overview
The Bridge Dashboard is a Node.js-based application that simulates market and inventory data for various commodities. It offers REST API endpoints for market data, inventory, trade simulation, and real-time updates. The application features a responsive front-end dashboard, dynamic charts, and real-time updates. This project is intended for beta testing on a closed network before integrating live API data.

## Features
- **API Endpoints:**  
  - `GET /scrap-metal` – Retrieve live scrap metal inventory data.
  - `POST /scrap-metal` – Create a new scrap metal record.
  - `GET /analytics` – Retrieve analytics data.
  - `GET /futures` – Fetch current futures data.
  - `POST /futures` – Update or create futures data.
  - `GET /external/futures/{symbol}` – Fetch external futures data for a specific symbol.
  - `GET /orders` – Retrieve orders.
  - `POST /orders` – Create a new order.
  - `GET /order-matches` – Retrieve order match details.
  - `GET /risk-report` – Retrieve risk report data.
  - `POST /predict-price` – Predict scrap metal prices.
  - `POST /update-futures` – Trigger a futures update.
  - `GET /admin/health` – Check backend server health status.
  - `POST /token` – User authentication.
  - **WebSocket:** Connect to `ws://localhost:8000/ws/futures` for real-time updates.
  
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
Install Dependencies:

bash
Copy
npm install
Start the Application:

bash
Copy
npm start
Usage Examples
Example: Fetch Scrap Metal Inventory
bash
Copy
curl -X GET http://localhost:8000/scrap-metal
Response:

json
Copy
[
  { "metal_type": "Copper", "quantity": 10000, "price": 9500, "quality": "High", "location": "Warehouse A" },
  ...
]
Dashboard Screenshot

Configuration & Environment Variables
Create a .env file in the project root with:

plaintext
Copy
PORT=8000
NODE_ENV=development
API_KEY=yourapikey
DB_URL=mongodb://localhost:27017/bridge_dashboard
Development & Testing
Development Server:
Run:

bash
Copy
npm run dev
Testing:
Run:

bash
Copy
npm test
Linting:
Run:

bash
Copy
npm run lint
Deployment Guidelines
Deploy with Docker:

bash
Copy
docker build -t bridge-dashboard .
docker run -p 8000:8000 bridge-dashboard
For production, ensure environment variables are set appropriately and consider using a process manager like PM2.

Contributing
Contributions are welcome! Please follow these steps:

Fork the repository.

Create a feature branch: git checkout -b feature/my-new-feature

Commit your changes: git commit -am 'Add new feature'

Push the branch: git push origin feature/my-new-feature

Open a Pull Request.

For detailed guidelines, see CONTRIBUTING.md.

License
This project is licensed under the MIT License - see the LICENSE file for details.

Contact & Support
For any questions or support, please open an issue in the GitHub repository or email tbyers1233@gmail.com.

