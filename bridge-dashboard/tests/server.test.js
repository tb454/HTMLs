// tests/server.test.js
const request = require('supertest');
const app = require('../server');

request(app).get('/api/market').then;

describe('API Endpoints', () => {
  // Test the GET /api/market endpoint
  it('should return market data', async () => {
    const res = await request(app).get('/api/market');
    expect(res.statusCode).toEqual(200);
    expect(res.body).toHaveProperty('Copper');
  });

  // Test the GET /api/inventory endpoint
  it('should return inventory data', async () => {
    const res = await request(app).get('/api/inventory');
    expect(res.statusCode).toEqual(200);
    expect(res.body).toHaveProperty('Aluminum');
  });

  // Test the POST /api/trade endpoint with invalid input
  it('should return a 400 error for invalid trade input', async () => {
    const res = await request(app)
      .post('/api/trade')
      .send({ material: 123, contracts: -1 });
    expect(res.statusCode).toEqual(400);
    expect(res.body.success).toBe(false);
    expect(res.body).toHaveProperty('errors');
  });

  // Test the POST /api/trade endpoint with valid input
  it('should execute trade for valid input', async () => {
    const res = await request(app)
      .post('/api/trade')
      .send({ material: "Copper", contracts: 1, tradePrice: 4.19 });
    expect(res.statusCode).toEqual(200);
    expect(res.body.success).toBe(true);
  });
});
