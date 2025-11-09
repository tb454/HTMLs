# test_auth_rate_limit.py
def test_rate_limit(client):
    # hammer /login and assert 429 after threshold
   for _ in range(10):
       response = client.post("/login", json={"username": "test", "password": "test"})
       assert response.status_code == 200
   response = client.post("/login", json={"username": "test", "password": "test"})
   assert response.status_code == 429
