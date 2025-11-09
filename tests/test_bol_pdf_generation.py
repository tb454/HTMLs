## Test BOL PDF Generation
def test_bol_pdf(client):
    # create BOL
    response = client.post("/bol", json={"...": "..."})
    assert response.status_code == 201
    bol_id = response.json().get("id")

    # generate PDF
    response = client.get(f"/bol/{bol_id}/pdf")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/pdf"
    assert len(response.content) > 1024  # PDF size > 1KB