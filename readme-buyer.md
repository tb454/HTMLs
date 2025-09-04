# BRidge Platform (Buyer-Seller Clearing System)

BRidge is a digital commodities platform for facilitating scrap metal contracts, BOL (Bill of Lading) generation, and verification workflows — currently built for yard-to-yard and yard-to-institutional buyer interactions.

## 🔧 Tech Stack
- **Backend**: FastAPI (Python) + Postgres (Dockerized)
- **Frontend**: HTML + Bootstrap + Vanilla JS
- **Signature Capture**: HTML5 Canvas + Base64 encoding
- **PDF Generation**: ReportLab

---

## 🗂️ Folder Structure

```bash
HTMLs/
├── login.html                # Login page with role-based redirects
├── bridge-buyer.html         # Buyer dashboard — create + sign BOLs
├── bridge-yard.html          # Yard/seller dashboard (manages inventory)
├── admin-dashboard.html      # Admin panel — real-time tracking + CSV export
├── README.md                 # This file
└── js/                       # Optional JS folder for extracted logic
```

---

## 🚀 How to Run Backend (FastAPI)

1. Install requirements:
```bash
pip install -r requirements.txt
```

2. Run the server:
```bash
uvicorn main:app --reload
```

3. API will be live at: `http://localhost:8000`
4. API Docs: `http://localhost:8000/docs`

---

## 🧰 Docker Quick Start
```bash
docker-compose up --build
```
- Spins up: FastAPI backend + Postgres DB
- Visit `localhost:8000` for the API

---

## 🔑 Default Test Users (insert manually in Postgres)
| Username      | Password   | Role   |
|---------------|------------|--------|
| `winski`      | demo123    | yard   |
| `lewis`       | demo123    | buyer  |
| `cyglobal`    | demo123    | buyer  |
| `admin`       | adminpass  | admin  |

---

## 📦 Key API Routes

| Route | Method | Description |
|-------|--------|-------------|
| `/login` | POST | Authenticate and get role |
| `/create_bol` | POST | Create BOL from buyer UI |
| `/update_status/{bol_id}` | POST | Change status (e.g., to In Transit) |
| `/add_delivery_signature/{bol_id}` | POST | Add delivery confirmation |
| `/bols` | GET | Get all contracts/BOLs |
| `/bol_pdf/{bol_id}` | GET | Generate PDF with signatures |
| `/export_csv` | GET | Export full trade log as CSV |
| `/sync_dossier` | GET | JSON export for Dossier HR system |

---

## ✅ Integration Readiness (for ICE or others)
- ✅ Real-time contract lifecycle
- ✅ Signature verification (pickup + delivery)
- ✅ Audit trail export (CSV + JSON)
- ✅ Modular login flow (yard, buyer, admin)
- ✅ Dockerized deployment


# BRidge Data Retention & Recovery

- Retention Target: 7+ years
- Daily Backup:
  - Enable in Supabase → Project Settings → Backups
  - OR use: pg_dump "$DATABASE_URL" > backups/bridge-$(date +%F).sql
- Weekly Snapshot:
  - Call `/admin/export_all` (ZIP with contracts.csv + bols.csv)
  - Store in S3 (enable Object Lock)
- Restore Plan:
  1. Restore DB via Supabase UI or psql
  2. Reapply constraints/indexes (see constraints.sql)
  3. Smoke test:
     - `GET /healthz` == 200
     - `POST /contracts` == 200
     - `GET /bols` returns rows
     - `/docs` loads cleanly
- Secrets:
  - Managed via Render + GitHub Secrets (never commit `.env`)
  - Rotate on staff changes or breach

![CI](https://github.com/tb454/HTMLs/actions/workflows/ci.yml/badge.svg)


---

For help with setup or deployment, contact [info@atlasipholdingsllc.com] 
