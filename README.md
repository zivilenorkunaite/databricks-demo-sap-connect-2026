# SAP Finance Intelligence Agent

AI-powered finance analysis across live SAP S/4HANA data (via BDC Delta Sharing), external supplier risk intelligence, and corporate governance documents. Built for the **SAP Connect for Data & IT 2026** demo.

## Architecture

```
Browser (Chat UI)
  │
  ├── SSE stream (/api/chat/stream)
  │
FastAPI (Databricks App)
  ├── Agent (Claude Sonnet 4 via Foundation Model API)
  │   ├── Tool: query_sap_data → Genie Room API → SAP BDC tables + supplier_risk_scores
  │   ├── Tool: search_documents → corporate docs (keyword search)
  │   └── Tool: get_document → full document retrieval
  ├── Conversation history → Lakebase (Postgres)
  └── User auth → x-forwarded-access-token JWT
```

## Data Sources

| Source | Catalog | Tables | Type |
|--------|---------|--------|------|
| SAP BDC (Delta Sharing) | `zivile_bdc` | PurchaseOrder, SalesOrder, BillingDocument, JournalEntry, GLAccount, SupplierInvoice (headers + items) | Read-only |
| Supplier Risk Intelligence | `zivile.default` | `supplier_risk_scores` — 80 suppliers with credit ratings, ESG scores, risk levels | Synthetic |
| Corporate Documents | `zivile.default` | 5 finance documents (Procurement Policy, Code of Conduct, Audit Report, Treasury Policy, Close Procedures) | Static |

## Prerequisites

- Databricks workspace with CLI configured (`~/.databrickscfg`)
- SAP BDC Delta Share accepted as catalog `zivile_bdc`
- Genie Room created with SAP BDC tables
- Lakebase instance `sap-finance-intel` provisioned
- Foundation Model endpoint `databricks-claude-sonnet-4-6` available
- Python 3.10+ with `psycopg2-binary`, `databricks-sdk`, `fpdf2`, `requests`

## Deployment

```bash
chmod +x deploy.sh
./deploy.sh
```

Single command handles everything:
1. `databricks bundle deploy` — app + resources (warehouse, Genie Room, serving endpoint, Lakebase)
2. Resolves app service principal
3. Creates Lakebase tables (conversations, messages)
4. Creates and populates supplier risk data in Unity Catalog
5. Generates corporate document PDFs and uploads to UC Volume
6. Configures Genie Room column descriptions
7. Grants minimum required permissions to app SP
8. Deploys the app

Optionally set `DATABRICKS_WAREHOUSE_ID` env var; if unset, the script auto-discovers the first running warehouse.

### Incremental updates

```bash
databricks bundle deploy
```

## Project Structure

```
├── databricks.yml              # DAB bundle config (app + resources)
├── deploy.sh                   # Full deployment script
├── app/
│   ├── app.yaml                # App runtime config (uvicorn + env via valueFrom)
│   ├── requirements.txt
│   ├── frontend/
│   │   └── index.html          # Chat UI (vanilla JS + marked.js)
│   └── server/
│       ├── main.py             # FastAPI endpoints (chat, stream, conversations, auth)
│       ├── agent.py            # Claude tool-use loop (Genie + documents)
│       ├── genie_tools.py      # Genie Room API client
│       ├── documents.py        # Corporate document store
│       └── db.py               # Lakebase connection (conversation CRUD)
└── setup/
    ├── 02_setup_lakebase.py    # Lakebase table creation
    ├── 03_create_supplier_risk_data.sql
    ├── 04_genie_column_comments.py
    └── 05_generate_and_upload_pdfs.py
```

## Key Features

- **X-Ray Mode** — toggle shows real-time agent reasoning (SSE-streamed steps)
- **Document Viewer** — click source chips to view full corporate document content
- **Per-user History** — conversations isolated by JWT email, persisted in Lakebase
- **Supplier Risk Cross-reference** — Genie joins SAP PO data with external risk scores
- **Steps Persistence** — X-Ray timeline persisted, works on historical conversations

## DABs Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `sql-warehouse` | SQL Warehouse | Genie Room query execution |
| `serving-endpoint` | Foundation Model | Claude Sonnet 4 for agent reasoning |
| `genie-room` | Genie Space | NL→SQL over SAP data + supplier risk |
| `lakebase` | Database | Conversation history (Postgres) |

The app receives resource references via `valueFrom` in `app.yaml` — no hardcoded credentials.
