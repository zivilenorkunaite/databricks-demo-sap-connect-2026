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
  │   ├── Tool: search_documents → in-memory corporate docs (keyword search)
  │   └── Tool: get_document → full document retrieval
  ├── Conversation history → Lakebase (Postgres)
  └── User auth → x-forwarded-access-token JWT
```

## Data Sources

| Source | Catalog | Tables | Type |
|--------|---------|--------|------|
| SAP BDC (Delta Sharing) | `zivile_bdc` | PurchaseOrder, SalesOrder, BillingDocument, JournalEntry, GLAccount, SupplierInvoice (headers + items) | Read-only |
| Supplier Risk Intelligence | `zivile.default` | `supplier_risk_scores` (80 suppliers, credit ratings, ESG, risk levels) | Synthetic |
| Corporate Documents | In-memory | 5 PDFs (Procurement Policy, Code of Conduct, Audit Report, Treasury Policy, Close Procedures) | Static |

## Prerequisites

- Databricks workspace with CLI configured (`~/.databrickscfg`, profile `DEFAULT`)
- SAP BDC Delta Share accepted as catalog `zivile_bdc`
- Genie Room created with SAP BDC tables
- Lakebase instance `sap-finance-intel` provisioned
- SQL Warehouse `c2abb17a6c9e6bc0` running
- Foundation Model endpoint `databricks-claude-sonnet-4-6` available
- Python 3.10+ with `psycopg2-binary` and `databricks-sdk`

## Deployment

### Full deploy (recommended)

```bash
chmod +x deploy.sh
./deploy.sh
```

This runs all steps:
1. `databricks bundle deploy` — creates the app with all resources (warehouse, Genie Room, serving endpoint, Lakebase) via DABs
2. Resolves the app service principal
3. Creates Lakebase tables (conversations, messages with user_email + steps columns)
4. Creates and populates `supplier_risk_scores` table in Unity Catalog
5. Grants UC permissions (both `zivile_bdc` and `zivile` catalogs), warehouse CAN_USE, and Genie Room CAN_RUN to the app SP
6. Uploads source and deploys the app

### Post-deploy setup

```bash
# 1. Generate PDF documents (run as notebook in workspace)
#    setup/01_generate_pdfs.py — creates PDFs in UC Volume

# 2. Add column descriptions to Genie Room
python setup/04_genie_column_comments.py
```

### Incremental updates

After code changes, redeploy just the app:

```bash
databricks workspace import-dir ./app /Workspace/Users/<you>/sap-finance-intel --overwrite --profile DEFAULT
databricks apps deploy sap-finance-intel --source-code-path /Workspace/Users/<you>/sap-finance-intel --profile DEFAULT
```

Or via DABs:

```bash
databricks bundle deploy --profile DEFAULT
```

## Project Structure

```
├── databricks.yml              # DAB bundle config (app + resources)
├── deploy.sh                   # Full deployment script
├── app/
│   ├── app.yaml                # App runtime config (uvicorn + env via valueFrom)
│   ├── requirements.txt        # Python dependencies
│   ├── frontend/
│   │   └── index.html          # Chat UI (vanilla JS + marked.js)
│   └── server/
│       ├── main.py             # FastAPI endpoints (chat, stream, conversations, auth)
│       ├── agent.py            # Claude tool-use loop (Genie + documents)
│       ├── genie_tools.py      # Genie Room API client
│       ├── documents.py        # In-memory corporate document store
│       └── db.py               # Lakebase connection (conversation CRUD)
└── setup/
    ├── 01_generate_pdfs.py     # Notebook: create PDF docs in UC Volume
    ├── 02_setup_lakebase.py    # Script: create Lakebase tables
    ├── 03_create_supplier_risk_data.sql  # SQL: supplier risk synthetic data
    └── 04_genie_column_comments.py       # Script: Genie Room column descriptions
```

## Key Features

- **X-Ray Mode** — toggle in sidebar shows real-time agent reasoning (SSE-streamed steps)
- **Document Viewer** — click source chips to view full corporate document content
- **Per-user History** — conversations isolated by JWT email, persisted in Lakebase
- **Supplier Risk Cross-reference** — Genie joins SAP PO data with external risk scores
- **Steps Persistence** — X-Ray timeline stored in Lakebase, works on historical conversations

## DABs Resources (databricks.yml)

| Resource | Type | Purpose |
|----------|------|---------|
| `sql-warehouse` | SQL Warehouse | Genie Room query execution |
| `serving-endpoint` | Foundation Model | Claude Sonnet 4 for agent reasoning |
| `genie-room` | Genie Space | NL→SQL over SAP data + supplier risk |
| `lakebase` | Database | Conversation history (Postgres) |

The app receives resource references via `valueFrom` in `app.yaml` — no hardcoded credentials.
