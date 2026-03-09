#!/bin/bash
# ============================================================================
# SAP Finance Intelligence — Full Deployment Script
# ============================================================================
# Deploys everything needed for the demo:
#   1. databricks bundle deploy (app + resources via DABs)
#   2. Lakebase schema setup (conversations + messages tables)
#   3. Supplier risk data table (external enrichment data in UC)
#   4. UC permissions for app service principal
#   5. Warehouse permissions for app service principal
#   6. App deployment (source upload + deploy)
#
# Prerequisites:
#   - Delta Share from SAP BDC workspace accepted as catalog `zivile_bdc`
#   - Genie Room created with SAP BDC tables (space_id in databricks.yml)
#   - Lakebase instance provisioned (instance name in databricks.yml)
#   - databricks CLI configured with DEFAULT profile
#   - Python 3.10+ with psycopg2-binary, databricks-sdk installed
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE="DEFAULT"
WAREHOUSE_ID="c2abb17a6c9e6bc0"
LAKEBASE_INSTANCE="sap-finance-intel"
LAKEBASE_DB="sap_finance_intel"
BDC_CATALOG="zivile_bdc"
LOCAL_CATALOG="zivile"
BDC_SCHEMAS=("PurchaseOrder" "SalesOrder" "BillingDocument" "JournalEntryHeader" "GeneralLedgerAccount" "SupplierInvoice")
GENIE_ROOM_ID="01f11b67acc9177abc5f344f75fe4389"

# Helper: run SQL statement against warehouse
run_sql() {
  local stmt="$1"
  local result
  result=$(databricks api post /api/2.0/sql/statements/ \
    --profile "$PROFILE" \
    --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": $(python3 -c "import json; print(json.dumps('$stmt'))"), \"format\": \"JSON_ARRAY\", \"wait_timeout\": \"30s\"}" 2>/dev/null)
  local state
  state=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null)
  if [ "$state" = "FAILED" ]; then
    local err
    err=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('error',{}).get('message','unknown'))" 2>/dev/null)
    echo "    FAILED: $err"
    return 1
  fi
  echo "    OK ($state)"
  return 0
}

# Helper: run SQL from file (splits on semicolons)
run_sql_file() {
  local file="$1"
  echo "  Executing $file..."
  python3 -c "
import json, requests, sys, time
stmts = open('$file').read()
# Split on semicolons, skip empty/comment-only
parts = [s.strip() for s in stmts.split(';') if s.strip() and not s.strip().startswith('--')]
for i, stmt in enumerate(parts):
    print(f'    Statement {i+1}/{len(parts)}...', end=' ', flush=True)
    resp = requests.post(
        get_host() + '/api/2.0/sql/statements/',
        headers={'Authorization': f'Bearer {get_token()}'},
        json={'warehouse_id': '$WAREHOUSE_ID', 'statement': stmt, 'format': 'JSON_ARRAY', 'wait_timeout': '50s'},
        timeout=60)
    r = resp.json()
    state = r.get('status',{}).get('state','UNKNOWN')
    if state == 'FAILED':
        err = r.get('status',{}).get('error',{}).get('message','unknown')
        print(f'FAILED: {err}')
    else:
        print(f'OK')

def get_host():
    import configparser, os
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser('~/.databrickscfg'))
    return cfg.get('$PROFILE', 'host').rstrip('/')

def get_token():
    import subprocess, json as j
    raw = subprocess.run(['databricks', 'auth', 'token', '--profile', '$PROFILE'], capture_output=True, text=True).stdout.strip()
    try: return j.loads(raw)['access_token']
    except: return raw
" 2>&1
}

echo "============================================"
echo " SAP Finance Intelligence — Deployment"
echo "============================================"
echo ""

# ─── Step 1: Bundle Deploy (App + Resources) ───────────────────────────────
echo "[1/6] Running databricks bundle deploy..."
databricks bundle deploy --profile "$PROFILE"
echo "  Done"
echo ""

# ─── Step 2: Get App Service Principal ──────────────────────────────────────
echo "[2/6] Resolving app service principal..."
APP_NAME="sap-finance-intel"

# Wait for app to be available after bundle deploy
sleep 3

APP_JSON=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null)
APP_SP_ID=$(echo "$APP_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('service_principal_id') or d.get('effective_service_principal_id') or '')
" 2>/dev/null)

if [ -z "$APP_SP_ID" ]; then
  echo "  WARNING: Could not find app service principal. Permissions must be granted manually."
else
  # Get the applicationId (UUID) for UC grants
  SP_APP_ID=$(databricks service-principals get "$APP_SP_ID" --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId',''))" 2>/dev/null) || SP_APP_ID=""
  GRANT_ID="${SP_APP_ID:-$APP_SP_ID}"
  echo "  SP numeric ID: $APP_SP_ID"
  echo "  SP application ID: $GRANT_ID"
fi
echo ""

# ─── Step 3: Lakebase Tables ───────────────────────────────────────────────
echo "[3/6] Setting up Lakebase tables..."
python3 setup/02_setup_lakebase.py
echo ""

# ─── Step 4: Supplier Risk Data ────────────────────────────────────────────
echo "[4/6] Creating supplier risk data table..."

# Run SQL file using databricks API
python3 << 'PYEOF'
import subprocess, json, configparser, os, requests, time

PROFILE = "DEFAULT"
WAREHOUSE_ID = "c2abb17a6c9e6bc0"

cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
host = cfg.get(PROFILE, "host").rstrip("/")

raw = subprocess.run(["databricks", "auth", "token", "--profile", PROFILE], capture_output=True, text=True).stdout.strip()
try:
    token = json.loads(raw)["access_token"]
except:
    token = raw

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

with open("setup/03_create_supplier_risk_data.sql") as f:
    sql = f.read()

# Split on semicolons, skip comments and empty
stmts = [s.strip() for s in sql.split(";") if s.strip() and not all(line.strip().startswith("--") or not line.strip() for line in s.strip().split("\n"))]

for i, stmt in enumerate(stmts):
    # Skip pure comment blocks
    lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
    if not lines:
        continue
    clean = "\n".join(lines)
    label = clean[:60].replace("\n", " ")
    print(f"  [{i+1}/{len(stmts)}] {label}...")
    resp = requests.post(f"{host}/api/2.0/sql/statements/",
        headers=headers,
        json={"warehouse_id": WAREHOUSE_ID, "statement": clean, "format": "JSON_ARRAY", "wait_timeout": "50s"},
        timeout=60)
    r = resp.json()
    state = r.get("status", {}).get("state", "UNKNOWN")
    if state == "FAILED":
        err = r.get("status", {}).get("error", {}).get("message", "unknown")
        print(f"    FAILED: {err}")
    else:
        print(f"    OK")

print("  Supplier risk data ready")
PYEOF
echo ""

# ─── Step 5: UC + Warehouse Permissions ─────────────────────────────────────
echo "[5/6] Granting permissions to app service principal..."

if [ -n "${GRANT_ID:-}" ]; then
  # BDC catalog permissions
  echo "  USE CATALOG on ${BDC_CATALOG}..."
  run_sql "GRANT USE CATALOG ON CATALOG \`${BDC_CATALOG}\` TO \`${GRANT_ID}\`" || true

  for SCHEMA in "${BDC_SCHEMAS[@]}"; do
    echo "  USE SCHEMA + SELECT on ${BDC_CATALOG}.${SCHEMA}..."
    run_sql "GRANT USE SCHEMA ON SCHEMA \`${BDC_CATALOG}\`.\`${SCHEMA}\` TO \`${GRANT_ID}\`" || true
    run_sql "GRANT SELECT ON SCHEMA \`${BDC_CATALOG}\`.\`${SCHEMA}\` TO \`${GRANT_ID}\`" || true
  done

  # Local catalog permissions (for supplier_risk_scores)
  echo "  USE CATALOG on ${LOCAL_CATALOG}..."
  run_sql "GRANT USE CATALOG ON CATALOG \`${LOCAL_CATALOG}\` TO \`${GRANT_ID}\`" || true
  echo "  USE SCHEMA on ${LOCAL_CATALOG}.default..."
  run_sql "GRANT USE SCHEMA ON SCHEMA \`${LOCAL_CATALOG}\`.\`default\` TO \`${GRANT_ID}\`" || true
  echo "  SELECT on supplier_risk_scores..."
  run_sql "GRANT SELECT ON TABLE \`${LOCAL_CATALOG}\`.\`default\`.\`supplier_risk_scores\` TO \`${GRANT_ID}\`" || true

  # Warehouse CAN_USE permission
  echo "  Warehouse CAN_USE..."
  databricks api patch /api/2.0/permissions/sql/warehouses/${WAREHOUSE_ID} \
    --profile "$PROFILE" \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"${GRANT_ID}\", \"permission_level\": \"CAN_USE\"}]}" \
    >/dev/null 2>&1 && echo "    OK" || echo "    FAILED (may already exist)"

  # Genie Room CAN_RUN permission
  echo "  Genie Room CAN_RUN..."
  databricks api patch /api/2.0/permissions/genie/${GENIE_ROOM_ID} \
    --profile "$PROFILE" \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"${GRANT_ID}\", \"permission_level\": \"CAN_RUN\"}]}" \
    >/dev/null 2>&1 && echo "    OK" || echo "    FAILED (may already exist)"
else
  echo "  SKIPPED — no service principal found"
fi
echo ""

# ─── Step 6: Deploy App ────────────────────────────────────────────────────
echo "[6/6] Deploying app..."
# Upload source (needed because bundle deploy creates app but may not deploy latest source)
WS_SOURCE_PATH=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('default_source_code_path',''))" 2>/dev/null) || WS_SOURCE_PATH=""
if [ -z "$WS_SOURCE_PATH" ]; then
  WS_SOURCE_PATH="/Workspace/Users/$(databricks current-user me --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")/sap-finance-intel"
fi

databricks workspace import-dir ./app "$WS_SOURCE_PATH" --profile "$PROFILE" --overwrite 2>&1 | tail -1
databricks apps deploy "$APP_NAME" --source-code-path "$WS_SOURCE_PATH" --profile "$PROFILE" 2>&1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('status',{}).get('state','UNKNOWN')
print(f'  Deploy state: {state}')
"
echo ""

# ─── Summary ────────────────────────────────────────────────────────────────
echo "============================================"
echo " Deployment Complete"
echo "============================================"

APP_URL=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null) || APP_URL=""
echo ""
echo "App URL:       ${APP_URL:-https://${APP_NAME}-<workspace>.databricksapps.com}"
echo "Genie Room:    ${GENIE_ROOM_ID}"
echo "Warehouse:     ${WAREHOUSE_ID}"
echo "Lakebase:      ${LAKEBASE_INSTANCE} / ${LAKEBASE_DB}"
echo "BDC Catalog:   ${BDC_CATALOG} (Delta Sharing — read-only)"
echo "Risk Table:    ${LOCAL_CATALOG}.default.supplier_risk_scores"
echo ""
echo "Remaining manual steps:"
echo "  1. Run setup/01_generate_pdfs.py notebook in workspace to create PDF documents"
echo "  2. Configure Genie Room column comments via setup/04_genie_column_comments.py"
echo ""
