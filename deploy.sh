#!/bin/bash
# ============================================================================
# SAP Finance Intelligence — Full Deployment Script
# ============================================================================
# Single command deploys everything:
#   1. databricks bundle deploy (app + resources via DABs)
#   2. Lakebase schema setup
#   3. Supplier risk data table in UC
#   4. Corporate document PDFs to UC Volume
#   5. Genie Room column descriptions
#   6. UC + warehouse + Genie permissions for app SP
#   7. App deployment
#
# Prerequisites:
#   - Delta Share from SAP BDC workspace accepted as catalog `zivile_bdc`
#   - Genie Room created with SAP BDC tables (space_id in databricks.yml)
#   - Lakebase instance provisioned (instance name in databricks.yml)
#   - databricks CLI configured with DEFAULT profile
#   - Python 3.10+ with psycopg2-binary, databricks-sdk, fpdf2, requests
#
# Optional env vars:
#   DATABRICKS_WAREHOUSE_ID — SQL warehouse to use (auto-discovered if unset)
#   DATABRICKS_PROFILE      — CLI profile (default: DEFAULT)
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
LAKEBASE_INSTANCE="sap-finance-intel"
LAKEBASE_DB="sap_finance_intel"
BDC_CATALOG="zivile_bdc"
LOCAL_CATALOG="zivile"
BDC_SCHEMAS=("PurchaseOrder" "SalesOrder" "BillingDocument" "JournalEntryHeader" "GeneralLedgerAccount" "SupplierInvoice")
GENIE_ROOM_ID="01f11b67acc9177abc5f344f75fe4389"
APP_NAME="sap-finance-intel"

# ─── Resolve warehouse ID ──────────────────────────────────────────────────
if [ -n "${DATABRICKS_WAREHOUSE_ID:-}" ]; then
  WAREHOUSE_ID="$DATABRICKS_WAREHOUSE_ID"
  echo "Using warehouse from env: $WAREHOUSE_ID"
else
  echo "Auto-discovering SQL warehouse..."
  WAREHOUSE_ID=$(databricks api get /api/2.0/sql/warehouses --profile "$PROFILE" 2>/dev/null \
    | python3 -c "
import sys, json
whs = json.load(sys.stdin).get('warehouses', [])
# Prefer running warehouses, then any
running = [w for w in whs if w.get('state') == 'RUNNING']
pick = (running or whs or [{}])[0]
print(pick.get('id', ''))
" 2>/dev/null) || WAREHOUSE_ID=""
  if [ -z "$WAREHOUSE_ID" ]; then
    echo "  ERROR: No SQL warehouse found. Set DATABRICKS_WAREHOUSE_ID and retry."
    exit 1
  fi
  echo "  Found: $WAREHOUSE_ID"
fi
export WAREHOUSE_ID
echo ""

echo "============================================"
echo " SAP Finance Intelligence — Deployment"
echo "============================================"
echo ""

# ─── Step 1: Bundle Deploy (App + Resources) ───────────────────────────────
echo "[1/8] Running databricks bundle deploy..."
databricks bundle deploy --profile "$PROFILE"
echo "  Done"
echo ""

# ─── Step 2: Get App Service Principal ──────────────────────────────────────
echo "[2/8] Resolving app service principal..."

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
  SP_APP_ID=$(databricks service-principals get "$APP_SP_ID" --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId',''))" 2>/dev/null) || SP_APP_ID=""
  GRANT_ID="${SP_APP_ID:-$APP_SP_ID}"
  echo "  SP numeric ID: $APP_SP_ID"
  echo "  SP application ID: $GRANT_ID"
fi
echo ""

# ─── Step 3: Lakebase Tables ───────────────────────────────────────────────
echo "[3/8] Setting up Lakebase tables..."
python3 setup/02_setup_lakebase.py
echo ""

# ─── Step 4: Supplier Risk Data ────────────────────────────────────────────
echo "[4/8] Creating supplier risk data table..."
python3 << 'PYEOF'
import subprocess, json, configparser, os, requests

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]

cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
host = cfg.get(PROFILE, "host").rstrip("/")

raw = subprocess.run(["databricks", "auth", "token", "--profile", PROFILE], capture_output=True, text=True).stdout.strip()
try:
    token = json.loads(raw)["access_token"]
except (json.JSONDecodeError, KeyError):
    token = raw

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

with open("setup/03_create_supplier_risk_data.sql") as f:
    sql = f.read()

stmts = [s.strip() for s in sql.split(";") if s.strip() and not all(
    line.strip().startswith("--") or not line.strip() for line in s.strip().split("\n"))]

for i, stmt in enumerate(stmts):
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
        print(f"    FAILED: {r.get('status',{}).get('error',{}).get('message','unknown')}")
    else:
        print(f"    OK")

print("  Supplier risk data ready")
PYEOF
echo ""

# ─── Step 5: Generate & Upload PDFs ────────────────────────────────────────
echo "[5/8] Generating corporate document PDFs..."
python3 setup/05_generate_and_upload_pdfs.py
echo ""

# ─── Step 6: Genie Room Column Comments ────────────────────────────────────
echo "[6/8] Configuring Genie Room column descriptions..."
python3 setup/04_genie_column_comments.py
echo ""

# ─── Step 7: Permissions ───────────────────────────────────────────────────
echo "[7/8] Granting permissions to app service principal..."

# Helper: run SQL grant
run_grant() {
  local stmt="$1"
  local result
  result=$(databricks api post /api/2.0/sql/statements/ \
    --profile "$PROFILE" \
    --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": $(python3 -c "import json; print(json.dumps('''$stmt'''))"), \"format\": \"JSON_ARRAY\", \"wait_timeout\": \"30s\"}" 2>/dev/null)
  local state
  state=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null)
  if [ "$state" = "FAILED" ]; then
    echo "    FAILED: $(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('error',{}).get('message','')[:80])" 2>/dev/null)"
  else
    echo "    OK"
  fi
}

if [ -n "${GRANT_ID:-}" ]; then
  # BDC catalog: USE CATALOG + per-schema USE SCHEMA + SELECT
  echo "  BDC catalog access..."
  run_grant "GRANT USE CATALOG ON CATALOG \`${BDC_CATALOG}\` TO \`${GRANT_ID}\`"
  for SCHEMA in "${BDC_SCHEMAS[@]}"; do
    echo "  ${BDC_CATALOG}.${SCHEMA}..."
    run_grant "GRANT USE SCHEMA ON SCHEMA \`${BDC_CATALOG}\`.\`${SCHEMA}\` TO \`${GRANT_ID}\`"
    run_grant "GRANT SELECT ON SCHEMA \`${BDC_CATALOG}\`.\`${SCHEMA}\` TO \`${GRANT_ID}\`"
  done

  # Local catalog: only USE CATALOG + USE SCHEMA on default + SELECT on specific table + volume access
  echo "  Local catalog access..."
  run_grant "GRANT USE CATALOG ON CATALOG \`${LOCAL_CATALOG}\` TO \`${GRANT_ID}\`"
  run_grant "GRANT USE SCHEMA ON SCHEMA \`${LOCAL_CATALOG}\`.\`default\` TO \`${GRANT_ID}\`"
  run_grant "GRANT SELECT ON TABLE \`${LOCAL_CATALOG}\`.\`default\`.\`supplier_risk_scores\` TO \`${GRANT_ID}\`"
  run_grant "GRANT READ VOLUME ON VOLUME \`${LOCAL_CATALOG}\`.\`default\`.\`finance_docs\` TO \`${GRANT_ID}\`"

  # Warehouse CAN_USE
  echo "  Warehouse CAN_USE..."
  databricks api patch /api/2.0/permissions/sql/warehouses/${WAREHOUSE_ID} \
    --profile "$PROFILE" \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"${GRANT_ID}\", \"permission_level\": \"CAN_USE\"}]}" \
    >/dev/null 2>&1 && echo "    OK" || echo "    FAILED"

  # Genie Room CAN_RUN
  echo "  Genie Room CAN_RUN..."
  databricks api patch /api/2.0/permissions/genie/${GENIE_ROOM_ID} \
    --profile "$PROFILE" \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"${GRANT_ID}\", \"permission_level\": \"CAN_RUN\"}]}" \
    >/dev/null 2>&1 && echo "    OK" || echo "    FAILED"
else
  echo "  SKIPPED — no service principal found"
fi
echo ""

# ─── Step 8: Deploy App ────────────────────────────────────────────────────
echo "[8/8] Deploying app..."
WS_SOURCE_PATH=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('default_source_code_path',''))" 2>/dev/null) || WS_SOURCE_PATH=""
if [ -z "$WS_SOURCE_PATH" ]; then
  WS_SOURCE_PATH="/Workspace/Users/$(databricks current-user me --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")/sap-finance-intel"
fi

databricks workspace import-dir ./app "$WS_SOURCE_PATH" --profile "$PROFILE" --overwrite 2>&1 | tail -1
databricks apps deploy "$APP_NAME" --source-code-path "$WS_SOURCE_PATH" --profile "$PROFILE" 2>&1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  Deploy state: {d.get(\"status\",{}).get(\"state\",\"UNKNOWN\")}')
"
echo ""

# ─── Summary ────────────────────────────────────────────────────────────────
echo "============================================"
echo " Deployment Complete"
echo "============================================"
APP_URL=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null) || APP_URL=""
echo ""
echo "App URL: ${APP_URL:-https://${APP_NAME}-<workspace>.databricksapps.com}"
echo ""
