"""Generate corporate finance PDFs and upload to UC Volume.

Run locally: python setup/05_generate_and_upload_pdfs.py
Requires: fpdf2, requests
"""

import json
import subprocess
import configparser
import os
import time
import tempfile

import requests
from fpdf import FPDF

PROFILE = "DEFAULT"
VOLUME_PATH = "/Volumes/zivile/default/finance_docs"
CATALOG = "zivile"
SCHEMA = "default"
VOLUME_NAME = "finance_docs"


# ---------------------------------------------------------------------------
# Auth (same pattern as 04_genie_column_comments.py)
# ---------------------------------------------------------------------------

def get_auth():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg.get(PROFILE, "host").rstrip("/")
    raw = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True,
    ).stdout.strip()
    try:
        token = json.loads(raw)["access_token"]
    except (json.JSONDecodeError, KeyError):
        token = raw
    return host, token


# ---------------------------------------------------------------------------
# SQL Statement execution (for CREATE VOLUME)
# ---------------------------------------------------------------------------

def execute_sql(host, token, sql, warehouse_id):
    """Execute a SQL statement via the Databricks SQL Statements API."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    }
    resp = requests.post(f"{host}/api/2.0/sql/statements", headers=headers, json=payload)
    resp.raise_for_status()
    result = resp.json()
    status = result.get("status", {}).get("state", "UNKNOWN")
    if status == "FAILED":
        error = result.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise RuntimeError(f"SQL failed: {error}")
    return result


def get_warehouse_id(host, token):
    """Get warehouse ID from env var or auto-discover the first available warehouse."""
    wh = os.environ.get("WAREHOUSE_ID", os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    if wh:
        return wh
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers)
    resp.raise_for_status()
    warehouses = resp.json().get("warehouses", [])
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Set DATABRICKS_WAREHOUSE_ID env var.")
    # Prefer a running warehouse
    for w in warehouses:
        if w.get("state") == "RUNNING":
            return w["id"]
    return warehouses[0]["id"]


# ---------------------------------------------------------------------------
# PDF generation (FinancePDF class from 01_generate_pdfs.py)
# ---------------------------------------------------------------------------

class FinancePDF(FPDF):
    def __init__(self, doc_title, doc_id, category):
        super().__init__()
        self.doc_title = doc_title
        self.doc_id = doc_id
        self.category = category

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 6, f"{self.doc_id}  |  {self.category.upper()}", align="R")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}  |  CONFIDENTIAL", align="C")

    def add_title(self, title, subtitle=""):
        self.set_font("Helvetica", "B", 18)
        self.set_text_color(27, 49, 57)
        self.cell(0, 12, title, ln=True)
        if subtitle:
            self.set_font("Helvetica", "", 10)
            self.set_text_color(100, 100, 100)
            self.cell(0, 6, subtitle, ln=True)
        self.ln(6)
        # Accent line
        self.set_draw_color(255, 54, 33)
        self.set_line_width(0.8)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(8)

    def add_section(self, title):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(0, 112, 242)
        self.cell(0, 10, title, ln=True)
        self.ln(2)

    def add_subsection(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(53, 74, 95)
        self.cell(0, 8, title, ln=True)
        self.ln(1)

    def add_body(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(3)

    def add_table(self, headers, rows):
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(240, 240, 240)
        col_w = (self.w - 20) / len(headers)
        for h in headers:
            self.cell(col_w, 7, h, border=1, fill=True, align="C")
        self.ln()
        self.set_font("Helvetica", "", 9)
        for row in rows:
            for val in row:
                self.cell(col_w, 6, str(val), border=1, align="C")
            self.ln()
        self.ln(4)

    def add_bullet(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(6, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(1)


# ---------------------------------------------------------------------------
# PDF builders — one function per document (content from documents.py)
# ---------------------------------------------------------------------------

def build_procurement_policy():
    pdf = FinancePDF("Corporate Procurement Policy", "PROC-POL-001", "Policy")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.add_title("Corporate Procurement Policy", "Document ID: PROC-POL-001  |  Version 3.2  |  Effective: 1 January 2026")

    pdf.add_section("1. Purpose and Scope")
    pdf.add_body("This policy establishes the procurement framework for all purchasing activities across all company codes (1000 - Sydney Head Office, 2000 - Melbourne Operations, 3000 - Brisbane Operations, 4000 - Auckland NZ). It applies to all employees authorised to requisition or approve purchases.")

    pdf.add_section("2. Approval Thresholds")
    pdf.add_body("All purchase orders must be approved according to the following matrix:")
    pdf.add_table(
        ["PO Value", "Required Approver", "Additional"],
        [
            ["Up to 5,000", "Dept Manager", "None"],
            ["5,001 - 25,000", "Director / VP", "Min 2 quotes"],
            ["25,001 - 100,000", "SVP / C-Suite", "3 quotes + Procurement"],
            ["100,001 - 500,000", "CFO", "Competitive tender"],
            ["Above 500,000", "Board Approval", "Full business case"],
        ],
    )

    pdf.add_section("3. Supplier Management")
    pdf.add_subsection("3.1 Preferred Suppliers")
    pdf.add_body("All purchases must prioritise preferred (contracted) suppliers. Non-preferred supplier purchases above 10,000 require written justification and Procurement team approval.")
    pdf.add_subsection("3.2 Single-Source Procurement")
    pdf.add_body("Single-source procurement is discouraged. Where a single supplier accounts for more than 40% of spend in any category, a risk mitigation plan must be documented and reviewed quarterly.")
    pdf.add_subsection("3.3 New Supplier Onboarding")
    pdf.add_body("New suppliers must complete vendor qualification including financial stability assessment, Code of Conduct compliance, data security questionnaire (IT/Cloud), and insurance verification (min 2M liability).")

    pdf.add_section("4. Purchase Order Compliance")
    pdf.add_bullet("All purchases above 1,000 must have an approved PO BEFORE goods/services are received")
    pdf.add_bullet("Retrospective POs are a policy violation - repeated violations escalated to VP and Internal Audit")
    pdf.add_bullet("PO splitting to avoid thresholds is strictly prohibited and may result in disciplinary action")

    pdf.add_section("5. Payment Terms")
    pdf.add_table(
        ["Supplier Category", "Standard Terms"],
        [["Strategic", "NET60"], ["Preferred", "NET45"], ["Standard", "NET30"], ["Small/Local", "NET30"], ["Prepayment", "CFO approval + bank guarantee"]],
    )
    pdf.add_body("Early payment discounts (e.g., 2/10 NET30) should be taken when discount exceeds cost of capital (currently 5.2% p.a.).")

    pdf.add_section("6. Cost Centre Allocation")
    pdf.add_body("Every PO must be assigned to a valid cost centre. Cross-cost-centre purchases require sign-off from all affected owners. IT purchases (CC-5000 to CC-5020) must be co-approved by CIO.")

    pdf.add_section("7. Sustainability and ESG")
    pdf.add_body("From Q2 2026, all new supplier contracts above 50,000 annual spend must include ESG compliance clauses per the Supplier Code of Conduct. Procurement must track and report supplier ESG ratings quarterly.")

    return pdf, "corporate_procurement_policy.pdf"


def build_supplier_code_of_conduct():
    pdf = FinancePDF("Supplier Code of Conduct", "DOC-COC-001", "Compliance")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.add_title("Supplier Code of Conduct", "Document ID: DOC-COC-001  |  Version 2.1  |  Effective: 1 March 2026")

    pdf.add_section("1. Introduction")
    pdf.add_body("This Code outlines minimum standards for all suppliers. Compliance is mandatory and assessed during onboarding and periodic audits.")

    pdf.add_section("2. Labour and Human Rights")
    pdf.add_bullet("Comply with all applicable labour laws (minimum wage, working hours, overtime)")
    pdf.add_bullet("Forced labour, child labour, and human trafficking strictly prohibited")
    pdf.add_bullet("Safe working conditions per ISO 45001 or equivalent")
    pdf.add_bullet("Freedom of association and collective bargaining rights respected")

    pdf.add_section("3. Environmental Standards")
    pdf.add_bullet("Environmental management system required (ISO 14001 preferred)")
    pdf.add_bullet("Carbon emissions tracked and reported annually; suppliers >100K spend must provide Scope 1+2 data")
    pdf.add_bullet("Documented waste reduction and recycling programme required")
    pdf.add_bullet("By 2027, all strategic suppliers must commit to Science-Based Targets (SBTi)")

    pdf.add_section("4. Ethical Business Practices")
    pdf.add_subsection("Anti-Corruption")
    pdf.add_body("No bribery, corruption, or facilitation payments. All gifts/hospitality disclosed if >150 AUD/NZD.")
    pdf.add_subsection("Data Protection")
    pdf.add_body("Suppliers handling personal data must comply with the Australian Privacy Act 1988 and NZ Privacy Act 2020. Data Processing Agreement required.")

    pdf.add_section("5. Quality and Delivery Targets")
    pdf.add_table(
        ["Metric", "Target"],
        [["On-time delivery", "95% within agreed lead time"], ["Defect rate", "Below 1%"], ["Business continuity", "24hr notification of disruptions"]],
    )

    pdf.add_section("6. Risk Assessment Framework")
    pdf.add_table(
        ["Rating", "Criteria"],
        [["LOW", "Full compliance, strong financials, diversified"], ["MEDIUM", "Minor non-compliance, adequate financials"], ["HIGH", "Significant issues, instability, geopolitical risk"]],
    )
    pdf.add_body("High-risk suppliers reviewed monthly. Failure to remediate within 90 days may result in termination.")

    pdf.add_section("7. Performance Scorecards")
    pdf.add_body("All suppliers with >50,000 annual spend receive quarterly scorecards covering Quality, Delivery, Cost, Compliance, and Innovation. Score below 60% for two quarters triggers formal improvement plan.")

    return pdf, "supplier_code_of_conduct.pdf"


def build_audit_report():
    pdf = FinancePDF("Q4 2025 Internal Audit Report", "AUD-RPT-Q4-2025", "Audit")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.add_title("Internal Audit Report - Procure-to-Pay", "Report ID: AUD-RPT-Q4-2025  |  Period: Oct-Dec 2025  |  CONFIDENTIAL")

    pdf.add_section("Executive Summary")
    pdf.add_body("Comprehensive review of P2P process across all four company codes. Covered 847 purchase orders totalling 64.5M AUD equivalent.")
    pdf.add_body("Overall Assessment: NEEDS IMPROVEMENT (3 out of 5)")

    pdf.add_section("Finding 1: Retrospective Purchase Orders (HIGH RISK)")
    pdf.add_body("127 POs (15% of sample) created after goods/services received. Total value: 5.8M AUD.")
    pdf.add_bullet("Worst offenders: IT Applications (CC-5010) - 34 cases, Marketing (CC-7000) - 28 cases")
    pdf.add_bullet("Root cause: Urgency of cloud/SaaS renewals; lack of forward planning")
    pdf.add_bullet("Recommendation: Automated alerts for invoices without matching POs")

    pdf.add_section("Finding 2: Approval Threshold Breaches (HIGH RISK)")
    pdf.add_body("23 POs above 100,000 approved by Directors instead of required CFO/SVP. 8 POs above 25,000 lacked minimum 3 quotes. Total non-compliant: 7.9M AUD.")
    pdf.add_bullet("Root cause: Approval delegation during vacations without proper documentation")
    pdf.add_bullet("Recommendation: System-enforced approval workflows in SAP")

    pdf.add_section("Finding 3: Supplier Concentration (MEDIUM RISK)")
    pdf.add_body("4 categories exceed the 40% single-supplier policy limit:")
    pdf.add_table(
        ["Category", "Supplier %", "Spend"],
        [["Cloud Infrastructure", "78%", "3.2M AUD"], ["IT Services", "52%", "1.4M AUD"], ["Logistics (Interstate)", "61%", "2.1M AUD"], ["Professional Services", "44%", "950K AUD"]],
    )
    pdf.add_body("No risk mitigation plans documented. Recommendation: Diversification plans for all categories >40%.")

    pdf.add_section("Finding 4: Overdue Purchase Orders (MEDIUM RISK)")
    pdf.add_body("189 open POs overdue >14 days, total value 13.2M AUD. 43 overdue >60 days with no follow-up. Brisbane (CC 3000) highest overdue rate at 22%.")
    pdf.add_bullet("Recommendation: Weekly overdue PO reviews per region; auto-escalate >30 days to VP")

    pdf.add_section("Finding 5: Payment Terms Non-Compliance (LOW RISK)")
    pdf.add_body("12% of new supplier setups had shorter-than-policy payment terms. 7 suppliers on NET15 without CFO approval. Estimated cost: 275K AUD/year in lost float.")

    pdf.add_section("Positive Observations")
    pdf.add_bullet("Three-way match rate improved from 82% to 91%")
    pdf.add_bullet("Early payment discount capture at 73% (target 80%)")
    pdf.add_bullet("Supplier onboarding down to 8 days from 14")
    pdf.add_bullet("Zero PO splitting incidents (improvement from 6 in Q3)")

    pdf.add_body("Management Response Due: 15 February 2026")
    pdf.add_body("Follow-up Audit Planned: Q2 2026")

    return pdf, "q4_2025_internal_audit_report.pdf"


def build_treasury_policy():
    pdf = FinancePDF("Treasury & Cash Management Policy", "FIN-TRES-001", "Policy")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.add_title("Treasury & Cash Management Policy", "Document ID: FIN-TRES-001  |  Version 4.0  |  Effective: 1 January 2026")

    pdf.add_section("1. Cash Management")
    pdf.add_subsection("Minimum Operating Balances")
    pdf.add_table(
        ["Company Code", "Entity", "Min Balance"],
        [["1000", "Sydney (HQ)", "3.0M AUD"], ["2000", "Melbourne", "2.0M AUD"], ["3000", "Brisbane", "1.5M AUD"], ["4000", "Auckland (NZ)", "1.2M NZD"]],
    )
    pdf.add_body("All entities must sweep excess cash to central treasury daily. 13-week rolling forecast updated weekly; accuracy target within 10% at 4-week horizon.")

    pdf.add_subsection("Investment Policy")
    pdf.add_bullet("Bank term deposits (max 6-month tenor) with A-rated or above banks")
    pdf.add_bullet("Government securities of domicile countries")
    pdf.add_bullet("Money market funds with same-day liquidity")
    pdf.add_bullet("Max exposure to any single bank: 25% of total cash")

    pdf.add_section("2. Foreign Exchange Management")
    pdf.add_subsection("FX Exposure Limits")
    pdf.add_body("Unhedged FX exposure must not exceed 5M AUD equivalent per currency pair or 15M AUD equivalent total.")
    pdf.add_subsection("Hedging Policy")
    pdf.add_bullet("Months 1-3 forecast: minimum 50% hedged")
    pdf.add_bullet("Months 4-6 forecast: minimum 25% hedged")
    pdf.add_bullet("Approved instruments: forwards (preferred), FX swaps, vanilla options (purchased only)")
    pdf.add_bullet("Exotic derivatives prohibited without Board approval")

    pdf.add_section("3. Payment Operations")
    pdf.add_table(
        ["Payment Value", "Authorisation"],
        [["Up to 50,000", "Treasury Analyst"], ["50,001 - 250,000", "Treasury Manager"], ["250,001 - 1,000,000", "VP Finance + Treasury Manager"], ["Above 1,000,000", "CFO + VP Finance"]],
    )
    pdf.add_body("Domestic payments: Tuesday/Thursday. International: Wednesday. Emergency: dual authorisation required.")
    pdf.add_body("Fraud prevention: maker-checker for all payment uploads. Supplier bank detail changes require verbal confirmation via independently sourced number.")

    pdf.add_section("4. Intercompany Transactions")
    pdf.add_body("All IC loans at arm's-length rates per OECD transfer pricing. Monthly netting, settlement on 5th business day.")
    pdf.add_table(
        ["Currency", "IC Rate"],
        [["AUD", "BBSW + 150bps"], ["NZD", "BKBM + 175bps"], ["USD", "SOFR + 160bps"]],
    )

    return pdf, "treasury_cash_management_policy.pdf"


def build_financial_close():
    pdf = FinancePDF("Financial Close Procedures", "FIN-CLOSE-001", "Procedures")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.add_title("Financial Close Procedures Manual", "Document ID: FIN-CLOSE-001  |  Version 5.1  |  Effective: 1 January 2026")

    pdf.add_section("1. Close Calendar (5-Day Cycle)")
    pdf.add_table(
        ["Day", "Activity", "Owner"],
        [
            ["Day 1", "Sub-ledger closures (AP, AR, FA)", "Entity Controllers"],
            ["Day 1", "Inventory valuation and cut-off", "Supply Chain Finance"],
            ["Day 2", "Revenue recognition and accruals", "Revenue Accounting"],
            ["Day 2", "Intercompany reconciliation", "Treasury / IC Team"],
            ["Day 3", "Journal entry review and posting", "Group Accounting"],
            ["Day 3", "Balance sheet reconciliations", "Entity Controllers"],
            ["Day 4", "Mgmt reporting & variance analysis", "FP&A"],
            ["Day 4", "Consolidation and eliminations", "Group Reporting"],
            ["Day 5", "CFO review and sign-off", "CFO / VP Finance"],
            ["Day 5", "Flash report distribution", "FP&A"],
        ],
    )
    pdf.add_body("Quarter-end close extends to 8 business days for additional disclosures and Board reporting.")

    pdf.add_section("2. Accrual Policy")
    pdf.add_subsection("Materiality Thresholds")
    pdf.add_bullet("Monthly close: accruals required above 7,500 AUD equivalent")
    pdf.add_bullet("Quarter-end: accruals required above 1,500 AUD equivalent")
    pdf.add_subsection("Manual Accruals")
    pdf.add_bullet("Supporting calculation or third-party documentation required")
    pdf.add_bullet("Approval by cost centre owner and entity controller")
    pdf.add_bullet("Reversal within first 3 business days of following month")
    pdf.add_bullet("Above 50,000: VP Finance approval required")
    pdf.add_body("Accrual accuracy target: variance within 15%. Variance >25% for two months requires root cause analysis.")

    pdf.add_section("3. Journal Entry Controls")
    pdf.add_table(
        ["Entry Type", "Value", "Approver"],
        [
            ["Standard/Recurring", "Any", "Entity Controller"],
            ["Manual Non-recurring", "Up to 50,000", "Entity Controller"],
            ["Manual Non-recurring", "50,001-250,000", "VP Finance"],
            ["Manual Non-recurring", "Above 250,000", "CFO"],
            ["Top-side/Consolidation", "Any", "VP Finance + Ext Audit"],
        ],
    )
    pdf.add_body("All manual journals must have business purpose. Journals posted in last 2 hours before close are subject to enhanced review.")

    pdf.add_section("4. Reconciliation Requirements")
    pdf.add_bullet("All balance sheet accounts reconciled monthly")
    pdf.add_bullet("Items older than 60 days escalated to VP Finance")
    pdf.add_bullet("Items older than 90 days must have documented resolution plan")
    pdf.add_bullet("IC reconciliation tolerance: 750 AUD per entity pair")

    pdf.add_section("5. Continuous Improvement")
    pdf.add_body("Target: reduce close from 5 days to 4 days by Q4 2026 through expanded SAP auto-accruals, real-time IC matching, automated bank reconciliation, and AI-assisted variance commentary (pilot Q2 2026).")

    return pdf, "financial_close_procedures.pdf"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    host, token = get_auth()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 1. Get warehouse ID
    warehouse_id = get_warehouse_id(host, token)
    print(f"Using warehouse: {warehouse_id}")

    # 2. Create volume via SQL Statements API
    sql = f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}"
    print(f"Creating volume: {CATALOG}.{SCHEMA}.{VOLUME_NAME} ...")
    execute_sql(host, token, sql, warehouse_id)
    print("  Volume ready.")

    # 3. Generate and upload PDFs
    builders = [
        build_procurement_policy,
        build_supplier_code_of_conduct,
        build_audit_report,
        build_treasury_policy,
        build_financial_close,
    ]

    for builder in builders:
        pdf, filename = builder()
        # Write PDF to temp file
        tmp_path = os.path.join(tempfile.gettempdir(), filename)
        pdf.output(tmp_path)
        file_size = os.path.getsize(tmp_path)

        # Upload via Files API
        upload_url = f"{host}/api/2.0/fs/files{VOLUME_PATH}/{filename}?overwrite=true"
        with open(tmp_path, "rb") as f:
            resp = requests.put(
                upload_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/octet-stream",
                },
                data=f,
            )
        resp.raise_for_status()
        print(f"  Uploaded: {VOLUME_PATH}/{filename} ({file_size:,} bytes)")
        os.remove(tmp_path)

    print(f"\nDone! 5 PDFs uploaded to {VOLUME_PATH}")


if __name__ == "__main__":
    main()
