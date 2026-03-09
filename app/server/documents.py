"""Corporate finance policy documents — text extracted from PDFs.

These simulate corporate governance documents that a finance team would reference
alongside SAP transactional data. Stored in-memory for fast retrieval.
"""

DOCUMENTS: dict[str, dict] = {
    "PROC-POL-001": {
        "title": "Corporate Procurement Policy",
        "filename": "corporate_procurement_policy.pdf",
        "category": "policy",
        "content": """CORPORATE PROCUREMENT POLICY
Document ID: PROC-POL-001 | Version: 3.2 | Effective: 1 January 2026
Approved by: Chief Financial Officer | Review Date: 30 June 2026

1. PURPOSE AND SCOPE

This policy establishes the procurement framework for all purchasing activities across all company codes (1000 – Sydney Head Office, 2000 – Melbourne Operations, 3000 – Brisbane Operations, 4000 – Auckland NZ). It applies to all employees authorised to requisition or approve purchases.

2. APPROVAL THRESHOLDS

All purchase orders must be approved according to the following matrix:

| PO Value (Local Currency) | Required Approver | Additional Requirements |
|---|---|---|
| Up to 5,000 | Department Manager | None |
| 5,001 – 25,000 | Director / VP | Minimum 2 quotes |
| 25,001 – 100,000 | SVP / C-Suite | Minimum 3 quotes + Procurement review |
| 100,001 – 500,000 | CFO | Competitive tender required |
| Above 500,000 | Board Approval | Full business case + Competitive tender |

Emergency purchases above 25,000 require retrospective CFO approval within 5 business days.

3. SUPPLIER MANAGEMENT

3.1 Preferred Suppliers
All purchases must prioritise preferred (contracted) suppliers. Non-preferred supplier purchases above 10,000 require written justification and Procurement team approval.

3.2 Single-Source Procurement
Single-source procurement (sole supplier for a category) is discouraged. Where a single supplier accounts for more than 40% of spend in any category, a risk mitigation plan must be documented and reviewed quarterly.

3.3 New Supplier Onboarding
New suppliers must complete the vendor qualification process including:
- Financial stability assessment (Dun & Bradstreet or equivalent)
- Compliance with our Supplier Code of Conduct (DOC-COC-001)
- Data security questionnaire for IT/Cloud suppliers
- Insurance verification (minimum 2M liability coverage)

4. PURCHASE ORDER COMPLIANCE

4.1 All purchases above 1,000 must have an approved purchase order BEFORE goods/services are received.
4.2 Retrospective POs ("after-the-fact orders") are a policy violation. Repeated violations will be escalated to the employee's VP and the Internal Audit team.
4.3 PO splitting (dividing a purchase into multiple smaller POs to avoid approval thresholds) is strictly prohibited and may result in disciplinary action.

5. PAYMENT TERMS

Standard payment terms by supplier category:
- Strategic suppliers: NET60
- Preferred suppliers: NET45
- Standard suppliers: NET30
- Small / local suppliers: NET30
- Prepayment: Only with CFO approval and bank guarantee

Early payment discounts (e.g., 2/10 NET30) should be taken when the discount exceeds the company's cost of capital (currently 5.2% p.a.).

6. COST CENTRE ALLOCATION

Every purchase order must be assigned to a valid cost centre. Cross-cost-centre purchases require sign-off from all affected cost centre owners. IT purchases (CC-5000 to CC-5020) must be co-approved by the CIO.

7. SUSTAINABILITY AND ESG

From Q2 2026, all new supplier contracts above 50,000 annual spend must include ESG compliance clauses per the Supplier Code of Conduct. Procurement must track and report supplier ESG ratings quarterly.

8. POLICY VIOLATIONS

Violations will be reported to Internal Audit and the employee's management chain. Repeated violations may result in removal of purchasing authority, formal warning, or termination.""",
    },

    "DOC-COC-001": {
        "title": "Supplier Code of Conduct",
        "filename": "supplier_code_of_conduct.pdf",
        "category": "compliance",
        "content": """SUPPLIER CODE OF CONDUCT
Document ID: DOC-COC-001 | Version: 2.1 | Effective: 1 March 2026
Approved by: General Counsel & Chief Procurement Officer

1. INTRODUCTION

This Code of Conduct outlines the minimum standards we expect from all suppliers. Compliance is mandatory and will be assessed during onboarding and through periodic audits.

2. LABOUR AND HUMAN RIGHTS

2.1 Suppliers must comply with all applicable labour laws including minimum wage, working hours, and overtime regulations.
2.2 Forced labour, child labour, and human trafficking are strictly prohibited throughout the supply chain.
2.3 Suppliers must provide safe working conditions and maintain occupational health and safety standards per ISO 45001 or equivalent.
2.4 Freedom of association and collective bargaining rights must be respected.

3. ENVIRONMENTAL STANDARDS

3.1 Suppliers must maintain an environmental management system (ISO 14001 preferred).
3.2 Carbon emissions must be tracked and reported annually. Suppliers with annual spend above 100,000 must provide Scope 1 and 2 emissions data.
3.3 Suppliers must have a documented waste reduction and recycling programme.
3.4 Hazardous materials must be managed per local regulations and REACH (EU) standards.
3.5 By 2027, all strategic suppliers must commit to Science-Based Targets (SBTi) or equivalent.

4. ETHICAL BUSINESS PRACTICES

4.1 Anti-Corruption: Suppliers must not engage in bribery, corruption, or facilitation payments. All gifts and hospitality must be disclosed if value exceeds 150 AUD/NZD.
4.2 Fair Competition: Suppliers must not engage in anti-competitive behaviour including price-fixing, market allocation, or bid-rigging.
4.3 Conflicts of Interest: Suppliers must disclose any conflicts of interest with our employees.
4.4 Data Protection: Suppliers handling personal data must comply with the Australian Privacy Act 1988, NZ Privacy Act 2020, and applicable data protection laws. A Data Processing Agreement is required.

5. QUALITY AND DELIVERY

5.1 Suppliers must maintain a quality management system (ISO 9001 preferred).
5.2 On-time delivery target: 95% of orders delivered within agreed lead time.
5.3 Defect rate target: Below 1% of delivered goods/services.
5.4 Suppliers must maintain business continuity plans and notify us within 24 hours of any event that may impact delivery.

6. RISK ASSESSMENT FRAMEWORK

Suppliers are rated on a risk scale:
- LOW RISK: Full compliance, strong financials, diversified customer base
- MEDIUM RISK: Minor non-compliance findings, adequate financials, some concentration
- HIGH RISK: Significant non-compliance, financial instability, high dependency, geopolitical exposure

High-risk suppliers are reviewed monthly. Failure to remediate findings within 90 days may result in contract termination.

7. PERFORMANCE SCORECARDS

All suppliers with annual spend above 50,000 receive quarterly scorecards covering:
- Quality (defect rates, returns)
- Delivery (on-time %, lead time adherence)
- Cost (price competitiveness, invoice accuracy)
- Compliance (Code of Conduct, certifications)
- Innovation (value engineering, process improvements)

Overall score below 60% for two consecutive quarters triggers a formal performance improvement plan.

8. AUDIT RIGHTS

We reserve the right to audit supplier operations with 10 business days notice. Critical findings must be remediated within 30 days. Suppliers must provide access to relevant records, facilities, and personnel.""",
    },

    "AUD-RPT-Q4-2025": {
        "title": "Q4 2025 Internal Audit Report — Procure-to-Pay",
        "filename": "q4_2025_internal_audit_report.pdf",
        "category": "audit",
        "content": """INTERNAL AUDIT REPORT — PROCURE-TO-PAY PROCESS
Report ID: AUD-RPT-Q4-2025 | Period: October – December 2025
Classification: CONFIDENTIAL | Distribution: CFO, CPO, VP Internal Audit

EXECUTIVE SUMMARY

The Internal Audit team conducted a comprehensive review of the procure-to-pay (P2P) process across all four company codes during Q4 2025. The audit covered 847 purchase orders totalling 64.5M AUD equivalent across all regions.

Overall Assessment: NEEDS IMPROVEMENT (3 out of 5)

KEY FINDINGS

Finding 1: RETROSPECTIVE PURCHASE ORDERS (HIGH RISK)
- 127 purchase orders (15% of sample) were created after goods/services were received
- Total value of retrospective POs: 5.8M AUD
- Worst offending departments: IT Applications (CC-5010) — 34 cases, Marketing (CC-7000) — 28 cases
- Root cause: Urgency of cloud/SaaS renewals and agency work; lack of forward planning
- Recommendation: Implement automated alerts when invoices arrive without matching POs. Require mandatory PO creation as part of contract renewal workflow.

Finding 2: APPROVAL THRESHOLD BREACHES (HIGH RISK)
- 23 purchase orders above 100,000 were approved by Directors instead of the required CFO/SVP level
- 8 purchase orders above 25,000 lacked the required minimum 3 quotes
- Total value of non-compliant POs: 7.9M AUD
- Root cause: Approval delegation during vacation periods without proper documentation
- Recommendation: Implement system-enforced approval workflows in SAP. Remove manual override capability for threshold-based approvals.

Finding 3: SUPPLIER CONCENTRATION (MEDIUM RISK)
- 4 supplier categories have single-supplier dependency exceeding the 40% policy limit:
  * Cloud Infrastructure: 1 supplier at 78% of category spend (3.2M AUD)
  * IT Services: 1 supplier at 52% of category spend (1.4M AUD)
  * Logistics (Interstate): 1 supplier at 61% of category spend (2.1M AUD)
  * Professional Services: 1 supplier at 44% of category spend (950K AUD)
- No risk mitigation plans were documented for any of these concentrations
- Recommendation: Procurement must create diversification plans for all categories exceeding 40% single-supplier share. Quarterly reviews with CPO.

Finding 4: OVERDUE PURCHASE ORDERS (MEDIUM RISK)
- 189 open POs have delivery dates past due by more than 14 days
- Total value of overdue POs: 13.2M AUD
- 43 of these have been overdue for more than 60 days with no documented follow-up
- Brisbane (Company Code 3000) has the highest overdue rate at 22% of open POs
- Recommendation: Implement weekly overdue PO review meetings per region. Auto-escalate POs overdue >30 days to VP level.

Finding 5: PAYMENT TERMS NON-COMPLIANCE (LOW RISK)
- 12% of new supplier setups in Q4 had payment terms shorter than policy standard
- 7 suppliers on NET15 terms without documented CFO approval
- Estimated annual cost of early payments: 275K AUD in lost float
- Recommendation: Lock payment terms in SAP vendor master to policy defaults. Exceptions require documented approval.

POSITIVE OBSERVATIONS

- Three-way match rate improved from 82% to 91% vs Q3 2025
- Early payment discount capture rate increased to 73% (target: 80%)
- New supplier onboarding now averages 8 business days (down from 14 in Q3)
- No instances of PO splitting were detected (improvement from 6 cases in Q3)

MANAGEMENT RESPONSE DUE: 15 February 2026
FOLLOW-UP AUDIT PLANNED: Q2 2026""",
    },

    "FIN-TRES-001": {
        "title": "Treasury & Cash Management Policy",
        "filename": "treasury_cash_management_policy.pdf",
        "category": "policy",
        "content": """TREASURY & CASH MANAGEMENT POLICY
Document ID: FIN-TRES-001 | Version: 4.0 | Effective: 1 January 2026
Approved by: CFO and Board Audit Committee

1. PURPOSE

This policy governs treasury operations including cash management, foreign exchange risk, banking relationships, and intercompany financing across all company codes.

2. CASH MANAGEMENT

2.1 Cash Concentration
All operating entities must sweep excess cash to the central treasury account daily. Minimum operating balances per entity:
- Company Code 1000 (Sydney HQ): 3.0M AUD
- Company Code 2000 (Melbourne): 2.0M AUD
- Company Code 3000 (Brisbane): 1.5M AUD
- Company Code 4000 (Auckland NZ): 1.2M NZD

2.2 Cash Forecasting
Each entity must submit a 13-week rolling cash forecast updated weekly. Forecast accuracy target: within 10% of actuals at the 4-week horizon. Treasury will consolidate forecasts by Wednesday 12:00 UTC each week.

2.3 Investment Policy
Surplus cash may only be invested in:
- Bank term deposits (maximum 6-month tenor) with A-rated or above banks
- Government securities of domicile countries
- Money market funds with same-day liquidity
Maximum exposure to any single bank: 25% of total cash holdings.

3. FOREIGN EXCHANGE MANAGEMENT

3.1 FX Exposure Limits
Unhedged FX exposure must not exceed:
- 5M AUD equivalent per currency pair
- 15M AUD equivalent total across all currencies

3.2 Hedging Policy
Forecast FX exposures for the next 3 months must be hedged at minimum 50%. Exposures for months 4-6 should be hedged at minimum 25%.

Approved hedging instruments:
- Forward contracts (preferred)
- FX swaps
- Vanilla options (purchased only, no written options)
Exotic derivatives are prohibited without Board approval.

3.3 FX Reporting
Treasury must report FX positions and mark-to-market valuations monthly to the CFO. Realised and unrealised FX gains/losses must be reported separately in the GL (accounts 800000 series).

4. PAYMENT OPERATIONS

4.1 Payment Runs
Domestic payments: Processed every Tuesday and Thursday
International payments: Processed every Wednesday
Emergency payments: Require dual authorisation (Treasury Manager + CFO)

4.2 Payment Authorisation
| Payment Value | Authorisation Required |
|---|---|
| Up to 50,000 | Treasury Analyst |
| 50,001 – 250,000 | Treasury Manager |
| 250,001 – 1,000,000 | VP Finance + Treasury Manager |
| Above 1,000,000 | CFO + VP Finance |

4.3 Fraud Prevention
All payment file uploads require maker-checker verification. Changes to supplier bank details require verbal confirmation via independently sourced phone number. Suspicious payment requests must be escalated to Treasury Manager and IT Security immediately.

5. INTERCOMPANY TRANSACTIONS

5.1 All intercompany loans must be documented with arm's-length interest rates based on OECD transfer pricing guidelines.
5.2 Current intercompany lending rates (updated quarterly):
- AUD: BBSW + 150bps
- NZD: BKBM + 175bps
- USD: SOFR + 160bps (for international procurement)
5.3 Intercompany netting is performed monthly. Net settlement date: 5th business day of each month.

6. BANKING RELATIONSHIPS

Primary banking partners:
- Primary: Commonwealth Bank of Australia (cash management, domestic payments)
- Secondary: ANZ Banking Group (FX, trade finance, NZ operations)
- International: Westpac Institutional Bank (international payments, supply chain finance)
- NZ: ASB Bank (NZD operations, local payments)

Bank fee benchmarking must be conducted annually. RFP for banking services every 3 years.

7. REPORTING AND CONTROLS

Treasury must prepare:
- Daily: Cash position report by entity and currency
- Weekly: Consolidated cash forecast vs actuals
- Monthly: FX exposure report, bank fee analysis, investment portfolio summary
- Quarterly: Treasury KPI dashboard for Board Audit Committee""",
    },

    "FIN-CLOSE-001": {
        "title": "Financial Close Procedures Manual",
        "filename": "financial_close_procedures.pdf",
        "category": "procedures",
        "content": """FINANCIAL CLOSE PROCEDURES MANUAL
Document ID: FIN-CLOSE-001 | Version: 5.1 | Effective: 1 January 2026
Owner: VP Finance — Group Reporting

1. CLOSE CALENDAR

Monthly close follows a 5-business-day cycle (Day 1 = first business day after month end):

| Day | Activity | Owner |
|---|---|---|
| Day 1 | Sub-ledger closures (AP, AR, FA) | Entity Controllers |
| Day 1 | Inventory valuation and cut-off | Supply Chain Finance |
| Day 2 | Revenue recognition and accruals | Revenue Accounting |
| Day 2 | Intercompany reconciliation | Treasury / IC Team |
| Day 3 | Journal entry review and posting | Group Accounting |
| Day 3 | Balance sheet reconciliations | Entity Controllers |
| Day 4 | Management reporting and variance analysis | FP&A |
| Day 4 | Consolidation and eliminations | Group Reporting |
| Day 5 | CFO review and sign-off | CFO / VP Finance |
| Day 5 | Flash report distribution | FP&A |

Quarter-end close extends to 8 business days to accommodate additional disclosures and Board reporting.

2. ACCRUAL POLICY

2.1 Materiality Threshold
Accruals are required for all known liabilities above:
- 7,500 AUD equivalent for monthly close
- 1,500 AUD equivalent for quarter-end close

2.2 Standard Accruals (auto-posted)
The following accruals are posted automatically by SAP:
- Employee compensation (salary, bonus provisions)
- Rent and facility costs (straight-line lease recognition)
- Depreciation and amortisation
- Interest expense on debt facilities

2.3 Manual Accruals
Manual accruals require:
- Supporting calculation or third-party documentation
- Approval by Cost Centre owner and Entity Controller
- Reversal in the first 3 business days of the following month
- All manual journal entries above 50,000 require VP Finance approval

2.4 Accrual Accuracy
Accrual accuracy is tracked quarterly. Target: actual variance within 15% of accrued amount. Accruals with variance >25% for two consecutive months must have root cause analysis documented.

3. RECONCILIATION REQUIREMENTS

3.1 All balance sheet accounts must be reconciled monthly.
3.2 Reconciliation must include:
- GL balance confirmation
- Supporting documentation (bank statements, sub-ledger reports, third-party confirmations)
- Ageing analysis for receivables and payables
- Explanation of significant movements (>10% or >50K variance)

3.3 Stale Items
Reconciling items older than 60 days must be escalated to VP Finance. Items older than 90 days must have a documented resolution plan.

4. INTERCOMPANY CLOSE

4.1 All intercompany transactions must be recorded by Day 1.
4.2 IC reconciliation tolerance: 750 AUD equivalent per entity pair.
4.3 Breaks above tolerance must be resolved by Day 2 or escalated to Group Reporting.
4.4 Elimination entries are posted centrally by Group Reporting on Day 4.

5. JOURNAL ENTRY CONTROLS

5.1 Approval Matrix:
| Entry Type | Value | Approver |
|---|---|---|
| Standard / Recurring | Any | Entity Controller |
| Manual — Non-recurring | Up to 50,000 | Entity Controller |
| Manual — Non-recurring | 50,001 – 250,000 | VP Finance |
| Manual — Non-recurring | Above 250,000 | CFO |
| Top-side / Consolidation | Any | VP Finance + External Audit awareness |

5.2 All manual journals must have a business purpose description.
5.3 Journals posted in the last 2 hours before close deadline are subject to enhanced review.

6. REPORTING OUTPUTS

Monthly close produces:
- Entity-level P&L and Balance Sheet
- Consolidated P&L, Balance Sheet, and Cash Flow
- Variance analysis vs budget and prior year
- Flash commentary by entity (max 1 page)

Quarter-end additionally produces:
- Board reporting pack
- Segment reporting
- Related party transaction summary
- Going concern assessment (annual)

7. CONTINUOUS IMPROVEMENT

Close cycle time target: Reduce from 5 days to 4 days by Q4 2026 through:
- Expanding SAP auto-accruals coverage
- Implementing real-time IC matching
- Automating bank reconciliation via SAP integration
- Deploying AI-assisted variance commentary (pilot in Q2 2026)""",
    },
}


def search_documents(query: str) -> list[dict]:
    """Search across all documents for relevant content.

    Simple keyword search — returns matching documents with relevance context.
    """
    query_lower = query.lower()
    keywords = [w.strip() for w in query_lower.split() if len(w.strip()) > 2]

    results = []
    for doc_id, doc in DOCUMENTS.items():
        content_lower = doc["content"].lower()
        title_lower = doc["title"].lower()

        # Score by keyword matches
        score = 0
        matched_keywords = []
        for kw in keywords:
            count = content_lower.count(kw)
            if count > 0:
                score += count
                matched_keywords.append(kw)
            if kw in title_lower:
                score += 5

        if score > 0:
            results.append({
                "doc_id": doc_id,
                "title": doc["title"],
                "filename": doc["filename"],
                "category": doc["category"],
                "content": doc["content"],
                "relevance_score": score,
                "matched_keywords": matched_keywords,
            })

    results.sort(key=lambda x: x["relevance_score"], reverse=True)
    return results


def get_document(doc_id: str) -> dict | None:
    """Get a specific document by ID."""
    doc = DOCUMENTS.get(doc_id)
    if doc:
        return {"doc_id": doc_id, **doc}
    return None


def list_documents() -> list[dict]:
    """List all available documents (without full content)."""
    return [
        {"doc_id": did, "title": d["title"], "filename": d["filename"], "category": d["category"]}
        for did, d in DOCUMENTS.items()
    ]
