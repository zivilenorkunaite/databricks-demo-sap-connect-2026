"""Add column-level descriptions to all Genie Room tables.

Run locally: python setup/04_genie_column_comments.py
Requires: requests, databricks-sdk
"""

import json
import subprocess
import configparser
import os
import requests

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
GENIE_ROOM_ID = os.environ.get("GENIE_ROOM_ID", "01f11b67acc9177abc5f344f75fe4389")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", os.environ.get("DATABRICKS_WAREHOUSE_ID", "c2abb17a6c9e6bc0"))

# Column descriptions for all tables (must be sorted by column_name per table)
COLUMN_COMMENTS = {
    "zivile.default.supplier_risk_scores": {
        "abn": "Australian Business Number (null for international suppliers)",
        "annual_revenue_aud": "Annual revenue in Australian dollars",
        "assessor_notes": "Free-text notes from the risk assessor",
        "country": "Country of incorporation",
        "credit_rating": "Credit agency rating: AAA, AA, A, BBB, BB, B, CCC, D",
        "credit_score": "Numeric credit score 0-100 (higher = better)",
        "current_ratio": "Current assets / current liabilities (>1 = can cover short-term debts)",
        "debt_to_equity_ratio": "Total debt / total equity (lower = less leveraged)",
        "esg_score": "Environmental, Social, Governance score 0-100",
        "financial_health_score": "Composite financial health 0-10 (higher = healthier)",
        "industry": "Industry classification",
        "last_assessment_date": "Date of most recent risk assessment",
        "next_review_date": "Scheduled date for next risk review",
        "payment_default_history": "True if supplier has any history of payment defaults",
        "risk_level": "Risk classification: Low, Medium, High, Critical",
        "sanctions_flagged": "True if supplier appears on any sanctions watchlist",
        "supplier_id": "SAP Supplier ID — matches PurchaseOrder.Supplier for joins",
        "supplier_name": "Supplier legal entity name",
    },
    "zivile_bdc.BillingDocument.BillingDocument": {
        "BillingDocument": "Unique billing document number (PK)",
        "BillingDocumentDate": "Billing date (may differ from creation date)",
        "BillingDocumentIsCancelled": "Whether this billing document has been cancelled",
        "BillingDocumentType": "Document type: F2=Invoice, G2=Credit memo, S1=Cancellation",
        "CompanyCode": "Company code: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "CreationDate": "Date billing document was created",
        "CustomerPaymentTerms": "Payment terms code",
        "OverallBillingStatus": "Billing status: A=Not yet processed, B=Partially processed, C=Completely processed",
        "PayerParty": "Customer number responsible for payment",
        "SalesOrganization": "Sales org: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "TotalNetAmount": "Total net amount excluding tax",
        "TotalTaxAmount": "Total tax amount (GST)",
        "TransactionCurrency": "Currency code (AUD, NZD)",
    },
    "zivile_bdc.BillingDocument.BillingDocumentItem": {
        "BillingDocument": "Billing document number (FK to BillingDocument)",
        "BillingDocumentItem": "Line item number within billing document",
        "BillingQuantity": "Quantity billed",
        "BillingQuantityUnit": "Unit of measure for billing quantity",
        "Material": "Material/product number",
        "NetAmount": "Net amount for this line item",
        "Plant": "Plant/warehouse code",
        "SalesOrder": "Reference sales order number (FK to SalesOrder)",
        "SalesOrderItem": "Reference sales order item number",
        "TaxAmount": "Tax amount for this line item",
        "TransactionCurrency": "Currency code",
    },
    "zivile_bdc.GeneralLedgerAccount.GeneralLedgerAccount": {
        "AccountIsBlockedForPosting": "Whether posting to this account is blocked",
        "AccountIsMarkedForDeletion": "Whether account is marked for deletion",
        "ChartOfAccounts": "Chart of accounts identifier",
        "GLAccount": "General ledger account number (PK)",
        "GLAccountGroup": "Account group classification",
        "GLAccountType": "Account type classification",
        "IsBalanceSheetAccount": "True if balance sheet account, false if P&L",
        "IsProfitLossAccount": "True if profit & loss account",
        "ProfitLossAccountType": "P&L account type (revenue, expense, etc.)",
    },
    "zivile_bdc.GeneralLedgerAccount.GeneralLedgerAccountText": {
        "ChartOfAccounts": "Chart of accounts identifier",
        "GLAccount": "GL account number (FK to GeneralLedgerAccount)",
        "GLAccountLongName": "Full GL account description",
        "GLAccountName": "Short GL account name",
        "Language": "Language key — filter Language='EN' for English",
    },
    "zivile_bdc.JournalEntryHeader.JournalEntry": {
        "AccountingDocCreatedByUser": "User who created the document",
        "AccountingDocument": "Unique accounting document number (PK with CompanyCode+FiscalYear)",
        "AccountingDocumentCategory": "Document category",
        "AccountingDocumentHeaderText": "Free-text header description",
        "AccountingDocumentType": "Doc type: SA=GL posting, AB=clearing, KR=vendor invoice, DR=customer invoice",
        "CompanyCode": "Company code: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "DocumentDate": "Document date",
        "DocumentReferenceID": "External reference number",
        "FiscalPeriod": "Fiscal period (month) 001-012",
        "FiscalYear": "Fiscal year of the posting",
        "IsReversalDocument": "Whether this is a reversal of another document",
        "IsReversed": "Whether this document has been reversed",
        "PostingDate": "Date posted to general ledger",
        "TransactionCurrency": "Currency code",
    },
    "zivile_bdc.PurchaseOrder.PurchaseOrder": {
        "CompanyCode": "Company code: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "CreatedByUser": "User who created the PO",
        "CreationDate": "Date PO was created",
        "DocumentCurrency": "Currency code (AUD, NZD)",
        "IncotermsClassification": "Incoterms (shipping terms)",
        "PaymentTerms": "Payment terms code",
        "PurchaseOrder": "Unique purchase order number (PK)",
        "PurchaseOrderDate": "PO document date",
        "PurchaseOrderType": "PO type: NB=Standard, FO=Framework",
        "PurchasingGroup": "Purchasing group responsible",
        "PurchasingOrganization": "Purchasing organisation code",
        "PurchasingProcessingStatus": "Processing status of the PO",
        "PurgReleaseSequenceStatus": "Release/approval status",
        "PurgReleaseTimeTotalAmount": "Total released amount",
        "Supplier": "Supplier number — join to supplier_risk_scores.supplier_id for risk data",
    },
    "zivile_bdc.PurchaseOrder.PurchaseOrderItem": {
        "CompanyCode": "Company code",
        "DocumentCurrency": "Currency code",
        "IsCompletelyDelivered": "Whether goods receipt is complete",
        "IsFinallyInvoiced": "Whether invoice verification is complete",
        "Material": "Material/product number",
        "MaterialGroup": "Material group classification",
        "NetAmount": "Total net amount for this line item",
        "NetPriceAmount": "Unit price",
        "OrderQuantity": "Ordered quantity",
        "Plant": "Plant/warehouse code",
        "PurchaseOrder": "PO number (FK to PurchaseOrder)",
        "PurchaseOrderItem": "Line item number within PO",
        "PurchaseOrderItemText": "Item description text",
        "PurchaseOrderQuantityUnit": "Unit of measure",
    },
    "zivile_bdc.SalesOrder.SalesOrder": {
        "CreatedByUser": "User who created the SO",
        "CreationDate": "Date SO was created",
        "CustomerGroup": "Customer group classification",
        "DistributionChannel": "Distribution channel code",
        "OverallDeliveryStatus": "Delivery status: A=Not delivered, B=Partial, C=Complete",
        "OverallSDProcessStatus": "Overall processing status: A=Open, B=Partial, C=Complete",
        "SDDocumentReason": "Reason code for the order",
        "SalesOrder": "Unique sales order number (PK)",
        "SalesOrderDate": "SO document date",
        "SalesOrderType": "SO type: OR=Standard, RE=Return",
        "SalesOrganization": "Sales org: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "SoldToParty": "Customer number (sold-to)",
        "TotalCreditCheckStatus": "Credit check result",
        "TotalNetAmount": "Total net amount of the sales order",
        "TransactionCurrency": "Currency code (AUD, NZD)",
    },
    "zivile_bdc.SalesOrder.SalesOrderItem": {
        "DeliveryStatus": "Item delivery status",
        "IsReturnsItem": "Whether this is a returns line item",
        "Material": "Material/product number",
        "MaterialGroup": "Material group classification",
        "NetAmount": "Net amount for this line item",
        "Plant": "Plant/warehouse code",
        "RequestedQuantity": "Quantity requested by customer",
        "RequestedQuantityUnit": "Unit of measure",
        "SDProcessStatus": "Item processing status",
        "SalesOrder": "Sales order number (FK to SalesOrder)",
        "SalesOrderItem": "Line item number within SO",
        "TransactionCurrency": "Currency code",
    },
    "zivile_bdc.SupplierInvoice.SupplierInvoice": {
        "CompanyCode": "Company code: 1000=Sydney, 2000=Melbourne, 3000=Brisbane, 4000=Auckland",
        "DocumentCurrency": "Currency code",
        "DocumentDate": "Invoice document date",
        "DocumentHeaderText": "Free-text header description",
        "FiscalYear": "Fiscal year",
        "InvoiceGrossAmount": "Total invoice amount including tax",
        "InvoicingParty": "Supplier number who issued the invoice",
        "IsInvoice": "True if invoice, false if credit memo",
        "PostingDate": "Date posted to AP",
        "SupplierInvoice": "Unique supplier invoice number (PK)",
        "SupplierInvoiceIDByInvcgParty": "Invoice number assigned by the supplier",
        "SupplierInvoiceStatus": "Invoice processing status",
    },
    "zivile_bdc.SupplierInvoice.SupplierInvoiceItem": {
        "CompanyCode": "Company code",
        "DocumentCurrency": "Currency code",
        "FiscalYear": "Fiscal year",
        "Plant": "Plant/warehouse code",
        "PostingDate": "Posting date",
        "PurchaseOrder": "Reference PO number (FK to PurchaseOrder)",
        "PurchaseOrderItem": "Reference PO item number",
        "PurchaseOrderItemMaterial": "Material from the PO item",
        "PurchaseOrderQuantityUnit": "Unit of measure from PO",
        "QuantityInPurchaseOrderUnit": "Invoiced quantity in PO unit",
        "SupplierInvoice": "Supplier invoice number (FK to SupplierInvoice)",
        "SupplierInvoiceItem": "Line item number",
        "SupplierInvoiceItemAmount": "Amount for this invoice line item",
    },
}


def get_auth():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg.get(PROFILE, "host").rstrip("/")
    raw = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True
    ).stdout.strip()
    try:
        token = json.loads(raw)["access_token"]
    except (json.JSONDecodeError, KeyError):
        token = raw
    return host, token


def main():
    host, token = get_auth()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Read current Genie Room state (no-op PATCH returns serialized_space)
    print("Reading current Genie Room configuration...")
    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{GENIE_ROOM_ID}",
        headers=headers,
        json={"warehouse_id": WAREHOUSE_ID},
    )
    resp.raise_for_status()
    ss = json.loads(resp.json()["serialized_space"])

    # Remove any duplicate/stale tables
    seen = set()
    clean_tables = []
    for t in ss["data_sources"]["tables"]:
        tid = t["identifier"]
        if tid not in seen:
            seen.add(tid)
            clean_tables.append(t)
    ss["data_sources"]["tables"] = clean_tables

    # Apply column_configs to each table (sorted by column_name)
    updated = 0
    for t in ss["data_sources"]["tables"]:
        tid = t["identifier"]
        if tid in COLUMN_COMMENTS:
            t["column_configs"] = sorted(
                [{"column_name": col, "description": [desc]}
                 for col, desc in COLUMN_COMMENTS[tid].items()],
                key=lambda c: c["column_name"],
            )
            updated += 1
            print(f"  {tid}: {len(COLUMN_COMMENTS[tid])} columns")

    # Sort tables by identifier
    ss["data_sources"]["tables"].sort(key=lambda t: t["identifier"])

    # Patch
    print(f"\nApplying {updated} table column configs...")
    resp2 = requests.patch(
        f"{host}/api/2.0/genie/spaces/{GENIE_ROOM_ID}",
        headers=headers,
        json={"warehouse_id": WAREHOUSE_ID, "serialized_space": json.dumps(ss)},
    )
    if resp2.status_code == 200:
        result_ss = json.loads(resp2.json()["serialized_space"])
        total_cols = sum(len(t.get("column_configs", [])) for t in result_ss["data_sources"]["tables"])
        print(f"Done! {len(result_ss['data_sources']['tables'])} tables, {total_cols} total column comments")
    else:
        print(f"ERROR {resp2.status_code}: {resp2.text[:500]}")


if __name__ == "__main__":
    main()
