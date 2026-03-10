"""Finance Intelligence Agent — tool-use loop over Genie (SAP data) + corporate documents."""

import os
import json
import logging
from openai import OpenAI
from server.genie_tools import query_genie
from server.documents import search_documents, get_document, list_documents

logger = logging.getLogger(__name__)

_ws_client = None
_host: str | None = None


def _get_doc_index() -> str:
    """One-line-per-doc summary for the system prompt."""
    docs = list_documents()
    lines = ["## Corporate Documents Available\n"]
    for d in docs:
        lines.append(f"- **{d['doc_id']}** — {d['title']} [{d['category']}]")
    return "\n".join(lines)


SYSTEM_PROMPT = """You are a Finance Intelligence Agent powered by Databricks, analysing SAP S/4HANA data products shared via SAP Business Data Cloud and corporate governance documents.

You have THREE sources of information:

### Source 1 — SAP Data Products (live transactional data)
Queried via a Genie Room connected to SAP BDC data in Unity Catalog. The Genie Room has access to:
- Purchase Orders (headers + line items)
- Sales Orders (headers + line items)
- Billing Documents (headers + line items)
- Journal Entries
- General Ledger Accounts
- Supplier Invoices (headers + line items)
- **Supplier Risk Scores** — external third-party credit/risk data (credit ratings, financial health scores, risk levels, ESG scores, payment default history) joined to suppliers via supplier_id

### Source 2 — Corporate Documents (policies, audit reports, procedures)

{doc_index}

## Your tools
1. **query_sap_data** — Ask a natural language question about SAP finance data. The question is sent to a Genie Room which generates and executes the SQL automatically.
2. **search_documents** — Search corporate policy/audit/procedure documents by keywords
3. **get_document** — Retrieve a specific document by ID

## How to work
- Cross-reference ALL sources when relevant — e.g. check PO data against procurement policy thresholds, or flag high-risk suppliers with large open orders
- ALWAYS query data before answering numerical questions — never fabricate numbers
- Write clear, specific questions for query_sap_data
- When referencing a policy, cite the document ID
- Format currency values with appropriate symbols
- Use markdown tables where appropriate
- **Keep responses concise** — short paragraphs, bullet points preferred. This is a live demo for executives, not a whitepaper. 3-5 key points max per response.
- Minimise the number of tool calls — combine related questions into a single query where possible
- If you only need data OR documents (not both), don't query the other source
"""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_sap_data",
            "description": "Ask a natural language question about SAP finance data. The question is sent to a Genie Room connected to live SAP BDC data products (purchase orders, sales orders, billing documents, journal entries, GL accounts, supplier invoices). Returns structured data results.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "A clear natural language question about the SAP finance data. Be specific about what metrics, filters, or breakdowns you need.",
                    },
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_documents",
            "description": "Search across corporate finance documents (policies, audit reports, procedures) for relevant content. Use keywords related to the topic you need — e.g. 'approval threshold', 'supplier risk', 'accrual', 'payment terms'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search keywords to find relevant document sections.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_document",
            "description": "Retrieve the full text of a specific corporate document by its ID. Use when you need complete context from a known document.",
            "parameters": {
                "type": "object",
                "properties": {
                    "doc_id": {
                        "type": "string",
                        "description": "The document ID (e.g. PROC-POL-001, AUD-RPT-Q4-2025).",
                    },
                },
                "required": ["doc_id"],
            },
        },
    },
]


def _get_client() -> OpenAI:
    """Return an OpenAI client with a fresh token (SP tokens are short-lived)."""
    global _ws_client, _host

    from databricks.sdk import WorkspaceClient
    if _ws_client is None:
        _ws_client = WorkspaceClient()
        _host = _ws_client.config.host.rstrip("/")

    # Always get a fresh token
    token = _ws_client.config.token
    if not token:
        headers = _ws_client.config.authenticate()
        token = headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        raise ValueError("No Databricks token available — check app service principal config")

    return OpenAI(
        base_url=f"{_host}/serving-endpoints",
        api_key=token,
        timeout=120.0,
    )


def _emit_step(step: dict, steps: list, on_step=None):
    """Append step to list and fire callback if provided."""
    steps.append(step)
    if on_step:
        on_step(step)


def _handle_tool_call(tc, steps: list, on_step=None) -> tuple[str, dict]:
    """Execute a tool call and return (result_text, source_info). Appends to steps list."""
    name = tc.function.name
    args = json.loads(tc.function.arguments)

    if name == "query_sap_data":
        question = args.get("question", "")
        logger.info(f"Genie query: {question[:150]}")
        _emit_step({"type": "genie", "action": "query", "detail": question}, steps, on_step)
        result = query_genie(question)
        source = {
            "type": "sql",
            "purpose": question,
            "query": result.get("sql", ""),
            "row_count": result.get("row_count", 0),
            "error": result.get("error"),
        }
        if result.get("error"):
            _emit_step({"type": "genie", "action": "error", "detail": result["error"]}, steps, on_step)
            return f"ERROR: {result['error']}", source
        _emit_step({"type": "genie", "action": "result", "detail": f"{result['row_count']} rows returned", "sql": result.get("sql", "")}, steps, on_step)
        text = json.dumps({"columns": result["columns"], "rows": result["rows"][:100]}, default=str)
        if result.get("row_count", 0) > 100:
            text += f"\n(showing 100 of {result['row_count']} rows)"
        return text, source

    elif name == "search_documents":
        query = args.get("query", "")
        logger.info(f"Doc search: {query}")
        _emit_step({"type": "document", "action": "search", "detail": query}, steps, on_step)
        results = search_documents(query)
        doc_entries = [{"title": r["title"], "doc_id": r["doc_id"], "content": r["content"]} for r in results[:3]]
        source = {
            "type": "document",
            "purpose": f"Search: {query}",
            "documents": [r["title"] for r in results[:3]],
            "doc_contents": doc_entries,
        }
        if not results:
            _emit_step({"type": "document", "action": "result", "detail": "No matching documents"}, steps, on_step)
            return "No matching documents found.", source
        titles = [r["title"] for r in results[:3]]
        _emit_step({"type": "document", "action": "result", "detail": f"Found: {', '.join(titles)}"}, steps, on_step)
        parts = []
        for r in results[:3]:
            parts.append(f"## {r['title']} ({r['doc_id']})\n{r['content']}")
        return "\n\n---\n\n".join(parts), source

    elif name == "get_document":
        doc_id = args.get("doc_id", "")
        logger.info(f"Get doc: {doc_id}")
        _emit_step({"type": "document", "action": "fetch", "detail": doc_id}, steps, on_step)
        doc = get_document(doc_id)
        source = {
            "type": "document",
            "purpose": f"Document: {doc_id}",
            "documents": [doc["title"]] if doc else [],
            "doc_contents": [{"title": doc["title"], "doc_id": doc_id, "content": doc["content"]}] if doc else [],
        }
        if not doc:
            _emit_step({"type": "document", "action": "error", "detail": f"Document '{doc_id}' not found"}, steps, on_step)
            return f"Document '{doc_id}' not found.", source
        _emit_step({"type": "document", "action": "result", "detail": doc["title"]}, steps, on_step)
        return f"## {doc['title']} ({doc_id})\n\n{doc['content']}", source

    return "Unknown tool", {"type": "error", "purpose": name}


def run_agent(user_message: str, history: list[dict], on_step=None) -> dict:
    """Run the agent loop. Returns {"response": str, "sources": list[dict], "steps": list}.

    If on_step callback is provided, it is called with each step dict in real-time.
    """
    client = _get_client()
    doc_index = _get_doc_index()

    system = SYSTEM_PROMPT.format(doc_index=doc_index)

    messages = [{"role": "system", "content": system}]
    for msg in history[-10:]:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    sources = []
    steps = []
    max_iterations = 5

    _emit_step({"type": "agent", "action": "thinking", "detail": "Analysing your question and planning approach"}, steps, on_step)

    for iteration in range(max_iterations):
        response = client.chat.completions.create(
            model="databricks-claude-sonnet-4-6",
            messages=messages,
            tools=TOOLS,
            max_tokens=2048,
        )

        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" or choice.message.tool_calls:
            messages.append(choice.message)

            for tc in choice.message.tool_calls:
                result_text, source_info = _handle_tool_call(tc, steps, on_step)
                sources.append(source_info)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_text,
                })

            if iteration < max_iterations - 1:
                _emit_step({"type": "agent", "action": "thinking", "detail": "Reviewing results and deciding next step"}, steps, on_step)
        else:
            _emit_step({"type": "agent", "action": "done", "detail": "Composing final response"}, steps, on_step)
            return {
                "response": choice.message.content or "",
                "sources": sources,
                "steps": steps,
            }

    _emit_step({"type": "agent", "action": "done", "detail": "Reached maximum analysis steps"}, steps, on_step)
    return {
        "response": "I reached the maximum number of analysis steps. Here's what I found so far based on the queries above.",
        "sources": sources,
        "steps": steps,
    }
