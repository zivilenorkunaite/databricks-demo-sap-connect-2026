"""Query SAP BDC data via Genie Room (natural language → SQL → results)."""

import os
import time
import logging
import requests
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

GENIE_ROOM_ID = os.environ.get("GENIE_ROOM_ID", "")

_ws_client: WorkspaceClient | None = None


def _get_ws() -> WorkspaceClient:
    global _ws_client
    if _ws_client is None:
        _ws_client = WorkspaceClient()
    return _ws_client


def _get_auth() -> tuple[str, str]:
    """Return (host, bearer_token)."""
    w = _get_ws()
    host = w.config.host.rstrip("/")
    token = w.config.token
    if not token:
        headers = w.config.authenticate()
        token = headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        logger.error("No auth token available for Genie API")
    return host, token


def query_genie(question: str, timeout_seconds: int = 45) -> dict:
    """Send a natural language question to the Genie room and wait for results.

    Returns {"columns": [...], "rows": [...], "row_count": N, "sql": "...", "error": "..."}
    """
    if not GENIE_ROOM_ID:
        return {"error": "GENIE_ROOM_ID not configured", "columns": [], "rows": [], "row_count": 0}

    host, token = _get_auth()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base = f"{host}/api/2.0/genie/spaces/{GENIE_ROOM_ID}"

    # 1. Start a conversation
    try:
        resp = requests.post(
            f"{base}/start-conversation",
            headers=headers,
            json={"content": question},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Genie start-conversation HTTP error: {e} — response: {e.response.text if e.response else 'none'}")
        return {"error": f"Genie API error: {e}", "columns": [], "rows": [], "row_count": 0}
    except Exception as e:
        logger.error(f"Genie start-conversation failed: {e}")
        return {"error": str(e), "columns": [], "rows": [], "row_count": 0}

    conversation_id = data.get("conversation_id", "")
    message_id = data.get("message_id", "")

    if not conversation_id or not message_id:
        return {"error": "No conversation/message ID returned", "columns": [], "rows": [], "row_count": 0}

    # 2. Poll for completion
    poll_url = f"{base}/conversations/{conversation_id}/messages/{message_id}"
    start = time.time()
    status = ""
    result_data = {}

    while time.time() - start < timeout_seconds:
        try:
            resp = requests.get(poll_url, headers=headers, timeout=10)
            resp.raise_for_status()
            result_data = resp.json()
            status = result_data.get("status", "")

            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                break
        except Exception as e:
            logger.warning(f"Genie poll error: {e}")

        time.sleep(2)

    if status == "FAILED":
        error = result_data.get("error", "Genie query failed")
        return {"error": str(error), "columns": [], "rows": [], "row_count": 0}

    if status != "COMPLETED":
        return {"error": f"Genie timed out (status: {status})", "columns": [], "rows": [], "row_count": 0}

    # 3. Extract SQL and results from attachments
    attachments = result_data.get("attachments", [])
    sql_text = ""
    columns = []
    rows = []

    for att in attachments:
        query_att = att.get("query", {})
        if query_att:
            sql_text = query_att.get("query", "")
            description = query_att.get("description", "")

        text_att = att.get("text", {})
        if text_att:
            # Text response (no SQL needed)
            pass

    # 4. If there's a SQL query, fetch the result
    if sql_text:
        # Get result from the attachment
        for att in attachments:
            query_att = att.get("query", {})
            if query_att and query_att.get("query"):
                # Try to get results via the query result endpoint
                att_id = att.get("attachment_id", "")
                if att_id:
                    try:
                        result_resp = requests.get(
                            f"{poll_url}/query-result/{att_id}",
                            headers=headers,
                            timeout=30,
                        )
                        result_resp.raise_for_status()
                        qr = result_resp.json()

                        # Extract columns
                        stmt_resp = qr.get("statement_response", {})
                        manifest = stmt_resp.get("manifest", {})
                        schema_cols = manifest.get("schema", {}).get("columns", [])
                        columns = [c.get("name", "") for c in schema_cols]

                        # Extract rows
                        result_obj = stmt_resp.get("result", {})
                        rows = result_obj.get("data_array", [])
                    except Exception as e:
                        logger.warning(f"Failed to fetch query result: {e}")

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "sql": sql_text,
    }
