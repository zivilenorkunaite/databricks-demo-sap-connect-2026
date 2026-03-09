"""Lakebase connection for conversation history.

Uses auto-injected PG* env vars from the Databricks App database resource.
Password is a short-lived token from /api/2.0/database/credentials.
"""

import os
import json
import uuid
import logging
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# Auto-injected by Databricks when a database resource is attached
PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")

LAKEBASE_INSTANCE = "sap-finance-intel"


def _get_auth_token() -> str:
    """Get workspace OAuth token via WorkspaceClient."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    if w.config.token:
        return w.config.token
    headers = w.config.authenticate()
    tok = headers.get("Authorization", "").replace("Bearer ", "")
    if tok:
        return tok
    raise ValueError("No auth token available")


def _get_db_password() -> str:
    """Get Lakebase credential token via REST API, falling back to workspace token."""
    token = _get_auth_token()
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host:
        from databricks.sdk import WorkspaceClient
        host = WorkspaceClient().config.host.rstrip("/")

    try:
        resp = requests.post(
            f"{host}/api/2.0/database/credentials",
            headers={"Authorization": f"Bearer {token}"},
            json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
            timeout=10,
        )
        data = resp.json()
        db_token = data.get("token")
        if db_token:
            return db_token
        logger.info("Database credentials API returned no token, falling back to workspace token")
    except Exception as e:
        logger.warning(f"Database credentials API failed ({e}), falling back to workspace token")

    return token


def get_conn():
    """Get a psycopg2 connection to Lakebase."""
    password = _get_db_password()
    return psycopg2.connect(
        host=PGHOST,
        port=int(PGPORT),
        dbname=PGDATABASE,
        user=PGUSER,
        password=password,
        sslmode=PGSSLMODE,
    )


# --- Conversation CRUD ---

def list_conversations(user_email: str, limit: int = 50) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, session_id, title, created_at, updated_at "
                "FROM conversations WHERE user_email = %s ORDER BY updated_at DESC LIMIT %s",
                (user_email, limit),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_conversation(session_id: str, user_email: str) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, session_id, title, created_at FROM conversations "
                "WHERE session_id = %s AND user_email = %s",
                (session_id, user_email),
            )
            conv = cur.fetchone()
            if not conv:
                return None

            cur.execute(
                "SELECT role, content, sources, steps, created_at FROM messages "
                "WHERE session_id = %s ORDER BY created_at",
                (session_id,),
            )
            messages = []
            for m in cur.fetchall():
                msg = dict(m)
                if msg.get("sources") and isinstance(msg["sources"], str):
                    msg["sources"] = json.loads(msg["sources"])
                if msg.get("steps") and isinstance(msg["steps"], str):
                    msg["steps"] = json.loads(msg["steps"])
                messages.append(msg)

            result = dict(conv)
            result["messages"] = messages
            return result
    finally:
        conn.close()


def save_message(session_id: str, role: str, content: str, sources: list | None = None,
                 title: str | None = None, user_email: str | None = None,
                 steps: list | None = None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO conversations (session_id, title, user_email) VALUES (%s, %s, %s) "
                "ON CONFLICT (session_id) DO UPDATE SET updated_at = NOW(), "
                "title = COALESCE(EXCLUDED.title, conversations.title)",
                (session_id, title, user_email),
            )
            cur.execute(
                "INSERT INTO messages (session_id, role, content, sources, steps) VALUES (%s, %s, %s, %s, %s)",
                (session_id, role, content,
                 json.dumps(sources) if sources else None,
                 json.dumps(steps) if steps else None),
            )
        conn.commit()
    finally:
        conn.close()


def delete_conversation(session_id: str, user_email: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM messages WHERE session_id IN "
                        "(SELECT session_id FROM conversations WHERE session_id = %s AND user_email = %s)",
                        (session_id, user_email))
            cur.execute("DELETE FROM conversations WHERE session_id = %s AND user_email = %s",
                        (session_id, user_email))
        conn.commit()
    finally:
        conn.close()
