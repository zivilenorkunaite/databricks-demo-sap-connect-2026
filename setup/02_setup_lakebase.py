"""Setup Lakebase tables for conversation history.

Run locally: python setup/02_setup_lakebase.py
Requires: psycopg2-binary, databricks-sdk
"""

import json
import uuid
import psycopg2

LAKEBASE_INSTANCE = "sap-finance-intel"
LAKEBASE_DB = "sap_finance_intel"


def get_connection():
    from databricks.sdk import WorkspaceClient
    import requests

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")

    # Get Lakebase PGHOST
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
    pghost = inst.get("read_write_dns", "")
    if not pghost:
        raise RuntimeError(f"Lakebase instance '{LAKEBASE_INSTANCE}' not available")

    # Get short-lived credential
    resp = requests.post(
        f"{host}/api/2.0/database/credentials",
        headers={"Authorization": f"Bearer {token}"},
        json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
        timeout=10,
    )
    db_token = resp.json().get("token", token)

    user = w.current_user.me().user_name
    return psycopg2.connect(host=pghost, port=5432, dbname=LAKEBASE_DB, user=user, password=db_token, sslmode="require")


def setup_tables():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Create database (ignore if exists — must connect to 'postgres' first)
            pass  # database creation handled by Lakebase resource

            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(64) UNIQUE NOT NULL,
                    title TEXT,
                    user_email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(64) NOT NULL,
                    role VARCHAR(16) NOT NULL,
                    content TEXT NOT NULL,
                    sources TEXT,
                    steps TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)")
            cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC")
            cur.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO PUBLIC")

            # Add columns if they don't exist (idempotent for upgrades)
            for col, typ in [("user_email", "VARCHAR(255)"), ("steps", "TEXT")]:
                try:
                    tbl = "conversations" if col == "user_email" else "messages"
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {typ}")
                except Exception:
                    pass

        conn.commit()
        print("Lakebase tables ready")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_tables()
