import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, Optional

from .crypto import encrypt_value

DEFAULT_DB_PATH = os.getenv("SDP_CLIENT_DB_PATH", "token_vault.db")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True) if os.path.dirname(db_path) else None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS token_vault (
                token_id TEXT PRIMARY KEY,
                original_value_encrypted TEXT NOT NULL,
                token_value TEXT NOT NULL,
                token_type TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_column TEXT NOT NULL,
                created_at TEXT NOT NULL,
                batch_id TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_token_vault_token
            ON token_vault (token_value, source_table, source_column);
            """
        )
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_connection(db_path: str = DEFAULT_DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def insert_token_record(
    original_value: str,
    token_value: str,
    token_type: str,
    source_table: str,
    source_column: str,
    batch_id: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    """
    Insert a token record into the local Token Vault.
    If the (token_value, source_table, source_column) already exists, we ignore it.
    """
    init_db(db_path)

    encrypted = encrypt_value(original_value)
    token_id = str(uuid.uuid4())
    created_at = _utcnow_iso()

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO token_vault (
                token_id,
                original_value_encrypted,
                token_value,
                token_type,
                source_table,
                source_column,
                created_at,
                batch_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                token_id,
                encrypted,
                token_value,
                token_type,
                source_table,
                source_column,
                created_at,
                batch_id,
            ),
        )
        conn.commit()
