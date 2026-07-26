"""SQLite database for multi-account management."""
import sqlite3
import json
from pathlib import Path
from typing import Optional
from cryptography.fernet import Fernet

from .proxy_nodes import PROXY_NODES

DB_PATH = Path(__file__).parent.parent.parent / "data" / "accounts.db"

# The generated key is stored beside the database; this is local at-rest
# protection, not a replacement for host-level access control.
_KEY_FILE = Path(__file__).parent.parent.parent / "data" / ".key"


def _get_cipher():
    if not _KEY_FILE.exists():
        _KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
        key = Fernet.generate_key()
        _KEY_FILE.write_bytes(key)
    else:
        key = _KEY_FILE.read_bytes()
    return Fernet(key)


_cipher = None


def cipher():
    global _cipher
    if _cipher is None:
        _cipher = _get_cipher()
    return _cipher


def encrypt(value: str) -> str:
    if not value:
        return ""
    return cipher().encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    if not value:
        return ""
    return cipher().decrypt(value.encode()).decode()


def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            api_key TEXT NOT NULL,
            secret_key TEXT NOT NULL,
            passphrase TEXT NOT NULL,
            agent_private_key TEXT NOT NULL,
            proxy_node TEXT NOT NULL,
            base_url TEXT NOT NULL DEFAULT 'https://us.okx.com',
            depth_threshold INTEGER DEFAULT 90,
            price_tick INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS global_config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS sniper_state (
            account_id TEXT PRIMARY KEY,
            config_json TEXT NOT NULL DEFAULT '{}',
            runtime_json TEXT NOT NULL DEFAULT '{}',
            running INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
    """)
    conn.commit()
    conn.close()


def get_used_proxies() -> set:
    """Return set of proxy_node values already in use."""
    conn = get_db()
    rows = conn.execute("SELECT proxy_node FROM accounts").fetchall()
    conn.close()
    return {r["proxy_node"] for r in rows}


def get_available_proxies() -> list:
    """Return proxy nodes not yet assigned to an account."""
    used = get_used_proxies()
    return [
        {
            "id": k,
            "label": v["label"],
            "location": v["location"],
            "route": v["route"],
        }
        for k, v in PROXY_NODES.items()
        if k not in used
    ]


# ─── Account CRUD ───

def create_account(name: str, api_key: str, secret_key: str, passphrase: str,
                   agent_private_key: str, proxy_node: str) -> dict:
    import uuid
    account_id = str(uuid.uuid4())[:8]

    if proxy_node not in PROXY_NODES:
        raise ValueError(f"Invalid proxy node: {proxy_node}")
    if proxy_node in get_used_proxies():
        raise ValueError(f"Proxy node {proxy_node} already in use")

    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO accounts (id, name, api_key, secret_key, passphrase, agent_private_key, proxy_node)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (account_id, name, encrypt(api_key), encrypt(secret_key),
             encrypt(passphrase), encrypt(agent_private_key), proxy_node)
        )
        conn.commit()
    finally:
        conn.close()

    return {"id": account_id, "name": name, "proxy_node": proxy_node}


def get_accounts() -> list:
    conn = get_db()
    rows = conn.execute("SELECT * FROM accounts ORDER BY created_at").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_account(account_id: str) -> Optional[dict]:
    conn = get_db()
    row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_account_decrypted(account_id: str) -> Optional[dict]:
    """Get account with decrypted credentials."""
    acc = get_account(account_id)
    if not acc:
        return None
    acc["api_key"] = decrypt(acc["api_key"])
    acc["secret_key"] = decrypt(acc["secret_key"])
    acc["passphrase"] = decrypt(acc["passphrase"])
    acc["agent_private_key"] = decrypt(acc["agent_private_key"])
    return acc


def update_account(account_id: str, **kwargs) -> bool:
    ALLOWED_COLS = {"api_key", "secret_key", "passphrase", "agent_private_key", "proxy_node", "base_url", "depth_threshold", "price_tick"}
    for k in kwargs:
        if k not in ALLOWED_COLS:
            raise ValueError(f"Invalid column: {k}")
    conn = get_db()
    sets = []
    values = []
    encrypt_fields = {"api_key", "secret_key", "passphrase", "agent_private_key"}
    for k, v in kwargs.items():
        if k in encrypt_fields:
            v = encrypt(v)
        sets.append(f"{k} = ?")
        values.append(v)
    sets.append("updated_at = datetime('now')")
    values.append(account_id)
    try:
        cursor = conn.execute(
            f"UPDATE accounts SET {', '.join(sets)} WHERE id = ?", values
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_account(account_id: str) -> bool:
    conn = get_db()
    conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
    conn.commit()
    conn.close()
    return True


# ─── Global Config ───

def get_global_config(key: str, default: str = "") -> str:
    conn = get_db()
    row = conn.execute("SELECT value FROM global_config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_global_config(key: str, value: str):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO global_config (key, value) VALUES (?, ?)",
        (key, value)
    )
    conn.commit()
    conn.close()


def get_sniper_state(account_id: str) -> Optional[dict]:
    """Return the persisted sniper configuration and last runtime snapshot."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT config_json, runtime_json, running, updated_at FROM sniper_state WHERE account_id = ?",
            (account_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    corrupt_fields = []

    def _load_object(field_name: str) -> dict:
        raw_value = row[field_name]
        if not raw_value:
            corrupt_fields.append(field_name)
            return {}
        try:
            value = json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            corrupt_fields.append(field_name)
            return {}
        if not isinstance(value, dict):
            corrupt_fields.append(field_name)
            return {}
        return value

    config_data = _load_object("config_json")
    runtime_data = _load_object("runtime_json")
    return {
        "config": config_data,
        "runtime": runtime_data,
        "running": bool(row["running"]),
        "updated_at": row["updated_at"],
        "corrupt": bool(corrupt_fields),
        "corrupt_fields": corrupt_fields,
    }


def save_sniper_state(account_id: str, config_data: dict, runtime_data: dict,
                       running: bool) -> None:
    """Atomically persist a sniper snapshot for page and process recovery."""
    config_json = json.dumps(
        config_data or {}, ensure_ascii=False, separators=(",", ":")
    )
    runtime_json = json.dumps(
        runtime_data or {}, ensure_ascii=False, separators=(",", ":")
    )
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO sniper_state (account_id, config_json, runtime_json, running, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(account_id) DO UPDATE SET
                config_json = excluded.config_json,
                runtime_json = excluded.runtime_json,
                running = excluded.running,
                updated_at = datetime('now')
            """,
            (account_id, config_json, runtime_json, 1 if running else 0),
        )
        conn.commit()
    finally:
        conn.close()
