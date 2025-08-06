import sqlite3
import datetime as dt
from typing import Iterable, Tuple, Any

DB_PATH = "paper_trader.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    # Enforce foreign keys for safer integrity
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Accounts & cash ledger
    cur.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        starting_cash REAL NOT NULL,
        created_at TIMESTAMP NOT NULL
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        kind TEXT NOT NULL,
        amount REAL NOT NULL,
        note TEXT,
        timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    # Equity orders/trades/positions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,                -- BUY/SELL
        qty REAL NOT NULL,
        order_type TEXT NOT NULL,          -- MARKET/LIMIT/STOP/STOP_LIMIT
        limit_price REAL,
        stop_price REAL,
        status TEXT NOT NULL,              -- OPEN/FILLED/CANCELLED
        created_at TIMESTAMP NOT NULL,
        filled_qty REAL NOT NULL,
        avg_fill_price REAL,
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        qty REAL NOT NULL,
        price REAL NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY(order_id) REFERENCES orders(id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        qty REAL NOT NULL,
        avg_price REAL NOT NULL,
        UNIQUE(account_id, symbol),
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    # Options tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS option_orders (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,              -- underlying
        expiry TEXT NOT NULL,              -- YYYY-MM-DD
        right TEXT NOT NULL,               -- C or P
        strike REAL NOT NULL,
        side TEXT NOT NULL,                -- BUY/SELL
        qty INTEGER NOT NULL,              -- contracts
        order_type TEXT NOT NULL,          -- MARKET/LIMIT
        limit_price REAL,
        status TEXT NOT NULL,              -- OPEN/FILLED/CANCELLED
        created_at TIMESTAMP NOT NULL,
        filled_qty INTEGER NOT NULL,
        avg_fill_price REAL,
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS option_trades (
        id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        expiry TEXT NOT NULL,
        right TEXT NOT NULL,
        strike REAL NOT NULL,
        qty INTEGER NOT NULL,
        price REAL NOT NULL,               -- per contract premium
        timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY(order_id) REFERENCES option_orders(id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS option_positions (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        expiry TEXT NOT NULL,
        right TEXT NOT NULL,
        strike REAL NOT NULL,
        qty INTEGER NOT NULL,
        avg_price REAL NOT NULL,
        UNIQUE(account_id, symbol, expiry, right, strike),
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    # Bot settings
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bot_settings (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL UNIQUE,
        config_json TEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        FOREIGN KEY(account_id) REFERENCES accounts(id)
    )""")

    conn.commit()
    conn.close()

def get_or_create_default_account(starting_cash: float = 100_000.0) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM accounts WHERE name = ?", ("default",))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO accounts (name, starting_cash, created_at) VALUES (?, ?, ?)",
            ("default", starting_cash, dt.datetime.utcnow()),
        )
        account_id = cur.lastrowid
        cur.execute(
            "INSERT INTO ledger (account_id, kind, amount, note, timestamp) VALUES (?, ?, ?, ?, ?)",
            (account_id, "deposit", starting_cash, "Initial cash", dt.datetime.utcnow()),
        )
        conn.commit()
        conn.close()
        return account_id
    conn.close()
    return row["id"]

def reset_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS option_positions;
        DROP TABLE IF EXISTS option_trades;
        DROP TABLE IF EXISTS option_orders;
        DROP TABLE IF EXISTS positions;
        DROP TABLE IF EXISTS trades;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS ledger;
        DROP TABLE IF EXISTS bot_settings;
        DROP TABLE IF EXISTS accounts;
    """)
    conn.commit()
    conn.close()
    init_db()

def fetchall(query: str, params: Tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def execute(query: str, params: Tuple[Any, ...] = ()) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    lastrowid = cur.lastrowid
    conn.close()
    return lastrowid

def executemany(query: str, rows: Iterable[Tuple[Any, ...]]):
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(query, rows)
    conn.commit()
    conn.close()

def get_cash_balance(account_id: int) -> float:
    rows = fetchall("SELECT SUM(amount) as cash FROM ledger WHERE account_id = ?", (account_id,))
    return float(rows[0]["cash"] or 0.0)

def upsert_position(account_id: int, symbol: str, qty_delta: float, price: float):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, qty, avg_price FROM positions WHERE account_id = ? AND symbol = ?", (account_id, symbol))
    row = cur.fetchone()
    if row is None:
        if qty_delta < 0:
            raise ValueError("Cannot sell a non-existing position")
        cur.execute("INSERT INTO positions (account_id, symbol, qty, avg_price) VALUES (?, ?, ?, ?)",
                    (account_id, symbol, qty_delta, price))
    else:
        pos_id, qty, avg_price = row["id"], row["qty"], row["avg_price"]
        new_qty = qty + qty_delta
        if new_qty < -1e-9:
            raise ValueError("Resulting position would be negative; shorting not supported in this demo.")
        if qty_delta > 0:
            new_avg = (qty * avg_price + qty_delta * price) / (qty + qty_delta)
        elif qty_delta < 0:
            new_avg = avg_price if new_qty > 0 else avg_price
        else:
            new_avg = avg_price
        if new_qty == 0:
            cur.execute("DELETE FROM positions WHERE id = ?", (pos_id,))
        else:
            cur.execute("UPDATE positions SET qty = ?, avg_price = ? WHERE id = ?", (new_qty, new_avg, pos_id))
    conn.commit()
    conn.close()

def upsert_option_position(account_id: int, symbol: str, expiry: str, right: str, strike: float, qty_delta: int, price: float):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT id, qty, avg_price FROM option_positions 
                   WHERE account_id = ? AND symbol = ? AND expiry = ? AND right = ? AND strike = ?""",
                (account_id, symbol, expiry, right, strike))
    row = cur.fetchone()
    if row is None:
        if qty_delta < 0:
            raise ValueError("Cannot sell a non-existing option position")
        cur.execute("""INSERT INTO option_positions (account_id, symbol, expiry, right, strike, qty, avg_price) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (account_id, symbol, expiry, right, strike, qty_delta, price))
    else:
        pos_id, qty, avg_price = row["id"], row["qty"], row["avg_price"]
        new_qty = qty + qty_delta
        if new_qty < 0:
            raise ValueError("Negative option position not supported in this demo.")
        if qty_delta > 0:
            new_avg = (qty * avg_price + qty_delta * price) / (qty + qty_delta)
        elif qty_delta < 0:
            new_avg = avg_price if new_qty > 0 else avg_price
        else:
            new_avg = avg_price
        if new_qty == 0:
            cur.execute("DELETE FROM option_positions WHERE id = ?", (pos_id,))
        else:
            cur.execute("UPDATE option_positions SET qty = ?, avg_price = ? WHERE id = ?", (new_qty, new_avg, pos_id))
    conn.commit()
    conn.close()
