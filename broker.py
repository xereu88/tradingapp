from typing import Optional, Literal
import datetime as dt
import logging
import db
from data import last_price

log = logging.getLogger("broker")

Side = Literal["BUY", "SELL"]
Type = Literal["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]

def place_order(account_id: int, symbol: str, side: Side, qty: float, order_type: Type,
                limit_price: Optional[float]=None, stop_price: Optional[float]=None) -> int:
    order_id = db.execute(
        """INSERT INTO orders
        (account_id, symbol, side, qty, order_type, limit_price, stop_price, status, created_at, filled_qty, avg_fill_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (account_id, symbol.upper(), side, float(qty), order_type, limit_price, stop_price, "OPEN", dt.datetime.utcnow(), 0.0, None)
    )
    try_fill_open_orders(account_id, order_id_specific=order_id)
    return order_id

def try_fill_open_orders(account_id: int, order_id_specific: Optional[int] = None):
    query = "SELECT * FROM orders WHERE account_id = ? AND status = 'OPEN'"
    params = (account_id,)
    if order_id_specific is not None:
        query += " AND id = ?"
        params = (account_id, order_id_specific)
    rows = db.fetchall(query, params)
    for r in rows:
        _maybe_fill(r)

def _maybe_fill(order_row):
    oid = order_row["id"]
    account_id = order_row["account_id"]
    symbol = order_row["symbol"]
    side = order_row["side"]
    qty = float(order_row["qty"])
    otype = order_row["order_type"]
    limit_px = order_row["limit_price"]
    stop_px = order_row["stop_price"]

    try:
        lp = last_price(symbol)
    except Exception as exc:
        log.debug("last_price failed for %s: %s", symbol, exc)
        return

    def execute_fill(fill_qty: float, price: float):
        db.execute("INSERT INTO trades (order_id, symbol, qty, price, timestamp) VALUES (?, ?, ?, ?, ?)",
                   (oid, symbol, fill_qty, price, dt.datetime.utcnow()))
        db.execute("UPDATE orders SET status = ?, filled_qty = ?, avg_fill_price = ? WHERE id = ?",
                   ("FILLED", fill_qty, price, oid))
        if side == "BUY":
            cash_out = -fill_qty * price
            db.execute("INSERT INTO ledger (account_id, kind, amount, note, timestamp) VALUES (?, ?, ?, ?, ?)",
                       (account_id, "trade_buy", cash_out, f"BUY {fill_qty} {symbol} @ {price}", dt.datetime.utcnow()))
            db.upsert_position(account_id, symbol, +fill_qty, price)
        else:
            cash_in = fill_qty * price
            db.execute("INSERT INTO ledger (account_id, kind, amount, note, timestamp) VALUES (?, ?, ?, ?, ?)",
                       (account_id, "trade_sell", cash_in, f"SELL {fill_qty} {symbol} @ {price}", dt.datetime.utcnow()))
            db.upsert_position(account_id, symbol, -fill_qty, price)

    if otype == "MARKET":
        execute_fill(qty, lp)
        return

    if otype == "LIMIT":
        if side == "BUY" and lp <= (limit_px or -1):
            execute_fill(qty, lp)
        elif side == "SELL" and lp >= (limit_px or 1e18):
            execute_fill(qty, lp)
        return

    if otype == "STOP":
        if side == "BUY" and lp >= (stop_px or 1e18):
            execute_fill(qty, lp)
        elif side == "SELL" and lp <= (stop_px or -1):
            execute_fill(qty, lp)
        return

    if otype == "STOP_LIMIT":
        triggered = False
        if side == "BUY" and lp >= (stop_px or 1e18):
            triggered = True
        elif side == "SELL" and lp <= (stop_px or -1):
            triggered = True
        if triggered:
            if side == "BUY" and lp <= (limit_px or -1):
                execute_fill(qty, lp)
            elif side == "SELL" and lp >= (limit_px or 1e18):
                execute_fill(qty, lp)
        return
