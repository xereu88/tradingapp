from dataclasses import dataclass
from typing import Optional
import datetime as dt
import yfinance as yf
import db

@dataclass
class OptionContract:
    symbol: str     # underlying
    expiry: str     # YYYY-MM-DD
    right: str      # "C" or "P"
    strike: float

    def occ(self) -> str:
        y, m, d = self.expiry.split("-")
        return f"{self.symbol.upper()}{y[2:]}{m}{d}{self.right}{int(round(self.strike*1000)):08d}"

def get_option_chain(symbol: str, expiry: Optional[str] = None):
    t = yf.Ticker(symbol)
    if expiry is None:
        exps = t.options
        if not exps:
            raise ValueError("No expirations available.")
        expiry = exps[0]
    chain = t.option_chain(expiry)
    calls = chain.calls.copy()
    puts = chain.puts.copy()
    calls["right"] = "C"
    puts["right"] = "P"
    calls["expiry"] = expiry
    puts["expiry"] = expiry
    return calls, puts, expiry

def place_option_order(account_id: int, contract: OptionContract, side: str, qty: int, order_type: str = "MARKET", limit_price: Optional[float] = None):
    t = yf.Ticker(contract.symbol)
    chain = t.option_chain(contract.expiry)
    chain_df = chain.calls if contract.right == "C" else chain.puts
    row = chain_df.loc[chain_df["strike"] == contract.strike]
    if row.empty:
        raise ValueError("Contract not found in chain.")
    bid = float(row["bid"].iloc[0] or 0.0)
    ask = float(row["ask"].iloc[0] or 0.0)
    last = float(row["lastPrice"].iloc[0] or 0.0)
    mid = (bid + ask) / 2 if (bid and ask) else (last or 0.0)
    if mid <= 0:
        raise ValueError("No valid quote for this contract.")
    fill_price = mid
    if order_type.upper() == "LIMIT" and limit_price is not None:
        if side.upper() == "BUY" and mid > limit_price:
            raise ValueError("Limit not met for BUY.")
        if side.upper() == "SELL" and mid < limit_price:
            raise ValueError("Limit not met for SELL.")
        fill_price = limit_price

    oid = db.execute("""
        INSERT INTO option_orders (account_id, symbol, expiry, right, strike, side, qty, order_type, limit_price, status, created_at, filled_qty, avg_fill_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'FILLED', ?, ?, ?)
    """, (account_id, contract.symbol.upper(), contract.expiry, contract.right, contract.strike, side.upper(), int(qty), order_type.upper(), limit_price, dt.datetime.utcnow(), int(qty), fill_price))

    db.execute("""
        INSERT INTO option_trades (order_id, symbol, expiry, right, strike, qty, price, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (oid, contract.symbol.upper(), contract.expiry, contract.right, contract.strike, int(qty), fill_price, dt.datetime.utcnow()))

    notional = fill_price * qty * 100.0
    if side.upper() == "BUY":
        cash_delta = -notional
        note = f"OPT BUY {qty} {contract.occ()} @ {fill_price:.2f}"
        db.execute("INSERT INTO ledger (account_id, kind, amount, note, timestamp) VALUES (?, 'trade_buy', ?, ?, ?)", (account_id, cash_delta, note, dt.datetime.utcnow()))
        db.upsert_option_position(account_id, contract.symbol.upper(), contract.expiry, contract.right, contract.strike, qty, fill_price)
    else:
        cash_delta = notional
        note = f"OPT SELL {qty} {contract.occ()} @ {fill_price:.2f}"
        db.execute("INSERT INTO ledger (account_id, kind, amount, note, timestamp) VALUES (?, 'trade_sell', ?, ?, ?)", (account_id, cash_delta, note, dt.datetime.utcnow()))
        db.upsert_option_position(account_id, contract.symbol.upper(), contract.expiry, contract.right, contract.strike, -qty, fill_price)

    return oid
