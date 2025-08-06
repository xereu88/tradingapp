import math
import json
from typing import List
from datetime import datetime, time as dtime

import db
import broker
from data import history, last_price

def is_market_open_now() -> bool:
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:
        return False
    return dtime(9,30) <= now.time() < dtime(16,0)

def momentum_5d(symbol: str) -> float:
    try:
        df = history(symbol, period="10d", interval="1d")
        if len(df) < 6:
            return 0.0
        c0 = float(df["Close"].iloc[-6])
        c1 = float(df["Close"].iloc[-1])
        return (c1 - c0) / c0 if c0 > 0 else 0.0
    except Exception:
        return 0.0

def run_once(account_id: int, config: dict) -> dict:
    report = {"timestamp": datetime.utcnow().isoformat(), "actions": []}
    if not is_market_open_now():
        report["note"] = "Market closed"
        return report

    watchlist: List[str] = [s.strip().upper() for s in config.get("watchlist", "").split(",") if s.strip()] or ["SPY","AAPL","MSFT","NVDA"]
    buy_thresh: float = float(config.get("buy_threshold", 0.20))
    sell_thresh: float = float(config.get("sell_threshold", -0.20))
    buy_cash_frac: float = float(config.get("buy_cash_fraction", 0.10))
    sell_pos_frac: float = float(config.get("sell_position_fraction", 0.10))
    min_notional: float = float(config.get("min_notional", 200.0))

    rows = db.fetchall("SELECT symbol, qty FROM positions WHERE account_id = ?", (account_id,))
    held = {r["symbol"]: float(r["qty"]) for r in rows}
    cash = db.get_cash_balance(account_id)

    for sym in watchlist:
        try:
            px = float(last_price(sym))
        except Exception:
            continue
        # simple signal via 5d momentum
        try:
            df = history(sym, period="10d", interval="1d")
            if len(df) >= 6:
                c0 = float(df["Close"].iloc[-6])
                c1 = float(df["Close"].iloc[-1])
                mom = (c1 - c0) / c0 if c0 > 0 else 0.0
            else:
                mom = 0.0
        except Exception:
            mom = 0.0

        score = mom
        if score >= buy_thresh:
            notional = cash * buy_cash_frac
            qty = int(notional // px)
            if qty > 0 and notional >= min_notional:
                broker.place_order(account_id, sym, "BUY", qty, "MARKET", None, None)
                cash -= qty * px
                report["actions"].append({"symbol": sym, "side": "BUY", "qty": qty, "px": px, "reason": f"score {score:.3f} >= {buy_thresh:.2f}"})
        elif score <= sell_thresh and held.get(sym, 0.0) > 0:
            qty = max(1, int(held.get(sym, 0.0) * sell_pos_frac))
            broker.place_order(account_id, sym, "SELL", qty, "MARKET", None, None)
            report["actions"].append({"symbol": sym, "side": "SELL", "qty": qty, "px": px, "reason": f"score {score:.3f} <= {sell_thresh:.2f}"})
        else:
            report["actions"].append({"symbol": sym, "side": "HOLD", "qty": 0, "px": px, "reason": f"score {score:.3f} hold zone"})
    return report

def load_config(account_id: int) -> dict:
    rows = db.fetchall("SELECT config_json FROM bot_settings WHERE account_id = ?", (account_id,))
    if rows:
        try:
            return json.loads(rows[0]["config_json"])
        except Exception:
            return {}
    return {}

def save_config(account_id: int, config: dict):
    import datetime as dt, json
    rows = db.fetchall("SELECT id FROM bot_settings WHERE account_id = ?", (account_id,))
    js = json.dumps(config)
    if rows:
        db.execute("UPDATE bot_settings SET config_json = ?, updated_at = ? WHERE account_id = ?", (js, dt.datetime.utcnow(), account_id))
    else:
        db.execute("INSERT INTO bot_settings (account_id, config_json, updated_at) VALUES (?, ?, ?)", (account_id, js, dt.datetime.utcnow()))
