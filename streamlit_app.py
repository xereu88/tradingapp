# streamlit_app.py
from typing import Optional
import json

import pandas as pd
import plotly.express as px
import streamlit as st
import yfinance as yf

import db
import broker
from data import history, last_price
import options
import bot_engine

st.set_page_config(page_title="📈 Paper Trader+ (v2)", layout="wide")

# ---------- Helpers ----------
def rows_to_df(rows) -> pd.DataFrame:
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

def safe_last_price(symbol: str) -> Optional[float]:
    try:
        return float(last_price(symbol))
    except Exception:
        return None

def show_df(df: pd.DataFrame, empty_msg: str):
    if df is not None and not df.empty:
        st.dataframe(df)
    else:
        st.info(empty_msg)

@st.cache_resource
def _init():
    db.init_db()
    return True

_init()

st.title("📈 Paper Trader+ (v2)")
st.caption("Equities + Options • Bot Dashboard • Manual price refresh")

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Settings")
    if st.button("🧨 Reset database", key="sb_reset_db"):
        db.reset_db()
        st.success("Database reset.")

    starting_cash = st.number_input(
        "Starting cash (USD)", min_value=1_000.0, value=100_000.0, step=1_000.0, key="sb_start_cash"
    )
    acct_id = db.get_or_create_default_account(starting_cash=starting_cash)

    cash = db.get_cash_balance(acct_id)
    st.metric("Cash balance", f"${cash:,.2f}")

    if st.button("🔁 Refresh & Recalculate (orders)", key="sb_refresh_orders"):
        broker.try_fill_open_orders(acct_id)
        st.toast("Open equity orders re-evaluated.", icon="🔄")

    # Update prices button: clear yfinance cache and rerun
    if st.button("🔃 Update prices now", key="sb_update_prices"):
        try:
            from data import history as _hist
            _hist.cache_clear()
        except Exception:
            pass
        st.success("Price cache cleared; reloading…")
        st.rerun()

# ---------- Tabs ----------
tab_dash, tab_trade, tab_options, tab_orders, tab_positions, tab_opt_positions, tab_ledger, tab_bot = st.tabs(
    ["Dashboard", "Trade (Equities)", "Trade (Options)", "Orders", "Positions", "Option Positions", "Ledger", "Bot"]
)

# ----- Trade (Equities) -----
with tab_trade:
    st.subheader("Place Equity Order")

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        symbol = st.text_input("Symbol", value="AAPL", key="eq_symbol").upper().strip()
    with col2:
        side = st.selectbox("Side", ["BUY", "SELL"], key="eq_side")
    with col3:
        qty = st.number_input("Quantity", min_value=1.0, value=10.0, step=1.0, key="eq_qty")
    with col4:
        otype = st.selectbox("Order Type", ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"], key="eq_order_type")

    c1, c2, c3 = st.columns(3)
    limit_px: Optional[float] = None
    stop_px: Optional[float] = None
    with c1:
        if otype in ("LIMIT", "STOP_LIMIT"):
            limit_px = st.number_input("Limit Price", min_value=0.0, value=0.0, key="eq_limit_price")
    with c2:
        if otype in ("STOP", "STOP_LIMIT"):
            stop_px = st.number_input("Stop Price", min_value=0.0, value=0.0, key="eq_stop_price")
    with c3:
        if symbol:
            lp = safe_last_price(symbol)
            st.metric("Last Price", f"${lp:,.2f}" if lp is not None else "N/A")

    if st.button("Submit Equity Order", key="eq_submit"):
        # Validation for limit/stop entries
        if otype in ("LIMIT", "STOP_LIMIT") and (limit_px is None or limit_px <= 0):
            st.error("Please enter a positive Limit Price.")
        elif otype in ("STOP", "STOP_LIMIT") and (stop_px is None or stop_px <= 0):
            st.error("Please enter a positive Stop Price.")
        else:
            try:
                oid = broker.place_order(acct_id, symbol, side, qty, otype, limit_px, stop_px)
                st.success(f"Order #{oid} submitted.")
            except Exception as e:
                st.error(str(e))

    st.divider()
    st.subheader("Chart")
    period = st.selectbox(
        "Period", ["1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"], index=3, key="eq_period"
    )
    interval = st.selectbox("Interval", ["1d", "1wk", "1mo"], index=0, key="eq_interval")
    if symbol:
        try:
            df = history(symbol, period=period, interval=interval)
            fig = px.line(df, x="Datetime", y="Close", title=f"{symbol} Close")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Chart error: {e}")

# ----- Trade (Options) -----
with tab_options:
    st.subheader("Options: View Chain & Trade")
    o_sym = st.text_input("Underlying", value="AAPL", key="opt_underlying").upper().strip()

    if st.button("Load expirations", key="opt_load_exps"):
        try:
            exps = yf.Ticker(o_sym).options
            st.session_state["opt_expirations"] = exps
            st.success(f"Loaded {len(exps)} expirations.")
        except Exception as e:
            st.error(str(e))

    exps = st.session_state.get("opt_expirations", [])
    expiry = (
        st.selectbox("Expiry", exps, key="opt_expiry_select")
        if exps
        else st.text_input("Expiry (YYYY-MM-DD)", value="", key="opt_expiry_text")
    )

    if st.button("Show option chain", key="opt_show_chain"):
        if not o_sym or not expiry:
            st.warning("Select or type a valid expiry (YYYY-MM-DD) first.")
        else:
            try:
                calls, puts, _ = options.get_option_chain(o_sym, expiry)
                st.write("**Calls**")
                show_df(calls[["strike", "bid", "ask", "lastPrice", "impliedVolatility"]], "No calls.")
                st.write("**Puts**")
                show_df(puts[["strike", "bid", "ask", "lastPrice", "impliedVolatility"]], "No puts.")
                st.session_state["opt_chain_calls"] = calls
                st.session_state["opt_chain_puts"] = puts
            except Exception as e:
                st.error(str(e))

    st.markdown("---")
    st.subheader("Place Option Order")
    right = st.selectbox("Right", ["C", "P"], key="opt_right")
    strike = st.number_input("Strike", min_value=0.0, value=150.0, step=1.0, key="opt_strike")
    o_side = st.selectbox("Side", ["BUY", "SELL"], key="opt_side")
    o_qty = st.number_input("Contracts", min_value=1, value=1, step=1, key="opt_qty")
    otype2 = st.selectbox("Order Type", ["MARKET", "LIMIT"], key="opt_order_type")
    lim2 = (
        st.number_input("Limit Price (premium per contract)", min_value=0.0, value=0.0, key="opt_limit_price")
        if otype2 == "LIMIT"
        else None
    )

    if st.button("Submit Option Order", key="opt_submit"):
        if not o_sym or not expiry:
            st.error("Please select an expiry first (load expirations or type YYYY-MM-DD).")
        else:
            try:
                contract = options.OptionContract(symbol=o_sym, expiry=expiry, right=right, strike=float(strike))
                oid = options.place_option_order(acct_id, contract, o_side, int(o_qty), otype2, lim2)
                st.success(f"Option order #{oid} filled.")
            except Exception as e:
                st.error(str(e))

# ----- Orders / Trades -----
with tab_orders:
    st.subheader("Equity Orders")
    odf = rows_to_df(
        db.fetchall("SELECT * FROM orders WHERE account_id = ? ORDER BY created_at DESC", (acct_id,))
    )
    show_df(odf, "No equity orders yet.")

    st.subheader("Option Orders")
    oodf = rows_to_df(
        db.fetchall("SELECT * FROM option_orders WHERE account_id = ? ORDER BY created_at DESC", (acct_id,))
    )
    show_df(oodf, "No option orders yet.")

    st.subheader("Equity Trades")
    tdf = rows_to_df(
        db.fetchall(
            """
        SELECT t.* FROM trades t JOIN orders o ON o.id = t.order_id
        WHERE o.account_id = ? ORDER BY t.timestamp DESC
        """,
            (acct_id,),
        )
    )
    show_df(tdf, "No equity trades yet.")

    st.subheader("Option Trades")
    otdf = rows_to_df(
        db.fetchall(
            """
        SELECT * FROM option_trades WHERE order_id IN (
            SELECT id FROM option_orders WHERE account_id = ?
        ) ORDER BY timestamp DESC
        """,
            (acct_id,),
        )
    )
    show_df(otdf, "No option trades yet.")

# ----- Positions -----
with tab_positions:
    st.subheader("Equity Positions")
    pdf = rows_to_df(
        db.fetchall("SELECT * FROM positions WHERE account_id = ? ORDER BY symbol", (acct_id,))
    )
    if not pdf.empty:
        prices = {sym: (safe_last_price(sym) or float("nan")) for sym in pdf["symbol"].unique()}
        pdf["last_price"] = pdf["symbol"].map(prices)
        pdf["market_value"] = (pdf["qty"] * pdf["last_price"]).fillna(0.0)
        pdf["unrealized_pnl"] = ((pdf["last_price"] - pdf["avg_price"]) * pdf["qty"]).fillna(0.0)
        st.dataframe(pdf)
    else:
        st.info("No equity positions.")

with tab_opt_positions:
    st.subheader("Option Positions (with approx MV)")
    opdf = rows_to_df(
        db.fetchall(
            """
        SELECT symbol, expiry, right, strike, qty, avg_price
        FROM option_positions
        WHERE account_id = ?
        ORDER BY symbol, expiry, right, strike
        """,
            (acct_id,),
        )
    )
    if not opdf.empty:
        rows = []
        total_opt_mv = 0.0
        for _, r in opdf.iterrows():
            t = yf.Ticker(r["symbol"])
            try:
                ch = t.option_chain(r["expiry"])
                df = ch.calls if r["right"] == "C" else ch.puts
                m = df.loc[df["strike"] == r["strike"]]
                if m.empty:
                    continue
                bid = float(m["bid"].iloc[0] or 0.0)
                ask = float(m["ask"].iloc[0] or 0.0)
                last = float(m["lastPrice"].iloc[0] or 0.0)
                mid = (bid + ask) / 2 if (bid and ask) else (last or 0.0)
                mv = mid * r["qty"] * 100.0
                total_opt_mv += mv
                row = dict(r)
                row["mark"] = mid
                row["market_value"] = mv
                rows.append(row)
            except Exception:
                continue
        if rows:
            st.dataframe(pd.DataFrame(rows))
            st.metric("Option MV (approx)", f"${total_opt_mv:,.2f}")
        else:
            st.info("No quotes available for current option positions.")
    else:
        st.info("No option positions.")

# ----- Ledger -----
with tab_ledger:
    st.subheader("Cash Ledger")
    ldf = rows_to_df(
        db.fetchall("SELECT * FROM ledger WHERE account_id = ? ORDER BY timestamp DESC", (acct_id,))
    )
    show_df(ldf, "No ledger entries.")

# ----- Dashboard -----
with tab_dash:
    st.subheader("Portfolio Overview")
    pdf = rows_to_df(db.fetchall("SELECT * FROM positions WHERE account_id = ?", (acct_id,)))
    if not pdf.empty:
        prices = {sym: (safe_last_price(sym) or float("nan")) for sym in pdf["symbol"].unique()}
        pdf["last_price"] = pdf["symbol"].map(prices)
        pdf["market_value"] = (pdf["qty"] * pdf["last_price"]).fillna(0.0)
        total_equity = float(pdf["market_value"].sum() + cash)
        st.metric("Portfolio Value (equities only)", f"${total_equity:,.2f}")
        fig = px.bar(pdf, x="symbol", y="market_value", title="Market Value by Symbol")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.metric("Portfolio Value (equities only)", f"${cash:,.2f}")
        st.info("Place a trade to start building your portfolio.")

# ----- Bot Dashboard -----
with tab_bot:
    st.subheader("Trading Bot Configuration")

    config = bot_engine.load_config(acct_id)
    colA, colB = st.columns(2)
    with colA:
        watchlist = st.text_input(
            "Watchlist (comma-separated)",
            value=config.get("watchlist", "SPY,AAPL,MSFT,NVDA"),
            key="bot_watchlist",
        )
        buy_threshold = st.number_input(
            "Buy threshold (score)",
            min_value=-1.0,
            max_value=1.0,
            value=float(config.get("buy_threshold", 0.20)),
            step=0.01,
            key="bot_buy_thresh",
        )
        sell_threshold = st.number_input(
            "Sell threshold (score)",
            min_value=-1.0,
            max_value=1.0,
            value=float(config.get("sell_threshold", -0.20)),
            step=0.01,
            key="bot_sell_thresh",
        )
        min_notional = st.number_input(
            "Min notional per trade ($)",
            min_value=0.0,
            value=float(config.get("min_notional", 200.0)),
            step=50.0,
            key="bot_min_notional",
        )
    with colB:
        buy_cash_fraction = st.number_input(
            "Buy cash fraction",
            min_value=0.0,
            max_value=1.0,
            value=float(config.get("buy_cash_fraction", 0.10)),
            step=0.01,
            key="bot_buy_cash_frac",
        )
        sell_position_fraction = st.number_input(
            "Sell position fraction",
            min_value=0.0,
            max_value=1.0,
            value=float(config.get("sell_position_fraction", 0.10)),
            step=0.01,
            key="bot_sell_pos_frac",
        )
        freq_val = config.get("frequency", "30 min")
        freq_index = {"15 min": 0, "30 min": 1, "60 min": 2}.get(freq_val, 1)
        frequency = st.selectbox(
            "Frequency",
            ["15 min", "30 min", "60 min"],
            index=freq_index,
            key="bot_frequency",
        )

    colS1, colS2 = st.columns(2)
    if colS1.button("💾 Save settings", key="bot_save"):
        new_cfg = {
            "watchlist": watchlist,
            "buy_threshold": buy_threshold,
            "sell_threshold": sell_threshold,
            "buy_cash_fraction": buy_cash_fraction,
            "sell_position_fraction": sell_position_fraction,
            "min_notional": min_notional,
            "frequency": frequency,
        }
        bot_engine.save_config(acct_id, new_cfg)
        st.success("Settings saved.")

    if colS2.button("▶️ Run bot once now", key="bot_run_once"):
        cfg = bot_engine.load_config(acct_id)
        report = bot_engine.run_once(acct_id, cfg)
        st.json(report)

    st.caption(
        "Note: For full social/news sentiment and background scheduling, run the separate automated bot "
        "script you configured earlier. This built-in bot uses price momentum as a simple signal."
    )
