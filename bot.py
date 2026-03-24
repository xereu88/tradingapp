import os
import json
import time
import threading
from datetime import datetime, date, timezone

import state
import risk
import executor
from strategy import evaluate
from signals import news_flow
from signals.classifier import classify, passes_quality_filter

PAPER = os.environ.get('PAPER', 'true').lower() == 'true'
# /tmp persists across Railway restarts within the same deployment
STRATEGY_FILE = '/tmp/kalshi_strategy.json'


# ── PARAM PERSISTENCE ─────────────────────────────────────

def load_params():
    try:
        with open(STRATEGY_FILE, 'r') as f:
            saved = json.load(f)
            state.params.update(saved)
            state.log('info', f'[BOT] Loaded params from {STRATEGY_FILE}')
    except FileNotFoundError:
        state.log('info', '[BOT] No strategy.json — using defaults')
    except Exception as e:
        state.log('warn', f'[BOT] Could not load strategy.json: {e}')


def save_params():
    try:
        with open(STRATEGY_FILE, 'w') as f:
            json.dump(state.params, f, indent=2)
    except Exception as e:
        state.log('warn', f'[BOT] Could not save params: {e}')


# ── DAILY RESET ───────────────────────────────────────────

def check_daily_reset():
    today = date.today()
    if today != state.daily_reset_date:
        state.reset_daily_pnl()


# ── SIGNAL SCAN ───────────────────────────────────────────

def run_signal_scan():
    """
    Adaptive scan loop.
    1. Refresh balance
    2. Fetch markets across categories (parlays excluded)
    3. Classify each market (LONG / SHORT / INTRADAY)
    4. Route to appropriate signal engine
    5. Risk check → place order
    """
    if not state.bot_enabled:
        return

    state.log('info', '[BOT] Signal scan starting…')
    state.last_scan_at = datetime.now(timezone.utc)
    categories = state.params.get('categories', ['ec'])

    # ── Balance ────────────────────────────────────────────
    balance = executor.get_balance()
    if balance > 0:
        with state._lock:
            state.current_balance = balance
            if state.starting_balance == 0:
                state.starting_balance = balance
                state.log('info', f'[BOT] Starting balance: ${balance/100:.2f}')

    # ── Sync live portfolio from Kalshi ────────────────────
    # Picks up manually-opened positions + removes settled ones
    executor.sync_portfolio()

    # ── Daily loss check ───────────────────────────────────
    if state.check_daily_loss_limit():
        state.log('error', '[BOT] Daily loss limit hit — suspending trades for today')
        return

    # ── News signal (category-wide, compute once) ──────────
    nf_score = news_flow.compute()

    # ── Markets ────────────────────────────────────────────
    markets = executor.get_markets(categories=categories, limit=500)
    state.log('info', f'[BOT] Evaluating {len(markets)} markets across {categories}')

    # Track type breakdown for logging
    type_counts = {'long': 0, 'short': 0, 'intraday': 0, 'skipped': 0}
    signals_fired = 0

    # Max hours to close — only trade live/imminent events
    MAX_HOURS = state.params.get('max_hours_to_close', 4.0)
    # Min price on best side — only trade near-certainties
    MIN_PRICE = state.params.get('min_win_price', 75)

    for mkt in markets:

        # ── STOP CHECK — respect disable button mid-scan ───
        if not state.bot_enabled:
            state.log('warn', '[BOT] Bot disabled mid-scan — stopping')
            break

        ticker    = mkt['ticker']
        yes_price = mkt['yes_price']
        no_price  = 100 - yes_price

        # Already in portfolio
        if any(p['ticker'] == ticker for p in state.open_positions):
            continue

        # Max positions reached
        if len(state.open_positions) >= state.params['max_positions']:
            state.log('info', '[BOT] Max positions reached, stopping scan')
            break

        # ── FILTER 1: Only currently happening events ──────
        close_ts = mkt.get('close_time_ts')
        if close_ts:
            now_ts = datetime.now(timezone.utc).timestamp()
            hours_to_close = (close_ts - now_ts) / 3600
            if hours_to_close > MAX_HOURS:
                type_counts['skipped'] += 1
                continue
            if hours_to_close < 0:
                type_counts['skipped'] += 1
                continue  # already closed
            # Log markets that pass the time filter so we can verify
            state.log('info', f'[BOT] {ticker}: closes in {hours_to_close:.1f}h ✓')
        else:
            # No close time returned by API — skip
            type_counts['skipped'] += 1
            continue

        # ── FILTER 2: 75%+ win probability on either side ──
        best_price = max(yes_price, no_price)
        if best_price < MIN_PRICE:
            type_counts['skipped'] += 1
            continue

        # ── Fetch history ──────────────────────────────────
        history = executor.get_market_history(ticker)

        # ── Classify market ────────────────────────────────
        mc = classify(ticker, history, close_ts)

        # Standard quality filter (min hours still applies)
        if not passes_quality_filter(mc, ticker):
            type_counts['skipped'] += 1
            continue

        type_counts[mc.market_type.value] += 1

        # ── Store signal state for dashboard ───────────────
        with state._lock:
            state.latest_signals[ticker] = {
                'title':       mkt['title'],
                'category':    mkt['category'],
                'price':       yes_price,
                'best_price':  best_price,
                'market_type': mc.market_type.value,
                'history_pts': mc.history_pts,
                'mr':  0.0, 'mo': 0.0, 'nf': round(nf_score, 3),
                'combined': 0.0,
                'ts':  datetime.utcnow().isoformat(),
            }

        # ── Evaluate via adaptive strategy ─────────────────
        # If best side is NO (no_price >= yes_price), flip yes_price
        effective_price = yes_price if yes_price >= no_price else no_price
        signal = evaluate(
            ticker    = ticker,
            title     = mkt['title'],
            category  = mkt['category'],
            yes_price = yes_price,
            history   = history,
            nf_score  = nf_score,
            balance   = balance,
            mc        = mc,
            yes_bid   = mkt.get('yes_bid', 0),
            yes_ask   = mkt.get('yes_ask', 0),
        )

        # Update signal state with computed scores
        if signal:
            with state._lock:
                if ticker in state.latest_signals:
                    d = signal.signal_detail
                    state.latest_signals[ticker].update({
                        'mr':       d.get('mr', 0.0),
                        'mo':       d.get('mo', d.get('smo', 0.0)),
                        'nf':       d.get('nf', round(nf_score,3)),
                        'combined': d.get('combined', signal.score),
                    })

        if signal is None:
            continue

        # ── Risk checks ────────────────────────────────────
        allowed, reason = risk.check_all(signal)
        if not allowed:
            continue

        # ── Place order ────────────────────────────────────
        success = executor.place_order(signal, paper=PAPER)
        if success:
            signals_fired += 1
            time.sleep(0.5)  # brief pause between orders

    state.log('info',
        f'[BOT] Scan complete — {signals_fired} orders placed '
        f'({"paper" if PAPER else "live"}) | '
        f'long={type_counts["long"]} short={type_counts["short"]} '
        f'intraday={type_counts["intraday"]} skipped={type_counts["skipped"]}'
    )


# ── POSITION MONITOR ──────────────────────────────────────

def run_position_monitor():
    """Check all open positions for TP/SL hits."""
    if not state.open_positions:
        return
    state.last_monitor_at = datetime.now(timezone.utc)
    executor.monitor_positions(paper=PAPER)


# ── SCHEDULER THREADS ─────────────────────────────────────

def _scan_loop():
    """Runs signal scan on interval."""
    # Initial delay so server is fully up before first scan
    time.sleep(10)
    while True:
        try:
            check_daily_reset()
            if state.bot_enabled:
                run_signal_scan()
        except Exception as e:
            state.log('error', f'[BOT] Scan loop error: {e}')
        time.sleep(state.params['scan_interval'])


def _monitor_loop():
    """Runs position monitor on interval."""
    time.sleep(15)
    while True:
        try:
            if state.bot_enabled:
                run_position_monitor()
        except Exception as e:
            state.log('error', f'[BOT] Monitor loop error: {e}')
        time.sleep(state.params['monitor_interval'])


def start():
    """Start both background threads. Called by server.py on startup."""
    load_params()
    state.bot_enabled = state.params.get('enabled', False)

    mode = 'PAPER' if PAPER else 'LIVE'
    state.log('info', f'[BOT] Starting — mode={mode} env={os.environ.get("KALSHI_ENV","demo")}')
    state.log('info', f'[BOT] Scan every {state.params["scan_interval"]//60}min, '
                      f'monitor every {state.params["monitor_interval"]}s')
    if state.bot_enabled:
        state.log('success', '[BOT] Auto-trader ENABLED')
    else:
        state.log('warn', '[BOT] Auto-trader DISABLED — enable in dashboard')

    scan_thread    = threading.Thread(target=_scan_loop,    daemon=True, name='scan')
    monitor_thread = threading.Thread(target=_monitor_loop, daemon=True, name='monitor')
    scan_thread.start()
    monitor_thread.start()
    state.log('info', '[BOT] Threads started')
