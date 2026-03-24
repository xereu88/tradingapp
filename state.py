"""
state.py — shared in-memory state for the Kalshi bot.

Single source of truth for positions, P&L, signals, and log.
All modules import from here. Flask serves it to the dashboard.
"""

import os
import copy
import threading
from datetime import datetime, date
from collections import deque

# ── THREAD SAFETY ─────────────────────────────────────────
_lock = threading.RLock()

# ── BOT CONTROL ───────────────────────────────────────────
bot_enabled     = False
order_lock      = False
last_scan_at    = None
last_monitor_at = None

# ── BANKROLL & P&L ────────────────────────────────────────
starting_balance   = 0.0   # cents
current_balance    = 0.0   # cents
daily_realized_pnl = 0.0   # cents
daily_reset_date   = date.today()

# ── POSITIONS ─────────────────────────────────────────────
open_positions   = []          # list of position dicts
closed_positions = deque(maxlen=100)

# ── SIGNAL STATE ──────────────────────────────────────────
latest_signals = {}   # { ticker: { mr, mo, nf, combined, ts, ... } }

# ── ACTIVITY LOG ──────────────────────────────────────────
activity_log = deque(maxlen=500)

# ── STRATEGY PARAMS ───────────────────────────────────────
params = {
    'enabled':               False,
    'categories':            ['ec', 'ai', 'po', 'sp'],
    'max_positions':         8,
    'max_position_pct':      0.08,
    'max_total_exposure_pct':0.50,
    'daily_loss_limit':      0.05,
    'min_score':             0.60,
    'min_hours_to_close':    0.1,    # don't trade markets closing in < 6 min
    'max_hours_to_close':    1.0,    # only trade events closing within 1 hour
    'min_win_price':         75,     # only trade when best side is 75c+
    'take_profit_pct':       0.40,
    'stop_loss_pct':         0.25,
    'scan_interval':         900,
    'monitor_interval':      120,
    'weights': {
        'mean_reversion': 0.40,
        'momentum':       0.35,
        'news_flow':      0.25,
    },
    'mr': {
        'lookback_days': 30,
        'z_threshold':   1.5,
    },
    'mo': {
        'window_24h':    True,
        'window_6h':     True,
        'min_move_pct':  0.08,
    },
    'nf': {
        'keywords': [
            'federal reserve', 'fed rate', 'interest rate', 'fomc',
            'cpi', 'inflation', 'consumer price', 'pce', 'gdp',
            'unemployment', 'jobs report', 'payrolls', 'recession',
            'rate cut', 'rate hike', 'basis points', 'pivot',
        ],
        'max_age_hours':          24,
        'recency_halflife_hours':  6,
    },
}

# ── HELPERS ───────────────────────────────────────────────

def log(level: str, msg: str):
    """level: 'info' | 'success' | 'warn' | 'error' | 'trade'"""
    with _lock:
        entry = {
            'ts':    datetime.utcnow().strftime('%H:%M:%S'),
            'level': level,
            'msg':   msg,
        }
        activity_log.appendleft(entry)
    prefix = {'info':'ℹ','success':'✅','warn':'⚠','error':'❌','trade':'📝'}.get(level,'·')
    print(f"[{entry['ts']}] {prefix} {msg}")


def get_snapshot():
    """
    Thread-safe deep-copy snapshot of all state for the dashboard API.
    Deep-copies mutable objects so the dashboard never reads a
    partially-written dict mid-scan (fixes flickering/blank issue).
    """
    with _lock:
        return {
            'bot_enabled':      bot_enabled,
            'paper':            os.environ.get('PAPER', 'true').lower() == 'true',
            'env':              os.environ.get('KALSHI_ENV', 'demo'),
            'last_scan_at':     last_scan_at.isoformat() if last_scan_at else None,
            'last_monitor_at':  last_monitor_at.isoformat() if last_monitor_at else None,
            'current_balance':  round(current_balance / 100, 2),
            'starting_balance': round(starting_balance / 100, 2),
            'daily_pnl':        round(daily_realized_pnl / 100, 2),
            'open_positions':   copy.deepcopy(open_positions),
            'closed_positions': list(closed_positions),
            'latest_signals':   copy.deepcopy(latest_signals),
            'activity_log':     list(activity_log)[:100],
            'params':           copy.deepcopy(params),
        }


def reset_daily_pnl():
    global daily_realized_pnl, daily_reset_date
    with _lock:
        daily_realized_pnl = 0.0
        daily_reset_date   = date.today()
    log('info', f'Daily P&L reset for {daily_reset_date}')


def check_daily_loss_limit() -> bool:
    with _lock:
        if starting_balance <= 0:
            return False
        limit = starting_balance * params['daily_loss_limit']
        return daily_realized_pnl < -limit
