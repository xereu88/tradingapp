"""
executor.py — Order placement and position monitoring.

Uses kalshi-python-sync v3+ (new official SDK).
  - kalshi-python is deprecated, replaced by kalshi-python-sync
  - OrdersApi is now separate from PortfolioApi
  - MarketApi (singular), not MarketsApi
  - Fixed-point migration: balance in cents, prices in cents (int fields still valid)
  - Sign path WITHOUT query parameters per latest docs
  - Rate limits: Basic tier = 20 reads/sec, 10 writes/sec
"""

import os
import time
import threading
from datetime import datetime

import state
from strategy import TradeSignal

# ── KALSHI CLIENT SETUP ───────────────────────────────────
_env        = os.environ.get('KALSHI_ENV', 'demo')
_key_id     = os.environ.get('KALSHI_KEY_ID', '').strip()
_key_pem    = os.environ.get('KALSHI_PRIVATE_KEY', '').strip()

# Support literal \n in Railway env var
if _key_pem and '\\n' in _key_pem:
    _key_pem = _key_pem.replace('\\n', '\n')

_sdk_available = False
_client        = None
_market_api    = None   # MarketApi (singular) in v3+
_portfolio_api = None   # get_balance, get_positions, get_fills
_orders_api    = None   # create_order, cancel_order (separate from portfolio in v3+)

try:
    import kalshi_python_sync as k

    cfg      = k.Configuration()
    cfg.host = (
        'https://demo-api.kalshi.co/trade-api/v2'
        if _env == 'demo'
        else 'https://api.elections.kalshi.com/trade-api/v2'
    )

    if _key_id and _key_pem:
        cfg.api_key_id      = _key_id
        cfg.private_key_pem = _key_pem
        _client        = k.KalshiClient(cfg)
        _market_api    = k.MarketApi(_client)
        _portfolio_api = k.PortfolioApi(_client)
        _orders_api    = k.OrdersApi(_client)
        _sdk_available = True
        state.log('success', f'[EXEC] Kalshi SDK ready ({_env}) key={_key_id[:8]}…')
    else:
        state.log('warn', '[EXEC] KALSHI_KEY_ID or KALSHI_PRIVATE_KEY not set — paper mode only')

except ImportError:
    state.log('warn', '[EXEC] kalshi-python-sync not installed — paper mode only')
except Exception as e:
    state.log('error', f'[EXEC] SDK init error: {e}')


# ── DIRECT REST HELPER ────────────────────────────────────
# All market data calls use direct REST — SDK Pydantic models
# break when Kalshi adds fields (fixed-point migration Mar 2026).
# Orders still use SDK since CreateOrderRequest is stable.

import base64
import requests as _requests
from cryptography.hazmat.primitives import hashes as _hashes, serialization as _ser
from cryptography.hazmat.primitives.asymmetric import padding as _pad
from cryptography.hazmat.backends import default_backend as _backend

_BASE = (
    'https://demo-api.kalshi.co'
    if _env == 'demo'
    else 'https://api.elections.kalshi.com'
)

def _signed_headers(method: str, path: str) -> dict:
    """RSA-PSS SHA256 auth headers. Sign path WITHOUT query params.
    Handles both PKCS#1 (-----BEGIN RSA PRIVATE KEY-----) 
    and PKCS#8 (-----BEGIN PRIVATE KEY-----) key formats.
    """
    if not _key_id or not _key_pem:
        return {}
    try:
        ts         = str(int(time.time() * 1000))
        clean_path = path.split('?')[0]
        msg        = (ts + method.upper() + clean_path).encode('utf-8')
        pem_bytes  = _key_pem.encode() if isinstance(_key_pem, str) else _key_pem

        # Handle PKCS#1 format (-----BEGIN RSA PRIVATE KEY-----)
        if b'RSA PRIVATE KEY' in pem_bytes:
            pk = _ser.load_pem_private_key(pem_bytes, password=None, backend=_backend())
        # Handle PKCS#8 format (-----BEGIN PRIVATE KEY-----)
        else:
            pk = _ser.load_pem_private_key(pem_bytes, password=None, backend=_backend())

        sig = pk.sign(
            msg,
            _pad.PSS(mgf=_pad.MGF1(_hashes.SHA256()), salt_length=_pad.PSS.DIGEST_LENGTH),
            _hashes.SHA256()
        )
        return {
            'KALSHI-ACCESS-KEY':       _key_id,
            'KALSHI-ACCESS-TIMESTAMP': ts,
            'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
            'Content-Type':            'application/json',
        }
    except Exception as e:
        state.log('error', f'[EXEC] signing error: {e}')
        return {}

def _rest_get(path: str) -> dict:
    """Direct signed GET. Returns parsed JSON or {} on error."""
    hdrs = _signed_headers('GET', path)
    if not hdrs:
        return {}
    try:
        r = _requests.get(_BASE + path, headers=hdrs, timeout=10)
        if r.status_code == 404:
            return {}   # silent 404 — endpoint not found, try fallback
        if r.status_code == 401:
            state.log('error', f'[EXEC] 401 on {path} — check KALSHI_KEY_ID / KALSHI_PRIVATE_KEY')
            return {}
        r.raise_for_status()
        return r.json()
    except Exception as e:
        state.log('error', f'[EXEC] REST GET {path.split("?")[0]} → {e}')
        return {}

def _rest_post(path: str, body: dict) -> dict:
    """Direct signed POST. Returns parsed JSON or {} on error."""
    hdrs = _signed_headers('POST', path)
    if not hdrs:
        return {}
    try:
        r = _requests.post(_BASE + path, headers=hdrs, json=body, timeout=10)
        if not r.ok:
            # Log actual Kalshi error body so we can debug
            try:
                err = r.json()
            except Exception:
                err = r.text
            state.log('error', f'[EXEC] POST {path} {r.status_code}: {err}')
            return {}
        return r.json()
    except Exception as e:
        state.log('error', f'[EXEC] REST POST {path} error: {e}')
        return {}




def get_balance() -> int:
    """Returns balance in cents (int). Uses direct REST."""
    data = _rest_get('/trade-api/v2/portfolio/balance')
    if not data:
        data = _rest_get('/portfolio/balance')
    bal = data.get('balance', 0)
    try:
        return int(float(bal))
    except (TypeError, ValueError):
        return 0


def sync_portfolio():
    """
    Sync live Kalshi positions into state.open_positions.
    Adds manually-opened positions, updates prices, removes settled ones.
    """
    if not _key_id:
        return
    data      = _rest_get('/trade-api/v2/portfolio/positions')
    positions = data.get('market_positions', [])
    live = {}
    for p in positions:
        ticker = p.get('ticker', '')
        try:
            qty = int(float(p.get('position') or 0))
        except (TypeError, ValueError):
            qty = 0
        if qty != 0:
            live[ticker] = {
                'qty':  abs(qty),
                'side': 'YES' if qty > 0 else 'NO',
                'cost': int(float(p.get('market_exposure_dollars') or p.get('market_exposure') or 0) * 100),
            }
    with state._lock:
        known = {p['ticker'] for p in state.open_positions}
        for ticker, lp in live.items():
            if ticker not in known:
                mkt = _rest_get(f'/trade-api/v2/markets/{ticker}').get('market', {})
                title = mkt.get('title', ticker)
                def _p(v):
                    try: return max(1, min(99, round(float(v)*100))) if v else 0
                    except: return 0
                cur = _p(mkt.get('yes_ask_dollars') or mkt.get('yes_bid_dollars') or mkt.get('last_price_dollars')) or _p(mkt.get('yes_ask') or mkt.get('yes_bid') or mkt.get('last_price')) or 50
                entry = round(lp['cost'] / lp['qty']) if lp['qty'] > 0 else cur
                unreal = round((cur - entry)*lp['qty']/100 if lp['side']=='YES' else (entry - cur)*lp['qty']/100, 2)
                state.open_positions.append({
                    'ticker': ticker, 'title': title, 'category': _detect_category(ticker),
                    'side': lp['side'], 'qty': lp['qty'], 'entry_price': entry,
                    'current_price': cur, 'take_profit': min(99, round(entry*1.40)),
                    'stop_loss': max(1, round(entry*0.75)), 'strategy': 'external',
                    'signal_score': 0.0, 'signal_detail': {}, 'opened_at': datetime.utcnow().isoformat(),
                    'unrealized_pnl': unreal, 'source': 'manual',
                })
                state.log('info', f'[SYNC] Added manual position: {lp["side"]} {ticker}')
        for pos in state.open_positions:
            if pos['ticker'] in live:
                mkt = _rest_get(f'/trade-api/v2/markets/{pos["ticker"]}').get('market', {})
                def _p2(v):
                    try: return max(1, min(99, round(float(v)*100))) if v else 0
                    except: return 0
                cur = _p2(mkt.get('yes_ask_dollars') or mkt.get('yes_bid_dollars') or mkt.get('last_price_dollars')) or _p2(mkt.get('yes_ask') or mkt.get('yes_bid') or mkt.get('last_price')) or pos['current_price']
                pos['current_price'] = cur
                pos['unrealized_pnl'] = round((cur-pos['entry_price'])*pos['qty']/100 if pos['side']=='YES' else (pos['entry_price']-cur)*pos['qty']/100, 2)
        before = len(state.open_positions)
        state.open_positions = [p for p in state.open_positions if p['ticker'] in live]
        if before - len(state.open_positions) > 0:
            state.log('info', f'[SYNC] Removed {before - len(state.open_positions)} settled position(s)')
    if live:
        state.log('info', f'[SYNC] {len(live)} live Kalshi position(s)')


def _detect_category(ticker: str) -> str:
    t = ticker.upper()
    for s in ['KXFOMC','KXCPI','KXBTC','KXETH','KXSP500','KXGDP','KXNFP','KXPCE','KXOIL','KXGOLD']:
        if t.startswith(s): return 'ec'
    for s in ['KXNBA','KXNFL','KXNHL','KXMLB','KXNCAA','KXVALORANT','KXDARTS']:
        if t.startswith(s): return 'sp'
    for s in ['KXSENATE','KXHOUSE','KXPRES','KXELECTION','KXGOV']:
        if t.startswith(s): return 'po'
    for s in ['KXAI','KXGPT','KXCLAUDE','KXOPENAI']:
        if t.startswith(s): return 'ai'
    return 'ec'


def _dollars_to_cents(val) -> int:
    """Convert Kalshi _dollars string or int/float to cents. '0.5600' → 56."""
    if not val:
        return 0
    try:
        return max(1, min(99, round(float(val) * 100)))
    except (TypeError, ValueError):
        return 0

# Known Kalshi series ticker prefixes by category.
# More reliable than keyword matching — uses actual Kalshi series naming.
_CAT_SERIES = {
    'ec': ['KXFOMC','KXCPI','KXPCE','KXGDP','KXJOBLESSCLAIMS','KXUNEMPLOYMENT',
           'KXNFP','KXBTC','KXETH','KXSP500','KXNASDAQ','KXDOW','KXHOUSING',
           'KXDEBT','KXOIL','KXGOLD','KXINFLATION','KXFED','KXRATE','KXHOME',
           'KXUSHOMEVAL','KXHOUHOMEVAL','KXDENHOMEVAL','KXSEAHOMEVAL',
           'KXBOSHOMEVAL','KXSDHOMEVAL','KXGDPNOM'],
    'ai': ['KXAI','KXCLAUDE','KXGPT','KXOPENAI','KXGOOGLE','KXANTHROPIC'],
    'po': ['KXSENATE','KXHOUSE','KXPRESIDENCY','KXGOV','KXAPPROVAL','KXPRES',
           'KXCONGRESS','KXELECTION'],
    'sp': ['KXNBA','KXNFL','KXNHL','KXMLB','KXNCAA','KXSOCCER','KXTENNIS'],
}

# Keyword fallback for markets whose tickers don't match series prefixes
_CAT_KEYWORDS = {
    'ec': ['fed','rate','cpi','inflation','gdp','unemployment','jobs','payroll',
           'recession','fomc','bitcoin','btc','crypto','s&p','nasdaq','dow',
           'treasury','interest','pce','yield','bond','housing','mortgage',
           'oil','gold','debt','deficit','spending','retail','tariff'],
    'ai': ['ai','artificial intelligence','gpt','claude','openai','anthropic',
           'gemini','llm','language model','benchmark','deepmind'],
    'po': ['election','senate','house','congress','president','vote','poll',
           'democrat','republican','legislation','bill','approval','governor'],
    'sp': ['nba','nfl','mlb','nhl','playoff','championship','super bowl',
           'world series','finals','bracket','ncaa tournament'],
}


def get_markets(categories: list[str], limit: int = 500) -> list[dict]:
    """
    Fetch active single-contract markets (mve_filter=exclude removes parlays).
    Paginates up to `limit` markets, then filters by series prefix + keywords.
    Prices use new _dollars string format from fixed-point migration (Mar 2026).
    """
    if not _key_id:
        return []

    all_markets = []
    cursor = ''

    while len(all_markets) < limit:
        page_size = min(200, limit - len(all_markets))
        path = f'/trade-api/v2/markets?limit={page_size}&status=open&mve_filter=exclude'
        if cursor:
            path += f'&cursor={cursor}'

        data   = _rest_get(path)
        page   = data.get('markets', [])
        cursor = data.get('cursor', '')

        if not page:
            break
        all_markets.extend(page)
        if not cursor:
            break

    if not all_markets:
        state.log('warn', '[EXEC] get_markets: no markets returned')
        return []

    # Build filter sets
    series_prefixes = set()
    keywords        = set()
    for cat in categories:
        series_prefixes.update(_CAT_SERIES.get(cat, []))
        keywords.update(_CAT_KEYWORDS.get(cat, []))

    matched = []
    for m in all_markets:
        ticker = m.get('ticker', '')
        title  = (m.get('title') or '').lower()

        series_hit  = any(ticker.startswith(s) for s in series_prefixes)
        keyword_hit = any(kw in title for kw in keywords)

        if not series_hit and not keyword_hit:
            continue

        # Detect category (series prefix takes priority over keyword)
        detected = categories[0] if categories else 'ec'
        for cat in categories:
            if any(ticker.startswith(s) for s in _CAT_SERIES.get(cat, [])):
                detected = cat
                break
        else:
            for cat in categories:
                if any(kw in title for kw in _CAT_KEYWORDS.get(cat, [])):
                    detected = cat
                    break

        # Price: prefer new _dollars fields, fall back to legacy int fields
        yes_bid   = _dollars_to_cents(m.get('yes_bid_dollars') or m.get('yes_bid')) or 0
        yes_ask   = _dollars_to_cents(m.get('yes_ask_dollars') or m.get('yes_ask')) or 0
        yes_price = yes_ask or yes_bid or _dollars_to_cents(m.get('last_price_dollars') or m.get('last_price')) or 50

        # Volume: prefer new _fp field
        try:
            volume = int(float(m.get('volume_fp') or m.get('volume') or 0))
        except (TypeError, ValueError):
            volume = 0

        # Close time for time-to-close calculation
        close_ts = None
        for tf in ['close_time', 'expiration_time', 'latest_expiration_time']:
            raw = m.get(tf)
            if raw:
                try:
                    from datetime import datetime, timezone
                    close_ts = int(datetime.fromisoformat(
                        str(raw).replace('Z', '+00:00')
                    ).timestamp())
                    break
                except Exception:
                    pass

        matched.append({
            'ticker':       ticker,
            'title':        m.get('title', ''),
            'yes_price':    yes_price,
            'yes_bid':      yes_bid,
            'yes_ask':      yes_ask,
            'volume':       volume,
            'category':     detected,
            'close_time_ts': close_ts,
        })

    state.log('info', f'[EXEC] {len(matched)} markets matched from {len(all_markets)} (parlays excluded)')
    return matched



def get_market_history(ticker: str) -> list[dict]:
    """
    Fetch hourly candlestick history via direct REST.
    Returns list of { ts, yes_price, volume } oldest → newest.

    Candlestick response format (post fixed-point migration):
      yes_bid/yes_ask/price are NESTED OBJECTS:
      { "close_dollars": "0.5600", "open_dollars": ..., ... }
      NOT flat fields. Must read .close_dollars from the nested dict.
    """
    if not _key_id:
        return []

    now      = int(time.time())
    start_ts = now - (30 * 24 * 3600)

    # Series ticker = first segment of market ticker
    # e.g. KXBTC-25DEC-T100000 → KXBTC
    # e.g. KXNHLFIRSTGOAL-26MAR19FLAEDM-FLAMTKACHUK19 → KXNHLFIRSTGOAL
    series = ticker.split('-')[0] if '-' in ticker else ticker

    path = (
        f'/trade-api/v2/series/{series}/markets/{ticker}/candlesticks'
        f'?start_ts={start_ts}&end_ts={now}&period_interval=60'
    )
    data    = _rest_get(path)
    candles = data.get('candlesticks', [])

    def _extract_price(field) -> int:
        """
        Extract cents from a candlestick price field.
        Field is either:
          - a nested dict: { "close_dollars": "0.5600", ... }  ← new format
          - a plain number (int/float)                          ← old format
          - None
        """
        if field is None:
            return 0
        if isinstance(field, dict):
            # New format: use close price as the representative value
            val = (field.get('close_dollars')
                   or field.get('mean_dollars')
                   or field.get('open_dollars'))
            if val is None:
                return 0
            try:
                return max(1, min(99, round(float(val) * 100)))
            except (TypeError, ValueError):
                return 0
        # Old flat format
        try:
            return max(1, min(99, round(float(field) * 100)))
        except (TypeError, ValueError):
            return 0

    history = []
    for c in candles:
        # Try yes_ask first (best signal of where market is trading),
        # fall back to yes_bid, then price (last trade)
        yes_price = (
            _extract_price(c.get('yes_ask'))
            or _extract_price(c.get('yes_bid'))
            or _extract_price(c.get('price'))
        )
        if yes_price == 0:
            continue  # skip candles with no price data

        try:
            volume = int(float(c.get('volume_fp') or c.get('volume') or 0))
        except (TypeError, ValueError):
            volume = 0

        history.append({
            'ts':        str(c.get('end_period_ts', '')),
            'yes_price': yes_price,
            'volume':    volume,
        })

    history.sort(key=lambda x: x['ts'])

    if history:
        state.log('info', f'[EXEC] {ticker}: {len(history)} candles fetched')
    return history


def get_current_price(ticker: str):
    """Fetch current yes_ask price via direct REST. Returns int cents or None."""
    if not _key_id:
        return None
    data = _rest_get(f'/trade-api/v2/markets/{ticker}')
    m    = data.get('market', {})
    if not m:
        return None
    def _price(val):
        try: return max(1, min(99, int(float(val)))) if val else None
        except: return None
    return _price(m.get('yes_ask') or m.get('yes_bid') or m.get('last_price'))


# ── ORDER PLACEMENT ───────────────────────────────────────

def place_order(signal: TradeSignal, paper: bool = True) -> bool:
    """
    Place a limit order for the signal.
    paper=True: log only, no real order sent.
    """
    state.order_lock = True
    try:
        return _place_order_inner(signal, paper)
    finally:
        state.order_lock = False


def _place_order_inner(signal: TradeSignal, paper: bool) -> bool:
    cost = signal.entry_price * signal.qty / 100

    if paper or not _sdk_available:
        state.log('trade',
            f'[PAPER] {signal.side} {signal.qty}×{signal.ticker} '
            f'@ {signal.entry_price}¢  TP={signal.take_profit}¢  '
            f'SL={signal.stop_loss}¢  ${cost:.2f}  [{signal.strategy_tag}]'
        )
        _add_position(signal)
        return True

    import uuid
    client_id = str(uuid.uuid4())

    # Market order — fills immediately at best available price
    # No price field needed for market orders
    body = {
        'ticker':           signal.ticker,
        'side':             signal.side.lower(),
        'action':           'buy',
        'type':             'market',
        'count':            signal.qty,
        'client_order_id':  client_id,
    }

    for attempt in range(1, 4):
        try:
            data = _rest_post('/trade-api/v2/portfolio/orders', body)
            if not data:
                raise ValueError('Empty response from order endpoint')
            order_id = (data.get('order') or {}).get('order_id', '?')
            state.log('trade',
                f'[ORDER] {signal.side} {signal.qty}×{signal.ticker} '
                f'@ {signal.entry_price}¢  id={order_id}  ${cost:.2f}  '
                f'[{signal.strategy_tag}]'
            )
            _add_position(signal)
            with state._lock:
                state.current_balance -= signal.entry_price * signal.qty
            return True

        except Exception as e:
            state.log('warn', f'[EXEC] Order attempt {attempt}/3 failed: {e}')
            if attempt < 3:
                time.sleep(2 ** attempt)

    state.log('error', f'[EXEC] Order failed after 3 attempts: {signal.ticker}')
    return False


def _add_position(signal: TradeSignal):
    with state._lock:
        state.open_positions.append({
            'ticker':         signal.ticker,
            'title':          signal.title,
            'category':       signal.category,
            'side':           signal.side,
            'qty':            signal.qty,
            'entry_price':    signal.entry_price,
            'current_price':  signal.entry_price,
            'take_profit':    signal.take_profit,
            'stop_loss':      signal.stop_loss,
            'strategy':       signal.strategy_tag,
            'signal_score':   round(signal.score, 3),
            'signal_detail':  signal.signal_detail,
            'opened_at':      datetime.utcnow().isoformat(),
            'unrealized_pnl': 0.0,
        })


# ── POSITION MONITOR ─────────────────────────────────────

def monitor_positions(paper: bool = True):
    """Check open positions against live prices. Close on TP/SL."""
    if not state.open_positions:
        return

    to_close = []

    for pos in list(state.open_positions):
        current = get_current_price(pos['ticker'])
        if current is None:
            continue

        # Update price + P&L in state
        with state._lock:
            for p in state.open_positions:
                if p['ticker'] == pos['ticker']:
                    p['current_price']  = current
                    p['unrealized_pnl'] = (
                        (current - p['entry_price']) * p['qty'] / 100
                        if p['side'] == 'YES'
                        else (p['entry_price'] - current) * p['qty'] / 100
                    )

        # TP / SL check
        side   = pos['side']
        hit_tp = current >= pos['take_profit'] if side == 'YES' else current <= pos['take_profit']
        hit_sl = current <= pos['stop_loss']   if side == 'YES' else current >= pos['stop_loss']

        if hit_tp:
            state.log('success',
                f'[MONITOR] TP hit: {side} {pos["ticker"]} '
                f'entry={pos["entry_price"]}¢ now={current}¢'
            )
            to_close.append((pos, 'take_profit', current))
        elif hit_sl:
            state.log('warn',
                f'[MONITOR] SL hit: {side} {pos["ticker"]} '
                f'entry={pos["entry_price"]}¢ now={current}¢'
            )
            to_close.append((pos, 'stop_loss', current))

    for pos, reason, exit_price in to_close:
        _close_position(pos, reason, exit_price, paper)


def _close_position(pos: dict, reason: str, exit_price: int, paper: bool):
    """Close a position and record realized P&L."""
    side        = pos['side']
    qty         = pos['qty']
    pnl_cents   = (exit_price - pos['entry_price']) * qty if side == 'YES' \
                  else (pos['entry_price'] - exit_price) * qty
    pnl_dollars = pnl_cents / 100

    if not paper and _key_id:
        try:
            import uuid
            close_body = {
                'ticker':          pos['ticker'],
                'side':            side.lower(),
                'action':          'sell',
                'type':            'market',
                'count':           qty,
                'client_order_id': str(uuid.uuid4()),
            }
            _rest_post('/trade-api/v2/portfolio/orders', close_body)
        except Exception as e:
            state.log('error', f'[EXEC] Close order failed: {e}')

    with state._lock:
        state.open_positions = [
            p for p in state.open_positions
            if p['ticker'] != pos['ticker']
        ]
        closed = dict(pos)
        closed['exit_price']   = exit_price
        closed['closed_via']   = reason
        closed['closed_at']    = datetime.utcnow().isoformat()
        closed['realized_pnl'] = round(pnl_dollars, 2)
        state.closed_positions.appendleft(closed)
        state.daily_realized_pnl += pnl_cents
        state.current_balance    += exit_price * qty

    state.log(
        'success' if pnl_dollars >= 0 else 'warn',
        f'[CLOSED] {pnl_dollars:+.2f} | {side} {pos["ticker"]} | {reason}'
    )
