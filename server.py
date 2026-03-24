"""
server.py — Flask API server.

Serves the dashboard and exposes REST endpoints for all bot state.
Bot runs in background threads started at app startup.
"""

import os
import json
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import state
import bot

app = Flask(__name__, static_folder='static')
CORS(app)

# ── STARTUP ───────────────────────────────────────────────

@app.before_request
def _startup():
    """Start bot threads once on first request."""
    pass

def _init():
    bot.start()

# ── SERVE DASHBOARD ───────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

# ── STATUS ────────────────────────────────────────────────

@app.route('/api/status')
def status():
    """Quick health check + connection info."""
    from executor import _sdk_available, _env
    return jsonify({
        'ok':           True,
        'env':          os.environ.get('KALSHI_ENV', 'demo'),
        'paper':        os.environ.get('PAPER', 'true').lower() == 'true',
        'sdk':          _sdk_available,
        'bot_enabled':  state.bot_enabled,
        'positions':    len(state.open_positions),
    })

# ── DEBUG ─────────────────────────────────────────────────

@app.route('/api/debug')
def debug():
    """
    Diagnose SDK init failures.
    Shows env var presence, key format, and attempts a live SDK init.
    Remove this route before going to production.
    """
    import traceback

    key_id  = os.environ.get('KALSHI_KEY_ID', '')
    key_pem = os.environ.get('KALSHI_PRIVATE_KEY', '')

    # Fix literal \n in env var
    if key_pem and '\\n' in key_pem:
        key_pem = key_pem.replace('\\n', '\n')

    result = {
        'env_vars': {
            'KALSHI_ENV':         os.environ.get('KALSHI_ENV', '(not set)'),
            'KALSHI_KEY_ID':      key_id[:12] + '…' if key_id else '(not set)',
            'KALSHI_PRIVATE_KEY': '(not set)' if not key_pem else (
                'starts with: ' + key_pem[:40].replace('\n', '\\n')
            ),
            'PAPER':              os.environ.get('PAPER', '(not set)'),
        },
        'key_checks': {
            'key_id_present':      bool(key_id),
            'key_pem_present':     bool(key_pem),
            'has_begin_header':    '-----BEGIN' in key_pem,
            'has_end_footer':      '-----END' in key_pem,
            'has_real_newlines':   '\n' in key_pem,
            'line_count':          key_pem.count('\n') if key_pem else 0,
        },
        'sdk_init_test': None,
        'sdk_error':     None,
    }

    # Attempt live SDK init with current credentials
    try:
        import kalshi_python_sync as k
        cfg      = k.Configuration()
        cfg.host = (
            'https://demo-api.kalshi.co/trade-api/v2'
            if os.environ.get('KALSHI_ENV','demo') == 'demo'
            else 'https://api.elections.kalshi.com/trade-api/v2'
        )
        cfg.api_key_id      = key_id
        cfg.private_key_pem = key_pem
        client = k.KalshiClient(cfg)

        # Try a real API call
        portfolio_api = k.PortfolioApi(client)
        bal = portfolio_api.get_balance()
        result['sdk_init_test'] = 'SUCCESS'
        result['balance_cents'] = bal.balance
        result['balance_dollars'] = round(bal.balance / 100, 2)

    except Exception as e:
        result['sdk_init_test'] = 'FAILED'
        result['sdk_error']     = str(e)
        result['sdk_traceback'] = traceback.format_exc()

    return jsonify(result)


@app.route('/api/debug/env')
def debug_env():
    """Show raw env vars relevant to bot mode — use to verify Railway config."""
    return jsonify({
        'PAPER':       os.environ.get('PAPER', '(not set)'),
        'KALSHI_ENV':  os.environ.get('KALSHI_ENV', '(not set)'),
        'paper_parsed': os.environ.get('PAPER', 'true').lower() == 'true',
        'mode':        'PAPER' if os.environ.get('PAPER','true').lower()=='true' else 'LIVE',
    })


@app.route('/api/debug/markets')
def debug_markets():
    """Show first 20 single-contract market titles (parlays excluded)."""
    from executor import _rest_get
    data = _rest_get('/trade-api/v2/markets?limit=20&status=open&mve_filter=exclude')
    markets = data.get('markets', [])
    return jsonify({
        'count': len(markets),
        'titles':  [m.get('title','')  for m in markets],
        'tickers': [m.get('ticker','') for m in markets],
    })

@app.route('/api/debug/history')
def debug_history():
    """
    Test candlestick fetch for a specific ticker.
    Usage: /api/debug/history?ticker=KXNHLFIRSTGOAL-26MAR19FLAEDM-FLAMTKACHUK19
    Shows exactly what get_market_history returns so we can diagnose 0-score signals.
    """
    import time as _time
    from executor import _rest_get, get_market_history

    ticker = request.args.get('ticker', '')
    if not ticker:
        # Get the first available market ticker automatically
        data    = _rest_get('/trade-api/v2/markets?limit=5&status=open&mve_filter=exclude')
        markets = data.get('markets', [])
        ticker  = markets[0].get('ticker', '') if markets else ''

    if not ticker:
        return jsonify({'error': 'no ticker available'})

    # Try both candlestick path formats
    now      = int(_time.time())
    start_ts = now - (30 * 24 * 3600)
    series   = ticker.split('-')[0] if '-' in ticker else ticker

    path1 = f'/trade-api/v2/series/{series}/markets/{ticker}/candlesticks?start_ts={start_ts}&end_ts={now}&period_interval=60'
    path2 = f'/trade-api/v2/markets/{ticker}/candlesticks?start_ts={start_ts}&end_ts={now}&period_interval=60'

    raw1 = _rest_get(path1)
    raw2 = _rest_get(path2)

    history = get_market_history(ticker)

    return jsonify({
        'ticker':          ticker,
        'series':          series,
        'path1_candles':   len(raw1.get('candlesticks', [])),
        'path2_candles':   len(raw2.get('candlesticks', [])),
        'path1_keys':      list(raw1.get('candlesticks', [{}])[0].keys()) if raw1.get('candlesticks') else [],
        'path2_keys':      list(raw2.get('candlesticks', [{}])[0].keys()) if raw2.get('candlesticks') else [],
        'first_raw_candle': raw1.get('candlesticks', [None])[0] if raw1.get('candlesticks') else raw2.get('candlesticks', [None])[0],
        'history_pts':     len(history),
        'history_sample':  history[:3] if history else [],
        'msg': 'history_pts=0 means candlestick fetch is failing or market too new',
    })


# ── FULL DASHBOARD DATA ───────────────────────────────────

@app.route('/api/dashboard')
def dashboard():
    """All data the dashboard needs in one call."""
    return jsonify(state.get_snapshot())

# ── BOT CONTROL ───────────────────────────────────────────

@app.route('/api/bot/enable', methods=['POST'])
def enable_bot():
    state.bot_enabled = True
    state.params['enabled'] = True
    bot.save_params()
    state.log('success', '[API] Bot ENABLED via dashboard')
    return jsonify({'ok': True, 'bot_enabled': True})

@app.route('/api/bot/disable', methods=['POST'])
def disable_bot():
    state.bot_enabled = False
    state.params['enabled'] = False
    bot.save_params()
    state.log('warn', '[API] Bot DISABLED via dashboard')
    return jsonify({'ok': True, 'bot_enabled': False})

@app.route('/api/bot/scan', methods=['POST'])
def force_scan():
    """Trigger an immediate signal scan (for testing)."""
    import threading
    t = threading.Thread(target=bot.run_signal_scan, daemon=True)
    t.start()
    return jsonify({'ok': True, 'msg': 'Scan triggered'})

# ── PARAMS ────────────────────────────────────────────────

@app.route('/api/params', methods=['GET'])
def get_params():
    return jsonify(state.params)

@app.route('/api/params', methods=['POST'])
def update_params():
    """
    Update strategy parameters. Only whitelisted keys are accepted.
    Nested keys use dot notation: 'weights.momentum' = 0.4
    """
    body = request.get_json()
    if not body:
        return jsonify({'error': 'No body'}), 400

    ALLOWED = {
        'max_positions', 'max_position_pct', 'daily_loss_limit',
        'min_score', 'take_profit_pct', 'stop_loss_pct',
        'scan_interval', 'monitor_interval', 'categories',
        'max_total_exposure_pct', 'max_total_exposure_dollars',
        'max_hours_to_close', 'min_win_price', 'min_hours_to_close',
    }

    updated = []
    for key, val in body.items():
        if key in ALLOWED:
            state.params[key] = val
            updated.append(key)
        elif key.startswith('weights.'):
            sub = key.split('.', 1)[1]
            if sub in state.params['weights']:
                state.params['weights'][sub] = val
                updated.append(key)
        elif key.startswith('mr.'):
            sub = key.split('.', 1)[1]
            if sub in state.params['mr']:
                state.params['mr'][sub] = val
                updated.append(key)
        elif key.startswith('mo.'):
            sub = key.split('.', 1)[1]
            if sub in state.params['mo']:
                state.params['mo'][sub] = val
                updated.append(key)

    if updated:
        bot.save_params()
        state.log('info', f'[API] Params updated: {updated}')

    return jsonify({'ok': True, 'updated': updated, 'params': state.params})

# ── POSITIONS ─────────────────────────────────────────────

@app.route('/api/positions', methods=['GET'])
def get_positions():
    return jsonify({
        'open':   state.open_positions,
        'closed': list(state.closed_positions),
    })

@app.route('/api/positions/<ticker>', methods=['DELETE'])
def close_position(ticker):
    """Manually close a position at market."""
    pos = next((p for p in state.open_positions if p['ticker'] == ticker), None)
    if not pos:
        return jsonify({'error': f'No open position for {ticker}'}), 404

    from executor import _get_current_price, _close_position
    paper = os.environ.get('PAPER', 'true').lower() == 'true'
    price = _get_current_price(ticker) or pos['current_price']
    _close_position(pos, 'manual', price, paper)
    return jsonify({'ok': True, 'msg': f'Closed {ticker} at {price}¢'})

# ── LOG ───────────────────────────────────────────────────

@app.route('/api/log')
def get_log():
    n = int(request.args.get('n', 100))
    return jsonify(list(state.activity_log)[:n])

# ── RUN ───────────────────────────────────────────────────

if __name__ == '__main__':
    _init()
    port = int(os.environ.get('PORT', 5000))
    state.log('info', f'[SERVER] Listening on port {port}')
    app.run(host='0.0.0.0', port=port, debug=False)


# For gunicorn: call _init() at module level
_init()
