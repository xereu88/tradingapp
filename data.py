import pandas as pd
import yfinance as yf
from functools import lru_cache

@lru_cache(maxsize=256)
def history(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    t = yf.Ticker(symbol)
    df = t.history(period=period, interval=interval, auto_adjust=False)
    if df.empty:
        raise ValueError(f"No data for {symbol}.")
    df.reset_index(inplace=True)
    if "Date" in df.columns:
        df.rename(columns={"Date": "Datetime"}, inplace=True)
    return df

def last_price(symbol: str) -> float:
    df = history(symbol, period="5d", interval="1d")
    return float(df["Close"].iloc[-1])
