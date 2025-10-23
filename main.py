import os, json, sys, time, logging, random, re
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# Config
# =========================
SHEET_NAME     = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET      = os.getenv("WORKSHEET", "log")
SCRAPER_WS     = os.getenv("SCRAPER_WS", "scraper")
HTTP_TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "15"))

# Source toggles
NASDAQ_ENABLED            = os.getenv("NASDAQ_ENABLED", "0") not in {"0", "false", "False"}
STOCKTWITS_ENABLED       = os.getenv("STOCKTWITS_ENABLED", "1") not in {"0", "false", "False"}
STOCKTWITS_SENTIMENT_EN  = os.getenv("STOCKTWITS_SENTIMENT_ENABLED", "1") not in {"0", "false", "False"}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

UA = {"User-Agent": "Mozilla/5.0 (compatible; AletheiaBot/1.0; +https://example.org/bot)"}
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# =========================
# Google Sheets helpers
# =========================
def get_client():
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if not creds_json:
        print("ERROR: GOOGLE_CREDS_JSON env var is missing.", file=sys.stderr)
        sys.exit(1)
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def open_sheet(gc):
    return gc.open(SHEET_NAME)

def ensure_worksheet(sh, title: str, rows: int = 1000, cols: int = 10):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def replace_sheet(ws, rows: List[List[str]]):
    ws.clear()
    ws.append_row(["source", "date_utc", "symbol"], value_input_option="RAW")
    if rows:
        CHUNK = 500
        for i in range(0, len(rows), CHUNK):
            ws.append_rows(rows[i:i+CHUNK], value_input_option="RAW")
    logging.info(f"Wrote {len(rows)} rows to '{ws.title}' (replaced with latest scrape).")

# =========================
# HTTP helpers (retry/backoff)
# =========================
def fetch_json_with_retries(url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
                            retries=3, backoff_base=0.6) -> Dict:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(backoff_base * (2 ** attempt) * (0.8 + 0.4 * random.random()))
    raise last_exc

def fetch_text_with_retries(url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
                            retries=3, backoff_base=0.6) -> str:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            time.sleep(backoff_base * (2 ** attempt) * (0.8 + 0.4 * random.random()))
    raise last_exc

# =========================
# Fetchers: Stocks (Yahoo)
# =========================
def _yahoo_predefined(scr_id: str, count: int = 100) -> List[str]:
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": scr_id, "count": str(count), "lang": "en-US", "region": "US"}
    data = fetch_json_with_retries(url, params=params, headers=UA)
    out: List[str] = []
    for res in data.get("finance", {}).get("result", []):
        for item in res.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                out.append(sym)
    return out

def get_yahoo_trending_stocks() -> List[str]:
    url = "https://query1.finance.yahoo.com/v1/finance/trending/US"
    data = fetch_json_with_retries(url, headers=UA)
    out: List[str] = []
    for result in data.get("finance", {}).get("result", []):
        for item in result.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                out.append(sym)
    return out

def get_yahoo_most_active() -> List[str]:
    return _yahoo_predefined("most_actives", 100)

# =========================
# Fetchers: Stocks (Nasdaq — optional)
# =========================
def get_nasdaq_most_active() -> List[str]:
    if not NASDAQ_ENABLED:
        return []
    url = "https://api.nasdaq.com/api/quote/list-type/mostactive"
    headers = {
        **UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/most-active",
    }
    params = {"assetclass": "stocks"}
    data = fetch_json_with_retries(url, params=params, headers=headers, timeout=25)
    items = (data.get("data") or {}).get("data") or []
    return [(row.get("symbol") or "").strip().upper() for row in items if row.get("symbol")]

# =========================
# Fetchers: Crypto (CoinGecko)
# =========================
def get_coingecko_trending() -> List[str]:
    url = "https://api.coingecko.com/api/v3/search/trending"
    data = fetch_json_with_retries(url, headers=UA)
    return [((it.get("item") or {}).get("symbol") or "").strip().upper() for it in data.get("coins", [])]

def get_coingecko_top_by_volume(limit: int = 50) -> List[str]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "volume_desc", "per_page": str(limit), "page": "1"}
    data = fetch_json_with_retries(url, params=params, headers=UA)
    return [(coin.get("symbol") or "").strip().upper() for coin in data]

# =========================
# Fetchers: Social (Stocktwits)
# =========================
def get_stocktwits_trending_api() -> List[str]:
    if not STOCKTWITS_ENABLED:
        return []
    url = "https://api.stocktwits.com/api/2/trending/symbols.json"
    headers = {
        **UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://stocktwits.com",
        "Referer": "https://stocktwits.com/",
    }
    data = fetch_json_with_retries(url, headers=headers)
    return [(item.get("symbol") or "").strip().upper() for item in (data.get("symbols") or [])]

# ---- Stocktwits Sentiment (HTML scrape, no auth) ----
_STW_SENTIMENT_MAP = {
    "Stocktwits - Sentiment Trending":     "https://stocktwits.com/sentiment",
    "Stocktwits - Sentiment Most Active":  "https://stocktwits.com/sentiment/most-active",
    "Stocktwits - Sentiment Watchers":     "https://stocktwits.com/sentiment/watchers",
    "Stocktwits - Sentiment Most Bullish": "https://stocktwits.com/sentiment/most-bullish",
    "Stocktwits - Sentiment Most Bearish": "https://stocktwits.com/sentiment/most-bearish",
    "Stocktwits - Sentiment Top Gainers":  "https://stocktwits.com/sentiment/top-gainers",
    "Stocktwits - Sentiment Top Losers":   "https://stocktwits.com/sentiment/top-losers",
}
_SYMBOL_RE = re.compile(r"/symbol/([A-Za-z0-9\.\-_]+)")

def _parse_symbols_from_html(html: str) -> List[str]:
    raw = {m.group(1).upper() for m in _SYMBOL_RE.finditer(html)}
    return [s for s in sorted(raw) if 1 <= len(s) <= 12 and not s.startswith("-") and not s.endswith("-")]

def get_stocktwits_sentiment_sets() -> List[Tuple[str, List[str]]]:
    if not STOCKTWITS_SENTIMENT_EN:
        return []
    sources = []
    headers = {
        **UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for name, url in _STW_SENTIMENT_MAP.items():
        try:
            html = fetch_text_with_retries(url, headers=headers, timeout=20)
            syms = _parse_symbols_from_html(html)
            logging.info(f"{name}: scraped {len(syms)} symbols.")
            sources.append((name, syms))
        except Exception as e:
            logging.info(f"{name}: skipped due to error: {e}")
    return sources

# =========================
# Orchestration
# =========================
def collect_sources() -> List[Tuple[str, List[str]]]:
    sources = []

    def try_add(name, fn):
        try:
            syms = fn()
            logging.info(f"{name}: fetched {len(syms)} symbols.")
            sources.append((name, syms))
        except Exception as e:
            logging.info(f"{name}: skipped due to error: {e}")

    # Stocks (Yahoo)
    try_add("Yahoo Finance - Trending (US)", get_yahoo_trending_stocks)
    try_add("Yahoo Finance - Most Active", get_yahoo_most_active)

    # Nasdaq (optional)
    try_add("Nasdaq - Most Active", get_nasdaq_most_active)

    # Crypto (CoinGecko)
    try_add("CoinGecko - Trending", get_coingecko_trending)
    try_add("CoinGecko - Top by Volume", get_coingecko_top_by_volume)

    # Stocktwits (API + sentiment)
    try_add("Stocktwits - Trending (API)", get_stocktwits_trending_api)
    for name, syms in get_stocktwits_sentiment_sets():
        sources.append((name, syms))

    return sources

def combine_sources_to_rows(sources: List[Tuple[str, List[str]]]) -> List[List[str]]:
    ts = datetime.now(timezone.utc).isoformat()
    symbol_to_sources: Dict[str, Set[str]] = {}
    for source, symbols in sources:
        for sym in symbols:
            s = sym.strip().upper()
            if not s:
                continue
            symbol_to_sources.setdefault(s, set()).add(source)
    rows = []
    for symbol in sorted(symbol_to_sources):
        combined_source = ", ".join(sorted(symbol_to_sources[symbol]))
        rows.append([combined_source, ts, symbol])
    return rows

# =========================
# Entry points
# =========================
def run_scraper(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, SCRAPER_WS, rows=10000, cols=6)
    rows = combine_sources_to_rows(collect_sources())
    replace_sheet(ws, rows)

def run_demo_write(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, WORKSHEET, rows=200, cols=10)
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row(["Deployed on Railway 🎉", ts], value_input_option="RAW")

def main():
    gc = get_client()
    run_scraper(gc)
    run_demo_write(gc)
    print("Scrape complete and demo row written.")

if __name__ == "__main__":
    main()
