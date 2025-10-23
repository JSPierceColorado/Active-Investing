import os, json, sys, time, logging, random
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Iterable, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# Config
# =========================
SHEET_NAME  = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET   = os.getenv("WORKSHEET", "log")           # original demo write
SCRAPER_WS  = os.getenv("SCRAPER_WS", "scraper")      # new scraping tab
HTTP_TIMEOUT = 15
NASDAQ_ENABLED = os.getenv("NASDAQ_ENABLED", "1") not in {"0", "false", "False"}

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
    # If you prefer an ID: use gc.open_by_key(os.environ["SHEET_ID"])
    return gc.open(SHEET_NAME)

def ensure_worksheet(sh, title: str, rows: int = 1000, cols: int = 10):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def append_if_needed(ws, rows: List[List[str]]):
    """
    Append rows to ws. Creates header if the sheet is empty.
    Deduplicates by the first 3 columns: [source, date_utc, symbol].
    """
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(["source", "date_utc", "symbol"], value_input_option="RAW")
        existing = ws.get_all_values()  # refresh

    existing_set = set(tuple(r[:3]) for r in existing[1:]) if len(existing) > 1 else set()
    new_rows = [r for r in rows if tuple(r[:3]) not in existing_set]

    if new_rows:
        CHUNK = 500
        for i in range(0, len(new_rows), CHUNK):
            ws.append_rows(new_rows[i:i+CHUNK], value_input_option="RAW")
        logging.info(f"Appended {len(new_rows)} new rows to '{ws.title}'.")
    else:
        logging.info("No new rows to append (all deduplicated).")


# =========================
# HTTP helpers (retry/backoff)
# =========================
def fetch_json_with_retries(
    url: str,
    *,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    timeout: int = HTTP_TIMEOUT,
    retries: int = 3,
    backoff_base: float = 0.6,
) -> Dict:
    """
    Basic jittered exponential backoff: ~0.6s, ~1.2s, ~2.4s (jittered).
    Raises the last exception if all retries fail.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            sleep_s = backoff_base * (2 ** attempt) * (0.8 + 0.4 * random.random())
            time.sleep(sleep_s)
    raise last_exc


# =========================
# Fetchers: Stocks
# =========================
def get_yahoo_trending_stocks() -> List[str]:
    """
    Yahoo Finance trending tickers (US).
    """
    url = "https://query1.finance.yahoo.com/v1/finance/trending/US"
    data = fetch_json_with_retries(url, headers=UA, timeout=15, retries=3)
    symbols: List[str] = []
    for result in data.get("finance", {}).get("result", []):
        for item in result.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                symbols.append(sym)
    return symbols

def get_yahoo_most_active() -> List[str]:
    """
    Yahoo predefined screener: most actives (top ~100).
    """
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "most_actives", "count": "100", "lang": "en-US", "region": "US"}
    data = fetch_json_with_retries(url, params=params, headers=UA, timeout=15, retries=3)
    symbols: List[str] = []
    for res in data.get("finance", {}).get("result", []):
        for item in res.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                symbols.append(sym)
    return symbols

def get_nasdaq_most_active() -> List[str]:
    """
    Nasdaq most-active. Endpoint can be temperamental; use stricter headers & retries.
    Toggle via NASDAQ_ENABLED=0 to disable gracefully.
    """
    if not NASDAQ_ENABLED:
        return []

    url = "https://api.nasdaq.com/api/quote/list-type/mostactive"
    headers = {
        **UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/most-active",
        "Connection": "keep-alive",
    }
    params = {"assetclass": "stocks"}
    data = fetch_json_with_retries(url, params=params, headers=headers, timeout=25, retries=3, backoff_base=0.8)

    items = (data.get("data") or {}).get("data") or []
    symbols: List[str] = []
    for row in items:
        sym = (row.get("symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)
    return symbols


# =========================
# Fetchers: Crypto
# =========================
def get_coingecko_trending() -> List[str]:
    """
    CoinGecko trending search endpoint (~7 coins).
    """
    url = "https://api.coingecko.com/api/v3/search/trending"
    data = fetch_json_with_retries(url, headers=UA, timeout=15, retries=3)
    symbols: List[str] = []
    for it in data.get("coins", []):
        sym = ((it.get("item") or {}).get("symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)
    return symbols

def get_coingecko_top_by_volume(limit: int = 50) -> List[str]:
    """
    CoinGecko top by trading volume (USD).
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "volume_desc", "per_page": str(limit), "page": "1"}
    data = fetch_json_with_retries(url, params=params, headers=UA, timeout=15, retries=3)
    symbols: List[str] = []
    for coin in data:
        sym = (coin.get("symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)
    return symbols


# =========================
# Orchestration
# =========================
def collect_sources() -> List[Tuple[str, List[str]]]:
    """
    Return list of (source_name, symbols) pairs. Each source is tried independently.
    """
    sources: List[Tuple[str, List[str]]] = []

    def try_add(name: str, fn):
        try:
            syms = fn()
            logging.info(f"{name}: fetched {len(syms)} symbols.")
            sources.append((name, syms))
        except Exception as e:
            logging.warning(f"{name}: failed: {e}")

    # Stocks
    try_add("Yahoo Finance - Trending (US)", get_yahoo_trending_stocks)
    try_add("Yahoo Finance - Most Active", get_yahoo_most_active)
    try_add("Nasdaq - Most Active", get_nasdaq_most_active)

    # Crypto
    try_add("CoinGecko - Trending", get_coingecko_trending)
    try_add("CoinGecko - Top by Volume", get_coingecko_top_by_volume)

    return sources

def to_rows_for_sheet(sources: List[Tuple[str, List[str]]]) -> List[List[str]]:
    """
    Format as [source, date_utc, symbol]; dedupe within each source.
    """
    ts = datetime.now(timezone.utc).isoformat()
    out: List[List[str]] = []
    for source, symbols in sources:
        seen = set()
        for sym in symbols:
            s = sym.strip().upper()
            if s and s not in seen:
                out.append([source, ts, s])
                seen.add(s)
    return out


# =========================
# Entry points
# =========================
def run_scraper(gc):
    sh = open_sheet(gc)
    ws_scraper = ensure_worksheet(sh, SCRAPER_WS, rows=5000, cols=6)
    rows = to_rows_for_sheet(collect_sources())
    append_if_needed(ws_scraper, rows)

def run_demo_write(gc):
    """
    Keep your original behavior for the 'log' tab so you still see a heartbeat write.
    """
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
