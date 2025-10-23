import os, json, sys, time, logging
from datetime import datetime, timezone
from typing import List, Tuple, Iterable, Dict
import requests
import gspread
from google.oauth2.service_account import Credentials

# -------- Config --------
SHEET_NAME = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET  = os.getenv("WORKSHEET", "log")          # still used by your original write
SCRAPER_WS = os.getenv("SCRAPER_WS", "scraper")     # new tab for scraped symbols
HTTP_TIMEOUT = 15

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

UA = {"User-Agent": "Mozilla/5.0 (compatible; AletheiaBot/1.0; +https://example.org/bot)"}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------- Google Sheets helpers --------
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

def append_if_needed(ws, rows: List[List[str]]):
    """Append rows to ws. Creates header if the sheet is empty."""
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(["source", "date_utc", "symbol"], value_input_option="RAW")

    # Build a set of existing triplets to avoid dup inserts within/accross runs
    existing_set = set(tuple(r[:3]) for r in existing[1:]) if len(existing) > 1 else set()

    new_rows = [r for r in rows if tuple(r[:3]) not in existing_set]
    if new_rows:
        # Batch in chunks to avoid request size limits
        CHUNK = 500
        for i in range(0, len(new_rows), CHUNK):
            ws.append_rows(new_rows[i:i+CHUNK], value_input_option="RAW")
        logging.info(f"Appended {len(new_rows)} new rows to '{ws.title}'.")
    else:
        logging.info("No new rows to append (all deduplicated).")


# -------- Fetchers (Stocks) --------
def get_yahoo_trending_stocks() -> List[str]:
    """
    Yahoo Finance trending tickers (US).
    Endpoint returns symbols directly.
    """
    url = "https://query1.finance.yahoo.com/v1/finance/trending/US?lang=en-US&region=US"
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    symbols = []
    for result in data.get("finance", {}).get("result", []):
        for item in result.get("quotes", []):
            sym = item.get("symbol")
            if sym:
                symbols.append(sym.upper())
    return symbols

def get_yahoo_most_active() -> List[str]:
    """
    Yahoo Finance predefined screener: most actives.
    """
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "most_actives", "count": "100"}
    r = requests.get(url, headers=UA, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    symbols = []
    for res in data.get("finance", {}).get("result", []):
        for item in res.get("quotes", []):
            sym = item.get("symbol")
            if sym:
                symbols.append(sym.upper())
    return symbols

def get_nasdaq_most_active() -> List[str]:
    """
    Nasdaq most-active API. Sometimes rate-limited; wrapped in try/except.
    """
    url = "https://api.nasdaq.com/api/quote/list-type/mostactive"
    headers = {**UA, "Accept": "application/json"}
    params = {"assetclass": "stocks"}
    r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    items = data.get("data", {}).get("data", [])
    symbols = []
    for row in items:
        sym = row.get("symbol")
        if sym:
            symbols.append(sym.upper())
    return symbols


# -------- Fetchers (Crypto) --------
def get_coingecko_trending() -> List[str]:
    """
    CoinGecko trending search endpoint (top ~7).
    """
    url = "https://api.coingecko.com/api/v3/search/trending"
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    symbols = []
    for it in data.get("coins", []):
        sym = it.get("item", {}).get("symbol")
        if sym:
            symbols.append(sym.upper())
    return symbols

def get_coingecko_top_by_volume(limit: int = 50) -> List[str]:
    """
    CoinGecko markets by volume (USD).
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "volume_desc", "per_page": str(limit), "page": "1"}
    r = requests.get(url, headers=UA, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    symbols = []
    for coin in data:
        sym = coin.get("symbol")
        if sym:
            symbols.append(sym.upper())
    return symbols


# -------- Orchestration --------
def collect_sources() -> List[Tuple[str, List[str]]]:
    """
    Return list of (source_name, symbols) pairs. Each source is tried independently.
    """
    sources = []

    def try_add(name, fn):
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


# -------- Main --------
def run_scraper(gc):
    sh = open_sheet(gc)
    ws_scraper = ensure_worksheet(sh, SCRAPER_WS, rows=5000, cols=6)
    sources = collect_sources()
    rows = to_rows_for_sheet(sources)
    append_if_needed(ws_scraper, rows)

def run_demo_write(gc):
    """Keep your original behavior for the 'log' tab so you still see a heartbeat write."""
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
