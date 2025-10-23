import os, json, sys, time, logging, random, re, statistics
from datetime import datetime, timezone
from typing import List, Tuple, Dict

import requests
import gspread
from google.oauth2.service_account import Credentials
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ✅ Use GridRange + string "NUMBER" types (compatible with older gspread-formatting)
from gspread_formatting import (
    get_conditional_format_rules, ConditionalFormatRule, GradientRule,
    InterpolationPoint, Color, GridRange,
    format_cell_ranges, CellFormat, NumberFormat, set_frozen
)

# =========================
# Config
# =========================
SHEET_NAME      = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET       = os.getenv("WORKSHEET", "log")
SCRAPER_WS      = os.getenv("SCRAPER_WS", "scraper")
SENTIMENT_WS    = os.getenv("SENTIMENT_WS", "sentiment")

HTTP_TIMEOUT    = int(os.getenv("HTTP_TIMEOUT", "15"))

# Feature flags (kept for flexibility; NASDAQ flag currently unused)
NASDAQ_ENABLED            = os.getenv("NASDAQ_ENABLED", "0") not in {"0", "false", "False"}
STOCKTWITS_ENABLED        = os.getenv("STOCKTWITS_ENABLED", "1") not in {"0", "false", "False"}
STOCKTWITS_SENTIMENT_EN   = os.getenv("STOCKTWITS_SENTIMENT_ENABLED", "1") not in {"0", "false", "False"}

# Sentiment params
SENTIMENT_ENABLED          = os.getenv("SENTIMENT_ENABLED", "1") not in {"0", "false", "False"}
SENTIMENT_SYMBOL_LIMIT     = int(os.getenv("SENTIMENT_SYMBOL_LIMIT", "150"))
SENTIMENT_MSGS_PER_SYM     = int(os.getenv("SENTIMENT_MSGS_PER_SYM", "30"))
SENTIMENT_REQ_SLEEP_S      = float(os.getenv("SENTIMENT_REQ_SLEEP_S", "0.25"))

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

def replace_sheet(ws, rows: List[List[str]], header: List[str]):
    ws.clear()
    ws.append_row(header, value_input_option="RAW")
    if rows:
        CHUNK = 500
        for i in range(0, len(rows), CHUNK):
            ws.append_rows(rows[i:i+CHUNK], value_input_option="RAW")
    logging.info(f"Wrote {len(rows)} rows to '{ws.title}'.")

# =========================
# HTTP helpers (429-aware)
# =========================
def _sleep_with_jitter(seconds: float):
    time.sleep(seconds * random.uniform(0.8, 1.2))

def fetch_json_with_retries(url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
                            retries=4, backoff_base=0.7) -> Dict:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers or UA, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra else backoff_base * (2 ** attempt)
                logging.info(f"429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            last_exc = e
            if getattr(e.response, "status_code", None) == 429:
                ra = e.response.headers.get("Retry-After")
                delay = float(ra) if ra else backoff_base * (2 ** attempt)
                logging.info(f"HTTPError 429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            _sleep_with_jitter(backoff_base * (2 ** attempt))
        except Exception as e:
            last_exc = e
            _sleep_with_jitter(backoff_base * (2 ** attempt))
    raise last_exc

def fetch_text_with_retries(url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
                            retries=3, backoff_base=0.6) -> str:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers or UA, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra else backoff_base * (2 ** attempt)
                logging.info(f"429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            _sleep_with_jitter(backoff_base * (2 ** attempt))
    raise last_exc

# =========================
# Fetchers: Yahoo / Stocktwits (crypto removed)
# =========================
def get_yahoo_trending_stocks() -> List[str]:
    data = fetch_json_with_retries("https://query1.finance.yahoo.com/v1/finance/trending/US")
    out = []
    for result in data.get("finance", {}).get("result", []):
        for item in result.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                out.append(sym)
    return out

def get_yahoo_most_active() -> List[str]:
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "most_actives", "count": "100", "lang": "en-US", "region": "US"}
    data = fetch_json_with_retries(url, params=params)
    out = []
    for res in data.get("finance", {}).get("result", []):
        for item in res.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                out.append(sym)
    return out

# =========================
# Stocktwits
# =========================
_STW_SENTIMENT_MAP = {
    "Stocktwits - Sentiment Trending":     "https://stocktwits.com/sentiment",
    "Stocktwits - Sentiment Most Active":  "https://stocktwits.com/sentiment/most-active",
    "Stocktwits - Sentiment Watchers":     "https://stocktwits.com/sentiment/watchers",
    "Stocktwits - Sentiment Most Bullish": "https://stocktwits.com/sentiment/most-bullish",
    "Stocktwits - Sentiment Most Bearish": "https://stocktwits.com/sentiment/most-bearish",
}
_SYMBOL_RE = re.compile(r"/symbol/([A-Za-z0-9\.\-_]+)")

def _parse_symbols_from_html(html: str) -> List[str]:
    raw = {m.group(1).upper() for m in _SYMBOL_RE.finditer(html)}
    return [s for s in sorted(raw) if 1 <= len(s) <= 12]

def get_stocktwits_sentiment_sets() -> List[Tuple[str, List[str]]]:
    if not STOCKTWITS_SENTIMENT_EN:
        return []
    sources = []
    headers = {**UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    for name, url in _STW_SENTIMENT_MAP.items():
        try:
            html = fetch_text_with_retries(url, headers=headers, timeout=20)
            syms = _parse_symbols_from_html(html)
            logging.info(f"{name}: scraped {len(syms)} symbols.")
            sources.append((name, syms))
        except Exception as e:
            logging.info(f"{name}: skipped ({e})")
    return sources

def fetch_stocktwits_messages(symbol: str, limit: int) -> List[Dict]:
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    headers = {**UA, "Accept": "application/json, text/plain, */*"}
    params = {"limit": str(limit)}
    data = fetch_json_with_retries(url, headers=headers, params=params, timeout=15)
    return data.get("messages", []) or []

# =========================
# Scraper orchestration
# =========================
def collect_sources() -> List[Tuple[str, List[str]]]:
    sources: List[Tuple[str, List[str]]] = []

    def try_add(name, fn):
        try:
            syms = fn()
            logging.info(f"{name}: fetched {len(syms)} symbols.")
            sources.append((name, syms))
        except Exception as e:
            logging.info(f"{name}: skipped ({e})")

    # Stocks only
    try_add("Yahoo Finance - Trending (US)", get_yahoo_trending_stocks)
    try_add("Yahoo Finance - Most Active", get_yahoo_most_active)

    # Stocktwits sentiment lists (symbols only; scoring happens later)
    for name, syms in get_stocktwits_sentiment_sets():
        sources.append((name, syms))
    return sources

def combine_sources_to_rows(sources: List[Tuple[str, List[str]]]) -> List[List[str]]:
    ts = datetime.now(timezone.utc).isoformat()
    sym_to_src: Dict[str, set] = {}
    for src, syms in sources:
        for s in syms:
            sym_to_src.setdefault(s, set()).add(src)
    return [[", ".join(sorted(v)), ts, k] for k, v in sorted(sym_to_src.items())]

# =========================
# Sentiment analysis (stocks)
# =========================
_analyzer = SentimentIntensityAnalyzer()

def score_messages(msgs: List[Dict]) -> Dict:
    scores, pos, neg, neu = [], 0, 0, 0
    for m in msgs:
        text = (m.get("body") or "").strip()
        if not text:
            continue
        c = _analyzer.polarity_scores(text)["compound"]
        scores.append(c)
        if c >= 0.05: pos += 1
        elif c <= -0.05: neg += 1
        else: neu += 1
    if not scores:
        return {"mean": 0, "median": 0, "n": 0, "pos": 0, "neg": 0, "neu": 0}
    return {
        "mean": round(sum(scores)/len(scores), 4),
        "median": round(statistics.median(scores), 4),
        "n": len(scores),
        "pos": round(pos/len(scores), 4),
        "neg": round(neg/len(scores), 4),
        "neu": round(neu/len(scores), 4),
    }

# =========================
# Formatting helper (uses GridRange)
# =========================
def apply_sentiment_conditional_formats(ws):
    set_frozen(ws, rows=1)
    format_cell_ranges(ws, [
        ("B2:B", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("C2:C", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("E2:E", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("F2:F", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("G2:G", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
    ])

    red, white, green = Color(0.9, 0.2, 0.2), Color(1, 1, 1), Color(0.2, 0.7, 0.2)

    def rule(a1_range: str, min_, mid_, max_, invert=False):
        return ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(a1_range, ws)],
            gradientRule=GradientRule(
                minpoint=InterpolationPoint(type="NUMBER", value=str(min_), color=(green if invert else red)),
                midpoint=InterpolationPoint(type="NUMBER", value=str(mid_), color=white),
                maxpoint=InterpolationPoint(type="NUMBER", value=str(max_), color=(red if invert else green)),
            )
        )

    rules = get_conditional_format_rules(ws)
    rules.clear()
    # Mean/Median: -1 → 0 → +1
    rules.append(rule("B2:B", -1, 0, 1))
    rules.append(rule("C2:C", -1, 0, 1))
    # Positive ratio: greener when higher
    rules.append(rule("E2:E", 0, 0.5, 1))
    # Negative ratio: redder when higher (invert gradient)
    rules.append(rule("F2:F", 0, 0.5, 1, invert=True))
    rules.save()

# =========================
# Main pipeline
# =========================
def run_scraper(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, SCRAPER_WS, 10000, 6)
    data = combine_sources_to_rows(collect_sources())
    replace_sheet(ws, data, ["source","date_utc","symbol"])

def run_sentiment(gc):
    if not SENTIMENT_ENABLED:
        logging.info("Sentiment disabled via SENTIMENT_ENABLED=0")
        return

    sh = open_sheet(gc)
    ws_scraper = ensure_worksheet(sh, SCRAPER_WS)
    data = ws_scraper.get_all_values()
    if len(data) <= 1:
        return
    _, *rows = data
    syms = sorted({r[2].strip().upper() for r in rows if len(r) >= 3})[:SENTIMENT_SYMBOL_LIMIT]
    out = []
    for i, s in enumerate(syms, 1):
        try:
            msgs = fetch_stocktwits_messages(s, SENTIMENT_MSGS_PER_SYM)
            sc = score_messages(msgs)
            out.append([
                s, sc["mean"], sc["median"], sc["n"],
                sc["pos"], sc["neg"], sc["neu"],
                datetime.now(timezone.utc).isoformat()
            ])
        except Exception as e:
            logging.info(f"{s} sentiment skip: {e}")
        if i % 25 == 0:
            logging.info(f"Processed {i}/{len(syms)}")
        _sleep_with_jitter(SENTIMENT_REQ_SLEEP_S)
    ws = ensure_worksheet(sh, SENTIMENT_WS, 5000, 10)
    replace_sheet(ws, out, ["symbol","mean_compound","median_compound","n_msgs","pos_ratio","neg_ratio","neu_ratio","scored_at_utc"])
    apply_sentiment_conditional_formats(ws)

def run_demo(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, WORKSHEET)
    ws.append_row(["Deployed OK", datetime.now(timezone.utc).isoformat()], value_input_option="RAW")

def main():
    gc = get_client()
    run_scraper(gc)
    run_sentiment(gc)
    run_demo(gc)
    print("Done: scrape + sentiment + formatting")

if __name__ == "__main__":
    main()
