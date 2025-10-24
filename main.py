import os, json, sys, time, logging, random, re, statistics, math
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

# --- Reddit / Google Finance toggles: default DISABLED so you don't need envs yet ---
REDDIT_ENABLED            = os.getenv("REDDIT_ENABLED", "0") not in {"0", "false", "False"}
REDDIT_SUBREDDITS         = os.getenv("REDDIT_SUBREDDITS", "wallstreetbets,stocks,finance").split(",")
REDDIT_SORT               = os.getenv("REDDIT_SORT", "hot")  # hot|new|top|rising
REDDIT_LIMIT              = int(os.getenv("REDDIT_LIMIT", "150"))  # per subreddit
REDDIT_TIME_FILTER        = os.getenv("REDDIT_TIME_FILTER", "day")  # hour|day|week|month|year|all

GOOGLE_FINANCE_ENABLED    = os.getenv("GOOGLE_FINANCE_ENABLED", "0") not in {"0", "false", "False"}
# Only most-active by default (no gainers/losers wired in)
GOOGLE_FINANCE_PAGES      = os.getenv("GOOGLE_FINANCE_PAGES", "most-active").split(",")

# Optional: idempotent demo logging and dry-run switch
DEMO_APPEND_ENABLED       = os.getenv("DEMO_APPEND_ENABLED", "0") not in {"0", "false", "False"}
DRY_RUN                   = os.getenv("DRY_RUN", "0") not in {"0", "false", "False"}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

UA = {"User-Agent": "Mozilla/5.0 (compatible; AletheiaBot/1.0; +https://example.org/bot)"}

# =========================
# Optional JSON logs
# =========================
class _JsonHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = {
                "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "lvl": record.levelname,
                "msg": record.getMessage(),
                "module": record.module,
                "func": record.funcName,
            }
            sys.stdout.write(json.dumps(msg) + "\n")
        except Exception:
            super().emit(record)

if os.getenv("JSON_LOGS", "0") not in {"0", "false", "False"}:
    logging.getLogger().handlers = []
    logging.getLogger().addHandler(_JsonHandler())
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# =========================
# HTTP session with retries + pooling
# =========================
_session = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    _session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20))
    _session.headers.update(UA)
except Exception:
    pass  # gracefully fall back

# simple per-host pacing (token-ish)
_next_ok_at: Dict[str, float] = {}

def _pace(url: str, base_delay=0.15):
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    now = time.time()
    wait_until = _next_ok_at.get(host, now)
    if wait_until > now:
        time.sleep(wait_until - now)
    _next_ok_at[host] = time.time() + base_delay * random.uniform(0.9, 1.3)

# =========================
# JSON-safe writers (fix NaN crash)
# =========================
def _json_safe_cell(v):
    """Convert NaN/Inf/None/odd types to Sheets-safe JSON."""
    if v is None:
        return ""
    try:
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return ""
            return v
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            return v
        if hasattr(v, "__float__"):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return ""
            return f
    except Exception:
        pass
    return str(v)

def _json_safe_rows(rows: List[List[object]]) -> List[List[object]]:
    return [[_json_safe_cell(c) for c in row] for row in rows]

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
        if DRY_RUN:
            logging.info(f"[DRY_RUN] Would create worksheet '{title}'")
            return sh.add_worksheet(title=title, rows=1, cols=1) if False else sh.worksheet(sh.worksheets()[0].title)
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def replace_sheet(ws, rows: List[List[object]], header: List[str]):
    if DRY_RUN:
        logging.info(f"[DRY_RUN] Would write {len(rows)} rows to '{ws.title}' with header {header}")
        return
    ws.clear()
    ws.append_row(header, value_input_option="RAW")
    if rows:
        rows = _json_safe_rows(rows)  # <<< sanitize to avoid NaN/Inf JSON errors
        CHUNK = 500
        for i in range(0, len(rows), CHUNK):
            ws.append_rows(rows[i:i+CHUNK], value_input_option="RAW")
    logging.info(f"Wrote {len(rows) if rows else 0} rows to '{ws.title}'.")

# =========================
# HTTP helpers (429-aware) using session + pacing
# =========================
def _sleep_with_jitter(seconds: float):
    time.sleep(seconds * random.uniform(0.8, 1.2))

def fetch_json_with_retries(url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
                            retries=4, backoff_base=0.7) -> Dict:
    last_exc = None
    for attempt in range(retries):
        try:
            _pace(url)
            r = _session.get(url, params=params, headers=headers or UA, timeout=timeout)
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
            _pace(url)
            r = _session.get(url, params=params, headers=headers or UA, timeout=timeout)
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
    try:
        for result in data.get("finance", {}).get("result", []) or []:
            for item in result.get("quotes", []) or []:
                try:
                    sym = (item.get("symbol") or "").strip().upper()
                    if 1 <= len(sym) <= 12:
                        out.append(sym)
                except Exception:
                    continue
    except Exception:
        pass
    return out

def get_yahoo_most_active() -> List[str]:
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "most_actives", "count": "100", "lang": "en-US", "region": "US"}
    data = fetch_json_with_retries(url, params=params)
    out = []
    try:
        for res in data.get("finance", {}).get("result", []) or []:
            for item in (res.get("quotes") or []):
                try:
                    sym = (item.get("symbol") or "").strip().upper()
                    if 1 <= len(sym) <= 12:
                        out.append(sym)
                except Exception:
                    continue
    except Exception:
        pass
    return out

# =========================
# Stocktwits (lists + messages)
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

# Some symbols (often futures) 404 on Stocktwits; skip quietly
_STW_SKIP = {"YM", "ES", "NQ", "RTY", "CL", "GC", "ZN", "ZF", "ZB"}

def fetch_stocktwits_messages(symbol: str, limit: int) -> List[Dict]:
    if symbol in _STW_SKIP:
        return []
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    headers = {**UA, "Accept": "application/json, text/plain, */*"}
    params = {"limit": str(limit)}
    try:
        data = fetch_json_with_retries(url, headers=headers, params=params, timeout=15)
        return data.get("messages", []) or []
    except Exception as e:
        logging.info(f"{symbol} sentiment skip: {e}")
        return []

# =========================
# Lightweight ticker extraction for arbitrary text
# (kept as-is per request)
# =========================
_TICKER_RE = re.compile(r"\b[A-Z][A-Z0-9\.]{1,4}\b")  # 2–5 chars, allow '.' like BRK.B
_TICKER_BLACKLIST = {
    "A", "I", "DD", "CEO", "CFO", "CTO", "IMO", "TLDR", "USA", "USD", "ETF", "EPS",
    "GDP", "FOMO", "YOLO", "ATH", "AI", "EV", "OTC", "IPO", "P/E", "WSB", "FED",
    "CPI", "PPI", "MOM", "YOY", "PE", "ROI", "RSI", "MACD"
}

def _extract_tickers(text: str) -> List[str]:
    if not text:
        return []
    cands = {m.group(0).upper() for m in _TICKER_RE.finditer(text)}
    return [c for c in cands if c not in _TICKER_BLACKLIST and 1 < len(c) <= 5]

# =========================
# Reddit fetchers (r/wallstreetbets, r/finance, etc.)
# =========================
def _reddit_listing_url(sub: str, sort: str, limit: int, t: str) -> str:
    # e.g. https://www.reddit.com/r/wallstreetbets/hot.json?limit=100&t=day
    sort = sort.lower()
    base = f"https://www.reddit.com/r/{sub}/{sort}.json"
    params = {"limit": str(min(limit, 100))}
    if sort == "top":
        params["t"] = t
    return base + "?" + "&".join([f"{k}={v}" for k, v in params.items()])

def get_reddit_symbols_from_subreddit(subreddit: str, *, sort: str, limit: int, t: str) -> List[str]:
    url = _reddit_listing_url(subreddit, sort, limit, t)
    headers = {**UA, "Accept": "application/json"}
    data = fetch_json_with_retries(url, headers=headers, timeout=15)
    children = (data.get("data", {}) or {}).get("children", []) or []
    found: set = set()
    for ch in children:
        post = (ch.get("data") or {})
        title = post.get("title") or ""
        selftext = post.get("selftext") or ""
        for part in (title, selftext):
            for sym in _extract_tickers(part):
                found.add(sym)
    return sorted(found)

def get_reddit_symbols() -> List[str]:
    if not REDDIT_ENABLED:
        return []
    all_syms: set = set()
    for sub in [s.strip() for s in REDDIT_SUBREDDITS if s.strip()]:
        try:
            syms = get_reddit_symbols_from_subreddit(
                sub, sort=REDDIT_SORT, limit=REDDIT_LIMIT, t=REDDIT_TIME_FILTER
            )
            logging.info(f"Reddit r/{sub}: extracted {len(syms)} symbols.")
            all_syms.update(syms)
            _sleep_with_jitter(0.6)  # be nice to Reddit
        except Exception as e:
            logging.info(f"Reddit r/{sub}: skipped ({e})")
    return sorted(all_syms)

# =========================
# Google Finance markets (most-active only by default)
# =========================
_GOOG_FIN_BASE = "https://www.google.com/finance/markets/"
_GOOG_PAGES_MAP = {
    "most-active": "most-active",
}
# match /finance/quote/TSLA:NASDAQ  (exchange suffix optional)
_GOOG_TICKER_HREF_RE = re.compile(r"/finance/quote/([A-Z][A-Z0-9\.-]{0,11})(?::[A-Z]+)?")
# some components also carry data-symbol="TSLA"
_GOOG_DATA_SYMBOL_RE = re.compile(r'data-symbol="([A-Z][A-Z0-9\.-]{0,11})"')

def get_google_finance_page_symbols(page_key: str) -> List[str]:
    path = _GOOG_PAGES_MAP.get(page_key.strip().lower())
    if not path:
        return []
    # Force US/EN to reduce variability; avoid locale consent detours
    url = _GOOG_FIN_BASE + path + "?hl=en&gl=US&ceid=US:en"
    headers = {
        **UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    html = fetch_text_with_retries(url, headers=headers, timeout=20)
    syms = {m.group(1).upper() for m in _GOOG_TICKER_HREF_RE.finditer(html)}
    syms.update({m.group(1).upper() for m in _GOOG_DATA_SYMBOL_RE.finditer(html)})
    syms = {s for s in syms if 1 <= len(s) <= 12}
    return sorted(syms)

def get_google_finance_symbols() -> List[Tuple[str, List[str]]]:
    if not GOOGLE_FINANCE_ENABLED:
        return []
    out: List[Tuple[str, List[str]]] = []
    for key in [k.strip() for k in GOOGLE_FINANCE_PAGES if k.strip()]:
        try:
            syms = get_google_finance_page_symbols(key)
            logging.info(f"Google Finance - {key}: scraped {len(syms)} symbols.")
            out.append((f"Google Finance - {key.title()}", syms))
            _sleep_with_jitter(0.4)
        except Exception as e:
            logging.info(f"Google Finance - {key}: skipped ({e})")
    return out

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

    # Stocks only (trending/most active)
    try_add("Yahoo Finance - Trending (US)", get_yahoo_trending_stocks)
    try_add("Yahoo Finance - Most Active", get_yahoo_most_active)

    # Google Finance most-active (optional; disabled by default)
    if GOOGLE_FINANCE_ENABLED:
        for name, syms in get_google_finance_symbols():
            sources.append((name, syms))

    # Stocktwits sentiment lists (symbols only; scoring happens later)
    for name, syms in get_stocktwits_sentiment_sets():
        sources.append((name, syms))

    # Reddit aggregate across subs (optional; disabled by default)
    if REDDIT_ENABLED:
        try:
            r_syms = get_reddit_symbols()
            sources.append((f"Reddit ({','.join(REDDIT_SUBREDDITS)})[{REDDIT_SORT}/{REDDIT_TIME_FILTER}]", r_syms))
        except Exception as e:
            logging.info(f"Reddit aggregate: skipped ({e})")

    return sources

def combine_sources_to_rows(sources: List[Tuple[str, List[str]]]) -> List[List[str]]:
    ts = datetime.now(timezone.utc).isoformat()
    sym_to_src: Dict[str, set] = {}
    for src, syms in sources:
        for s in syms:
            sym_to_src.setdefault(s, set()).add(src)
    return [[", ".join(sorted(v)), ts, k] for k, v in sorted(sym_to_src.items())]

# =========================
# Sentiment analysis (stocks) + ranking
# =========================
_analyzer = SentimentIntensityAnalyzer()

def _trimmed_mean(vals: List[float], p: float = 0.1) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = max(0, int(len(vals) * p))
    core = vals[k:len(vals) - k] or vals
    return round(sum(core) / len(core), 4)

def score_messages(msgs: List[Dict]) -> Dict:
    scores, pos, neg, neu = [], 0, 0, 0
    for m in msgs:
        text = (m.get("body") or "").strip()
        if not text:
            continue
        c = _analyzer.polarity_scores(text)["compound"]
        scores.append(c)
        if c >= 0.05:
            pos += 1
        elif c <= -0.05:
            neg += 1
        else:
            neu += 1
    n = len(scores)
    if not n:
        return {"mean": 0, "median": 0, "tmean": 0, "n": 0, "pos": 0, "neg": 0, "neu": 0, "delta": 0}
    return {
        "mean": round(sum(scores) / n, 4),
        "median": round(statistics.median(scores), 4),
        "tmean": _trimmed_mean(scores, 0.1),
        "n": n,
        "pos": round(pos / n, 4),
        "neg": round(neg / n, 4),
        "neu": round(neu / n, 4),
        "delta": round((pos - neg) / n, 4),  # bull - bear density
    }

# =========================
# Alpaca indicators + flag rules (⭐ / 🔻 / ▲)
# =========================
ALPACA_API_KEY_ID    = os.getenv("ALPACA_API_KEY_ID", "")
ALPACA_API_SECRET_KEY= os.getenv("ALPACA_API_SECRET_KEY", "")
ALPACA_DATA_BASE     = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")
ALPACA_FEED          = os.getenv("ALPACA_FEED", "iex")  # "iex" or "sip" (paid)

SENT_POS_THRESH      = float(os.getenv("SENT_POS_THRESH", "0.05"))
SENT_NEG_THRESH      = float(os.getenv("SENT_NEG_THRESH", "-0.05"))
RSI_MOMO_MAX         = float(os.getenv("RSI_MOMO_MAX", "60"))
MOMENTUM_GOOD_THRESH = float(os.getenv("MOMENTUM_GOOD_THRESH", "0.10"))

def _alpaca_enabled() -> bool:
    return bool(ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY)

def get_alpaca_bars_15m(symbol: str, limit: int = 1000) -> List[float]:
    if not _alpaca_enabled():
        return []
    headers = {
        **UA,
        "APCA-API-KEY-ID": ALPACA_API_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET_KEY,
    }
    url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
    params = {"timeframe": "15Min", "limit": str(limit), "adjustment": "raw", "feed": ALPACA_FEED}
    data = fetch_json_with_retries(url, params=params, headers=headers, timeout=20)
    bars = data.get("bars", []) or []
    closes: List[float] = []
    for b in bars:
        try:
            closes.append(float(b.get("c")))
        except Exception:
            continue
    return closes

def sma(values: List[float], window: int) -> float:
    if len(values) < window:
        return float("nan")
    return round(sum(values[-window:]) / window, 4)

def rsi14(values: List[float]) -> float:
    period = 14
    if len(values) < period + 1:
        return float("nan")
    gains, losses = 0.0, 0.0
    for i in range(-period, 0):
        ch = values[i] - values[i-1]
        if ch > 0:
            gains += ch
        else:
            losses -= ch
    if losses == 0:
        return 100.0
    rs = gains / losses
    return round(100 - (100 / (1 + rs)), 2)

def pick_flag(sent_value: float, delta: float, rsi: float, ma60: float, ma240: float) -> str:
    """
    Flags priority:
      1) ⭐ if RSI<35 and MA60<MA240 (15m)
      2) 🔻 if MA60>MA240 and sentiment <= SENT_NEG_THRESH
      3) ▲  if MA60>MA240 and sentiment >= SENT_POS_THRESH and RSI<=RSI_MOMO_MAX and delta>=MOMENTUM_GOOD_THRESH
      else ""
    """
    if any(math.isnan(x) for x in (rsi, ma60, ma240)):
        return ""
    if rsi < 35 and ma60 < ma240:
        return "⭐"
    if ma60 > ma240:
        if sent_value <= SENT_NEG_THRESH:
            return "🔻"
        if sent_value >= SENT_POS_THRESH and rsi <= RSI_MOMO_MAX and delta >= MOMENTUM_GOOD_THRESH:
            return "▲"
    return ""

# =========================
# Formatting helper (uses GridRange)
# =========================
def apply_sentiment_conditional_formats(ws):
    set_frozen(ws, rows=1)
    # Column map (after adding flag + indicators):
    # A:flag B:symbol C:mean D:median E:trim_mean F:n_msgs G:pos H:neg I:neu J:delta
    # K:source_hits L:msgs_factor M:rank N:RSI14_15m O:MA60_15m P:MA240_15m Q:scored_at_utc
    format_cell_ranges(ws, [
        ("C2:E", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("F2:F", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0"))),
        ("G2:I", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("J2:J", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("K2:K", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0"))),
        ("L2:L", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("M2:M", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.000"))),
        ("N2:P", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
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
    # Mean/Median/Trimmed: -1 → 0 → +1
    rules.append(rule("C2:C", -1, 0, 1))
    rules.append(rule("D2:D", -1, 0, 1))
    rules.append(rule("E2:E", -1, 0, 1))
    # Positive ratio: greener when higher
    rules.append(rule("G2:G", 0, 0.5, 1))
    # Negative ratio: redder when higher (invert gradient)
    rules.append(rule("H2:H", 0, 0.5, 1, invert=True))
    # Delta (bull-bear): -1 → 0 → +1
    rules.append(rule("J2:J", -1, 0, 1))
    # Rank
    rules.append(rule("M2:M", -1, 0, 1))
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
        logging.info("No scraper data found.")
        return
    header, *rows = data

    # Build coverage map from scraper sheet (first column contains comma-separated sources)
    source_map: Dict[str, int] = {}
    for r in rows:
        if len(r) >= 3:
            sources_str = (r[0] or "").strip()
            sym = r[2].strip().upper()
            if not sym:
                continue
            if sources_str:
                hits = len([s for s in [x.strip() for x in sources_str.split(",")] if s])
            else:
                hits = 1
            source_map[sym] = hits

    # Collect symbols (unique), cap to limit
    syms = sorted({r[2].strip().upper() for r in rows if len(r) >= 3 and r[2].strip()})[:SENTIMENT_SYMBOL_LIMIT]

    out = []
    for i, s in enumerate(syms, 1):
        try:
            msgs = fetch_stocktwits_messages(s, SENTIMENT_MSGS_PER_SYM)
            sc = score_messages(msgs)

            coverage = source_map.get(s, 1)
            sent = 0.6 * sc["tmean"] + 0.4 * sc["median"]
            momentum = sc["delta"]
            msgs_factor = min(1.0, sc["n"] / max(1, SENTIMENT_MSGS_PER_SYM))

            # Composite rank (simple, tunable)
            coverage_term = math.log10(1 + coverage)  # 0.. ~
            rank = 0.5 * coverage_term + 0.35 * sent + 0.15 * momentum
            rank = round(rank, 4)

            # Indicators via Alpaca (15m)
            rsi = ma60 = ma240 = float("nan")
            if _alpaca_enabled():
                closes = get_alpaca_bars_15m(s, limit=1000)
                if closes:
                    rsi = rsi14(closes)
                    ma60 = sma(closes, 60)
                    ma240 = sma(closes, 240)

            # Decide leftmost flag
            flag = pick_flag(sent, momentum, rsi, ma60, ma240)

            out.append([
                flag, s, sc["mean"], sc["median"], sc["tmean"], sc["n"],
                sc["pos"], sc["neg"], sc["neu"], sc["delta"],
                coverage, round(msgs_factor, 4), rank,
                rsi, ma60, ma240,
                datetime.now(timezone.utc).isoformat()
            ])
        except Exception as e:
            logging.info(f"{s} sentiment skip: {e}")
        if i % 25 == 0:
            logging.info(f"Processed {i}/{len(syms)}")
        _sleep_with_jitter(SENTIMENT_REQ_SLEEP_S)

    # Note: new columns + flag at left; adjust column count accordingly
    ws = ensure_worksheet(sh, SENTIMENT_WS, 5000, 18)
    replace_sheet(ws, out, [
        "flag","symbol","mean_comp","median_comp","trim_mean","n_msgs",
        "pos_ratio","neg_ratio","neu_ratio","bull_bear_delta",
        "source_hits","msgs_factor","rank",
        "RSI14_15m","MA60_15m","MA240_15m","scored_at_utc"
    ])

    apply_sentiment_conditional_formats(ws)

    # Try to enable basic filter; ignore if unsupported
    try:
        if not DRY_RUN:
            ws.set_basic_filter()
    except Exception:
        pass

def run_demo(gc):
    if not DEMO_APPEND_ENABLED:
        return
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, WORKSHEET)
    if DRY_RUN:
        logging.info("[DRY_RUN] Would append demo row")
        return
    ws.append_row(["Deployed OK", datetime.now(timezone.utc).isoformat()], value_input_option="RAW")

def main():
    gc = get_client()
    run_scraper(gc)
    run_sentiment(gc)
    run_demo(gc)
    print("Done: scrape + sentiment + indicators + flags + formatting")

if __name__ == "__main__":
    main()
