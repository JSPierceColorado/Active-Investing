import os, json, sys, time, logging, random, re, statistics
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# NEW: formatting helpers
from gspread_formatting import (
    set_conditional_formatting, ConditionalFormatRule, GradientRule,
    InterpolationPoint, InterpolationPointType, Color,
    format_cell_ranges, CellFormat, NumberFormat, set_frozen
)

# =========================
# Config
# =========================
SHEET_NAME      = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET       = os.getenv("WORKSHEET", "log")             # heartbeat tab
SCRAPER_WS      = os.getenv("SCRAPER_WS", "scraper")        # scraped universe (latest only, deduped)
SENTIMENT_WS    = os.getenv("SENTIMENT_WS", "sentiment")    # stock sentiment tab (VADER/Stocktwits)
CRYPTO_SENT_WS  = os.getenv("CRYPTO_SENT_WS", "crypto_sentiment")

HTTP_TIMEOUT    = int(os.getenv("HTTP_TIMEOUT", "15"))

# Source toggles
NASDAQ_ENABLED            = os.getenv("NASDAQ_ENABLED", "0") not in {"0", "false", "False"}
STOCKTWITS_ENABLED       = os.getenv("STOCKTWITS_ENABLED", "1") not in {"0", "false", "False"}
STOCKTWITS_SENTIMENT_EN  = os.getenv("STOCKTWITS_SENTIMENT_ENABLED", "1") not in {"0", "false", "False"}

# Sentiment toggles/limits
SENTIMENT_ENABLED         = os.getenv("SENTIMENT_ENABLED", "1") not in {"0", "false", "False"}
SENTIMENT_SYMBOL_LIMIT    = int(os.getenv("SENTIMENT_SYMBOL_LIMIT", "150"))    # max stock symbols to VADER-score
SENTIMENT_MSGS_PER_SYM    = int(os.getenv("SENTIMENT_MSGS_PER_SYM", "30"))     # Stocktwits messages per stock symbol
SENTIMENT_REQ_SLEEP_S     = float(os.getenv("SENTIMENT_REQ_SLEEP_S", "0.25"))  # throttle between Stocktwits calls

# Crypto sentiment limits (more conservative to avoid 429)
CRYPTO_SENT_ENABLED       = os.getenv("CRYPTO_SENT_ENABLED", "1") not in {"0", "false", "False"}
CRYPTO_SENT_SYMBOL_LIMIT  = int(os.getenv("CRYPTO_SENT_SYMBOL_LIMIT", "120"))  # cap how many cryptos per run
CRYPTO_REQ_SLEEP_S        = float(os.getenv("CRYPTO_REQ_SLEEP_S", "0.35"))     # delay between CoinGecko detail calls
COINGECKO_IDS_BATCH       = 180  # batch size for /coins/markets by ids (<= 250 per docs, keep margin)

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
# HTTP helpers (retry/backoff with 429 handling)
# =========================
def _sleep_with_jitter(seconds: float):
    jitter = seconds * random.uniform(0.8, 1.2)
    time.sleep(jitter)

def fetch_json_with_retries(
    url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
    retries=4, backoff_base=0.7
) -> Dict:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers or UA, timeout=timeout)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff_base * (2 ** attempt)
                logging.info(f"429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)
            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff_base * (2 ** attempt)
                logging.info(f"HTTPError 429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            _sleep_with_jitter(backoff_base * (2 ** attempt))
        except Exception as e:
            last_exc = e
            _sleep_with_jitter(backoff_base * (2 ** attempt))
    raise last_exc

def fetch_text_with_retries(
    url: str, *, params=None, headers=None, timeout=HTTP_TIMEOUT,
    retries=3, backoff_base=0.6
) -> str:
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers or UA, timeout=timeout)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff_base * (2 ** attempt)
                logging.info(f"429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            r.raise_for_status()
            return r.text
        except requests.exceptions.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)
            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff_base * (2 ** attempt)
                logging.info(f"HTTPError 429 at {url} — sleeping {delay:.2f}s")
                _sleep_with_jitter(delay)
                continue
            _sleep_with_jitter(backoff_base * (2 ** attempt))
        except Exception as e:
            last_exc = e
            _sleep_with_jitter(backoff_base * (2 ** attempt))
    raise last_exc

# =========================
# Fetchers: Stocks (Yahoo)
# =========================
def _yahoo_predefined(scr_id: str, count: int = 100) -> List[str]:
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": scr_id, "count": str(count), "lang": "en-US", "region": "US"}
    data = fetch_json_with_retries(url, params=params)
    out: List[str] = []
    for res in data.get("finance", {}).get("result", []):
        for item in res.get("quotes", []):
            sym = (item.get("symbol") or "").strip().upper()
            if sym:
                out.append(sym)
    return out

def get_yahoo_trending_stocks() -> List[str]:
    url = "https://query1.finance.yahoo.com/v1/finance/trending/US"
    data = fetch_json_with_retries(url)
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
    data = fetch_json_with_retries(url)
    out = []
    for it in data.get("coins", []):
        sym = ((it.get("item") or {}).get("symbol") or "").strip().upper()
        if sym: out.append(sym)
    return out

def get_coingecko_top_by_volume(limit: int = 50) -> List[str]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "volume_desc", "per_page": str(limit), "page": "1"}
    data = fetch_json_with_retries(url, params=params)
    return [(coin.get("symbol") or "").strip().upper() for coin in data]

# =========================
# Fetchers: Social (Stocktwits lists & messages for STOCKS)
# =========================
def get_stocktwits_trending_api() -> List[str]:
    if not STOCKTWITS_ENABLED:
        return []
    url = "https://api.stocktwits.com/api/2/trending/symbols.json"
    headers = {
        **UA, "Accept": "application/json, text/plain, */*",
        "Origin": "https://stocktwits.com", "Referer": "https://stocktwits.com/",
    }
    data = fetch_json_with_retries(url, headers=headers)
    return [(item.get("symbol") or "").strip().upper() for item in (data.get("symbols") or [])]

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

def fetch_stocktwits_messages(symbol: str, limit: int) -> List[Dict]:
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    headers = {
        **UA, "Accept": "application/json, text/plain, */*",
        "Origin": "https://stocktwits.com", "Referer": f"https://stocktwits.com/symbol/{symbol}",
    }
    params = {"limit": str(limit)}
    data = fetch_json_with_retries(url, headers=headers, params=params, timeout=15)
    return data.get("messages", []) or []

# =========================
# Orchestration for scraping (universe)
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
    try_add("Yahoo Finance - Most Active",  get_yahoo_most_active)

    # Nasdaq (optional)
    try_add("Nasdaq - Most Active",         get_nasdaq_most_active)

    # Crypto (CoinGecko)
    try_add("CoinGecko - Trending",         get_coingecko_trending)
    try_add("CoinGecko - Top by Volume",    get_coingecko_top_by_volume)

    # Stocktwits (API + Sentiment pages)
    try_add("Stocktwits - Trending (API)",  get_stocktwits_trending_api)
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
# STOCK sentiment (VADER on Stocktwits messages)
# =========================
_analyzer = SentimentIntensityAnalyzer()

def score_messages(messages: List[Dict]) -> Dict:
    scores = []
    last_ts = None
    pos = neg = neu = 0
    for m in messages:
        body = (m.get("body") or "").strip()
        if not body:
            continue
        vs = _analyzer.polarity_scores(body)
        c = vs["compound"]
        scores.append(c)
        if c >= 0.05: pos += 1
        elif c <= -0.05: neg += 1
        else: neu += 1
        created = m.get("created_at")
        if created:
            last_ts = created
    n = len(scores)
    if n == 0:
        return {"n_msgs": 0, "mean": 0.0, "median": 0.0, "pos_ratio": 0.0, "neg_ratio": 0.0, "neu_ratio": 0.0, "last_message_at": last_ts or ""}
    mean = sum(scores)/n
    median = statistics.median(scores)
    return {
        "n_msgs": n, "mean": round(mean, 4), "median": round(median, 4),
        "pos_ratio": round(pos / n, 4), "neg_ratio": round(neg / n, 4), "neu_ratio": round(neu / n, 4),
        "last_message_at": last_ts or "",
    }

# =========================
# CRYPTO sentiment (CoinGecko community)
# =========================
def coingecko_all_coins_list() -> List[Dict]:
    url = "https://api.coingecko.com/api/v3/coins/list"
    return fetch_json_with_retries(url)

def coingecko_markets_for_ids(ids: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    if not ids:
        return out
    for i in range(0, len(ids), COINGECKO_IDS_BATCH):
        chunk = ids[i:i+COINGECKO_IDS_BATCH]
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "ids": ",".join(chunk), "per_page": "250", "page": "1"}
        data = fetch_json_with_retries(url, params=params)
        for row in data:
            cid = row.get("id")
            out[cid] = {"market_cap_rank": row.get("market_cap_rank")}
        _sleep_with_jitter(0.2)
    return out

def map_symbols_to_coingecko_ids(symbols: List[str]) -> Dict[str, str]:
    symbols = [s for s in symbols if s and len(s) >= 3]
    all_coins = coingecko_all_coins_list()
    sym_to_ids: Dict[str, List[str]] = {}
    for c in all_coins:
        sym = (c.get("symbol") or "").upper()
        cid = c.get("id")
        if not sym or not cid:
            continue
        sym_to_ids.setdefault(sym, []).append(cid)
    candidates: Dict[str, List[str]] = {s: sym_to_ids.get(s, []) for s in symbols}
    all_ids = sorted({cid for ids in candidates.values() for cid in ids})
    ranks = coingecko_markets_for_ids(all_ids)
    sym_best: Dict[str, str] = {}
    for s, ids in candidates.items():
        if not ids:
            continue
        best = None
        best_rank = 10**9
        for cid in ids:
            r = ranks.get(cid, {}).get("market_cap_rank")
            if isinstance(r, int) and r > 0 and r < best_rank:
                best = cid
                best_rank = r
        sym_best[s] = best if best else ids[0]
    return sym_best

# =========================
# Runners
# =========================
def run_scraper(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, SCRAPER_WS, rows=10000, cols=6)
    sources = collect_sources()
    rows = combine_sources_to_rows(sources)
    replace_sheet(ws, rows, ["source", "date_utc", "symbol"])

def run_stock_sentiment(gc, crypto_symbol_set: Set[str]):
    if not SENTIMENT_ENABLED:
        logging.info("Stock sentiment disabled (SENTIMENT_ENABLED=0).")
        return
    sh = open_sheet(gc)
    ws_scraper = ensure_worksheet(sh, SCRAPER_WS, rows=10000, cols=6)
    data = ws_scraper.get_all_values()
    if len(data) <= 1:
        logging.info("No symbols in scraper tab; skipping stock sentiment.")
        replace_sheet(ensure_worksheet(sh, SENTIMENT_WS, rows=2000, cols=12), [], [
            "symbol","mean_compound","median_compound","n_msgs","pos_ratio","neg_ratio","neu_ratio","last_message_at","scored_at_utc"
        ])
        return

    header, *rows = data
    all_syms = [(r[0], r[2].strip().upper()) for r in rows if len(r) >= 3 and r[2].strip()]
    stock_syms = sorted({sym for srcs, sym in all_syms if sym not in crypto_symbol_set})[:SENTIMENT_SYMBOL_LIMIT]

    out_rows: List[List[str]] = []
    for i, sym in enumerate(stock_syms, 1):
        try:
            msgs = fetch_stocktwits_messages(sym, limit=SENTIMENT_MSGS_PER_SYM)
            stats = score_messages(msgs)
            out_rows.append([
                sym,
                stats["mean"], stats["median"], stats["n_msgs"],
                stats["pos_ratio"], stats["neg_ratio"], stats["neu_ratio"],
                stats["last_message_at"],
                datetime.now(timezone.utc).isoformat(),
            ])
            if SENTIMENT_REQ_SLEEP_S > 0:
                _sleep_with_jitter(SENTIMENT_REQ_SLEEP_S)
        except Exception as e:
            logging.info(f"Stock sentiment skip {sym}: {e}")
            continue
        if i % 25 == 0:
            logging.info(f"VADER scored {i}/{len(stock_syms)} stock symbols...")

    ws_sent = ensure_worksheet(sh, SENTIMENT_WS, rows=5000, cols=12)
    replace_sheet(ws_sent, out_rows, [
        "symbol","mean_compound","median_compound","n_msgs",
        "pos_ratio","neg_ratio","neu_ratio","last_message_at","scored_at_utc"
    ])
    logging.info(f"Stock sentiment complete for {len(out_rows)} symbols.")
    # Apply formatting after write
    apply_sentiment_conditional_formats(ws_sent)

def run_crypto_sentiment(gc) -> Set[str]:
    if not CRYPTO_SENT_ENABLED:
        logging.info("Crypto sentiment disabled (CRYPTO_SENT_ENABLED=0).")
        return set()

    sh = open_sheet(gc)
    ws_scraper = ensure_worksheet(sh, SCRAPER_WS, rows=10000, cols=6)
    data = ws_scraper.get_all_values()
    if len(data) <= 1:
        logging.info("No symbols in scraper tab; skipping crypto sentiment.")
        replace_sheet(ensure_worksheet(sh, CRYPTO_SENT_WS, rows=2000, cols=12), [], [
            "symbol","coingecko_id","up_pct","down_pct","reddit_subs","twitter_followers","telegram_users","score","scored_at_utc"
        ])
        return set()

    header, *rows = data
    candidates = []
    for r in rows:
        if len(r) < 3:
            continue
        combined_sources, symbol = r[0], r[2].strip().upper()
        if "CoinGecko" in combined_sources and len(symbol) >= 3:
            candidates.append(symbol)

    crypto_syms = sorted(set(candidates))[:CRYPTO_SENT_SYMBOL_LIMIT]
    sym_to_id = map_symbols_to_coingecko_ids(crypto_syms)

    out_rows: List[List[str]] = []
    identified_crypto_syms: Set[str] = set(sym_to_id.keys())

    for idx, (sym, cid) in enumerate(sym_to_id.items(), 1):
        if not cid:
            continue
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{cid}"
            params = {
                "localization": "false",
                "tickers": "false",
                "market_data": "false",
                "community_data": "true",
                "developer_data": "false",
                "sparkline": "false",
            }
            data = fetch_json_with_retries(url, params=params)
            cd = data.get("community_data") or {}
            up = cd.get("sentiment_votes_up_percentage")
            down = cd.get("sentiment_votes_down_percentage")
            reddit_subs = cd.get("reddit_subscribers")
            twitter_f = cd.get("twitter_followers")
            telegram_u = cd.get("telegram_channel_user_count")

            score = None
            if isinstance(up, (int, float)) and isinstance(down, (int, float)):
                score = max(0.0, min(100.0, (up - down + 100.0) / 2.0))

            out_rows.append([
                sym, cid,
                round(up or 0.0, 2), round(down or 0.0, 2),
                int(reddit_subs or 0), int(twitter_f or 0), int(telegram_u or 0),
                round(score or 0.0, 2),
                datetime.now(timezone.utc).isoformat(),
            ])
        except Exception as e:
            logging.info(f"Crypto sentiment skip {sym} ({cid}): {e}")
        if CRYPTO_REQ_SLEEP_S > 0:
            _sleep_with_jitter(CRYPTO_REQ_SLEEP_S)
        if idx % 40 == 0:
            logging.info(f"CoinGecko community scored {idx}/{len(sym_to_id)} crypto symbols...")

    ws = ensure_worksheet(sh, CRYPTO_SENT_WS, rows=5000, cols=12)
    replace_sheet(ws, out_rows, [
        "symbol","coingecko_id","up_pct","down_pct","reddit_subs","twitter_followers","telegram_users","score","scored_at_utc"
    ])
    logging.info(f"Crypto sentiment complete for {len(out_rows)} symbols.")
    return identified_crypto_syms

def run_demo_write(gc):
    sh = open_sheet(gc)
    ws = ensure_worksheet(sh, WORKSHEET, rows=200, cols=10)
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row(["Deployed on Railway 🎉", ts], value_input_option="RAW")

# =========================
# Formatting: sentiment tab
# =========================
def apply_sentiment_conditional_formats(ws):
    """
    Applies:
    - 3-color gradient to mean/median with domain -1→0→+1 (red→white→green)
    - 3-color gradient to pos_ratio with domain 0→0.5→1 (red→white→green)
    - 3-color gradient to neg_ratio with domain 0→0.5→1 (green→white→red)
    - Percent formats on E:F:G, numeric on B:C, freeze header row
    """
    # Freeze header
    set_frozen(ws, rows=1)

    # Number formats
    format_cell_ranges(ws, [
        ("B2:B", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("C2:C", CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.00"))),
        ("E2:E", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("F2:F", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
        ("G2:G", CellFormat(numberFormat=NumberFormat(type="PERCENT", pattern="0.00%"))),
    ])

    red   = Color(0.90, 0.20, 0.20)
    white = Color(1, 1, 1)
    green = Color(0.20, 0.70, 0.20)

    def three_color_rule(a1_range: str, min_num: float, mid_num: float, max_num: float, invert=False):
        return ConditionalFormatRule(
            ranges=[a1_range],
            gradientRule=GradientRule(
                minpoint=InterpolationPoint(
                    type=InterpolationPointType.NUMBER,
                    value=str(min_num),
                    color=(green if invert else red)
                ),
                midpoint=InterpolationPoint(
                    type=InterpolationPointType.NUMBER,
                    value=str(mid_num),
                    color=white
                ),
                maxpoint=InterpolationPoint(
                    type=InterpolationPointType.NUMBER,
                    value=str(max_num),
                    color=(red if invert else green)
                ),
            )
        )

    rules = [
        # Mean & Median compound: -1 .. 0 .. +1
        three_color_rule("B2:B", -1, 0, 1, invert=False),
        three_color_rule("C2:C", -1, 0, 1, invert=False),

        # Positive ratio: 0 .. 0.5 .. 1 (more positive = greener)
        three_color_rule("E2:E", 0, 0.5, 1, invert=False),

        # Negative ratio: 0 .. 0.5 .. 1 (more negative = redder)
        three_color_rule("F2:F", 0, 0.5, 1, invert=True),
    ]

    # Overwrite any existing conditional formats for the sheet
    set_conditional_formatting(ws, rules)

# =========================
# Entry point
# =========================
def main():
    gc = get_client()
    # 1) scrape + write latest-only universe
    run_scraper(gc)
    # 2) crypto sentiment (CoinGecko)
    crypto_syms = run_crypto_sentiment(gc)
    # 3) stock sentiment (VADER on Stocktwits) + formatting
    run_stock_sentiment(gc, crypto_symbol_set=crypto_syms)
    # 4) heartbeat
    run_demo_write(gc)
    print("Scrape + Sentiment + Formatting complete and demo row written.")

if __name__ == "__main__":
    main()
