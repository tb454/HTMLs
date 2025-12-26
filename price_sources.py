import asyncio
import re, time
import asyncpg
import httpx
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

def _norm_symbol(s: str) -> str:
    return (s or "").upper()

ET = ZoneInfo("America/New_York")

def _et_trading_date(now_utc: datetime | None = None) -> datetime:
    """
    Map current time to the trading 'date' in New York.
    Use 17:00 ET close: if now < 17:00 ET -> use previous calendar day; else today.
    Returns a UTC datetime at 00:00 of that NY date.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    now_et  = now_utc.astimezone(ET)
    close   = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    d_et    = (now_et.date() if now_et >= close else (now_et - timedelta(days=1)).date())
    # normalize to 00:00 UTC for ts_market
    return datetime(d_et.year, d_et.month, d_et.day, tzinfo=timezone.utc)

USER_AGENT = "BRidgeBot/1.0 (+scrapfutures.com) contact: admin@scrapfutures.com"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}

# ---- TEMP PUBLIC PAGES (internal-only reference) ----
COMEXLIVE = {
    "COMEX_CU": "https://comexlive.org/copper/",
    "COMEX_AL": "https://comexlive.org/aluminum/",
    "COMEX_AU": "https://comexlive.org/gold/",
    "COMEX_AG": "https://comexlive.org/silver/",
    "COMEX_PL": "https://comexlive.org/platinum/",
    "COMEX_PA": "https://comexlive.org/palladium/",
    "COMEX_ZN": "https://comexlive.org/zinc/",
    "COMEX_HOME": "https://comexlive.org/",
}

# LME summary pages (format varies; treat as best-effort)
LME = {
    "LME_PB": "https://www.lme.com/en/Metals/Non-ferrous/LME-Lead#Summary",
    "LME_NI": "https://www.lme.com/Metals/Non-ferrous/LME-Nickel#Summary",
    "LME_ZN": "https://www.lme.com/Metals/Non-ferrous/LME-Zinc#Summary",
}

# --- Comex Live Reference ---
COMEX_HOME_URL = "https://comexlive.org/"

HOME_LABELS = {
    "COMEX Gold":      "COMEX_AU",
    "COMEX Silver":    "COMEX_AG",
    "COMEX Platinum":  "COMEX_PL",
    "COMEX Palladium": "COMEX_PA",
    "COMEX Copper":    "COMEX_CU",
    "COMEX Aluminum":  "COMEX_AL",
    "COMEX Zinc":      "COMEX_ZN",
    # Oils, grains, etc. are omitted on purpose for now
}

_MT_TO_LB = 2204.62262

def _maybe_to_per_lb(symbol: str, price: float) -> float:
    """
    COMEX home shows AL/ZN often as $/metric-ton. If value is huge (>20), convert.
    Copper is already $/lb. Precious metals left as-is (usually $/oz).
    """
    if symbol in ("COMEX_AL", "COMEX_ZN") and price > 20:
        return price / _MT_TO_LB
    return price


# ---------- HTML -> text + Last Trade extractor ----------
def _html_to_text(html: str) -> str:
    # drop scripts/styles first
    html = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.I | re.S)
    html = re.sub(r"<style\b[^>]*>.*?</style>", " ", html, flags=re.I | re.S)
    # strip tags
    text = re.sub(r"<[^>]+>", " ", html)
    # normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

_LAST_TRADE_RE = re.compile(
    r"Last\s*Trade\s*Change\s*Change\s*in\s*%[\s:]*([0-9][0-9,]*\.?[0-9]*)",
    re.I,
)

def _extract_last_trade_price(html: str) -> float | None:
    """
    Extract numeric immediately after "Last Trade Change Change in %"
    Example: "Last Trade Change Change in % 5.8515 +0.2765 +4.96%"
    """
    text = _html_to_text(html)
    m = _LAST_TRADE_RE.search(text)
    if not m:
        return None
    s = m.group(1).replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


async def pull_comex_home_once(pool):
    """
    Scrape comexlive.org homepage table and insert latest readings for key symbols.
    Best-effort; respects same audit strategy as others.
    """
    try:
        async with httpx.AsyncClient(timeout=12.0, headers=HEADERS, follow_redirects=True) as client:
            r = await client.get(COMEX_HOME_URL)
            r.raise_for_status()
            html = r.text

            # For each display label, grab the first float after the label.
            for label, symbol in HOME_LABELS.items():
                # e.g., "COMEX Copper  4.5870  -0.0045  -0.10% ..."
                price = _extract_near(html, [label])
                if price and price > 0:
                    price_norm = _maybe_to_per_lb(symbol, float(price))
                    ts = _market_ts(html)  # homepage also carries the "As on ..." banner (optional)
                    await _insert_ref_price(
                        pool,
                        symbol=symbol,
                        source=COMEX_HOME_URL,
                        price=price_norm,
                        ts_market=ts,
                        snippet=f"{label} {price}"
                    )
    except Exception:
        pass


# ---------- DB insert ----------
async def _insert_ref_price(pool, symbol, source, price, ts_market, snippet):
    symbol = _norm_symbol(symbol)

    # authoritative trading-day timestamp for storage (UTC midnight of ET trading date)
    ts_market = _et_trading_date()

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO reference_prices (symbol, source, price, ts_market, ts_server, raw_snippet)
            VALUES ($1, $2, $3, $4, now(), $5)
            ON CONFLICT (symbol, ts_market) DO UPDATE
              SET price = EXCLUDED.price,
                  source = EXCLUDED.source,
                  ts_server = now(),
                  raw_snippet = EXCLUDED.raw_snippet
        """, symbol, source, price, ts_market, (snippet or "")[:2000])


# ---------- Generic extractors ----------
_FLOAT = r"([-+]?\d+(?:\.\d+)?)"

def _first_float(text: str):
    m = re.search(_FLOAT, text)
    return float(m.group(1)) if m else None

def _extract_near(text: str, anchors):
    if isinstance(anchors, str):
        anchors = [anchors]
    for a in anchors:
        m = re.search(rf"{a}[^0-9\-\.]{{0,60}}{_FLOAT}", text, re.IGNORECASE | re.DOTALL)
        if m:
            return float(m.group(1))
    return None

def _market_ts(text: str):
    # comexlive hint: "As on Sep 23, 2025 01:05 PM, GMT Time"
    m = re.search(r"As on\s+([^,<]+(?:,[^<]+)?)\s*,\s*GMT Time", text, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).strip()
    for fmt in ("%b %d, %Y %I:%M %p", "%d %b %Y %H:%M"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


# ---------- Pullers ----------
async def _pull_one(client, pool, symbol, url, anchors=None, unit_hint="per lb"):
    try:
        r = await client.get(url)
        r.raise_for_status()
        html = r.text

        # Prefer "Last Trade" box for comexlive commodity pages (not HOME)
        price = None
        if symbol.startswith("COMEX_") and "comexlive.org" in url and symbol != "COMEX_HOME":
            price = _extract_last_trade_price(html)

        # Fallback to older heuristic
        if price is None:
            price = _extract_near(html, anchors) if anchors else _first_float(html)

        if not price or price <= 0:
            return

        # normalize / unit adjustments
        price = _maybe_to_per_lb(symbol, float(price))

        # Hard sanity bands so we never poison reference_prices with junk
        sanity = {
            "COMEX_CU": (2.0, 10.0),        # copper $/lb
            "COMEX_AL": (0.2, 5.0),         # aluminum $/lb-ish
            "COMEX_ZN": (0.2, 5.0),         # zinc $/lb-ish
            "COMEX_AU": (500.0, 20000.0),   # gold $/oz
            "COMEX_AG": (5.0, 500.0),       # silver $/oz
            "COMEX_PL": (200.0, 20000.0),
            "COMEX_PA": (200.0, 20000.0),
        }
        lo_hi = sanity.get(symbol)
        if lo_hi:
            lo, hi = lo_hi
            if not (lo <= float(price) <= hi):
                # Do NOT insert suspicious values (e.g., 1.0 defaults)
                return

        ts = _market_ts(html)  # optional; DB insert uses ET trading date anyway
        await _insert_ref_price(pool, symbol, url, float(price), ts, html[:1200])

    except Exception:
        pass


async def pull_comexlive_once(pool):
    # Anchors are now only a fallback; Last Trade parsing is primary
    anchors_map = {
        "COMEX_CU": ["COPPER", "CU"],
        "COMEX_AL": ["ALUMINIUM", "ALUMINUM", "AL"],
        "COMEX_AU": ["GOLD", "XAU"],
        "COMEX_AG": ["SILVER", "XAG"],
        "COMEX_PL": ["PLATINUM"],
        "COMEX_PA": ["PALLADIUM"],
        "COMEX_ZN": ["ZINC", "ZN"],
        "COMEX_HOME": ["COPPER","ALUMINIUM","GOLD","SILVER","PLATINUM","PALLADIUM","ZINC"],
    }
    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS, follow_redirects=True) as client:
        for sym, url in COMEXLIVE.items():
            await _pull_one(client, pool, sym, url, anchors_map.get(sym))
            await asyncio.sleep(1.2)

async def pull_lme_once(pool):
    anchors_map = {
        "LME_PB": ["Lead", "LME Lead", "PB"],
        "LME_NI": ["Nickel", "LME Nickel", "NI"],
        "LME_ZN": ["Zinc", "LME Zinc", "ZN"],
    }
    async with httpx.AsyncClient(timeout=12.0, headers=HEADERS, follow_redirects=True) as client:
        for sym, url in LME.items():
            await _pull_one(client, pool, sym, url, anchors_map.get(sym))
            await asyncio.sleep(1.2)


# ---------- API helpers ----------
async def latest_price(pool, symbol: str):
    symbol = _norm_symbol(symbol)
    q = """
      SELECT price, ts_market, ts_server, source
      FROM reference_prices
      WHERE symbol = $1
      ORDER BY ts_market DESC, ts_server DESC
      LIMIT 1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(q, symbol)
    if not row:
        return None
    return {
        "symbol": symbol,
        "price": float(row["price"]),
        "ts_market": row["ts_market"].isoformat() if row["ts_market"] else None,
        "ts_server": row["ts_server"].isoformat(),
        "source": row["source"]
    }
