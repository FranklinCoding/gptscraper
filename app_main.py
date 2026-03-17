import asyncio
import hashlib
import os
import re
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
CACHE_DIR = BASE_DIR / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))
MAX_ALERTS = 500
HIGH_CONFIDENCE_ALERT = 80
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "7"))
MARKET_TZ = ZoneInfo("America/New_York")

RSS_SOURCES = [
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    ("MarketWatch", "https://feeds.marketwatch.com/marketwatch/topstories/"),
    (
        "SEC 8-K",
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom",
    ),
]
FINVIZ_NEWS_URL = "https://finviz.com/news.ashx"

COMMON_TICKERS = {
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "BRK.B", "JPM",
    "V", "XOM", "UNH", "JNJ", "WMT", "PG", "MA", "HD", "AVGO", "CVX", "MRK", "PEP",
    "KO", "COST", "BAC", "ABBV", "NFLX", "ADBE", "AMD", "CRM", "ORCL", "INTC", "CSCO",
}

BUY_WORDS = {
    "beat", "beats", "surge", "soar", "record", "upgrade", "upside", "growth", "strong",
    "profit", "bullish", "outperform", "buyback", "raised", "expands", "partnership",
}
SELL_WORDS = {
    "miss", "falls", "plunge", "downgrade", "fraud", "investigation", "cuts", "warning",
    "loss", "lawsuit", "bearish", "recall", "bankruptcy", "probe", "decline", "drops",
}

TICKER_REGEX = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)\b")
WORD_REGEX = re.compile(r"\b[A-Z]{2,5}(?:\.[A-Z])?\b")
NOISE_TOKENS = {"THE", "AND", "FOR", "WITH", "FROM", "THIS", "THAT", "NEWS", "INC", "CEO", "ETF"}
SYMBOL_HINT_REGEX = re.compile(r"(?:NYSE|NASDAQ|AMEX|OTC)[:\s]+([A-Z]{1,5}(?:\.[A-Z])?)")
MACRO_HINT_WORDS = ("s&p", "dow", "nasdaq", "stocks", "markets", "fed", "treasury", "wall street")

@dataclass
class Alert:
    id: str
    signal: str
    confidence: int
    ticker: str
    headline: str
    source: str
    url: str
    summary: str
    reason: str
    published_at: str
    created_at: str

    def dict(self) -> dict[str, Any]:
        return self.__dict__


class StockNewsEngine:
    def __init__(self) -> None:
        self.alerts: deque[Alert] = deque(maxlen=MAX_ALERTS)
        self.seen_hashes: set[str] = set()
        self.clients: list[asyncio.Queue] = []
        self.tickers = self._load_tickers()

    def _parse_published_at(self, value: str | None) -> datetime | None:
        if not value:
            return None

        value = value.strip()
        if not value:
            return None

        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    def _load_tickers(self) -> set[str]:
        # Lightweight local base set; can be expanded via env var path.
        tickers = set(COMMON_TICKERS)
        custom_path = os.environ.get("TICKER_FILE")
        if custom_path and Path(custom_path).exists():
            with open(custom_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    symbol = line.strip().upper()
                    if symbol:
                        tickers.add(symbol)

        # Best-effort bootstrap from SEC public ticker map.
        try:
            resp = httpx.get("https://www.sec.gov/files/company_tickers.json", timeout=8.0)
            if resp.status_code == 200:
                data = resp.json()
                for _, row in data.items():
                    symbol = str(row.get("ticker", "")).upper().strip()
                    if symbol:
                        tickers.add(symbol)
        except Exception:
            pass
        return tickers

    def _extract_tickers(self, text: str, url: str = "", symbols: list[str] | None = None) -> list[str]:
        found = {m.group(1) for m in TICKER_REGEX.finditer(text)}

        for m in SYMBOL_HINT_REGEX.finditer(text):
            found.add(m.group(1))

        if symbols:
            for sym in symbols:
                up = sym.upper().strip()
                if re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", up):
                    found.add(up)

        parsed = urlparse(url) if url else None
        if parsed:
            q = parse_qs(parsed.query)
            for key in ("t", "ticker", "symbol", "s"):
                for val in q.get(key, []):
                    up = val.upper().strip()
                    if re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", up):
                        found.add(up)

        for token in WORD_REGEX.findall(text):
            if token in self.tickers:
                found.add(token)

        cleaned = {t for t in found if t not in NOISE_TOKENS}
        return sorted(cleaned)

    def _score_sentiment(self, text: str) -> tuple[str, int, str]:
        lowered = text.lower()
        buy_hits = sum(word in lowered for word in BUY_WORDS)
        sell_hits = sum(word in lowered for word in SELL_WORDS)
        raw = buy_hits - sell_hits
        if raw > 0:
            signal = "BUY"
        elif raw < 0:
            signal = "SELL"
        else:
            signal = "NEUTRAL"
        confidence = min(95, max(45, 55 + abs(raw) * 10)) if signal != "NEUTRAL" else 50
        if signal == "BUY":
            reason = "Positive catalysts and language suggest favorable momentum."
        elif signal == "SELL":
            reason = "Negative risk language indicates potential downside pressure."
        else:
            reason = "Mixed or limited directional cues; maintain neutral stance."
        return signal, confidence, reason

    async def _fetch_rss(self, client: httpx.AsyncClient, source: str, url: str) -> list[dict[str, str]]:
        try:
            resp = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0 (compatible; gptscraper/1.0)"})
            resp.raise_for_status()
            parsed = feedparser.parse(resp.text)
            articles = []
            for e in parsed.entries[:20]:
                articles.append(
                    {
                        "source": source,
                        "headline": e.get("title", "").strip(),
                        "url": e.get("link", "").strip(),
                        "summary": BeautifulSoup(e.get("summary", ""), "html.parser").get_text(" ", strip=True),
                        "published_at": e.get("published", "") or e.get("updated", ""),
                        "symbols": [str(t.get("term", "")).upper() for t in e.get("tags", []) if t.get("term")],
                    }
                )
            return articles
        except Exception:
            return []

    async def _fetch_finviz(self, client: httpx.AsyncClient) -> list[dict[str, str]]:
        try:
            resp = await client.get(FINVIZ_NEWS_URL, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            articles: list[dict[str, str]] = []
            for link in soup.select("a.nn-tab-link")[:30]:
                headline = link.get_text(" ", strip=True)
                url = link.get("href", "")
                if not headline or not url:
                    continue
                articles.append(
                    {
                        "source": "Finviz",
                        "headline": headline,
                        "url": url,
                        "summary": "",
                        "published_at": "",
                    }
                )
            return articles
        except Exception:
            return []

    async def _fetch_all_sources(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            tasks = [self._fetch_rss(client, name, url) for name, url in RSS_SOURCES]
            tasks.append(self._fetch_finviz(client))
            groups = await asyncio.gather(*tasks)
        return [item for group in groups for item in group]

    async def _process_article(self, article: dict[str, str], broadcast: bool = True) -> None:
        signature = hashlib.sha256(f"{article['url']}|{article['headline']}".encode()).hexdigest()
        if signature in self.seen_hashes:
            return
        self.seen_hashes.add(signature)

        text_blob = f"{article['headline']} {article['summary']}"
        tickers = self._extract_tickers(text_blob, article.get("url", ""), article.get("symbols", []))
        if not tickers:
            lowered = text_blob.lower()
            if any(word in lowered for word in MACRO_HINT_WORDS):
                tickers = ["SPY"]
            else:
                return

        signal, confidence, reason = self._score_sentiment(text_blob)
        published_dt = self._parse_published_at(article.get("published_at")) or datetime.now(timezone.utc)
        published_iso = published_dt.isoformat()
        for ticker in tickers[:3]:
            alert = Alert(
                id=hashlib.md5(f"{signature}-{ticker}".encode()).hexdigest(),
                signal=signal,
                confidence=confidence,
                ticker=ticker,
                headline=article["headline"],
                source=article["source"],
                url=article["url"],
                summary=article["summary"] or "No summary provided.",
                reason=reason,
                published_at=published_iso,
                created_at=published_iso,
            )
            self.alerts.appendleft(alert)
            if broadcast:
                await self.broadcast({"type": "alert", "payload": alert.dict()})

    async def poll_once(self) -> None:
        articles = await self._fetch_all_sources()
        for article in articles:
            await self._process_article(article, broadcast=True)

    async def backfill_days(self, days: int = 7) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        articles = await self._fetch_all_sources()
        for article in articles:
            published_dt = self._parse_published_at(article.get("published_at"))
            if not published_dt:
                continue
            if published_dt < cutoff:
                continue
            await self._process_article(article, broadcast=False)
    async def broadcast(self, data: dict[str, Any]) -> None:
        stale = []
        for q in self.clients:
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                stale.append(q)
        for q in stale:
            self.clients.remove(q)

    async def event_generator(self):
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.clients.append(q)
        try:
            yield f"data: {JSONResponse({'type': 'bootstrap', 'payload': [a.dict() for a in list(self.alerts)[:100]]}).body.decode()}\n\n"
            while True:
                item = await q.get()
                yield f"data: {JSONResponse(item).body.decode()}\n\n"
        finally:
            if q in self.clients:
                self.clients.remove(q)

    def leaderboard(self) -> list[dict[str, Any]]:
        one_hour_ago = datetime.now(timezone.utc).timestamp() - 3600
        count = Counter()
        for alert in self.alerts:
            created_ts = datetime.fromisoformat(alert.created_at).timestamp()
            if created_ts >= one_hour_ago:
                count[alert.ticker] += 1
        return [{"ticker": t, "count": c} for t, c in count.most_common(12)]


engine = StockNewsEngine()
app = FastAPI(title="Real-Time Stock News Alert Dashboard")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_market_status(now_utc: datetime | None = None) -> str:
    now_utc = now_utc or datetime.now(timezone.utc)
    ny = now_utc.astimezone(MARKET_TZ)

    if ny.weekday() >= 5:
        return "CLOSED"

    minutes = ny.hour * 60 + ny.minute
    pre_start = 4 * 60
    market_open = 9 * 60 + 30
    market_close = 16 * 60
    after_close = 20 * 60

    if market_open <= minutes < market_close:
        return "OPEN"
    if pre_start <= minutes < market_open:
        return "PRE"
    if market_close <= minutes < after_close:
        return "AFTER"
    return "CLOSED"
@app.on_event("startup")
async def startup() -> None:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(engine.poll_once, "interval", seconds=POLL_SECONDS, max_instances=1)
    scheduler.start()
    await engine.backfill_days(BACKFILL_DAYS)
    await engine.poll_once()
    app.state.scheduler = scheduler


@app.on_event("shutdown")
async def shutdown() -> None:
    scheduler = getattr(app.state, "scheduler", None)
    if scheduler:
        scheduler.shutdown(wait=False)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/history")
async def history() -> dict[str, Any]:
    alerts = [a.dict() for a in list(engine.alerts)[:200]]
    return {"alerts": alerts, "leaderboard": engine.leaderboard(), "total": len(engine.alerts)}


@app.get("/api/meta")
async def meta() -> dict[str, Any]:
    return {
        "market_status": get_market_status(),
        "total_alerts": len(engine.alerts),
    }
@app.get("/api/stream")
async def stream(request: Request):
    async def generator():
        async for event in engine.event_generator():
            if await request.is_disconnected():
                break
            yield event

    return StreamingResponse(generator(), media_type="text/event-stream")
