import asyncio
import csv
import hashlib
import os
import re
from collections import Counter, deque
from dataclasses import asdict, dataclass
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
DATA_DIR = BASE_DIR / "data"
CACHE_DIR.mkdir(exist_ok=True)

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))
MAX_ALERTS = 500
HIGH_CONFIDENCE_ALERT = 80
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "60"))
EVAL_WINDOW_MINUTES = int(os.environ.get("EVAL_WINDOW_MINUTES", "60"))
EVAL_THRESHOLD_PCT = float(os.environ.get("EVAL_THRESHOLD_PCT", "0.25"))
MARKET_TZ = ZoneInfo("America/New_York")
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; gptscraper/1.0)"}

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
    "SPY", "QQQ", "DIA", "IWM",
}
COMMON_COMPANY_ALIASES = {
    "APPLE": "AAPL",
    "MICROSOFT": "MSFT",
    "NVIDIA": "NVDA",
    "AMAZON": "AMZN",
    "META": "META",
    "ALPHABET": "GOOGL",
    "TESLA": "TSLA",
    "ELI LILLY": "LLY",
    "LILLY": "LLY",
    "NEBIUS": "NBIS",
    "NEBIUS GROUP": "NBIS",
}

BUY_WORDS = {
    "beat", "beats", "surge", "soar", "record", "upgrade", "upside", "growth", "strong",
    "profit", "bullish", "outperform", "buyback", "raised", "expands", "partnership",
}
SELL_WORDS = {
    "miss", "falls", "plunge", "downgrade", "fraud", "investigation", "cuts", "warning",
    "loss", "lawsuit", "bearish", "recall", "bankruptcy", "probe", "decline", "drops",
}

TICKER_REGEX = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)\b(?!-\d)")
WORD_REGEX = re.compile(r"\b[A-Z]{2,5}(?:\.[A-Z])?\b(?!-\d)")
NOISE_TOKENS = {"THE", "AND", "FOR", "WITH", "FROM", "THIS", "THAT", "NEWS", "INC", "CEO", "ETF"}
GENERIC_ACRONYM_TOKENS = {
    "AI", "IPO", "EPS", "FDA", "SEC", "EV", "USA", "US", "Q1", "Q2", "Q3", "Q4", "FY", "YOY", "CPU", "GPU",
}
SYMBOL_HINT_REGEX = re.compile(r"(?:NYSE|NASDAQ|AMEX|OTC)[:\s]+([A-Z]{1,5}(?:\.[A-Z])?)")
MACRO_HINT_WORDS = (
    "s&p 500",
    "dow jones",
    "nasdaq composite",
    "nasdaq 100",
    "russell 2000",
    "treasury yields",
    "10-year treasury",
    "federal reserve",
    "fed decision",
    "fed minutes",
    "interest rates",
    "wall street",
)
INDEX_PROXY_TICKERS = {"SPY", "QQQ", "DIA", "IWM"}
FINAL_EVAL_STATES = {"correct", "incorrect", "flat", "unavailable", "skipped"}
NYSE_TICKER_FILE = DATA_DIR / "nyse_tickers.csv"


@dataclass
class AlertEvaluation:
    status: str
    target_at: str
    evaluated_at: str | None = None
    before_at: str | None = None
    after_at: str | None = None
    before_price: float | None = None
    after_price: float | None = None
    pct_change: float | None = None
    notes: str | None = None


@dataclass
class Candle:
    time: str
    open: float
    high: float
    low: float
    close: float


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
    model_score: int | None = None
    evaluation: AlertEvaluation | None = None
    intraday_chart: list[Candle] | None = None

    def dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.evaluation is None:
            data["evaluation"] = None
        return data


class StockNewsEngine:
    def __init__(self) -> None:
        self.alerts: deque[Alert] = deque(maxlen=MAX_ALERTS)
        self.alert_index: dict[str, Alert] = {}
        self.seen_hashes: set[str] = set()
        self.clients: list[asyncio.Queue] = []
        self.tickers, self.company_aliases = self._load_reference_data()
        self.model_stats: dict[str, dict[str, float]] = {
            "global": {"wins": 1.0, "trials": 2.0},
        }
        self.price_cache: dict[tuple[str, int, int], list[tuple[datetime, float]]] = {}

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

    def _chart_symbol(self, ticker: str) -> str:
        return ticker.replace(".", "-")

    def _normalize_company_name(self, value: str) -> str:
        value = value.upper()
        value = re.sub(r"[^A-Z0-9& ]+", " ", value)
        value = re.sub(r"\b(CORPORATION|CORP|COMPANY|CO|HOLDINGS|HOLDING|GROUP|INCORPORATED|INC|LIMITED|LTD|PLC|N V|NV|S A|SA)\b", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _load_reference_data(self) -> tuple[set[str], dict[str, str]]:
        tickers = set(COMMON_TICKERS)
        aliases = dict(COMMON_COMPANY_ALIASES)

        if NYSE_TICKER_FILE.exists():
            with open(NYSE_TICKER_FILE, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    symbol = str(row.get("ticker", "")).upper().strip()
                    company_name = self._normalize_company_name(str(row.get("company_name", "")).strip())
                    if symbol:
                        tickers.add(symbol)
                    if symbol and company_name and len(company_name) >= 4:
                        aliases[company_name] = symbol

        custom_path = os.environ.get("TICKER_FILE")
        if custom_path and Path(custom_path).exists():
            with open(custom_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    symbol = line.strip().upper()
                    if symbol:
                        tickers.add(symbol)

        try:
            resp = httpx.get("https://www.sec.gov/files/company_tickers.json", timeout=8.0, headers=HTTP_HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                for row in data.values():
                    symbol = str(row.get("ticker", "")).upper().strip()
                    if symbol:
                        tickers.add(symbol)
                        title = self._normalize_company_name(str(row.get("title", "")).strip())
                        if title and len(title) >= 4:
                            aliases[title] = symbol
        except Exception:
            pass
        return tickers, aliases

    def _is_probable_macro_story(self, text: str, url: str = "") -> bool:
        lowered = f"{text} {url}".lower()
        macro_hits = sum(phrase in lowered for phrase in MACRO_HINT_WORDS)
        index_mentions = sum(token.lower() in lowered for token in ("spy", "qqq", "dia", "iwm"))
        return macro_hits + index_mentions >= 1

    def _extract_company_alias_tickers(self, text: str) -> set[str]:
        normalized = self._normalize_company_name(text)
        if not normalized:
            return set()
        found: set[str] = set()
        for alias, ticker in self.company_aliases.items():
            if alias in normalized:
                found.add(ticker)
        return found

    def _extract_tickers(self, text: str, url: str = "", symbols: list[str] | None = None) -> list[str]:
        explicit = {m.group(1) for m in TICKER_REGEX.finditer(text)}

        for m in SYMBOL_HINT_REGEX.finditer(text):
            explicit.add(m.group(1))

        if symbols:
            for sym in symbols:
                up = sym.upper().strip()
                if re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", up):
                    explicit.add(up)

        parsed = urlparse(url) if url else None
        if parsed:
            q = parse_qs(parsed.query)
            for key in ("t", "ticker", "symbol", "s"):
                for val in q.get(key, []):
                    up = val.upper().strip()
                    if re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", up):
                        explicit.add(up)

        found = set(explicit)
        if not found:
            found.update(self._extract_company_alias_tickers(text))

        if not found:
            for token in WORD_REGEX.findall(text):
                if token in self.tickers and token not in GENERIC_ACRONYM_TOKENS:
                    found.add(token)

        cleaned = {token for token in found if token not in NOISE_TOKENS}
        ordered = sorted(cleaned)
        if len(ordered) == 1 and ordered[0] in INDEX_PROXY_TICKERS and not self._is_probable_macro_story(text, url):
            return []
        return ordered

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

    def _stat_bucket(self, key: str) -> dict[str, float]:
        return self.model_stats.setdefault(key, {"wins": 1.0, "trials": 2.0})

    def _estimate_model_score(self, signal: str, source: str, ticker: str) -> int:
        weighted_total = 0.0
        weight_sum = 0.0
        for key, multiplier in (
            ("global", 1.0),
            (f"signal:{signal}", 1.5),
            (f"source:{source}|signal:{signal}", 2.0),
            (f"ticker:{ticker}|signal:{signal}", 2.5),
        ):
            bucket = self.model_stats.get(key)
            if not bucket:
                continue
            trials = max(bucket["trials"], 1.0)
            accuracy = bucket["wins"] / trials
            weight = min(trials, 20.0) * multiplier
            weighted_total += accuracy * weight
            weight_sum += weight

        if not weight_sum:
            return 50
        return round((weighted_total / weight_sum) * 100)

    def _record_model_result(self, alert: Alert, status: str) -> None:
        if alert.signal not in {"BUY", "SELL"}:
            return

        reward = 1.0 if status == "correct" else 0.5 if status == "flat" else 0.0
        for key in (
            "global",
            f"signal:{alert.signal}",
            f"source:{alert.source}|signal:{alert.signal}",
            f"ticker:{alert.ticker}|signal:{alert.signal}",
        ):
            bucket = self._stat_bucket(key)
            bucket["wins"] += reward
            bucket["trials"] += 1.0

    async def _fetch_rss(self, client: httpx.AsyncClient, source: str, url: str) -> list[dict[str, Any]]:
        try:
            resp = await client.get(url, timeout=10, headers=HTTP_HEADERS)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.text)
            articles = []
            for entry in parsed.entries[:20]:
                articles.append(
                    {
                        "source": source,
                        "headline": entry.get("title", "").strip(),
                        "url": entry.get("link", "").strip(),
                        "summary": BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(" ", strip=True),
                        "published_at": entry.get("published", "") or entry.get("updated", ""),
                        "symbols": [str(tag.get("term", "")).upper() for tag in entry.get("tags", []) if tag.get("term")],
                    }
                )
            return articles
        except Exception:
            return []

    async def _fetch_finviz(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        try:
            resp = await client.get(FINVIZ_NEWS_URL, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            articles: list[dict[str, Any]] = []
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
                        "symbols": [],
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

    async def _fetch_price_series(
        self,
        ticker: str,
        start_dt: datetime,
        end_dt: datetime,
    ) -> list[dict[str, Any]]:
        chart_symbol = self._chart_symbol(ticker)
        window_start = int((start_dt - timedelta(hours=2)).timestamp())
        window_end = int((end_dt + timedelta(hours=2)).timestamp())
        cache_key = (chart_symbol, window_start, window_end)
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{chart_symbol}"
        params = {
            "interval": "5m",
            "includePrePost": "true",
            "period1": str(window_start),
            "period2": str(window_end),
        }

        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                resp = await client.get(url, params=params, timeout=12, headers=HTTP_HEADERS)
                resp.raise_for_status()
                payload = resp.json()
        except Exception:
            return []

        results = payload.get("chart", {}).get("result") or []
        if not results:
            return []

        result = results[0]
        timestamps = result.get("timestamp") or []
        quotes = (result.get("indicators") or {}).get("quote") or [{}]
        quote = quotes[0]
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []

        series: list[dict[str, Any]] = []
        for raw_ts, raw_open, raw_high, raw_low, raw_close in zip(timestamps, opens, highs, lows, closes):
            if None in (raw_open, raw_high, raw_low, raw_close):
                continue
            point_dt = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
            series.append(
                {
                    "time": point_dt,
                    "open": float(raw_open),
                    "high": float(raw_high),
                    "low": float(raw_low),
                    "close": float(raw_close),
                }
            )

        self.price_cache[cache_key] = series
        return series

    async def _build_intraday_chart(self, ticker: str, alert_dt: datetime) -> list[Candle]:
        day_start = alert_dt.astimezone(MARKET_TZ).replace(hour=4, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        day_end = alert_dt.astimezone(MARKET_TZ).replace(hour=20, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        series = await self._fetch_price_series(ticker, day_start, day_end)
        candles: list[Candle] = []
        for point in series:
            point_dt = point["time"]
            if point_dt < day_start or point_dt > day_end:
                continue
            candles.append(
                Candle(
                    time=point_dt.isoformat(),
                    open=round(point["open"], 4),
                    high=round(point["high"], 4),
                    low=round(point["low"], 4),
                    close=round(point["close"], 4),
                )
            )
        return candles[-96:]

    async def _evaluate_alert(self, alert: Alert, broadcast: bool) -> None:
        created_dt = datetime.fromisoformat(alert.created_at)
        before_target_dt = created_dt - timedelta(minutes=EVAL_WINDOW_MINUTES)
        target_dt = created_dt + timedelta(minutes=EVAL_WINDOW_MINUTES)
        target_iso = target_dt.isoformat()

        if datetime.now(timezone.utc) < target_dt:
            alert.evaluation = AlertEvaluation(status="pending", target_at=target_iso, notes="Waiting for post-alert price window.")
            return

        prior_status = alert.evaluation.status if alert.evaluation else None
        series = await self._fetch_price_series(alert.ticker, before_target_dt, target_dt)
        if not series:
            alert.evaluation = AlertEvaluation(status="unavailable", target_at=target_iso, notes="Price series unavailable for ticker.")
            return

        before_points = [point for point in series if point["time"] <= before_target_dt]
        after_points = [point for point in series if point["time"] >= target_dt]
        before_candidates = [point["close"] for point in before_points]
        after_candidates = [point["close"] for point in after_points]
        if not before_candidates or not after_candidates:
            alert.evaluation = AlertEvaluation(status="unavailable", target_at=target_iso, notes="Not enough candles around alert time.")
            return

        before_price = before_candidates[-1]
        after_price = after_candidates[0]
        before_dt = before_points[-1]["time"]
        after_dt = after_points[0]["time"]
        pct_change = ((after_price - before_price) / before_price) * 100

        if abs(pct_change) < EVAL_THRESHOLD_PCT:
            status = "flat"
        elif alert.signal == "BUY":
            status = "correct" if pct_change > 0 else "incorrect"
        else:
            status = "correct" if pct_change < 0 else "incorrect"

        alert.evaluation = AlertEvaluation(
            status=status,
            target_at=target_iso,
            evaluated_at=datetime.now(timezone.utc).isoformat(),
            before_at=before_dt.isoformat(),
            after_at=after_dt.isoformat(),
            before_price=round(before_price, 4),
            after_price=round(after_price, 4),
            pct_change=round(pct_change, 3),
            notes=f"Compared price {EVAL_WINDOW_MINUTES} minutes before and after the alert.",
        )

        if prior_status not in FINAL_EVAL_STATES and status in {"correct", "incorrect", "flat"}:
            self._record_model_result(alert, status)
            if broadcast:
                await self.broadcast({"type": "alert_update", "payload": alert.dict()})

    def _store_alert(self, alert: Alert) -> None:
        self.alert_index[alert.id] = alert
        self.alerts.appendleft(alert)

    async def _process_article(self, article: dict[str, Any], broadcast: bool = True) -> None:
        signature = hashlib.sha256(f"{article['url']}|{article['headline']}".encode()).hexdigest()
        if signature in self.seen_hashes:
            return
        self.seen_hashes.add(signature)

        text_blob = f"{article['headline']} {article['summary']}"
        tickers = self._extract_tickers(text_blob, article.get("url", ""), article.get("symbols", []))
        if not tickers:
            if self._is_probable_macro_story(text_blob, article.get("url", "")):
                tickers = ["SPY"]
            else:
                return

        signal, confidence, reason = self._score_sentiment(text_blob)
        if signal == "NEUTRAL":
            return
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
                model_score=self._estimate_model_score(signal, article["source"], ticker),
                intraday_chart=await self._build_intraday_chart(ticker, published_dt),
            )
            await self._evaluate_alert(alert, broadcast=False)
            self._store_alert(alert)
            if broadcast:
                await self.broadcast({"type": "alert", "payload": alert.dict()})

    async def poll_once(self) -> None:
        articles = await self._fetch_all_sources()
        for article in articles:
            await self._process_article(article, broadcast=True)

    async def backfill_days(self, days: int = 60) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        articles = await self._fetch_all_sources()
        for article in articles:
            published_dt = self._parse_published_at(article.get("published_at"))
            if not published_dt or published_dt < cutoff:
                continue
            await self._process_article(article, broadcast=False)

    async def evaluate_pending_alerts(self) -> None:
        for alert in list(self.alerts):
            if alert.evaluation and alert.evaluation.status in FINAL_EVAL_STATES:
                continue
            await self._evaluate_alert(alert, broadcast=True)

    async def broadcast(self, data: dict[str, Any]) -> None:
        stale = []
        for queue in self.clients:
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self.clients.remove(queue)

    async def event_generator(self):
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.clients.append(queue)
        try:
            payload = [alert.dict() for alert in list(self.alerts)[:100]]
            yield f"data: {JSONResponse({'type': 'bootstrap', 'payload': payload}).body.decode()}\n\n"
            while True:
                item = await queue.get()
                yield f"data: {JSONResponse(item).body.decode()}\n\n"
        finally:
            if queue in self.clients:
                self.clients.remove(queue)

    def leaderboard(self) -> list[dict[str, Any]]:
        one_hour_ago = datetime.now(timezone.utc).timestamp() - 3600
        count = Counter()
        for alert in self.alerts:
            created_ts = datetime.fromisoformat(alert.created_at).timestamp()
            if created_ts >= one_hour_ago:
                count[alert.ticker] += 1
        return [{"ticker": ticker, "count": count_value} for ticker, count_value in count.most_common(12)]

    def evaluation_summary(self) -> dict[str, Any]:
        evaluated = [
            alert for alert in self.alerts
            if alert.signal in {"BUY", "SELL"} and alert.evaluation and alert.evaluation.status in {"correct", "incorrect", "flat"}
        ]
        correct = sum(alert.evaluation.status == "correct" for alert in evaluated if alert.evaluation)
        flat = sum(alert.evaluation.status == "flat" for alert in evaluated if alert.evaluation)
        pending = sum(
            1
            for alert in self.alerts
            if alert.signal in {"BUY", "SELL"} and (not alert.evaluation or alert.evaluation.status == "pending")
        )
        accuracy = round((correct / len(evaluated)) * 100, 1) if evaluated else None
        return {
            "evaluated": len(evaluated),
            "correct": correct,
            "flat": flat,
            "pending": pending,
            "accuracy_pct": accuracy,
            "window_minutes": EVAL_WINDOW_MINUTES,
            "threshold_pct": EVAL_THRESHOLD_PCT,
        }


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
    scheduler.add_job(engine.evaluate_pending_alerts, "interval", seconds=300, max_instances=1)
    scheduler.start()
    await engine.backfill_days(BACKFILL_DAYS)
    await engine.poll_once()
    await engine.evaluate_pending_alerts()
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
    alerts = [alert.dict() for alert in list(engine.alerts)[:200]]
    return {
        "alerts": alerts,
        "leaderboard": engine.leaderboard(),
        "total": len(engine.alerts),
        "evaluation_summary": engine.evaluation_summary(),
    }


@app.get("/api/meta")
async def meta() -> dict[str, Any]:
    return {
        "market_status": get_market_status(),
        "total_alerts": len(engine.alerts),
        "evaluation_summary": engine.evaluation_summary(),
    }


@app.get("/api/stream")
async def stream(request: Request):
    async def generator():
        async for event in engine.event_generator():
            if await request.is_disconnected():
                break
            yield event

    return StreamingResponse(generator(), media_type="text/event-stream")
