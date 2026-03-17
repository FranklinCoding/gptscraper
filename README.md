# gptscraper

This repository includes a **real-time stock news alert dashboard** powered by FastAPI + Server-Sent Events (SSE).

## What it does

- Polls public financial news every 15 seconds (configurable via `POLL_SECONDS`):
  - Finviz News
  - Yahoo Finance RSS
  - MarketWatch RSS
  - SEC EDGAR 8-K Atom feed
- Extracts ticker mentions with regex + ticker whitelist.
- Scores sentiment from headline + summary and emits:
  - `BUY`
  - `SELL`
- Streams live alerts to a dark terminal-style dashboard.
- Deduplicates stories by URL/headline hash.
- Keeps the latest 500 alerts in memory.
- Calculates real U.S. market status (PRE / OPEN / AFTER / CLOSED) using New York time.
- Backfills and scores up to the last 60 days of RSS articles at startup (`BACKFILL_DAYS`).
- Tracks post-alert price action and reports whether `BUY` / `SELL` calls were directionally correct.
- Learns rolling accuracy from prior outcomes to estimate per-alert model confidence.
- Includes a disclaimer for informational use.

## Run locally

```bash
pip install -r requirements.txt
uvicorn app_main:app --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000`.

Health check:

```bash
curl http://localhost:8000/healthz
```

---

## How to get this live (production checklist)

1. **Choose hosting (Render recommended)**
   - This repo includes `render.yaml` for one-click setup.
2. **Push repo to GitHub**
   - Render will deploy from your default branch.
3. **Create Web Service on Render**
   - Build command: `pip install -r requirements.txt`
   - Start command: `uvicorn app_main:app --host 0.0.0.0 --port $PORT`
4. **Set environment variables**
   - `POLL_SECONDS=15` (or higher to reduce source load)
   - `BACKFILL_DAYS=60` (how much historical RSS content to evaluate on boot)
   - `EVAL_WINDOW_MINUTES=60` (how long after an alert to judge price reaction)
   - `EVAL_THRESHOLD_PCT=0.25` (minimum move required to call a signal directional)
   - Optional: `TICKER_FILE=/opt/render/project/src/data/tickers.txt`
5. **Verify after deploy**
   - Check `/healthz`
   - Open `/` and confirm alerts stream.
6. **Operational improvements (recommended)**
   - Move in-memory alerts to SQLite/Redis for restart persistence.
   - Add structured logging + error monitoring (Sentry).
   - Add source-specific rate limiting/backoff.
   - Add authenticated admin page for tuning signal thresholds.

---

## Existing Semafor scraper

The original `semafor_business_scraper.py` script is still available:

```bash
python semafor_business_scraper.py
```
