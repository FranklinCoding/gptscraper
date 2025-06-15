# gptscraper

This repository contains simple utilities for scraping websites. The main script `semafor_business_scraper.py` retrieves article titles and URLs from [Semafor](https://www.semafor.com/) Business section.

## Usage

```bash
pip install -r requirements.txt  # install dependencies
python semafor_business_scraper.py
```

The script runs continuously, scraping the Business section once every minute
using APScheduler. Newly discovered URLs are printed and stored in
`seen_urls.txt` so they are only displayed once.

Each run prints the article title followed by its URL.

`semafor_business_scraper.py` also exposes a helper function `fetch_article_text(url)`
that returns the full text of a given Semafor article.

The module provides `contains_ma_or_scoop(text)` which uses OpenAI's API to
decide whether article text references mergers, acquisitions, or claims an
exclusive scoop. The function sends a short prompt directing the model to
answer with `true` if such language appears and `false` otherwise. Set the
`OPENAI_API_KEY` environment variable before calling this function.

When an article is flagged as mentioning M&A activity, the script sends an
email notification. Configure SMTP settings by defining the environment
variables `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`,
`EMAIL_FROM`, and `EMAIL_TO`.
