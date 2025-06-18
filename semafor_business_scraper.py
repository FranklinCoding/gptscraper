import os
from pathlib import Path
from urllib.parse import urljoin

from apscheduler.schedulers.blocking import BlockingScheduler

import openai
import requests
from bs4 import BeautifulSoup

SEEN_URLS_FILE = Path("seen_urls.txt")

BUSINESS_URL = "https://www.semafor.com/section/business"


# Slack configuration
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def fetch_articles():
    """Fetch article titles and URLs from Semafor's Business section."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(BUSINESS_URL, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    seen = set()
    articles = []

    # Look for headers that typically contain article links
    for header in soup.find_all(["h2", "h3"]):
        link = header.find("a", href=True)
        if not link:
            continue
        href = link["href"]
        title = link.get_text(strip=True)
        full_url = urljoin(BUSINESS_URL, href)
        if title and full_url not in seen:
            seen.add(full_url)
            articles.append((title, full_url))

    return articles


def fetch_article_text(url: str) -> str:
    """Return the full text for a Semafor article."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Many Semafor articles place the text inside <article>. Fallback to the
    # entire page if that tag is missing.
    container = soup.find("article") or soup

    paragraphs = [p.get_text(strip=True) for p in container.find_all("p")]
    return "\n".join(paragraphs)


def contains_ma_or_scoop(text: str) -> bool:
    """Return True if the article mentions M&A or claims an exclusive scoop.

    The function sends a concise prompt to OpenAI's API and expects "true" or
    "false" in response.
    """
    if not text.strip():
        return False

    openai.api_key = os.environ.get("OPENAI_API_KEY")
    system_msg = (
        "You classify business news. Reply 'true' if the text mentions a "
        "corporate merger or acquisition (including discussions or rumors) or "
        "claims to have an exclusive scoop. Otherwise reply 'false'."
    )
    user_msg = text

    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=1,
            temperature=0,
        )
        answer = resp.choices[0].message.get("content", "").strip().lower()
        return answer.startswith("t")
    except Exception:
        return False


def send_slack_alert(text: str) -> None:
    """Post a simple alert message to Slack using an incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        return

    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
    except Exception:
        pass


def scrape():
    seen_urls = set()
    if SEEN_URLS_FILE.exists():
        with SEEN_URLS_FILE.open("r", encoding="utf-8") as f:
            seen_urls = {line.strip() for line in f if line.strip()}

    new_urls = []
    for title, url in fetch_articles():
        if url in seen_urls:
            continue
        print(f"{title}\n{url}\n")
        seen_urls.add(url)
        new_urls.append(url)

        text = fetch_article_text(url)
        if contains_ma_or_scoop(text):
            send_slack_alert(f"Semafor M&A Alert\n{title}\n{url}")

    if new_urls:
        with SEEN_URLS_FILE.open("w", encoding="utf-8") as f:
            for url in sorted(seen_urls):
                f.write(url + "\n")


def main() -> None:
    """Run the scraper every minute using APScheduler."""
    scheduler = BlockingScheduler()
    scheduler.add_job(scrape, "interval", minutes=1)

    scrape()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
