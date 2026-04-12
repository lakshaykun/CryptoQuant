import concurrent.futures
import hashlib
import logging
import time
from datetime import UTC, datetime
from typing import Any

from curl_cffi import requests as cffi_requests # Used to bypass Cloudflare WAF
import requests as std_requests               # Used for the ISP proxy fallback
import feedparser
from bs4 import BeautifulSoup

from ingestion.common.engagement_utils import normalized_weights
from ingestion.common.kafka_producer import send
from ingestion.common.schemas import normalize_event
from utils.env import get_env
from utils.number_utils import percent, to_int
from utils.source_config import get_list_value, get_sources_section

logger = logging.getLogger(__name__)

# --- Configuration Constants ---
DEFAULT_FEEDS = [
    "https://bitcoinmagazine.com/.rss/full/",
    "https://news.bitcoin.com/feed/",
    "https://cointelegraph.com/rss/",           
    "https://www.coindesk.com/arc/outboundfeeds/rss/", 
    "https://decrypt.co/feed",                 
    "https://cryptoslate.com/feed/",           
    "https://blockworks.co/feed"               
]

DEFAULT_SYMBOL = "BTC"
DEFAULT_INTERVAL_SECONDS = 120
MAX_ENTRIES_PER_FEED = 25
KAFKA_TOPIC = "btc_news"
NEWS_SIGNAL_KEYWORDS = [
    "btc", "bitcoin", "etf", "fed", "halving", "breakout", "crash", "inflation", "whale", "liquidation"
]
DEFAULT_NEWS_ENGAGEMENT_WEIGHTS = {
    "comments": 0.45,
    "keywords": 0.25,
    "recency": 0.30,
}

# --- Helpers ---

def _strip_html(html_content: str) -> str:
    """Removes HTML tags from full article text to keep Kafka payloads clean."""
    if not html_content:
        return ""
    try:
        return BeautifulSoup(html_content, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        return html_content

def _configured_feeds() -> list[str]:
    section = get_sources_section("news")
    feeds = get_list_value(section, "feeds", [])
    if feeds:
        # Remove duplicates while preserving order
        return list(dict.fromkeys(feeds))
    raw = get_env("NEWS_FEEDS", default=",".join(DEFAULT_FEEDS))
    return list(dict.fromkeys([item.strip() for item in raw.split(",") if item.strip()])) or DEFAULT_FEEDS

def _news_symbol() -> str:
    section = get_sources_section("news")
    return str(section.get("currency", DEFAULT_SYMBOL)).strip() or DEFAULT_SYMBOL

def _news_interval_seconds() -> int:
    section = get_sources_section("news")
    try:
        return max(1, int(section.get("interval_seconds", DEFAULT_INTERVAL_SECONDS)))
    except (TypeError, ValueError):
        return DEFAULT_INTERVAL_SECONDS

def _news_engagement_weights() -> dict[str, float]:
    section = get_sources_section("news")
    raw = section.get("engagement_weights", {})
    if not isinstance(raw, dict):
        return DEFAULT_NEWS_ENGAGEMENT_WEIGHTS.copy()
    return normalized_weights(raw, DEFAULT_NEWS_ENGAGEMENT_WEIGHTS)

def _entry_id(entry: dict[str, Any], prefix: str = "") -> str:
    raw = entry.get("id") or entry.get("link") or entry.get("title") or str(time.time_ns())
    return hashlib.sha1(f"{prefix}{raw}".encode("utf-8")).hexdigest()

def _entry_datetime(entry: dict[str, Any]) -> datetime:
    now = datetime.now(UTC)
    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed:
            try:
                return datetime.fromtimestamp(time.mktime(parsed), tz=UTC)
            except (TypeError, ValueError, OverflowError):
                continue
    for key in ("published", "updated", "published_at"):
        raw = entry.get(key)
        if not raw:
            continue
        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).astimezone(UTC)
        except ValueError:
            continue
    return now

def _keyword_hits(text: str) -> int:
    lowered = text.lower()
    return sum(1 for token in NEWS_SIGNAL_KEYWORDS if token in lowered)

def _calculate_signals(text: str, published_dt: datetime, comment_count: int) -> dict[str, float]:
    now = datetime.now(UTC)
    age_hours = max(0.0, (now - published_dt).total_seconds() / 3600.0)
    recency_points = max(0.0, 100.0 - min(age_hours, 72.0) * (100.0 / 72.0))
    return {
        "comment_count": float(comment_count),
        "keyword_hits": float(_keyword_hits(text)),
        "recency_points": recency_points,
    }

# --- CryptoPanic Scraper Integration ---

def _scrape_cryptopanic() -> list[tuple[dict[str, Any], dict[str, float]]]:
    """Scrapes max data from CryptoPanic HTML using browser impersonation."""
    results = []
    symbol = _news_symbol()
    url = f"https://cryptopanic.com/news?currency={symbol}"
    
    try:
        # Using curl_cffi to bypass Cloudflare (impersonating Safari usually works well)
        response = cffi_requests.get(url, impersonate="safari15_5", timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        news_rows = soup.find_all("div", class_="news-row")[:MAX_ENTRIES_PER_FEED]
        
        for row in news_rows:
            title_tag = row.find("a", class_="news-cell-title")
            if not title_tag:
                continue
            
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href", "")
            if link.startswith("/"):
                link = f"https://cryptopanic.com{link}"
            
            time_tag = row.find("time")
            published_at = time_tag.get("datetime") if time_tag else datetime.now(UTC).isoformat()
            
            # Extract Original Source & Tags
            source_tag = row.find("span", class_="news-source-name")
            original_domain = source_tag.get_text(strip=True) if source_tag else "Unknown"
            
            currency_tags = row.find_all("span", class_="currency-code")
            extracted_tags = [t.get_text(strip=True) for t in currency_tags]

            engagement_count = 1
            votes_tag = row.find("span", class_="nc-votes-count")
            if votes_tag:
                try:
                    engagement_count = max(1, int(votes_tag.get_text(strip=True)))
                except ValueError:
                    pass

            post_data = {"id": link, "published_at": published_at}
            published_dt = _entry_datetime(post_data)

            event = {
                "id": _entry_id(post_data, prefix="cp_scrape_"),
                "timestamp": published_at,
                "source": "cryptopanic_web",
                "text": title,
                "engagement": 1,
                "symbol": symbol,
                "metadata": {
                    "url": link,
                    "original_domain": original_domain,
                    "crypto_tags": extracted_tags
                }
            }

            try:
                norm_event = normalize_event(event)
                signals = _calculate_signals(title, published_dt, engagement_count)
                results.append((norm_event, signals))
            except ValueError as e:
                logger.debug("Skipping invalid CryptoPanic event: %s", e)
                
    except Exception as e:
        logger.error("Failed to scrape CryptoPanic: %s", e)
        
    return results

# --- RSS Fetching Logic with ISP Bypass ---

def _process_rss_feed(feed_url: str) -> list[tuple[dict[str, Any], dict[str, float]]]:
    """Fetches maximum available metadata from RSS feeds, with an ISP bypass proxy fallback."""
    results = []
    entries = []

    try:
        # Attempt 1: Direct connection using curl_cffi to bypass Cloudflare
        req = cffi_requests.get(feed_url, impersonate="safari15_5", timeout=15)
        parsed = feedparser.parse(req.content)
        
        if not getattr(parsed, "bozo", False) and parsed.entries:
            entries = parsed.entries
        else:
            raise ValueError("Direct fetch returned malformed or empty feed")
            
    except Exception as e:
        logger.debug("Direct fetch failed for %s (%s). Attempting Proxy Fallback...", feed_url, e)
        try:
            # Attempt 2: Proxy Fallback via rss2json (Bypasses Indian ISP Connection Resets)
            proxy_url = f"https://api.rss2json.com/v1/api.json?rss_url={feed_url}"
            proxy_req = std_requests.get(proxy_url, timeout=15)
            proxy_req.raise_for_status()
            data = proxy_req.json()
            
            if data.get("status") == "ok":
                # Convert JSON proxy response into feedparser-like dicts
                for item in data.get("items", []):
                    entry = {
                        "title": item.get("title", ""),
                        "summary": item.get("description", ""),
                        "link": item.get("link", ""),
                        "published": item.get("pubDate", ""),
                        "author": item.get("author", ""),
                        "tags": [{"term": t} for t in item.get("categories", [])],
                        "comments": 1
                    }
                    if item.get("content"):
                        entry["content"] = [{"value": item.get("content")}]
                    if item.get("enclosure", {}).get("link"):
                        entry["media_content"] = [{"url": item.get("enclosure", {}).get("link")}]
                    entries.append(entry)
            else:
                logger.error("Proxy returned error for %s: %s", feed_url, data.get("message"))
        except Exception as proxy_e:
            logger.error("Both direct and proxy fetches failed for %s: %s", feed_url, proxy_e)
            return results

    # Process all successfully fetched entries
    for entry in entries[:MAX_ENTRIES_PER_FEED]:
        title = entry.get('title', '')
        summary = entry.get('summary', '')
        text = f"{title} {summary}".strip()
        if not title:
            continue

        # Extract Full Content (stripped of HTML to save Kafka bandwidth)
        full_content = ""
        content_list = entry.get("content", [])
        if content_list and len(content_list) > 0:
            full_content = _strip_html(content_list[0].get("value", ""))
        
        # Extract Author & Tags
        author = entry.get("author", "")
        tags = [tag.get("term", "") for tag in entry.get("tags", [])]
        
        # Extract Images
        image_url = ""
        media_list = entry.get("media_content", [])
        if media_list and len(media_list) > 0:
            image_url = media_list[0].get("url", "")
        elif entry.get("links"):
            for link_item in entry.get("links", []):
                if link_item.get("type", "").startswith("image/"):
                    image_url = link_item.get("href", "")
                    break

        event = {
            "id": _entry_id(entry),
            "timestamp": entry.get("published") or entry.get("updated") or datetime.now(UTC).isoformat(),
            "source": "rss",
            "text": text,
            "engagement": 1,
            "symbol": _news_symbol(),
            "metadata": {
                "url": entry.get("link", ""),
                "author": author,
                "tags": tags,
                "image_url": image_url,
                "full_content": full_content[:2000] # Cap length to avoid massive Kafka payloads
            }
        }

        try:
            norm_event = normalize_event(event)
            signals = _calculate_signals(
                text, 
                _entry_datetime(entry), 
                to_int(entry.get("slash_comments") or entry.get("comments", 1))
            )
            results.append((norm_event, signals))
        except ValueError:
            continue
            
    return results

# --- Main Logic ---

def _build_news_engagement(signals: list[dict[str, float]], index: int, weights: dict[str, float]) -> int:
    signal = signals[index]
    total_comments = sum(item["comment_count"] for item in signals) or 1
    total_keywords = sum(item["keyword_hits"] for item in signals) or 1
    total_recency = sum(item["recency_points"] for item in signals) or 1

    comments_pct = percent(signal["comment_count"], total_comments)
    keywords_pct = percent(signal["keyword_hits"], total_keywords)
    recency_pct = percent(signal["recency_points"], total_recency)

    engagement = (
        (weights["comments"] * comments_pct)
        + (weights["keywords"] * keywords_pct)
        + (weights["recency"] * recency_pct)
    )
    return max(1, int(round(engagement)))

def fetch_news_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    signals: list[dict[str, float]] = []
    weights = _news_engagement_weights()
    feeds = _configured_feeds()

    # Utilize ThreadPoolExecutor to run HTTP requests concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(feeds) + 1)) as executor:
        future_to_source = {executor.submit(_process_rss_feed, url): url for url in feeds}
        future_cp = executor.submit(_scrape_cryptopanic)
        
        for future in concurrent.futures.as_completed(future_to_source):
            for norm_event, signal_data in future.result():
                items.append(norm_event)
                signals.append(signal_data)
                
        for norm_event, signal_data in future_cp.result():
            items.append(norm_event)
            signals.append(signal_data)

    # Apply normalized engagement scores across all sources
    for idx, event in enumerate(items):
        event["engagement"] = _build_news_engagement(signals, idx, weights)

    return items

def publish_news() -> int:
    sent = 0
    start_time = time.time()
    items = fetch_news_items()
    
    logger.info("Total normalized items fetched: %d in %.2fs", len(items), time.time() - start_time)

    for item in items:
        try:
            send(KAFKA_TOPIC, item)
            sent += 1
        except Exception as e:
            logger.error("Failed publishing item %s: %s", item.get("id"), e)

    logger.info("Published %d new items successfully.", sent)
    return sent

def run_forever(interval_seconds: int | None = None):
    interval = interval_seconds or _news_interval_seconds()
    logger.info("Starting concurrent news producer with %d second interval", interval)
    while True:
        try:
            publish_news()
        except Exception as e:
            logger.exception("Producer loop failed: %s", e)
        time.sleep(interval)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    run_forever()