import concurrent.futures
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
import feedparser
import requests as std_requests

from pipelines.ingestion.streaming.sources.sentiment.shared.ingestion_state import apply_ingestion_policies
from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import (
    normalize_event,
)
from utils.number_utils import percent, to_int
from utils.source_config import get_sources_section, load_sources_config

logger = logging.getLogger(__name__)


# ─── Symbols ──────────────────────────────────────────────────────────────────

def _configured_symbols() -> list[str]:
    """Return the list of crypto symbols to track (e.g. BTC, ETH, SOL, BNB, XRP)."""
    cfg = load_sources_config()
    raw = cfg.get("symbols", ["BTC"])
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if str(s).strip()]
    return ["BTC"]


# ─── Per-symbol config helpers ────────────────────────────────────────────────

def _feeds_for_symbol(section: dict, symbol: str) -> list[str]:
    """Return deduplicated RSS feed URLs for *symbol*."""
    by_sym = section.get("feeds_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list):
        feeds = [str(f).strip() for f in raw if str(f).strip()]
        return list(dict.fromkeys(feeds))
    # Legacy fallback: flat feeds list
    raw_fallback = section.get("feeds", [])
    return list(dict.fromkeys(str(f).strip() for f in raw_fallback if str(f).strip()))


def _signal_keywords_for_symbol(section: dict, symbol: str) -> list[str]:
    """Return lower-cased signal keywords for *symbol*."""
    by_sym = section.get("signal_keywords_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list) and raw:
        return [str(k).strip().lower() for k in raw if str(k).strip()]
    # Legacy fallback: flat signal_keywords list
    raw_fallback = section.get("signal_keywords", [])
    return [str(k).strip().lower() for k in raw_fallback if str(k).strip()]


def _max_entries_per_feed(section: dict) -> int | None:
    raw_value = section.get("max_entries_per_feed")
    try:
        parsed = int(raw_value)
        return parsed if parsed > 0 else None
    except (TypeError, ValueError):
        return None


def news_interval_seconds() -> int:
    section = get_sources_section("news")
    try:
        return max(1, int(section.get("interval_seconds")))
    except (TypeError, ValueError):
        return 0


# ─── Engagement weights ───────────────────────────────────────────────────────

def _news_engagement_weights(section: dict) -> dict[str, float]:
    raw = section.get("engagement_weights")
    if not isinstance(raw, dict):
        return {}

    parsed: dict[str, float] = {}
    for key in ("comments", "keywords", "recency"):
        try:
            parsed[key] = max(0.0, float(raw.get(key, 0.0)))
        except (TypeError, ValueError):
            parsed[key] = 0.0

    total = sum(parsed.values())
    if total <= 0:
        return {}

    return {key: value / total for key, value in parsed.items()}


# ─── Text / ID / datetime helpers ────────────────────────────────────────────

def _strip_html(html_content: str) -> str:
    if not html_content:
        return ""
    try:
        return BeautifulSoup(html_content, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        return html_content


def _entry_id(entry: dict[str, Any], prefix: str = "") -> str:
    raw = entry.get("id") or entry.get("link") or entry.get("title") or str(time.time_ns())
    return hashlib.sha1(f"{prefix}{raw}".encode("utf-8")).hexdigest()


def _entry_datetime(entry: dict[str, Any]) -> datetime:
    now = datetime.now(timezone.utc)
    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed:
            try:
                return datetime.fromtimestamp(time.mktime(parsed), tz=timezone.utc)
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
            return datetime.fromisoformat(text).astimezone(timezone.utc)
        except ValueError:
            continue
    return now


def _keyword_hits(text: str, signal_keywords: list[str]) -> int:
    if not signal_keywords:
        return 0
    lowered = text.lower()
    return sum(1 for token in signal_keywords if token in lowered)


def _calculate_signals(
    text: str,
    published_dt: datetime,
    comment_count: int,
    signal_keywords: list[str],
) -> dict[str, float]:
    now = datetime.now(timezone.utc)
    age_hours = max(0.0, (now - published_dt).total_seconds() / 3600.0)
    recency_points = max(0.0, 100.0 - min(age_hours, 72.0) * (100.0 / 72.0))
    return {
        "comment_count": float(comment_count),
        "keyword_hits": float(_keyword_hits(text, signal_keywords)),
        "recency_points": recency_points,
    }


def _build_news_engagement(
    signals: list[dict[str, float]],
    index: int,
    weights: dict[str, float],
) -> int:
    signal = signals[index]
    total_comments = sum(item["comment_count"] for item in signals) or 1
    total_keywords = sum(item["keyword_hits"] for item in signals) or 1
    total_recency = sum(item["recency_points"] for item in signals) or 1

    comments_pct = percent(signal["comment_count"], total_comments)
    keywords_pct = percent(signal["keyword_hits"], total_keywords)
    recency_pct = percent(signal["recency_points"], total_recency)

    engagement = (
        (weights.get("comments", 0.0) * comments_pct)
        + (weights.get("keywords", 0.0) * keywords_pct)
        + (weights.get("recency", 0.0) * recency_pct)
    )
    return max(1, int(round(engagement)))


# ─── CryptoPanic scraper (per-symbol) ─────────────────────────────────────────

def _scrape_cryptopanic_for_symbol(
    symbol: str,
    section: dict,
) -> list[tuple[dict[str, Any], dict[str, float]]]:
    """Scrape CryptoPanic headlines filtered by *symbol*."""
    results = []
    max_entries = _max_entries_per_feed(section)
    signal_keywords = _signal_keywords_for_symbol(section, symbol)
    url = f"https://cryptopanic.com/news?currency={symbol}"

    try:
        response = cffi_requests.get(url, impersonate="safari15_5", timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("div", class_="news-row")
        if max_entries is not None:
            rows = rows[:max_entries]

        for row in rows:
            title_tag = row.find("a", class_="news-cell-title")
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            link = title_tag.get("href", "")
            if link.startswith("/"):
                link = f"https://cryptopanic.com{link}"

            time_tag = row.find("time")
            published_at = time_tag.get("datetime") if time_tag else datetime.now(timezone.utc).isoformat()

            votes_tag = row.find("span", class_="nc-votes-count")
            engagement_count = 1
            if votes_tag:
                try:
                    engagement_count = max(1, int(votes_tag.get_text(strip=True)))
                except ValueError:
                    pass

            post_data = {"id": link, "published_at": published_at}
            event = {
                "id": _entry_id(post_data, prefix=f"cp_{symbol}_"),
                "timestamp": published_at,
                "source": "cryptopanic_web",
                "text": title,
                "engagement": 1,
                "symbol": symbol,
            }

            try:
                norm_event = normalize_event(event)
                signals = _calculate_signals(
                    title, _entry_datetime(post_data), engagement_count, signal_keywords
                )
                results.append((norm_event, signals))
            except ValueError:
                continue
    except Exception as exc:
        logger.error("Failed to scrape CryptoPanic for symbol=%s: %s", symbol, exc)

    return results


# ─── RSS feed processor (per-symbol) ─────────────────────────────────────────

def _process_rss_feed_for_symbol(
    feed_url: str,
    symbol: str,
    section: dict,
) -> list[tuple[dict[str, Any], dict[str, float]]]:
    """Fetch and parse a single RSS *feed_url*, tagging each entry with *symbol*."""
    results = []
    entries = []
    max_entries = _max_entries_per_feed(section)
    signal_keywords = _signal_keywords_for_symbol(section, symbol)

    try:
        req = cffi_requests.get(feed_url, impersonate="safari15_5", timeout=15)
        parsed = feedparser.parse(req.content)
        if not getattr(parsed, "bozo", False) and parsed.entries:
            entries = parsed.entries
        else:
            raise ValueError("Direct fetch returned malformed or empty feed")
    except Exception:
        try:
            proxy_url = f"https://api.rss2json.com/v1/api.json?rss_url={feed_url}"
            proxy_req = std_requests.get(proxy_url, timeout=15)
            proxy_req.raise_for_status()
            data = proxy_req.json()
            if data.get("status") == "ok":
                for item in data.get("items", []):
                    entry = {
                        "title": item.get("title", ""),
                        "summary": item.get("description", ""),
                        "link": item.get("link", ""),
                        "published": item.get("pubDate", ""),
                        "author": item.get("author", ""),
                        "tags": [{"term": t} for t in item.get("categories", [])],
                        "comments": 1,
                    }
                    if item.get("content"):
                        entry["content"] = [{"value": item.get("content")}]
                    entries.append(entry)
        except Exception:
            return results

    for entry in (entries[:max_entries] if max_entries is not None else entries):
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        text = f"{title} {summary}".strip()
        if not title:
            continue

        full_content = ""
        content_list = entry.get("content", [])
        if content_list:
            full_content = _strip_html(content_list[0].get("value", ""))

        event = {
            "id": _entry_id(entry, prefix=f"{symbol}_"),
            "timestamp": entry.get("published") or entry.get("updated") or datetime.now(timezone.utc).isoformat(),
            "source": "rss",
            "text": text,
            "engagement": 1,
            "symbol": symbol,
        }

        try:
            norm_event = normalize_event(event)
            signals = _calculate_signals(
                text,
                _entry_datetime(entry),
                to_int(entry.get("slash_comments") or entry.get("comments", 1)),
                signal_keywords,
            )
            if full_content:
                norm_event["text"] = f"{norm_event['text']} {_strip_html(full_content)[:1500]}".strip()
            results.append((norm_event, signals))
        except ValueError:
            continue

    return results


# ─── Per-symbol news fetch ────────────────────────────────────────────────────

def _fetch_news_events_for_symbol(
    symbol: str,
    section: dict,
    weights: dict[str, float],
) -> list[dict[str, Any]]:
    """Fetch all news events (RSS + CryptoPanic) for a single *symbol*."""
    feeds = _feeds_for_symbol(section, symbol)
    items: list[dict[str, Any]] = []
    signals: list[dict[str, float]] = []

    if not feeds:
        logger.warning("No news feeds configured for symbol=%s — skipping.", symbol)
        return []

    max_workers = min(10, len(feeds) + 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all RSS feeds in parallel
        future_to_source = {
            executor.submit(_process_rss_feed_for_symbol, url, symbol, section): url
            for url in feeds
        }
        # Always scrape CryptoPanic per symbol
        future_cp = executor.submit(_scrape_cryptopanic_for_symbol, symbol, section)

        for future in concurrent.futures.as_completed(future_to_source):
            try:
                for norm_event, signal_data in future.result():
                    items.append(norm_event)
                    signals.append(signal_data)
            except Exception as exc:
                logger.debug("RSS feed error for symbol=%s: %s", symbol, exc)

        try:
            for norm_event, signal_data in future_cp.result():
                items.append(norm_event)
                signals.append(signal_data)
        except Exception as exc:
            logger.debug("CryptoPanic scrape error for symbol=%s: %s", symbol, exc)

    # Recalculate engagement scores using relative signal weights
    for idx, event in enumerate(items):
        event["engagement"] = _build_news_engagement(signals, idx, weights)

    return items


# ─── Public entry-point ───────────────────────────────────────────────────────

def fetch_news_events(
    lookback_minutes: int | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict[str, Any]]:
    """Fetch news / RSS events for ALL configured symbols and return a combined event list.

    Each event carries a ``symbol`` field (e.g. ``'BTC'``, ``'ETH'``) so that
    downstream bronze → silver → gold stages can group sentiment per coin.
    """
    section = get_sources_section("news")
    lookback_hours = section.get("lookback_hours", 6)
    weights = _news_engagement_weights(section)
    symbols = _configured_symbols()

    all_events: list[dict[str, Any]] = []
    for symbol in symbols:
        logger.info("Fetching news events for symbol=%s", symbol)
        symbol_events = _fetch_news_events_for_symbol(symbol, section, weights)
        logger.info("  -> fetched %d raw events for %s", len(symbol_events), symbol)
        all_events.extend(symbol_events)

    return apply_ingestion_policies(
        all_events,
        lookback_hours=lookback_hours,
        lookback_minutes=lookback_minutes,
        emit_current_timestamp=emit_current_timestamp,
    )
    
