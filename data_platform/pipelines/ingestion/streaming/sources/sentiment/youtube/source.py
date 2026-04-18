import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import normalize_event
from utils.env import load_env_file
from utils.number_utils import to_int
from utils.source_config import get_list_value, get_sources_section

load_env_file()


def _configured_channels() -> list[str]:
    section = get_sources_section("youtube")
    channel_ids = get_list_value(section, "channel_ids", [])
    return [cid for cid in channel_ids if cid.startswith("UC")]


def _configured_channel_limit() -> int:
    section = get_sources_section("youtube")
    raw_limit = section.get("max_comments_per_channel")
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return 0


def _youtube_client():
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        return None
    return build("youtube", "v3", developerKey=youtube_api_key)


def _video_like_percentage(youtube, video_id: str) -> float:
    response = youtube.videos().list(part="statistics", id=video_id).execute()
    items = response.get("items", [])
    if not items:
        return 0.0

    stats = items[0].get("statistics", {})
    like_count = to_int(stats.get("likeCount", 0))
    view_count = to_int(stats.get("viewCount", 0))
    if view_count <= 0:
        return 0.0
    return (like_count / view_count) * 100.0


def _list_comment_threads_for_channel(youtube, channel_id: str, max_results: int) -> list[dict]:
    items: list[dict] = []
    next_page_token: str | None = None

    while len(items) < max_results:
        response = youtube.commentThreads().list(
            part="snippet",
            allThreadsRelatedToChannelId=channel_id,
            searchTerms="bitcoin",
            maxResults=min(max_results - len(items), 100),
            order="time",
            pageToken=next_page_token,
        ).execute()

        page_items = response.get("items", [])
        if not page_items:
            break

        items.extend(page_items)
        if len(items) >= max_results:
            break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return items[:max_results]


def _fetch_channel_comments(youtube, channel_id: str, max_results: int) -> list[dict]:
    if youtube is None or max_results <= 0:
        return []

    thread_items = _list_comment_threads_for_channel(youtube, channel_id, max_results)
    if not thread_items:
        return []

    comments: list[dict] = []
    comment_rows = []
    video_like_cache: dict[str, float] = {}

    for item in thread_items:
        snippet = item.get("snippet", {})
        top_level = snippet.get("topLevelComment", {}).get("snippet", {})
        video_id = top_level.get("videoId")
        comment_like_count = to_int(top_level.get("likeCount", 0))
        comment_rows.append((item, top_level, comment_like_count, video_id))

        if video_id and video_id not in video_like_cache:
            video_like_cache[video_id] = _video_like_percentage(youtube, video_id)

    total_comment_likes = sum(row[2] for row in comment_rows)

    for item, comment, comment_like_count, video_id in comment_rows:
        comment_like_percent = 0.0
        if total_comment_likes > 0:
            comment_like_percent = (comment_like_count / total_comment_likes) * 100.0

        video_like_percent = 0.0
        if video_id:
            video_like_percent = video_like_cache.get(video_id, 0.0)

        payload = {
            "id": item.get("id", ""),
            "timestamp": comment.get("publishedAt"),
            "source": "youtube",
            "text": comment.get("textDisplay", ""),
            "engagement": int(round(video_like_percent + comment_like_percent)),
            "symbol": "BTC",
        }

        try:
            comments.append(normalize_event(payload))
        except ValueError:
            continue

    return comments


def fetch_youtube_events(max_results_per_channel: int | None = None) -> list[dict]:
    youtube = _youtube_client()
    if youtube is None:
        return []

    limit = max_results_per_channel or _configured_channel_limit()
    if limit <= 0:
        return []

    events: list[dict] = []

    channel_ids = _configured_channels()
    if not channel_ids:
        return []

    for channel_id in channel_ids:
        try:
            events.extend(_fetch_channel_comments(youtube, channel_id, limit))
        except HttpError:
            continue

    return events
