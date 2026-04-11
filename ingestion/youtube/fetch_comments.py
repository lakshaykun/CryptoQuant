import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from utils.env import load_env_file
from utils.number_utils import to_int


# Ensure configs/app.env is loaded for local runs.
load_env_file()


def _youtube_client():
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        print("YOUTUBE_API_KEY not set; skipping YouTube comments fetch.")
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


def fetch_channel_comments(channel_id: str, max_results: int = 100) -> list[dict]:
    youtube = _youtube_client()
    if youtube is None or max_results <= 0:
        return []

    try:
        thread_items = _list_comment_threads_for_channel(youtube, channel_id, max_results)
        if not thread_items:
            return []

        comments = []
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

            engagement_score = int(round(video_like_percent + comment_like_percent))

            payload = {
                "id": item["id"],
                "timestamp": comment.get("publishedAt"),
                "source": "youtube",
                "text": comment.get("textDisplay", ""),
                "engagement": engagement_score,
                "symbol": "BTC",
            }

            comments.append(payload)

        return comments

    except HttpError as e:
        print(f"Channel comment fetch failed for channel {channel_id}: {e}")
        return []