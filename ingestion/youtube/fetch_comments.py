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


def fetch_video_comments(video_id: str, max_results: int = 50) -> list[dict]:
    youtube = _youtube_client()
    if youtube is None:
        return []

    try:
        video_like_percent = _video_like_percentage(youtube, video_id)

        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            order="time"
        ).execute()

        comments = []
        comment_rows = []

        for item in response.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_like_count = to_int(comment.get("likeCount", 0))
            comment_rows.append((item, comment, comment_like_count))

        total_comment_likes = sum(row[2] for row in comment_rows)

        for item, comment, comment_like_count in comment_rows:
            comment_like_percent = 0.0
            if total_comment_likes > 0:
                comment_like_percent = (comment_like_count / total_comment_likes) * 100.0

            # Combined engagement score requested: video like % + comment like %.
            engagement_score = int(round(video_like_percent + comment_like_percent))

            payload = {
                "id": item["id"],
                "timestamp": comment["publishedAt"],
                "source": "youtube",
                "text": comment["textDisplay"],
                "engagement": engagement_score,
                "symbol": "BTC"
            }

            comments.append(payload)

        return comments

    except HttpError as e:
        print(f"Comment fetch failed: {e}")
        return []