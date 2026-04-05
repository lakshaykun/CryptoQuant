import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from utils.env import load_env_file


# Ensure configs/app.env is loaded for local runs.
load_env_file()


def _youtube_client():
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        print("YOUTUBE_API_KEY not set; skipping YouTube comments fetch.")
        return None
    return build("youtube", "v3", developerKey=youtube_api_key)


def fetch_video_comments(video_id: str, max_results: int = 50) -> list[dict]:
    youtube = _youtube_client()
    if youtube is None:
        return []

    try:
        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            order="time"
        ).execute()

        comments = []

        for item in response.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]

            payload = {
                "id": item["id"],
                "timestamp": comment["publishedAt"],
                "source": "youtube",
                "text": comment["textDisplay"],
                "engagement": int(comment["likeCount"]),
                "symbol": "BTC"
            }

            comments.append(payload)

        return comments

    except HttpError as e:
        print(f"Comment fetch failed: {e}")
        return []