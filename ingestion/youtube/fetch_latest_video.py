import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from utils.env import load_env_file


# Ensure configs/app.env is loaded for local runs.
load_env_file()


def _youtube_client():
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        print("YOUTUBE_API_KEY not set; skipping YouTube fetch.")
        return None
    return build("youtube", "v3", developerKey=youtube_api_key)


def get_recent_video_ids(upload_playlist_id: str, max_results: int = 5) -> list[str]:
    youtube = _youtube_client()
    if youtube is None or max_results <= 0:
        return []

    video_ids: list[str] = []
    next_page_token: str | None = None

    try:
        while len(video_ids) < max_results:
            response = youtube.playlistItems().list(
                part="snippet",
                playlistId=upload_playlist_id,
                maxResults=min(max_results - len(video_ids), 50),
                pageToken=next_page_token,
            ).execute()

            items = response.get("items", [])
            if not items:
                break

            for item in items:
                snippet = item.get("snippet", {})
                resource_id = snippet.get("resourceId", {})
                video_id = resource_id.get("videoId")
                if video_id:
                    video_ids.append(video_id)
                    if len(video_ids) >= max_results:
                        break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids

    except HttpError as e:
        print(f"YouTube API error: {e}")
        return []


def get_latest_video_id(upload_playlist_id: str) -> str | None:
    video_ids = get_recent_video_ids(upload_playlist_id, max_results=1)
    return video_ids[0] if video_ids else None