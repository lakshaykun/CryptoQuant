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


def _video_ids_from_playlist(youtube, upload_playlist_id: str, max_results: int) -> list[str]:
    video_ids: list[str] = []
    next_page_token: str | None = None

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


def _video_ids_from_channel_search(youtube, channel_id: str, max_results: int) -> list[str]:
    video_ids: list[str] = []
    next_page_token: str | None = None

    while len(video_ids) < max_results:
        response = youtube.search().list(
            part="id",
            channelId=channel_id,
            order="date",
            type="video",
            maxResults=min(max_results - len(video_ids), 50),
            pageToken=next_page_token,
        ).execute()

        items = response.get("items", [])
        if not items:
            break

        for item in items:
            video_id = item.get("id", {}).get("videoId")
            if video_id:
                video_ids.append(video_id)
                if len(video_ids) >= max_results:
                    break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids


def _uploads_playlist_for_channel(youtube, channel_id: str) -> str | None:
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    items = response.get("items", [])
    if not items:
        return None

    return (
        items[0]
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )


def get_recent_video_ids(upload_playlist_id: str, max_results: int = 5) -> list[str]:
    youtube = _youtube_client()
    if youtube is None or max_results <= 0:
        return []

    try:
        return _video_ids_from_playlist(youtube, upload_playlist_id, max_results)

    except HttpError as e:
        print(f"YouTube API error: {e}")
        # Some channels do not expose a resolvable uploads playlist; fallback to channel search.
        is_playlist_not_found = (
            getattr(getattr(e, "resp", None), "status", None) == 404
            and "playlistNotFound" in str(e)
        )
        if is_playlist_not_found and upload_playlist_id.startswith("UU"):
            derived_channel_id = f"UC{upload_playlist_id[2:]}"
            print(
                f"Falling back to search.list for channel {derived_channel_id} "
                f"because playlist {upload_playlist_id} is unavailable."
            )
            try:
                return _video_ids_from_channel_search(youtube, derived_channel_id, max_results)
            except HttpError as fallback_error:
                print(f"YouTube fallback search error: {fallback_error}")
        return []


def get_recent_video_ids_for_channel(channel_id: str, max_results: int = 5) -> list[str]:
    youtube = _youtube_client()
    if youtube is None or max_results <= 0:
        return []

    try:
        uploads_playlist_id = _uploads_playlist_for_channel(youtube, channel_id)
        if uploads_playlist_id:
            return _video_ids_from_playlist(youtube, uploads_playlist_id, max_results)

        print(f"No uploads playlist found for channel {channel_id}; using search fallback.")
        return _video_ids_from_channel_search(youtube, channel_id, max_results)
    except HttpError as e:
        print(f"YouTube API error for channel {channel_id}: {e}")
        return []


def get_latest_video_id(upload_playlist_id: str) -> str | None:
    video_ids = get_recent_video_ids(upload_playlist_id, max_results=1)
    return video_ids[0] if video_ids else None