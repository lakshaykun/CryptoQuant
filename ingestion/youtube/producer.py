import time
import os
from ingestion.youtube.fetch_comments import fetch_channel_comments
from ingestion.common.redis_dedup import is_duplicate, add_to_bloom
from ingestion.common.kafka_producer import send
from utils.source_config import get_list_value, get_sources_section

DEFAULT_TARGET_CHANNEL_IDS = [
    "UCqK_GSMbpiV8spgD3ZGloSw", "UC_8j6_0S5S_Xv7Rz0u_yX1w", "UCGy7SkBjcIAgTiwkXEtPnYg",
    "UCCatR7nWbYrkVXdxXb4cGXw", "UCUCY6fdZqmXbiUPscatfi0w", "UCN9Nj4tjXbVTLYWN0EKly_Q",
    "UCl2oCaw8hdR_kbqyqd2klIA", "UCmS89id7Y9X-O_Z7L89vH3A", "UCevXpeL8cNyAnzw-NqJ4m2w",
    "UC7ndli6X_Lz6vUvF-TscG6w", "UCjemQfjaXAzA-95RKoy9n_g", "UC_L6_vk5f7v-v3z_NfGZ76A",
    "UCV6KDgJskWaEckne5aPA0aQ", "UC9GnvZ6iR7vQ3TfD7H-86LA", "UC59m_R9oR_yF77qHqFm6iSw"
]


def _configured_channels() -> list[str]:
    section = get_sources_section("youtube")
    channel_ids = get_list_value(section, "channel_ids", DEFAULT_TARGET_CHANNEL_IDS)
    return [cid for cid in channel_ids if cid.startswith("UC")]


def poll_youtube():
    raw_max_comments = os.getenv("YOUTUBE_MAX_COMMENTS_PER_CHANNEL", "100")
    try:
        max_comments_per_channel = max(1, int(raw_max_comments))
    except ValueError:
        print(f"Invalid YOUTUBE_MAX_COMMENTS_PER_CHANNEL={raw_max_comments!r}, defaulting to 100")
        max_comments_per_channel = 100

    targets = _configured_channels()

    for channel_id in targets:
        comments = fetch_channel_comments(channel_id, max_results=max_comments_per_channel)
        print(f"Fetched {len(comments)} comments for channel {channel_id}")
        if not comments:
            continue

        for comment in comments:
            if is_duplicate(comment["id"]):
                continue

            send("btc_yt", comment)
            print(f"Sent comment {comment['id']} to Kafka")
            add_to_bloom(comment["id"])


def run_forever():
    while True:
        poll_youtube()
        print("pulling messages from youtube...")
        time.sleep(180)


if __name__ == "__main__":
    run_forever()