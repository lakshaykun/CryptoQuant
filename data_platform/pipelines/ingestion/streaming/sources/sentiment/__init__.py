"""Sentiment source package.

Keep this module side-effect free so stage-specific jobs (e.g., gold only)
do not require all optional ingestion dependencies at import time.
"""

__all__ = [
    "news",
    "reddit",
    "shared",
    "telegram",
    "youtube",
]
