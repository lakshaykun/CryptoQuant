# pipelines/orchestration/batch/utils.py

from datetime import datetime

def parse_metadata(metadata: dict) -> dict:
    """
    Parses metadata dictionary with ISO timestamp strings into datetime objects.
    """
    parsed_metadata = {
            symbol: datetime.fromisoformat(ts)
            for symbol, ts in metadata.items()
        }
    return parsed_metadata