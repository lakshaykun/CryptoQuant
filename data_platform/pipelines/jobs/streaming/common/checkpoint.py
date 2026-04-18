import json
import shutil
from pathlib import Path


def _delta_table_id(delta_path: str) -> str | None:
    log_dir = Path(delta_path) / "_delta_log"
    if not log_dir.exists():
        return None

    for log_file in sorted(log_dir.glob("*.json")):
        try:
            for line in log_file.read_text(encoding="utf-8").splitlines():
                payload = json.loads(line)
                metadata = payload.get("metaData")
                if isinstance(metadata, dict):
                    table_id = metadata.get("id")
                    if table_id:
                        return str(table_id)
        except Exception:
            continue

    return None


def _checkpoint_delta_table_id(checkpoint_path: str) -> str | None:
    metadata_file = Path(checkpoint_path) / "metadata"
    if not metadata_file.exists():
        return None

    try:
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    except Exception:
        return None

    table_id = metadata.get("id")
    return str(table_id) if table_id else None


def reset_stale_checkpoint(source_path: str, checkpoint_path: str) -> bool:
    current_source_id = _delta_table_id(source_path)
    current_checkpoint_id = _checkpoint_delta_table_id(checkpoint_path)

    if not current_checkpoint_id or not current_source_id:
        return False

    if current_checkpoint_id != current_source_id:
        shutil.rmtree(checkpoint_path, ignore_errors=True)
        return True

    return False
