from pathlib import Path

from pyspark.sql import DataFrame, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTAINER_ROOT_PREFIX = "/opt/app/"


def _resolve_runtime_table_path(path_text: str) -> str:
    """Resolve table path for local runs while keeping container paths intact.

    If config uses /opt/app/... (Docker) but that path is absent locally, map it
    to <project_root>/... so local Spark jobs read/write the same folders.
    """
    raw = str(path_text or "").strip()
    if not raw:
        return raw

    configured = Path(raw)
    if configured.exists():
        return str(configured)

    if configured.is_absolute() and raw.startswith(CONTAINER_ROOT_PREFIX):
        suffix = raw[len(CONTAINER_ROOT_PREFIX):]
        local_candidate = (PROJECT_ROOT / suffix).resolve()
        if local_candidate.exists() or str(local_candidate).startswith(str(PROJECT_ROOT)):
            return str(local_candidate)

    return raw


def get_table_config(table_name: str, config: dict) -> dict:
    table_config = config["tables"].get(table_name)
    if not table_config:
        raise ValueError(f"Table config not found for '{table_name}'")
    resolved = dict(table_config)
    if "path" in resolved:
        resolved["path"] = _resolve_runtime_table_path(str(resolved["path"]))
    if "checkpoint" in resolved:
        resolved["checkpoint"] = _resolve_runtime_table_path(str(resolved["checkpoint"]))
    return resolved


def add_metadata(df: DataFrame) -> DataFrame:
    df = df.withColumn("ingestion_time", F.current_timestamp())
    return df


def validate_partitions(df: DataFrame, partition_cols):
    if not partition_cols:
        return

    for col in partition_cols:
        if col not in df.columns:
            raise ValueError(f"Partition column '{col}' missing in DataFrame")
