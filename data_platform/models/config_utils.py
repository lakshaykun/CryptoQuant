DEFAULT_ALGOS = ["xgboost", "lightgbm", "catboost"]
TASK_SECTION_KEYS = {"regression", "classification", "common", "default"}


def resolve_task_algorithm_config(section: dict | None, task_type: str, algo_name: str) -> dict:
    section = section or {}
    if not isinstance(section, dict):
        return {}

    if not any(key in section for key in TASK_SECTION_KEYS):
        algo_cfg = section.get(algo_name, {})
        return dict(algo_cfg) if isinstance(algo_cfg, dict) else {}

    resolved = {}
    for bucket_name in ("common", "default", task_type):
        bucket = section.get(bucket_name, {})
        if isinstance(bucket, dict):
            algo_cfg = bucket.get(algo_name, {})
            if isinstance(algo_cfg, dict):
                resolved.update(algo_cfg)

    top_level_cfg = section.get(algo_name, {})
    if isinstance(top_level_cfg, dict):
        resolved.update(top_level_cfg)

    return resolved


def get_available_algorithms(model_config: dict | None) -> list[str]:
    section = (model_config or {}).get("model_params", {})
    if not isinstance(section, dict) or not section:
        return list(DEFAULT_ALGOS)

    if not any(key in section for key in TASK_SECTION_KEYS):
        algos = [name for name, value in section.items() if isinstance(value, dict)]
        return algos or list(DEFAULT_ALGOS)

    algos = []
    for bucket_name in ("common", "default", "regression", "classification"):
        bucket = section.get(bucket_name, {})
        if not isinstance(bucket, dict):
            continue
        for algo_name, algo_cfg in bucket.items():
            if isinstance(algo_cfg, dict) and algo_name not in algos:
                algos.append(algo_name)

    return algos or list(DEFAULT_ALGOS)