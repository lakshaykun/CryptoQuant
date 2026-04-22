# pipelines/utils/config_loader.py

from pathlib import Path

import yaml


def load_config(path: str):
    config_path = Path(path)

    if not config_path.is_absolute():
        cwd_candidate = Path.cwd() / config_path
        project_root = Path(__file__).resolve().parents[1]
        project_candidate = project_root / config_path

        if cwd_candidate.exists():
            config_path = cwd_candidate
        elif project_candidate.exists():
            config_path = project_candidate

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)