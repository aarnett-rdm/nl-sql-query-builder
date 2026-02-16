from pathlib import Path
import json
from typing import Dict, Any

# Default config directory (relative to project root)
CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_configs(config_dir: str | Path = CONFIG_DIR) -> Dict[str, Any]:
    config_dir = Path(config_dir)

    return {
        "schema": load_json(config_dir / "semantic_schema.json"),
        "metric_registry": load_json(config_dir / "metric_registry.json"),
        "domain_policy": load_json(config_dir / "domain_policy.json"),
        "filter_config": load_json(config_dir / "filter_config.json"),
        "validator_policy": load_json(config_dir / "validator_policy.json"),
    }
