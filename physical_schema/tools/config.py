"""
Centralized configuration for the NL-SQL service.

Loads from environment variables with sensible defaults.
Validates that all required config files exist and are valid JSON.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    from tools.exceptions import ConfigError
except ImportError:
    from exceptions import ConfigError


def _parse_bool(value: str) -> bool:
    """Parse a boolean env var. Only '1', 'true', 'yes' are truthy."""
    return value.strip().lower() in ("1", "true", "yes")


_APP_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class AppConfig:
    """Immutable application configuration."""

    # Config file paths
    metric_registry: Path = field(
        default_factory=lambda: _APP_ROOT / "current" / "metric_registry.json"
    )
    physical_schema: Path = field(
        default_factory=lambda: _APP_ROOT / "current" / "physical_schema.json"
    )
    filter_config: Path = field(
        default_factory=lambda: _APP_ROOT / "current" / "filter_config.json"
    )

    # LLM settings
    llm_enabled: bool = True
    llm_provider: str = "ollama"  # future: "azure_openai", "anthropic"
    ollama_url: str = "http://192.168.12.51:11434"
    ollama_model: str = "qwen3:14b"
    ollama_timeout: int = 300

    # Cost tracking (for eval framework; default 0.0 for local Ollama)
    cost_per_input_token: float = 0.0
    cost_per_output_token: float = 0.0

    # Schema retriever chunks (optional)
    chunks_dir: Optional[Path] = None

    # Logging
    log_level: str = "INFO"

    # API
    api_key_header: str = "X-API-Key"

    @classmethod
    def from_env(cls) -> AppConfig:
        """Build config from environment variables with defaults."""
        mr = os.getenv("NL_SQL_METRIC_REGISTRY")
        ps = os.getenv("NL_SQL_PHYSICAL_SCHEMA")
        fc = os.getenv("NL_SQL_FILTER_CONFIG")
        cd = os.getenv("NL_SQL_CHUNKS_DIR", "").strip()

        kwargs: dict = {}
        if mr:
            kwargs["metric_registry"] = Path(mr)
        if ps:
            kwargs["physical_schema"] = Path(ps)
        if fc:
            kwargs["filter_config"] = Path(fc)
        kwargs["llm_enabled"] = _parse_bool(
            os.getenv("NL_SQL_USE_LLM", "true")
        )
        kwargs["llm_provider"] = os.getenv(
            "NL_SQL_LLM_PROVIDER", "ollama"
        ).lower()
        kwargs["ollama_url"] = os.getenv(
            "OLLAMA_URL", "http://192.168.12.51:11434"
        )
        kwargs["ollama_model"] = os.getenv("OLLAMA_MODEL", "qwen3:14b")
        kwargs["ollama_timeout"] = int(
            os.getenv("OLLAMA_TIMEOUT", "300")
        )
        kwargs["cost_per_input_token"] = float(
            os.getenv("NL_SQL_COST_PER_INPUT_TOKEN", "0.0")
        )
        kwargs["cost_per_output_token"] = float(
            os.getenv("NL_SQL_COST_PER_OUTPUT_TOKEN", "0.0")
        )
        if cd:
            kwargs["chunks_dir"] = Path(cd)
        kwargs["log_level"] = os.getenv(
            "NL_SQL_LOG_LEVEL", "INFO"
        ).upper()
        kwargs["api_key_header"] = os.getenv(
            "NL_SQL_API_KEY_HEADER", "X-API-Key"
        )
        return cls(**kwargs)

    def validate(self) -> None:
        """Ensure required config files exist and contain valid JSON.

        Raises ConfigError with the specific file that failed.
        """
        for label, path in [
            ("metric_registry", self.metric_registry),
            ("physical_schema", self.physical_schema),
            ("filter_config", self.filter_config),
        ]:
            if not path.exists():
                raise ConfigError(
                    f"{label} not found: {path}", config_path=path
                )
            if not path.is_file():
                raise ConfigError(
                    f"{label} is not a file: {path}", config_path=path
                )
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                raise ConfigError(
                    f"{label} is not valid JSON: {exc}", config_path=path
                ) from exc
