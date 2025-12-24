""" Loads Configs """

from pathlib import Path
from pydantic import BaseModel, Field
import yaml
from weather_pipeline.models import Location


def load_config_yml(
        config_path: str | None = None,
        default_path: str = "config/default.yaml"
        ) -> dict:
        """Load configuration from a YAML file.

        Merges API-specific configs with default configs.
        """
        # Load Default Configs
        with open (default_path) as f:
            default_configs = yaml.safe_load(f)

        # Merge api=specific configs if provided
        if config_path and Path(config_path).exists():
              with open (config_path) as f:
                  api_specific_configs = yaml.safe_load(f)
               default_configs = _deep_merge(config, api_specific_configs)
              
        return default_configs


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
        return result
