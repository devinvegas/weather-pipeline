""" Loads Configs """

from pathlib import Path
from pydantic import BaseModel, Field
import yaml
from weather_pipeline.models import Location
from typing import Literal


def load_config_yml(
        config_path: str | None = None,
        default_path: str = "config/default.yaml"
        ) -> PipelineConfig:
        """Load configuration from a YAML file with Pydantic validation.

        Merges API-specific configs with default configs.
        """
        # Load Default Configs
        with open (default_path) as f:
            default_configs = yaml.safe_load(f)

        # Merge api=specific configs if provided
        if config_path and Path(config_path).exists():
              with open (config_path) as f:
                  api_specific_configs = yaml.safe_load(f)
              default_configs = _deep_merge(default_configs, api_specific_configs)

        # Validate with Pydantic
        return PipelineConfig(**default_configs)     


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
        return result
    
class APIConfig(BaseModel):
    """API Configuration"""

    base_url: str = "https://api.open-meteo.com/v1/forecast"
    timeout_seconds: int = 30
    max_concurrent_requests: int = 5

class StorageConfig(BaseModel):
    """Storage Configuration"""

    backend: str = "local"
    base_path: str = "data/weather"
    compression: str = "snappy"

class PipelineConfig(BaseModel):
    """Pipeline Configuration"""

    api: APIConfig = Field(default_factory=APIConfig)
    locations: list[Location]
    interval: Literal["hourly", "daily"] = "hourly"
    hourly_params: list[str] = Field(default_factory=list)
    daily_params: list[str] = Field(default_factory=list)
    forecast_days: int = 7
    timezone: str = "UTC"
    storage: StorageConfig = Field(default_factory=StorageConfig)

