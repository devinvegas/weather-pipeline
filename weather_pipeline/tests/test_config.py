"""Tests for config handler."""

import pytest
from pathlib import Path
from weather_pipeline.config_handler import _deep_merge


@pytest.fixture
def default_config():
    """Test fixture for default config."""
    return {
        "api": {
            "provider": "open_meteo",
            "base_url": "https://api.open-meteo.com/v1/forecast",
            "timeout_seconds": 30,
            "max_retries": 3
        },
        "interval": "daily",
        "forecast_days": 7,
        "storage": {
            "backend": "local",
            "compression": "snappy"
        }
    }


@pytest.fixture
def api_specific_config():
    """Test fixture for API-specific config (overrides some defaults)."""
    return {
        "api": {
            "provider": "open_meteo",
            "base_url": "https://api.open-meteo.com/v1/forecast",
            "timeout_seconds": 60  # Override timeout only
        },
        "interval": "hourly",  # Override interval
        "forecast_days": 14,  # Override forecast_days with different value
        "locations": [
            {"name": "London", "latitude": 51.5074, "longitude": -0.1278},
            {"name": "Tokyo", "latitude": 35.6762, "longitude": 139.6503}
        ]
    }


def test_config_merge_precedence(default_config, api_specific_config):
    """Test that API-specific config values take precedence over defaults."""
    merged = _deep_merge(default_config, api_specific_config)
    
    # API-specific overrides should be applied
    assert merged["api"]["timeout_seconds"] == 60
    assert merged["api"]["provider"] == "open_meteo"
    assert merged["api"]["base_url"] == "https://api.open-meteo.com/v1/forecast"
    assert merged["interval"] == "hourly"
    assert merged["forecast_days"] == 14  # Overridden value, not default 7
    
    # Defaults should still be present where not overridden
    assert merged["api"]["max_retries"] == 3
    assert merged["storage"]["backend"] == "local"
    
    # New keys from api_specific should be added
    assert "locations" in merged
    assert len(merged["locations"]) == 2


def test_deep_merge_empty_override():
    """Test deep merge with empty override."""
    base = {
        "api": {"timeout": 30, "retries": 3},
        "interval": "daily"
    }
    override = {}
    
    result = _deep_merge(base, override)

    assert result == base


def test_load_config_file_not_found():
    """Test error on missing config."""
    from weather_pipeline.config_handler import load_config_yml
    with pytest.raises(FileNotFoundError):
        load_config_yml("nonexistent.yml")
