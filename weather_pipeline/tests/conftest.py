"""Test configuration and fixtures."""

import pytest
import tempfile
from pathlib import Path



@pytest.fixture
def temp_dir():
    """Create temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def sample_api_response():
    """Sample OpenMeteo API response."""
    return {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "generationtime_ms": 0.5,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 11.0,
        "daily": {
            "time": ["2025-01-15", "2025-01-16"],
            "temperature_2m_max": [10.5, 12.3],
            "temperature_2m_min": [5.2, 6.1],
            "precipitation_sum": [0.0, 1.2],
        }
    }


@pytest.fixture
def sample_location():
    """Sample location object."""
    from weather_pipeline.models import Location
    return Location(name="London", latitude=51.5074, longitude=-0.1278)
