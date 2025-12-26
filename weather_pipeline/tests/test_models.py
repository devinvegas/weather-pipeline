"""Tests for models."""

import pytest
from weather_pipeline.models import Location, PipelineResult


def test_location_model():
    """Test location creation."""
    loc = Location(name="London", latitude=51.5074, longitude=-0.1278)
    
    assert loc.name == "London"
    assert loc.latitude == 51.5074
    assert loc.longitude == -0.1278


def test_pipeline_result_success():
    """Test successful pipeline result."""
    from datetime import datetime, timezone
    
    result = PipelineResult(
        success=True,
        run_id="test-123",
        run_start=datetime.now(timezone.utc),
        run_end=datetime.now(timezone.utc),
        records_processed=42,
        error=None,
        failed=[]
    )
    
    assert result.success is True
    assert result.records_processed == 42
    assert len(result.failed) == 0


def test_pipeline_result_failure():
    """Test failed pipeline result."""
    from datetime import datetime, timezone
    
    result = PipelineResult(
        success=False,
        run_id="test-123",
        run_start=datetime.now(timezone.utc),
        run_end=datetime.now(timezone.utc),
        records_processed=0,
        error="API timeout",
        failed=[]
    )
    
    assert result.success is False
    assert result.error == "API timeout"
