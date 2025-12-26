"""Tests for data transforms."""

import pytest
import polars as pl
from datetime import datetime, timezone
from weather_pipeline.transforms import transform_daily, transform_hourly
from weather_pipeline.models import FetchResult, Location, IngestionMetadata, APIMetadata


def test_transform_daily(sample_api_response, sample_location):
    """Test daily transform: output shape, columns, and data quality."""
    result = FetchResult(
        location=sample_location, 
        data=sample_api_response,
        ingestion_metadata=IngestionMetadata(
            ingestion_timestamp_utc=datetime.now(timezone.utc),
            request_url="https://api.open-meteo.com/v1/forecast",
            elapsed_ms=50,
            status_code=200
        ),
        api_metadata=APIMetadata(
            api_latitude=51.5074,
            api_longitude=-0.1278,
            elevation=11.0,
            generationtime_ms=0.5,
            timezone="GMT",
            utc_offset_seconds=0
        )
    )
    df = transform_daily(result, run_id="test-run-1")
    
    # Verify output is DataFrame with expected rows
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2  # 2 days in sample
    
    # Verify required columns exist
    required_cols = ["location_name", "date", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"
    
    # Verify data quality
    assert df["location_name"].null_count() == 0
    assert df["run_id"].null_count() == 0


def test_transform_hourly(sample_api_response, sample_location):
    """Test hourly transform: output shape, columns, and data availability."""
    # Add hourly data
    sample_api_response["hourly"] = {
        "time": ["2025-01-15T00:00", "2025-01-15T01:00"],
        "temperature_2m": [5.2, 5.5],
        "precipitation": [0.0, 0.1],
    }
    
    result = FetchResult(
        location=sample_location, 
        data=sample_api_response,
        ingestion_metadata=IngestionMetadata(
            ingestion_timestamp_utc=datetime.now(timezone.utc),
            request_url="https://api.open-meteo.com/v1/forecast",
            elapsed_ms=50,
            status_code=200
        ),
        api_metadata=APIMetadata(
            api_latitude=51.5074,
            api_longitude=-0.1278,
            elevation=11.0,
            generationtime_ms=0.5,
            timezone="GMT",
            utc_offset_seconds=0
        )
    )
    df = transform_hourly(result, run_id="test-run-1")
    
    # Verify output is DataFrame with expected hourly records
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2  # 2 hourly records in sample
    
    # Verify required columns
    assert "timestamp" in df.columns
    assert "temperature_2m" in df.columns
    assert "location_name" in df.columns
