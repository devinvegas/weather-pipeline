"""Tests for Parquet writer."""

import pytest
import polars as pl
from weather_pipeline.writers import ParquetWriter


def test_writer_init(temp_dir):
    """Test writer initialization."""
    writer = ParquetWriter(base_path=str(temp_dir), compression="zstd")
    assert writer.base_path == temp_dir


def test_read_written_parquet(temp_dir):
    """Test round-trip: write then read and verify data integrity."""
    writer = ParquetWriter(base_path=str(temp_dir), compression="zstd")
    
    original_df = pl.DataFrame({
        "location_name": ["London"],
        "date": ["2025-01-15"],
        "temperature_2m_max": [10.5],
        "temperature_2m_min": [5.2],
        "run_id": ["test-1"],
    })
    
    result = writer.write(
        df=original_df,
        partition_path="open_meteo/daily/London/2025/01/15/forecast_test123.parquet"
    )
    
    # Verify write result
    assert result.records_written == 1
    assert result.format == "parquet"
    assert "open_meteo" in result.path
    assert "daily" in result.path
    
    # Verify round-trip: read back and check data integrity
    read_df = pl.read_parquet(result.path)
    assert len(read_df) == 1
    assert read_df["location_name"][0] == "London"
    assert read_df["temperature_2m_max"][0] == 10.5
    assert read_df["temperature_2m_min"][0] == 5.2
