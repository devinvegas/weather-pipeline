"""Base writer protocol for storage abstraction."""

from typing import Protocol

import polars as pl

from weather_pipeline.models import WriteResult


class DataWriter(Protocol):
    """Protocol for data writers.
    
    Implement this to add new storage backends (currently only parquet but supports GCS, S3, DuckDB etc).
    """
    
    def write(self, df: pl.DataFrame, partition_path: str) -> WriteResult:
        """Write DataFrame to storage.
        
        Args:
            df: Data to write
            partition_path: Relative path for partitioning
        
        Returns:
            WriteResult with path and record count
        """
        ...
    
    def write_partitioned(
        self, df: pl.DataFrame, interval: str
    ) -> list[WriteResult]:
        """Write DataFrame partitioned by location.
        
        Args:
            df: Data to write (may contain multiple locations)
            interval: "hourly" or "daily"
        
        Returns:
            List of WriteResult (one per partition)
        """
        ...