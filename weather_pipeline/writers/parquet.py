"""Parquet writer implementation."""

from datetime import datetime
from pathlib import Path

import polars as pl

from weather_pipeline.models import WriteResult
from weather_pipeline.transforms import get_partition_path


class ParquetWriter:
    """Write DataFrames to local Parquet files.
    
    
    Implements the DataWriter protocol.
    """

    def __init__(self, base_path: str, compression: str = "snappy"):
        self.base_path = Path(base_path)
        self.compression = compression

    def write(self, df: pl.DataFrame, partition_path: str) -> WriteResult:
        """Write DataFrame to a single Parquet file."""

        # Build full path
        full_path = self.base_path / partition_path
        full_path.mkdir(parents=True, exist_ok=True)

        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        filename = f"data_{timestamp}.parquet"
        full_path = full_path / filename

        # Write with compression
        df.write_parquet(
            full_path, compression=self.compression
        )
        return WriteResult(
            path=str(full_path),
            records_written=len(df),
            format="parquet",
        )
    
    def write_partitioned(
        self, df: pl.DataFrame, interval: str, provider: str, run_uuid: str
    ) -> list[WriteResult]:
        """Write DataFrame partitioned by location.
        
        Args:
            df: DataFrame to write
            interval: "hourly" or "daily"
            provider: API provider name (e.g., "open_meteo")
            run_uuid: Pipeline run ID for filename uniqueness
        
        Returns:
            List of WriteResult objects
        """
        results = []

        for location_name in df["location_name"].unique():
            # Filter DataFrame for this location
            location_df = df.filter(pl.col("location_name") == location_name)

            # Get ingestion timestamp from first row and convert to ISO string
            ingestion_timestamp = location_df["ingestion_timestamp_utc"][0]
            ingestion_timestamp_str = ingestion_timestamp.isoformat()

            # Generate partition path
            partition_path = get_partition_path(
                provider=provider,
                location_name=location_name,
                interval=interval,
                run_uuid=run_uuid,
                ingestion_timestamp=ingestion_timestamp_str,
            )

            # Write
            result = self.write(location_df, partition_path)
            results.append(result)
        
        return results