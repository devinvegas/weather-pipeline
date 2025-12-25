"""Orchestrates the pipeline for fetching, transforming, and storing weather data."""

import asyncio
import uuid
from datetime import datetime, timezone
import polars as pl
import logging

from weather_pipeline.clients import OpenMeteoClient
from weather_pipeline.config_handler import load_config_yml, PipelineConfig
from weather_pipeline.models import FetchError, PipelineResult
from weather_pipeline.transforms import transform_hourly, transform_daily
from weather_pipeline.writers import ParquetWriter


async def run_pipeline_async(config: PipelineConfig) -> PipelineResult:
    """Run the ingestion pipeline asynchronously."""

    run_id = str(uuid.uuid4())
    run_start = datetime.now(timezone.utc)

    logging.info(f"Pipeline run {run_id} started at {run_start.isoformat()}")
    logging.info(f" Locations to process: {[loc.name for loc in config.locations]}")
    logging.info(f" Interval: {config.interval}")

    # Initialize clinet
    open_meteo_client = OpenMeteoClient(
        base_url=config.api.base_url,
        timeout=config.api.timeout_seconds,
        max_concurrent_requests=config.api.max_concurrent_requests,
        hourly_params=config.hourly_params if config.interval == "hourly" else None,
        daily_params=config.daily_params if config.interval == "daily" else None,
        forecast_days=config.forecast_days,
        timezone=config.timezone,
    )

    # Initialize writer
    writer_open_meteo = ParquetWriter(
        base_path=config.storage.base_path,
        compression=config.storage.compression,
    )

    # Select transform function
    transform_func_open_meteo = (
        transform_hourly if config.interval == "hourly" else transform_daily
    )

    # Fetch all locations concurrently
    results_open_meteo = await open_meteo_client.fetch_data(config.locations)

    # Transform
    dfs = []
    failed = []

    for result in results_open_meteo:
        if isinstance(result, FetchError):
            logging.error(
                f"Fetch error for {result.location.name}: {result.error}"
            )
            failed.append(result)
        else:
            logging.info(f"Transforming data for {result.location.name}")
            df = transform_func_open_meteo(result, run_id=run_id)
            dfs.append(df)
            logging.info(f"  ✓ {result.location.name}: {len(df)} records")

    if not dfs:
        logging.error("No data fetched successfully. Exiting pipeline.")
        return PipelineResult(
            success=False,
            run_id=run_id,
            run_start=run_start,
            run_end=datetime.now(timezone.utc),
            records_processed=0,
            error="All locations failed to fetch data.",
            failed=failed,
        )

    all_data_df = pl.concat(dfs)

    # Write Partitioned
    write_results = writer_open_meteo.write_partitioned(all_data_df, config.interval)
    total_records_written = sum(r.records_written for r in write_results)

    run_end = datetime.now(timezone.utc)

    print(f"\nPipeline complete:")
    logging.info(f"  Run ID: {run_id}")
    logging.info(f"  Duration: {(run_end - run_start).total_seconds():.2f}s")
    logging.info(f"  Total Records Written: {total_records_written}")
    logging.info(f"  Files written: {len(write_results)}")
    logging.info(f"  Failed Fetches: {len(failed)}")

    return PipelineResult(
        success=True,
        run_id=run_id,
        run_start=run_start,
        run_end=run_end,
        records_processed=total_records_written,
        files=write_results,
        failed=failed,
    )


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    """Synchronous wrapper to run the async pipeline."""
    return asyncio.run(run_pipeline_async(config))


async def run_from_config_path(config_path: str) -> PipelineResult:
    """Load config and run pipeline asynchronously."""
    config = load_config_yml(config_path=config_path)
    return await run_pipeline_async(config)
