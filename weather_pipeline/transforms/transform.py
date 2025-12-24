"""Transform API responses to standardized format."""

from datetime import datetime

import polars as pl

from weather_pipeline.models import (
    FetchResult,
    StandardizedWeatherHourly,
    StandardizedWeatherDaily,
)


def transform_hourly(result: FetchResult, run_id: str | None = None) -> pl.DataFrame:
    """Transform API response to standardized hourly weather format.

    Args:
        result (FetchResult): FetchResult from client
        run_id (str | None): Optional pipeline run ID for traceability

    Returns:
        pl.DataFrame: DataFrame with standardized hourly weather data.
    """
    hourly = result.data["hourly"]
    location = result.location
    api_metadata = result.api_metadata
    ingestion_timestamp = result.ingestion_metadata.ingestion_timestamp_utc

    records = []
    for i, time in enumerate(hourly["time"]):
        record = StandardizedWeatherHourly(
            location_name=location.name,
            requested_latitude=location.latitude,
            requested_longitude=location.longitude,
            api_latitude=api_metadata.api_latitude,
            api_longitude=api_metadata.api_longitude,
            timestamp=datetime.fromisoformat(time),
            temperature_2m=hourly.get("temperature_2m", [None] * len(hourly["time"]))[
                i
            ],
            precipitation=hourly.get("precipitation", [None] * len(hourly["time"]))[i],
            relative_humidity_2m=hourly.get(
                "relative_humidity_2m", [None] * len(hourly["time"])
            )[i],
            windspeed_10m=hourly.get("windspeed_10m", [None] * len(hourly["time"]))[i],
            wind_direction_10m=hourly.get(
                "wind_direction_10m", [None] * len(hourly["time"])
            )[i],
            cloud_cover=hourly.get("cloudcover", [None] * len(hourly["time"]))[i],
            weather_code=hourly.get("weathercode", [None] * len(hourly["time"]))[i],
            ingestion_timestamp_utc=ingestion_timestamp,
            run_id=run_id,
        )
        records.append(record)

        # Convert list of Pydantic models to Polars DataFrame
    df = pl.DataFrame([r.model_dump() for r in records])
    return df


def transform_daily(result: FetchResult, run_id: str | None = None) -> pl.DataFrame:
    """Transform API response to standardized daily weather format.

    Args:
        result (FetchResult): FetchResult from client
        run_id (str | None): Optional pipeline run ID for traceability

    Returns:
        pl.DataFrame: DataFrame with standardized daily weather data.
    """
    daily = result.data["daily"]
    location = result.location
    api_metadata = result.api_metadata
    ingestion_timestamp = result.ingestion_metadata.ingestion_timestamp_utc

    records = []
    for i, date in enumerate(daily["time"]):
        record = StandardizedWeatherDaily(
            location_name=location.name,
            requested_latitude=location.latitude,
            requested_longitude=location.longitude,
            api_latitude=api_metadata.api_latitude,
            api_longitude=api_metadata.api_longitude,
            date=datetime.fromisoformat(date),
            temperature_2m_max=daily.get(
                "temperature_2m_max", [None] * len(daily["time"])
            )[i],
            temperature_2m_min=daily.get(
                "temperature_2m_min", [None] * len(daily["time"])
            )[i],
            precipitation_sum=daily.get(
                "precipitation_sum", [None] * len(daily["time"])
            )[i],
            ingestion_timestamp_utc=ingestion_timestamp,
            run_id=run_id,
        )
        records.append(record)

    # Convert list of Pydantic models to Polars DataFrame
    df = pl.DataFrame([r.model_dump() for r in records])
    return df


def get_partition_path(
    location_name: str, interval: str, ingestion_timestamp: str
) -> str:
    """Generate partition path for storage.

    Format: {interval}/location={name}/date={YYYY-MM-DD}/
    """
    dt = datetime.fromisoformat(ingestion_timestamp.replace("Z", "+00:00"))
    date_str = dt.strftime("%Y-%m-%d")
    return f"{interval}/{location_name}/{date_str}"
