"""Pydantic models for weather data pipeline."""

from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


# Location
class Location(BaseModel):
    """Model representing a geographical location."""
    name: str
    latitude: float
    longitude: float

# Metadata
class IngestionMetadata(BaseModel):
    """Client-side metadata captured during API calls."""
    ingestion_timestamp_utc: datetime
    request_url: str
    elapsed_ms: float
    status_code: int

class APIMetadata(BaseModel):
    """Metadata returned by weather data API."""
    api_latitude: float
    api_longitude: float
    elevation: float
    generationtime_ms: float
    timezone: str
    utc_offset_seconds: int

# Fetch Data
class FetchResult(BaseModel):
    """Result from fetching weather data."""
    model_config = ConfigDict(extra="allow")

    data: dict
    location: Location
    ingestion_metadata: IngestionMetadata
    api_metadata: APIMetadata

class FetchError(BaseModel):
    """Error encountered during data fetch."""
    location: Location
    error: str
    status: str = "failed"

# Standardized Output
class StandardizedWeatherHourly(BaseModel):
    """Standardized weather data for hourly frequency."""
    
    # Location
    location_name: str
    requested_latitude: float
    requested_longitude: float
    api_latitude: float
    api_longitude: float

    # Time
    timestamp: datetime

    # Weather data 
    temperature_2m: float | None = None
    precipitation: float | None = None
    relative_humidity_2m: float | None = None
    windspeed_10m: float | None = None
    wind_direction_10m: float | None = None
    cloud_cover: float | None = None
    weather_code: int | None = None

    # Metadata
    source_api: str = "open-meteo"
    ingestion_timestamp_utc: datetime
    run_id: str | None = None


class StandardizedWeatherDaily(BaseModel):
    """Standardized weather data for daily frequency."""
    
    # Location
    location_name: str
    requested_latitude: float
    requested_longitude: float
    api_latitude: float
    api_longitude: float

    # Time
    date: datetime

    # Weather data 
    temperature_2m_max: float | None = None
    temperature_2m_min: float | None = None
    precipitation_sum: float | None = None

    # Metadata
    source_api: str = "open-meteo"
    ingestion_timestamp_utc: datetime
    run_id: str | None = None


# Write Results
class WriteResult(BaseModel):
    """Result from writing data to storage."""
    path: str
    records_written: int
    format: str = "parquet"


# Pipeline Results
class PipelineResult(BaseModel):
    """Result from pipeline run."""
    success: bool
    run_id: str
    run_start: datetime
    run_end: datetime
    records_processed: int = 0
    files: list[WriteResult] = Field(default_factory=list)
    failed: list[FetchError] = Field(default_factory=list)
    error: str | None = None
    