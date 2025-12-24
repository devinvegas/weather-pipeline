"""Models package with all schema definitions."""

from .schemas import (
    Location,
    IngestionMetadata,
    APIMetadata,
    FetchResult,
    FetchError,
    StandardizedWeatherHourly,
    StandardizedWeatherDaily,
    WriteResult,
    PipelineResult,
)

__all__ = [
    "Location",
    "IngestionMetadata",
    "APIMetadata",
    "FetchResult",
    "FetchError",
    "StandardizedWeatherHourly",
    "StandardizedWeatherDaily",
    "WriteResult",
    "PipelineResult",
]

