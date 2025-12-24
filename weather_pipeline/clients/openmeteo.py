"""OpenMeteo API client implementation."""

import asyncio
from datetime import datetime, timezone

import httpx

from weather_pipeline.models import (
    Location,
    IngestionMetadata,
    APIMetadata,
    FetchResult,
    FetchError,
)  


class OpenMeteoClient:
    """Client for OpenMeteo weather API.
    
    Implements the DataSourceClient protocol.
    """

    def __init__(
            self,
            base_url: str = "https://api.open-meteo.com/v1/forecast",
            timeout: int = 10,
            max_concurrent_requests: int = 5,
            hourly_params: list[str] | None = None,
            daily_params: list[str] | None = None,
            forecast_days: int = 7,
            timezone: str = "UTC",  
    ):
        
            self.base_url = base_url
            self.timeout = timeout
            self.max_concurrent_requests = max_concurrent_requests
            self.hourly_params = hourly_params
            self.daily_params = daily_params
            self.forecast_days = forecast_days
            self.timezone = timezone
            self._semaphore = asyncio.Semaphore(max_concurrent_requests)

    def _build_params(self, location: Location) -> dict:
        """Build query parameters for the API request."""
        
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "forecast_days": self.forecast_days,
            "timezone": self.timezone,
        }

        if self.hourly_params:
            params["hourly"] = ",".join(self.hourly_params)
        if self.daily_params:
            params["daily"] = ",".join(self.daily_params)
        return params
    
    async def _fetch_single(self, location: Location) -> FetchResult | FetchError:
        """Fetch weather data for a single location."""
        params = self._build_params(location)
        ingestion_timestamp = datetime.now(timezone.utc)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
             response = await client.get(self.base_url, params=params)

        raw_data = response.json()

        # Check for API errors
        if raw_data.get("error"):
             raise ValueError(f"API Error: {raw_data.get('reason')}")
        
        # Build metadata
        ingestion_metadata = IngestionMetadata(
            timestamp=ingestion_timestamp,
            request_url=str(response.url),
            elapsed_ms=response.elapsed.total_seconds() * 1000,
            status_code=response.status_code,
        )

        api_metadata = APIMetadata(
            latitude=raw_data.get("latitude"),
            longitude=raw_data.get("longitude"),
            elevation=raw_data.get("elevation"),
            generationtime_ms=raw_data.get("generationtime_ms"),
            timezone=raw_data.get("timezone"),
            utc_offset_seconds=raw_data["utc_offset_seconds"],
        )

        return FetchResult(
            data=raw_data,
            location=location,
            ingestion_metadata=ingestion_metadata,
            api_metadata=api_metadata,
        )
    
    async def _fetch_with_semaphore(
              self, location: Location
    ) -> FetchResult | FetchError:
        """Fetch data with concurrency limiting and error handling."""
        async with self._semaphore:
            try:
                return await self._fetch_single(location)
            except Exception as e:
                return FetchError(location=location, error=str(e))
            
    async def fetch_data(
            self, locations: list[Location]
    ) -> list[FetchResult | FetchError]:
        """Fetch weather data for given location(s)."""
        tasks = [
            self._fetch_with_semaphore(location) for location in locations
        ]

        results = await asyncio.gather(*tasks)
        return list(results)