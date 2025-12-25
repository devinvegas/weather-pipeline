"""OpenMeteo API client implementation."""

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from weather_pipeline.models import (
    Location,
    IngestionMetadata,
    APIMetadata,
    FetchResult,
    FetchError,
)

logger = logging.getLogger(__name__)

# Transient errors that should trigger a retry
RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
)


class OpenMeteoClient:
    """Client for OpenMeteo weather API.
    
    Implements the DataSourceClient protocol with retry logic for transient failures.
    """

    def __init__(
            self,
            base_url: str,
            timeout: int,
            max_concurrent_requests: int,
            forecast_days: int,
            timezone: str,
            hourly_params: list[str] | None = None,
            daily_params: list[str] | None = None,
            max_retries: int = 3,
            backoff_factor: float = 2.0,
              
    ):
        
            self.base_url = base_url
            self.timeout = timeout
            self.max_concurrent_requests = max_concurrent_requests
            self.hourly_params = hourly_params
            self.daily_params = daily_params
            self.forecast_days = forecast_days
            self.timezone = timezone
            self.max_retries = max_retries
            self.backoff_factor = backoff_factor
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
            ingestion_timestamp_utc=ingestion_timestamp,
            request_url=str(response.url),
            elapsed_ms=response.elapsed.total_seconds() * 1000,
            status_code=response.status_code,
        )

        api_metadata = APIMetadata(
            api_latitude=raw_data.get("latitude"),
            api_longitude=raw_data.get("longitude"),
            elevation=raw_data.get("elevation"),
            generationtime_ms=raw_data.get("generationtime_ms"),
            timezone=raw_data.get("timezone", "GMT").replace("UTC", "GMT"),  # Map UTC -> GMT for compatibility with Polars
            utc_offset_seconds=raw_data["utc_offset_seconds"],
        )

        return FetchResult(
            data=raw_data,
            location=location,
            ingestion_metadata=ingestion_metadata,
            api_metadata=api_metadata,
        )
    
    async def _fetch_with_retry(self, location: Location) -> FetchResult:
        """Fetch data with exponential backoff retry for transient failures.
        
        Args:
            location: Location to fetch data for
            
        Returns:
            FetchResult on success
            
        Raises:
            Exception: If all retries are exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await self._fetch_single(location)
            except RETRYABLE_EXCEPTIONS as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.warning(
                        f"Transient error for {location.name}: {e}. "
                        f"Retry {attempt + 1}/{self.max_retries} after {wait_time:.1f}s"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"All {self.max_retries} retries exhausted for {location.name}: {e}"
                    )
        
        # All retries exhausted
        raise last_exception
    
    async def _fetch_with_semaphore(
              self, location: Location
    ) -> FetchResult | FetchError:
        """Fetch data with concurrency limiting, retry logic, and error handling."""
        async with self._semaphore:
            try:
                return await self._fetch_with_retry(location)
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