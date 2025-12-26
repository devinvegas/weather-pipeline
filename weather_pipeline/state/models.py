"""State models for tracking fetch operations across locations and providers."""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class LocationFetchState(BaseModel):
    """Track fetch state for a specific location and provider."""

    location_name: str
    provider: str
    interval: str  

    # Status tracking
    last_fetch_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_fetch_status: Literal["success", "failure", "partial"] = "success"
    last_fetch_error: str | None = None

    # Data tracking
    records_fetched: int = 0
    forecast_end_date: str | None = None  

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def is_fresh(self, hours: int = 6) -> bool:
        """Check if fetch is recent enough to skip refetch.

        Args:
            hours: How recent the fetch must be (default 6 hours)

        Returns:
            True if last fetch was within N hours and successful
        """
        if self.last_fetch_status != "success":
            return False

        elapsed = (datetime.now(timezone.utc) - self.last_fetch_timestamp).total_seconds() / 3600
        return elapsed < hours

    def mark_success(
        self,
        records_fetched: int,
        forecast_end_date: str | None = None,
    ) -> None:
        """Mark a fetch as successful."""
        self.last_fetch_timestamp = datetime.now(timezone.utc)
        self.last_fetch_status = "success"
        self.last_fetch_error = None
        self.records_fetched = records_fetched
        if forecast_end_date:
            self.forecast_end_date = forecast_end_date

    def mark_failure(self, error: str) -> None:
        """Mark a fetch as failed."""
        self.last_fetch_timestamp = datetime.now(timezone.utc)
        self.last_fetch_status = "failure"
        self.last_fetch_error = error
        self.records_fetched = 0


class PipelineState(BaseModel):
    """Root state document tracking all locations across all providers."""

    version: str = "1.0"
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    locations: dict[str, LocationFetchState] = Field(default_factory=dict)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def get_location(self, location_name: str, provider: str, interval: str) -> LocationFetchState:
        """Get or create state for a location."""
        key = f"{provider}:{interval}:{location_name}"
        if key not in self.locations:
            self.locations[key] = LocationFetchState(
                location_name=location_name,
                provider=provider,
                interval=interval,
            )
        return self.locations[key]

    def update_last_modified(self) -> None:
        """Update the last modified timestamp."""
        self.last_updated = datetime.now(timezone.utc)
