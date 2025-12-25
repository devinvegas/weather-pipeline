"""Abstract protocol for state storage backends."""

from typing import Protocol

from weather_pipeline.state.models import LocationFetchState, PipelineState


class StateStore(Protocol):
    """Protocol for state persistence backends (JSON, SQLite, etc)."""

    def load(self) -> PipelineState:
        """Load state from storage."""
        ...

    def save(self, state: PipelineState) -> None:
        """Persist state to storage."""
        ...

    def get_location(
        self, location_name: str, provider: str, interval: str
    ) -> LocationFetchState:
        """Get state for a specific location."""
        ...

    def update_location(self, location_state: LocationFetchState) -> None:
        """Update state for a specific location."""
        ...
