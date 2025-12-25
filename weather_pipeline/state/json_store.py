"""JSON file-based state store implementation."""

import json
import logging
from pathlib import Path

from pydantic import ValidationError

from weather_pipeline.state.models import LocationFetchState, PipelineState

logger = logging.getLogger(__name__)


class JsonStateStore:
    """JSON file-based state persistence."""

    def __init__(self, state_file: str | Path = "data/pipeline_state.json"):
        """Initialize state store.

        Args:
            state_file: Path to JSON state file
        """
        self.state_file = Path(state_file)
        self._state: PipelineState | None = None

    def load(self) -> PipelineState:
        """Load state from JSON file, create empty if not found."""
        if self._state is not None:
            return self._state

        if not self.state_file.exists():
            logger.debug(f"State file not found at {self.state_file}, creating new")
            self._state = PipelineState()
            return self._state

        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            self._state = PipelineState(**data)
            logger.debug(f"Loaded state from {self.state_file}")
            return self._state
        except (json.JSONDecodeError, ValidationError) as e:
            logger.warning(f"Failed to load state from {self.state_file}: {e}, creating new")
            self._state = PipelineState()
            return self._state

    def save(self, state: PipelineState) -> None:
        """Persist state to JSON file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        with open(self.state_file, "w") as f:
            json.dump(state.model_dump(mode="json"), f, indent=2, default=str)

        logger.debug(f"Saved state to {self.state_file}")
        self._state = state

    def get_location(
        self, location_name: str, provider: str, interval: str
    ) -> LocationFetchState:
        """Get location state, creating if needed."""
        state = self.load()
        return state.get_location(location_name, provider, interval)

    def update_location(self, location_state: LocationFetchState) -> None:
        """Update location state and persist."""
        state = self.load()
        key = f"{location_state.provider}:{location_state.interval}:{location_state.location_name}"
        state.locations[key] = location_state
        state.update_last_modified()
        self.save(state)

    def mark_fetch_success(
        self,
        location_name: str,
        provider: str,
        interval: str,
        records_fetched: int,
        forecast_end_date: str | None = None,
    ) -> None:
        """Mark a fetch as successful."""
        location_state = self.get_location(location_name, provider, interval)
        location_state.mark_success(records_fetched, forecast_end_date)
        self.update_location(location_state)

    def mark_fetch_failure(
        self,
        location_name: str,
        provider: str,
        interval: str,
        error: str,
    ) -> None:
        """Mark a fetch as failed."""
        location_state = self.get_location(location_name, provider, interval)
        location_state.mark_failure(error)
        self.update_location(location_state)
