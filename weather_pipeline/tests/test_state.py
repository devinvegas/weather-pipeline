"""Tests for state management."""

import pytest
from weather_pipeline.state import JsonStateStore


def test_state_store_persistence(temp_dir):
    """Test state store persistence: save, load, and verify data integrity."""
    state_file = temp_dir / "test_state.json"
    
    store = JsonStateStore(state_file=state_file)
    
    # Mark fetch success
    store.mark_fetch_success("London", "open_meteo", "daily", 42)
    
    # Load and verify success
    state = store.load()
    key = "open_meteo:daily:London"
    assert key in state.locations
    assert state.locations[key].records_fetched == 42
    assert state.locations[key].last_fetch_status == "success"
    
    # Mark fetch failure
    store.mark_fetch_failure("Tokyo", "open_meteo", "daily", "Connection timeout")
    
    # Load and verify failure
    state = store.load()
    key = "open_meteo:daily:Tokyo"
    location_state = state.locations[key]
    assert location_state.last_fetch_status == "failure"
    assert "Connection timeout" in location_state.last_fetch_error