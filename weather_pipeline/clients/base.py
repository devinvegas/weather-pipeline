"""Base client protocol for data source abstraction."""

from typing import Protocol
from weather_pipeline.models import Location, FetchResult, FetchError

class DataSourceClient(Protocol):
    """Protocol defining the interface for data source clients.
    
    Implement this to add new data sources.
    """

    async def fetch_data(
            self, locations: list[Location]
    ) -> list[FetchResult | FetchError]:
        """Fetch weather data for given locations.

        Args:
            locations (list[Location]): List of locations to fetch data for.

        Returns:
            list[FetchResult | FetchError]: List of fetch results or errors for each location.

        """
        ...