"""
Entrypoint script for weather data ingestion.

Fetches forecast data from OpenMeteo API and write to Parquet.

Usage:
  python weather_ingestion.py
  python weather_ingestion.py configs/open-meteo.yml
"""

import sys

from weather_pipeline.config_handler.load_configs import load_config_yml
from weather_pipeline.pipeline import run_pipeline

def main(config_path: str | None = None)-> int:
    """Main entrypoint for the weather ingestion pipeline."""
    
    try:
      # Load configs
      config = load_config_yml(config_path=config_path)
      print(f"Loaded configuration for {len(config.locations)} locations, intervals: {config.interval}")

      # Run pipeline
      result = run_pipeline(config)

      # Reporting
      if result.success:
          print(f"\n✓ Pipeline succeeded: {result.records_written} records written")
          return 0
      else:
          print(f"\n✗ Pipeline failed: {result.error}")
          return 1
        
    except FileNotFoundError as e:
        print(f"Configuration file not found: {e}")
        return 1
    except Exception as e:
        print(f"Pipeline error: {e}")
        return 1
    
  
if __name__ == "__main__":
      config_file = sys.argv[1] if len(sys.argv) > 1 else None
      sys.exit(main(config_file))