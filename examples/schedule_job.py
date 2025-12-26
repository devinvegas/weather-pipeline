#!/usr/bin/env python
"""Example weather forecast pipeline scheduler using APScheduler."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from weather_ingestion import main as run_pipeline

# Configure logging
log_file = Path(__file__).parent / "scheduler.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

config = "configs/open-meteo.yml"


def run_job():
    """Run the pipeline."""
    logger.info("Starting pipeline run...")
    try:
        run_pipeline(config)
        logger.info("Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Scheduler starting...")
    scheduler = BackgroundScheduler()
    
    # Defines job to run every 3 hours
    scheduler.add_job(run_job, "interval", hours=3)
    
    # Run once immediately
    run_job()
    
    scheduler.start()
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        scheduler.shutdown()
        logger.info("Scheduler stopped.")
