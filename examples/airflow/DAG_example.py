"""
Example Airflow DAG for  pipeline orchestration.

This DAG demonstrates the standard approach to how I normally orchestrate pipelines in Airflow,
including error handling, notifications, and task dependencies.

"""

import os
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


# Configuration - Production References
ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")  # eg, dev, staging, prod 
SHORTCODE = Variable.get("ENVIRONMENT_SHORTCODE", "dev")

# Cloud Storage Paths
SCRIPTS_DIR = f"gs://{SHORTCODE}-weather-automation/airflow/scripts"
CONFIG_DIR = f"gs://{SHORTCODE}-weather-automation/airflow/config"
DATA_DIR = f"gs://{SHORTCODE}-weather-automation/data"

# Local pipeline config (could also be fetched from GCS)
PIPELINE_CONFIG = "configs/open-meteo.yml"

# MS Teams Notifications + email
TEAMS_WEBHOOK = Variable.get("TEAMS_WEBHOOK_URL", "")
EMAIL_RECIPIENTS = Variable.get("EMAIL_RECIPIENTS", "DENG-team@example.com").split(",")


default_args = {
    "owner": "data-engineering",
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_pipeline(**context):
    """Execute the weather pipeline."""
    import sys
    sys.path.insert(0, "/path/to/DataEngineer-2025")
    
    from weather_ingestion import main as pipeline_main
    
    try:
        pipeline_main(PIPELINE_CONFIG)
        return {"status": "success"}
    except Exception as e:
        raise Exception(f"Pipeline failed: {str(e)}")


def send_teams_notification(context, status):
    """Send notification to Teams channel."""
    if not TEAMS_WEBHOOK:
        return
    
    task = context["task_instance"]
    execution_date = context["execution_date"]
    
    payload = {
        "title": f"Pipeline {status.upper()}: weather-{ENVIRONMENT}",
        "text": (
            f"Task: {task.task_id}\n"
            f"Status: {status}\n"
            f"Environment: {ENVIRONMENT}\n"
            f"Execution Date: {execution_date}\n"
            f"Log: {task.log_url}"
        ),
    }
    
    try:
        requests.post(TEAMS_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        print(f"Failed to send Teams notification: {e}")


def validate_data(**context):
    """Validate pipeline output."""
    import sys
    sys.path.insert(0, "/path/to/DataEngineer-2025")
    
    from weather_pipeline.state import JsonStateStore
    
    state_store = JsonStateStore()
    state = state_store.load()
    
    total_records = sum(
        loc.records_fetched 
        for loc in state.locations.values()
    )
    
    if total_records == 0:
        raise ValueError("No records processed in pipeline run")
    
    return {"total_records": total_records}


# Create DAG
with DAG(
    f"weather-pipeline-{ENVIRONMENT}",
    default_args=default_args,
    description="Daily weather data ingestion pipeline",
    schedule_interval="0 2 * * *",  
    catchup=False,
    tags=["weather", "data-engineering", "openmeteo"],
) as dag:

    # Task 1: Run pipeline
    fetch_task = PythonOperator(
        task_id="fetch-weather-data",
        python_callable=run_pipeline,
        provide_context=True,
        on_failure_callback=lambda context: send_teams_notification(context, "failed"),
        on_success_callback=lambda context: send_teams_notification(context, "success"),
    )

    # Task 2: Validate data
    validate_task = PythonOperator(
        task_id="validate-data",
        python_callable=validate_data,
        provide_context=True,
    )


    completion_task = BashOperator(
        task_id="pipeline-complete",
        bash_command=f'echo "Weather forecast data pipeline run completed successfully"',
    )
