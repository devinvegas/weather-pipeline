# Weather Data Ingestion Pipeline

A production-ready Python pipeline that ingests weather forecast data from the OpenMeteo API and stores it in a scalable Parquet data lake.

## Installation

### Option 1: Using `pyproject.toml` (Recommended)

```bash
# Install package with dependencies
python -m pip install -e .

# Install with development dependencies (for testing)
python -m pip install -e ".[dev]"
```

### Option 2: Using `requirements.txt`

```bash
# Quick setup
python -m pip install -r requirements.txt

# For tests (optional)
python -m pip install pytest pytest-cov
```

### Python Version Requirements

**Minimum:** Python 3.11+  
**Tested on:** Python 3.14.2 (recommended)  

If you have `pyenv` installed, the `.python-version` file ensures the correct Python version is used:
```bash
pyenv install 3.14.2  # First time only
pyenv exec python -m pip install -e .
```

## Quick Start

### Prerequisites
- Python 3.11+ installed
- Dependencies installed (see Installation above)

### Run the Pipeline
```bash
python weather_ingestion.py configs/open-meteo.yml
```

**Output:**
- Fetches 7-day forecasts for 6 global locations (for 'daily' forecasts, change config in `configs/open-meteo.yml` to `hourly` for hourly forcasts)
- Writes ~42 records to `data/open_meteo/daily/{location}/{YYYY}/{MM}/{DD}/forecast_*.parquet`
- Tracks fetch state in `data/pipeline_state.json`

**Expected Output:**
```
[INFO] Pipeline run ... started at ...
[INFO] Locations to process: ['London', 'Tokyo', 'Sydney', 'New York', 'Cairo', 'Rio de Janeiro']
[INFO] Interval: daily
...
[SUCCESS] Pipeline succeeded: 42 records written
```

## Scheduling

### APScheduler Example

Run the pipeline on a schedule using APScheduler:

```bash
python examples/schedule_job.py
```

**Features:**
- Runs immediately on startup
- Then runs every 3 hours automatically
- Logs to both console and `examples/scheduler.log`
- Graceful shutdown (Ctrl+C)
- Error tracking and logging

**Output:**
```
2025-12-26 12:00:00 - INFO - Scheduler starting...
2025-12-26 12:00:01 - INFO - Starting pipeline run...
2025-12-26 12:05:00 - INFO - Pipeline completed successfully.
2025-12-26 12:05:01 - INFO - Scheduler running. Press Ctrl+C to stop.
```

**View Logs:**
Open another terminal and run:
```bash
# Linux/Mac 
tail -f examples/scheduler.log

# Windows PowerShell 
Get-Content examples/scheduler.log -Tail 10 -Wait

# Or view the file
cat examples/scheduler.log
```

### Customization

To modify the schedule interval, edit `examples/schedule_job.py`:
```python
scheduler.add_job(run_job, "interval", hours=6)  # Change 3 to desired hours
```

### Airflow DAG Example

Example Airflow DAG provided in `examples/airflow/` showing production orchestration patterns in production:
- Task dependencies (Fetch - Validate - Notify)
- Automatic retries
- Error handling
- State inspection

## Architecture

### Components

```
weather_pipeline/
├── clients/          # API integrations
│   └── openmeteo.py  # OpenMeteo client with retry logic
├── transforms/       # Data transformation
│   └── transform.py  # Standardize API responses
├── writers/          # Storage backends
│   └── parquet.py    # Columnar Parquet output
├── config_handler/   # Configuration management
│   └── load_configs.py  # YAML config + Pydantic validation
├── state/            # State management
│   └── json_store.py # Track fetch success/failure
└── models/           # Data schemas
    └── schemas.py    # Pydantic models for type safety
```

### Data Flow

```
OpenMeteo API
    +
[Async Fetch] (concurrent, rate-limited)
    +
[Transform] (standardize to common schema)
    +
[State Tracking] (prevent duplicates)
    +
[Parquet Write] (partitioned by location & date)
```

### Design Principles

1. **Protocol-Based Abstractions** - Swap clients (WeatherAPI, VisualCrossing) or writers (S3, DuckDB) without changing pipeline logic
2. **Async I/O** - Concurrent API calls with semaphore rate limiting 
3. **Fault Tolerance** - Exponential backoff retry logic, state management for resumability
4. **Scalable Storage** - Partitioned Parquet with semantic naming: `{provider}/{interval}/{location}/{YYYY}/{MM}/{DD}/`
5. **Configuration-Driven** - YAML-based config with Pydantic validation, supports both daily & hourly forecasts

The modular design makes it easy to add new clients, storage formats, and writers without modifying the core pipeline.

## Features

### Implemented

- **Async HTTP Client** - Concurrent requests with timeout & retry logic
- **Exponential Backoff** - 3 retries, 2x backoff factor for transient failures (configurable)
- **State Management** - JSON-based tracking of fetch success/failure per location
- **Parquet Storage** - Columnar format with zstd compression (configurable)
- **Semantic Partitioning** - Query-friendly hierarchy: provider/interval/location/date
- **Logging** - Module-level loggers with execution timing
- **Type Safety** - Pydantic models with enforced type validation for all data transformations
- **Configuration Merging** - Base + API-specific config with deep merge

### Validation & Observability

#### View Fetch State
```bash
cat data/pipeline_state.json | python -m json.tool
```
Shows: last fetch timestamp, status, record count, forecast end date per location

#### Validate Data Quality
```bash
jupyter notebook notebooks/data_validation_duckdb.ipynb
```
DuckDB-based validation covering:
- Schema inspection
- Row counts per location
- Null value detection
- Duplicate detection
- Data ranges (temperature, precipitation)
- Cross-location comparison

## Configuration

### Example: `configs/open-meteo.yml`

```yaml
api:
  provider: open_meteo
  base_url: "https://api.open-meteo.com/v1/forecast"
  timeout_seconds: 30
  max_concurrent_requests: 5
  max_retries: 3
  retry_backoff_factor: 2.0

interval: daily
forecast_days: 7
timezone: GMT

locations:
  - name: London
    latitude: 51.5074
    longitude: -0.1278
  # ... additional locations
```

## Storage Format

### Parquet Schema (Daily Forecast)

```
location_name: string
timestamp: date
temperature_2m_max: double (°C)
temperature_2m_min: double (°C)
precipitation_sum: double (mm)
requested_latitude: double
requested_longitude: double
api_latitude: double
api_longitude: double
elevation: double (meters)
timezone: string (UTC/GMT)
ingestion_timestamp_utc: timestamp
run_id: string (UUID)
```

### File Organization

```
data/open_meteo/
└── daily/
    ├── London/
    │   └── 2025/12/25/forecast_20251225.parquet
    ├── Cairo/
    │   └── 2025/12/25/forecast_20251225.parquet
    └── ...
```

## Production Deployment

See `ASSESSMENT_REVIEW.md` for detailed answers on:
- Orchestration (Airflow DAG patterns)
- Monitoring & alerting
- Scaling to 1000+ locations
- Multi-API integration

## Assessment Notes

Time spent: ~2.5 hours

See `notes.txt` for:
- Time breakdown
- AI tool usage (Copilot for boilerplate)
- Design decisions & tradeoffs
- Future improvements

## Testing

### Unit Tests (Optional)
```bash
pytest tests/
```

### Integration Test
```bash
python weather_ingestion.py configs/open-meteo.yml
jupyter notebook notebooks/data_validation.ipynb
```

## Dependencies

- **polars** - DataFrame operations
- **pydantic** - Data validation & schemas
- **httpx** - Async HTTP client
- **pyyaml** - Configuration parsing
- **duckdb** - Data validation (optional, for notebook)

## Example Queries (DuckDB)

### How many records per location?
```sql
SELECT location_name, COUNT(*) FROM parquet_scan('data/open_meteo/daily/*/*.parquet')
GROUP BY location_name;
```

### What's the temperature range?
```sql
SELECT location_name, 
  ROUND(MIN(temperature_2m_max), 1) as min_temp,
  ROUND(MAX(temperature_2m_max), 1) as max_temp
FROM parquet_scan('data/open_meteo/daily/*/*.parquet')
GROUP BY location_name;
```

### Any duplicates?
```sql
SELECT location_name, timestamp, COUNT(*) 
FROM parquet_scan('data/open_meteo/daily/*/*.parquet')
GROUP BY location_name, timestamp
HAVING COUNT(*) > 1;
```

---

**Next Steps:**
1. Run the pipeline: `python weather_ingestion.py configs/open-meteo.yml`
2. Check the state: `cat data/pipeline_state.json`
3. Validate data quality: `jupyter notebook notebooks/data_validation.ipynb`
4. Query the data: See examples above
