# Distributed Data Processing Pipeline

A production-style PySpark project that simulates a ride-share company processing millions of trip records through bronze -> silver -> gold layers with validation, partitioning, retry logic, and monitoring.

## Architecture Overview
```
        +-------------+        +--------------+        +----------------+
        | Data Gen    |        | Ingestion    |        | Validation     |
        | (Python)    +------->+ (PySpark CSV)+------->+ (Rules, Schema)|
        +------+------+        +------+-------+        +------+---------+
               |                       |                       |
               v                       v                       v
        +------+------+        +------+-------+        +------+---------+
        | Bronze (raw)|        | Silver (clean)|      | Quarantine     |
        +------+------+        +------+-------+        +------+---------+
               |                       |
               v                       v
        +-----------------------+     +-------------------------+
        | Transformations       |-->--| Gold Aggregations       |
        | (features, partition) |     | (daily revenue, zones)  |
        +-----------+-----------+     +-----------+-------------+
                    |                             |
                    v                             v
             Monitoring & Reports         Analytics-ready outputs
```

## Folder Structure
```
distributed-data-processing-pipeline/
+-- config/               # YAML configs + schema definitions
+-- data/                 # bronze/silver/gold/quarantine/reports layers
+-- logs/                 # Centralized pipeline logs
+-- src/
¦   +-- main.py           # Typer CLI entry point
¦   +-- generator/        # Synthetic data generation
¦   +-- ingestion/        # Raw ingestion into Spark
¦   +-- validation/       # Business rule validators
¦   +-- transformation/   # Feature engineering + silver writer
¦   +-- aggregation/      # Gold layer builders
¦   +-- monitoring/       # Metrics tracker & summaries
¦   +-- utils/            # Logging, Spark session, retry helpers
¦   +-- models/           # Schema builders
+-- tests/                # Pytest suites (unit + integration)
+-- Dockerfile            # Containerized execution
+-- docker-compose.yml    # One-command local run
+-- Makefile              # Developer workflows
+-- requirements.txt      # Python dependencies
+-- README.md
```

## Key Capabilities
- **PySpark ETL** with explicit schemas, fault tolerance, and partition-aware Parquet writes.
- **Data quality** layer that separates invalid records, emits JSON reports, and stores quarantined rows.
- **Feature engineering** for analytics-ready metrics (duration, speed, fare per mile, pickup hour).
- **Gold tables** for business KPIs: vendor revenue, distance by zone, hourly trends, payment summaries.
- **Retry + observability** built into CLI (structured logging, metrics JSON, monitoring summary command).
- **Synthetic generator** that produces dirty ride-share datasets with duplicates, corrupt rows, and edge cases.
- **Docker + Makefile** workflows to mirror AWS-style S3 lakes locally.

## Tech Stack
- Python 3.11, PySpark, Typer CLI, Faker, Pandas (generation helper)
- Parquet storage for silver/gold layers
- Pytest for validation/transformation/gold tests
- Docker & docker-compose for reproducible runs

## Configuration
- `config/config.yaml` centralizes storage paths, Spark configs, validation thresholds, and aggregation specs.
- `config/schemas.json` defines the trip schema shared by ingestion and validation layers.

## Pipeline Stages
1. **Bronze** – CSV files produced by `generate-data`, stored as `data/bronze/trips/ingestion_date=YYYY-MM-DD/part*.csv`.
2. **Ingestion** – Spark reads bronze with fail-safe mode, logs stats, and routes corrupt rows to quarantine.
3. **Validation** – Mandatory columns + business rules (fare = 0, distance > 0, passenger counts, chronology).
4. **Transformation (Silver)** – Deduplication, derived metrics, normalized categorical fields, partitioned Parquet.
5. **Gold** – Aggregated analytics tables partitioned by date/vendor/zone/hour depending on KPI.
6. **Monitoring** – JSON metric artifacts capturing durations, row counts, and output paths for observability.

## Fault Tolerance & Partitioning
- Critical steps (ingestion + gold build) include retry logic with exponential backoff.
- Dynamic partition overwrite ensures idempotent reruns per ingestion_date.
- Quarantine layer isolates invalid records and preserves validation reasons.
- Silver Parquet outputs partitioned by `route_date` and `vendor_id`; gold tables partition on reporting dimensions.

## Monitoring & Reporting
- Every CLI run emits `data/reports/pipeline_metrics_<timestamp>.json`.
- `monitoring-summary` command prints the most recent summaries for quick health checks.
- Logs stored under `logs/pipeline.log` for auditability.

## Setup & Usage
```bash
# 1. Install deps
make install

# 2. Generate dirty synthetic raw data
make data INGESTION_DATE=2024-01-01 DATA_SIZE=small

# 3. Run full pipeline (bronze->silver->gold) for that date
make pipeline INGESTION_DATE=2024-01-01

# 4. Inspect outputs
ls data/silver/ingestion_date=2024-01-01
ls data/gold/daily_vendor_revenue/ingestion_date=2024-01-01

# 5. Review metrics + validation details
ls data/reports/
python src/main.py monitoring-summary --limit 3
```

### Windows without `make`
`make` is not available in a vanilla PowerShell session. Either install GNU Make (via Winget/Chocolatey/Git Bash) or run the equivalent commands manually:
```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\python src/main.py generate-data --size small --ingestion-date 2024-01-01
.\.venv\Scripts\python src/main.py run-pipeline --ingestion-date 2024-01-01
```
You can substitute other CLI commands (e.g., `validate-only`, `build-gold`) in place of `run-pipeline` as needed.

### Docker Flow
```bash
docker-compose up --build    # builds image, runs pipeline inside container
```

## CLI Commands
- `python src/main.py generate-data --size medium --ingestion-date 2024-01-02`
- `python src/main.py run-pipeline --ingestion-date 2024-01-02 --generate-size medium`
- `python src/main.py validate-only --ingestion-date 2024-01-02`
- `python src/main.py build-gold --ingestion-date 2024-01-02`
- `python src/main.py monitoring-summary --limit 5`

## Testing
```bash
make test
```

## Resume-Ready Highlights
- Distributed PySpark ETL with schema enforcement, partitioned Parquet, and gold KPI tables.
- Data quality isolation with quarantine + JSON reports and retry-wrapped ingestion.
- Full DevOps story: Typer CLI, Makefile, Docker, tests, monitoring artifacts, and documentation.

## Future Enhancements
- Wire to LocalStack S3 buckets for object storage parity.
- Add Airflow DAG scaffolding or Dagster job definitions.
- Extend monitoring to push metrics into Prometheus/OpenTelemetry exporters.
- Add quality expectations via libraries like Great Expectations for richer rule sets.
