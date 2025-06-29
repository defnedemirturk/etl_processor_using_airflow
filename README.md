# ETL Pipeline with Docker Compose
@author: Defne Demirtuerk

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed

## Quick Start

### Objective

Build a simple ETL pipeline that ingests raw JSON logs, transforms the data into a clean format, models it into a star schema, and loads it into a relational database (PostgreSQL).

### Files
- `raw_logs.json`: Sample raw data from the mobile app logs.

### Pipeline Summary
1. Ingest the data from `raw_logs.json`.
2. Clean and transform the data :
   - Convert timestamp to uniform ISO format.
   - Remove entries with missing user_id or action_type.
   - Extract useful fields like device, location.
3. Data modeling:
   - Create star schema with:
     - `fact_user_actions`
     - `dim_users`
     - `dim_actions`
4. Load the transformed data into a PostgreSQL database.
5. Implement basic data quality checks (e.g., null values, duplicates).
6. Infrastructure: Airflow for orchestration and Dockerize the pipeline.


### How to run the pipeline
1. **Build and start all services:**
   ```bash
   docker compose up -d
   ```

2. **Stop all services:**
   ```bash
   docker compose down -v
   ```

### Notes

- Configuration details are stated in `config.yaml`.
- Dependencies are managed in `requirements.txt`.
- Login details and database credentials are stated in `docker-compose.yaml`.
- The pipeline is set to be triggered every 24hrs but user can manually trigger the run through Airflow UI. 
