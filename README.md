# LA Crime Data Pipeline

LA Crime Data Pipeline is an end‑to‑end ELT/ETL/data‑warehouse pipeline that ingests crime data for Los Angeles (2020–present), transforms and loads it into a structured data warehouse (star schema), making it ready for analytics and dashboarding (e.g. Power BI).

## Motivation

Public crime data in raw format is often messy, with inconsistent schemas, duplicates, missing values etc.
This pipeline aims to:

automate ingestion of raw crime data,

apply cleaning, transformation and structuring,

load into a normalized dimensional model for efficient analytics,

support incremental updates (so new data can be appended without reprocessing everything),

provide an easy, repeatable, reproducible workflow using modern data‑engineering tools.

## What the pipeline does

Data acquisition: fetches raw crime data (source, format, update frequency)

Staging / raw layer: stores raw data without transformation

Transformation: clean data, handle missing values / duplicates / type conversions, standardize schema

Warehouse load: load transformed data into a star‑schema data warehouse (fact + dimension tables)

Incremental loading: only process new data (not entire dataset every time)

Orchestration: pipelines are orchestrated using Apache Airflow + dbt, containerized using Docker / Astro CLI.

Output: Analytics‑ready warehouse; compatible with BI tools (e.g. Power BI) for reporting / dashboards.

## Tech Stack & Dependencies

Apache Airflow (via Astro Docker image) — for orchestration

dbt — for transformations and modeling (dimensional modeling / star schema)

PostgreSQL — data warehouse / final storage

Docker / Docker‑Compose — for containerization and easy deployment

Python (with relevant libraries) — for any custom scripts / data ingestion / transformations

