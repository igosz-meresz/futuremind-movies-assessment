# Movie Box Office Data Warehouse

A data engineering assessment project that ingests daily movie revenue data, enriches it with metadata from the OMDb API, and builds a dimensional data model in BigQuery for analytics.

## Overview

This project demonstrates an end-to-end data pipeline that:

1. **Extracts** daily box office revenue data from CSV (~340k records, 6,500+ movies)
2. **Enriches** top movies with metadata from the OMDb API (ratings, genres, directors, etc.)
3. **Loads** data into BigQuery staging tables
4. **Transforms** data into a dimensional model using dbt
5. **Visualizes** rankings and trends in Looker Studio

![Architecture Diagram](docs/architecture.png)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────────────┤
│  [revenues_per_day.csv]              [OMDb API]                         │
│         │                                 │                             │
│         ▼                                 ▼                             │
│  ┌─────────────────────────────────────────────────┐                    │
│  │     Python Pipeline (src/)                      │                    │
│  │     • CSV parsing with data quality handling    │                    │
│  │     • API enrichment with caching               │                    │
│  │     • BigQuery loading                          │                    │
│  └─────────────────────────────────────────────────┘                    │
│                           │                                             │
│                           ▼                                             │
│              ┌────────────────────────┐                                 │
│              │  BigQuery Staging      │                                 │
│              │  • stg_revenues_raw    │                                 │
│              │  • stg_movies_enriched │                                 │
│              └────────────────────────┘                                 │
│                           │                                             │
│                           ▼                                             │
│              ┌────────────────────────┐                                 │
│              │  dbt Transformations   │                                 │
│              └────────────────────────┘                                 │
│                           │                                             │
│         ┌─────────────────┼─────────────────┐                           │
│         ▼                 ▼                 ▼                           │
│    ┌──────────┐    ┌──────────────┐   ┌─────────────────┐               │
│    │ dim_date │    │  dim_movie   │   │ dim_distributor │               │
│    └──────────┘    └──────────────┘   └─────────────────┘               │
│         │                 │                 │                           │
│         └─────────────────┼─────────────────┘                           │
│                           ▼                                             │
│              ┌────────────────────────┐                                 │
│              │  fact_daily_revenue    │                                 │
│              └────────────────────────┘                                 │
│                           │                                             │
│                           ▼                                             │
│              ┌────────────────────────┐                                 │
│              │   Looker Studio        │                                 │
│              │   Dashboard            │                                 │
│              └────────────────────────┘                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Model

### Entity-Relationship Diagram

```
┌─────────────────────┐          ┌─────────────────────────────┐
│     dim_date        │          │        dim_movie            │
├─────────────────────┤          ├─────────────────────────────┤
│ date_key (PK)       │          │ movie_key (PK)              │
│ full_date           │          │ title                       │
│ day_of_week         │          │ year_released               │
│ day_of_week_name    │          │ rated                       │
│ month_name          │          │ runtime                     │
│ quarter             │          │ genre                       │
│ year                │          │ director                    │
│ is_weekend          │          │ imdb_rating                 │
└─────────┬───────────┘          │ imdb_votes                  │
          │                      │ is_enriched                 │
          │ 1                    └──────────────┬──────────────┘
          │                                     │
          ▼ ∞                                 1 │
┌─────────────────────────────────────────────────────────────┐
│                    fact_daily_revenue                       │
├─────────────────────────────────────────────────────────────┤
│ revenue_id (PK)                                             │
│ date_key (FK)                                               │
│ movie_key (FK)                                              │
│ distributor_key (FK)                                        │
│ daily_revenue                                               │
│ theater_count                                               │
│ revenue_per_theater                                         │
└─────────────────────────────────────────────────────────────┘
          ▲ ∞
          │
          │ 1
┌─────────┴───────────┐
│   dim_distributor   │
├─────────────────────┤
│ distributor_key (PK)│
│ distributor_name    │
│ is_major_studio     │
└─────────────────────┘
```

### Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Surrogate keys** | Decouples warehouse from source system quirks; handles special characters in titles |
| **`is_enriched` flag** | Not all movies have OMDb data due to API limits; allows filtering by data completeness |
| **`revenue_per_theater` in fact** | Pre-calculated for query performance; commonly used metric |
| **String genre field** | Trade-off for simplicity; in production, would normalize to bridge table |
| **Top 800 movies enriched** | API rate limit constraint (1,000/day); covers ~95% of total revenue (Pareto principle) |

## Project Structure

```
futuremind-assessment/
├── README.md
├── requirements.txt
├── Dockerfile
├── .env.example
├── .gitignore
│
├── src/
│   ├── __init__.py
│   ├── config.py                 # Environment configuration
│   ├── main.py                   # Pipeline orchestration
│   ├── extract/
│   │   ├── csv_parser.py         # CSV ingestion with quality handling
│   │   └── omdb_client.py        # OMDb API client with caching
│   └── load/
│       └── bigquery_loader.py    # BigQuery staging loader
│
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── _staging.yml
│       │   ├── stg_revenues.sql
│       │   └── stg_movies.sql
│       ├── dimensions/
│       │   ├── dim_date.sql
│       │   ├── dim_movie.sql
│       │   └── dim_distributor.sql
│       └── marts/
│           └── fact_daily_revenue.sql
│
├── data/
│   ├── raw/                      # Input CSV (gitignored)
│   └── cache/                    # OMDb API cache
│
└── docs/
    └── architecture.png
```

## Setup Instructions

### Prerequisites

- Python 3.11+
- Google Cloud SDK (`gcloud`)
- dbt-bigquery
- OMDb API key ([register here](https://www.omdbapi.com/apikey.aspx))

### 1. Clone and Install Dependencies

```bash
git clone <repository-url>
cd futuremind-assessment

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy example env file
cp .env.example .env

# Edit with your values
# OMDB_API_KEY=your_key_here
# GCP_PROJECT_ID=your-project-id
# BQ_DATASET=futuremind_movies
```

### 3. Setup GCP

```bash
# Authenticate
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project your-project-id

# Create BigQuery dataset
bq mk --dataset --location=EU your-project-id:futuremind_movies
```

### 4. Place Data File

```bash
# Copy revenues CSV to data directory
cp /path/to/revenues_per_day.csv data/raw/
```

### 5. Run Pipeline

```bash
# Run Python pipeline (extract, enrich, load staging)
python -m src.main

# Run dbt transformations
cd dbt
dbt deps
dbt build
```

## Pipeline Results

| Metric | Value |
|--------|-------|
| Revenue records loaded | 337,818 |
| Unique movies | 6,545 |
| Date range | 2000-01-01 to 2023-03-06 |
| Total revenue | $205.1 billion |
| Movies enriched via API | 790/800 (98.75% match rate) |
| API calls used | 800 |

### Data Quality Summary

| Issue | Count | Handling |
|-------|-------|----------|
| Empty theater count | 161 | Loaded as NULL |
| Zero revenue records | 63 | Filtered in staging |
| Missing distributor | 7,419 | Loaded as NULL |
| OMDb title mismatches | 10 | Logged, marked as not enriched |

## Dashboard

The Looker Studio dashboard provides:

- **Top Movies by Total Revenue** — Ranked bar chart
- **Distributor Market Share** — Revenue breakdown by studio
- **Revenue Trends Over Time** — Time series with date filtering
- **Top Rated Movies with Revenue** — Combined view of ratings and box office performance

[View Dashboard](https://lookerstudio.google.com/reporting/dbc501e8-6c64-41cc-a1fa-612e8b501f2d)

![Dashboard Screenshot](docs/dashboard_screenshot.png)

## Containerization

The pipeline is containerized and ready for Cloud Run deployment:

```bash
# Build image
docker build -t futuremind-pipeline .

# Run locally
docker run \
  -v ~/.config/gcloud:/home/appuser/.config/gcloud:ro \
  -v $(pwd)/data:/app/data \
  -e OMDB_API_KEY=your_key \
  -e GCP_PROJECT_ID=your-project \
  futuremind-pipeline
```

## Production Considerations

This assessment demonstrates a working pipeline. For production deployment, I would add:

### Orchestration (Dagster)

```
┌─────────────────────────────────────────────────────────────┐
│  Dagster would provide:                                     │
│  • Asset-based scheduling and dependency management         │
│  • Built-in retry logic and alerting                        │
│  • Incremental processing for daily data                    │
│  • Observability and lineage tracking                       │
└─────────────────────────────────────────────────────────────┘
```

### Additional Enhancements

| Area | Enhancement |
|------|-------------|
| **Data Quality** | Great Expectations or dbt tests for schema validation |
| **Incremental Loads** | Process only new dates, not full reload |
| **SCD Type 2** | Track distributor name changes over time |
| **Genre Normalization** | Bridge table for multi-genre movies |
| **CI/CD** | GitHub Actions for dbt testing and deployment |
| **Monitoring** | Cloud Monitoring alerts for pipeline failures |
| **Cost Optimization** | Partition fact table by date, cluster by movie_key |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| Data Warehouse | Google BigQuery |
| Transformations | dbt-bigquery |
| API Integration | OMDb API |
| Visualization | Looker Studio |
| Containerization | Docker |

## Author

Igor Remesz

---

*Built as part of the Futuremind GCP Data Engineer assessment*