import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

# source files
REVENUES_CSV_PATH = RAW_DATA_DIR / "revenues_per_day.csv"
OMDB_CACHE_PATH = CACHE_DIR / "omdb_cache.json"

# OMDB api
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_BASE_URL = "http://www.omdbapi.com/"
TOP_N_MOVIES_TO_ENRICH = 800

# BigQuery
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "futuremind-assesment")
BQ_DATASET = os.getenv("BQ_DATASET", "futuremind_movies")
BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

# Table names
BQ_STG_REVENUES = f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_revenues_raw"
BQ_STG_MOVIES = f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_movies_enriched"