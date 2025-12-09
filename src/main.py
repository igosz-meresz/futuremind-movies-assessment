import logging
import sys
from pathlib import Path

"""
Main orchestration script for the Futuremind assessment pipeline.

Workflow:
1. Parse CSV file
2. Identify top N movies by revenue
3. Enrich movies via OMDb API
4. Load staging tables to BigQuery

Usage:
    python -m src.main
"""

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log'),
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Run the full pipeline"""
    
    # import here to ensure logging is configured first
    from src.config import (
        REVENUES_CSV_PATH,
        OMDB_API_KEY,
        OMDB_CACHE_PATH,
        TOP_N_MOVIES_TO_ENRICH,
        GCP_PROJECT_ID,
        BQ_DATASET,
        BQ_LOCATION,
    )
    from src.extract.csv_parser import parse_revenues_csv, get_unique_movies_by_revenue
    from src.extract.omdb_client import OMDbClient, enrich_movies
    from src.load.bigquery_loader import BigQueryLoader
    
    logger.info("="*60)
    logger.info("STARTING PIPELINE")
    logger.info("="*60)

    # step 1: parse csv
    logger.info("Parsing CSV file")

    if not REVENUES_CSV_PATH.exists():
        logger.error(f"CSV file not found: {REVENUES_CSV_PATH}")
        sys.exit(1)

    # parse all records into memory (good enough for a take home assignment, 340k rows only)
    revenue_records = list(parse_revenues_csv(REVENUES_CSV_PATH))
    logger.info(f"Parsed {len(revenue_records)} revenue records")

    # step 2: get top movies for enrichment
    logger.info(f"Identifying top {TOP_N_MOVIES_TO_ENRICH} movies by revenue")

    top_movies = get_unique_movies_by_revenue(
        REVENUES_CSV_PATH,
        top_n=TOP_N_MOVIES_TO_ENRICH
    )
    logger.info(f"Top movie: {top_movies[0]['title']} (${top_movies[0]['total_revenue']:,.0f})")

    # step 3: enrich via omdb api
    logger.info("Enriching movie data via OMDb API")

    if not OMDB_API_KEY:
        logger.error("OMDB_API_KEY not set in environment")
        sys.exit(1)

    client = OMDbClient(
        api_key=OMDB_API_KEY,
        cache_path=OMDB_CACHE_PATH,
    )

    enriched_movies = enrich_movies(client, top_movies, progress_interval=100)

    stats = client.get_stats()
    logger.info(f"Enrichment stats: {stats}")
    
    # step 4: load into BigQuery
    logger.info("Loading into BigQuery")
    
    loader = BigQueryLoader(
        project_id=GCP_PROJECT_ID,
        dataset=BQ_DATASET,
        location=BQ_LOCATION,
    )

    # load revenues
    revenues_loaded = loader.load_revenues(revenue_records)
    logger.info(f"Loaded {revenues_loaded} revenue records")

    # load enriched movies
    movies_loaded = loader.load_movies(enriched_movies)
    logger.info(f"Loaded {movies_loaded} movie records")

    # step 5: validate
    logger.info("Validating loaded data")

    validation = loader.validate_load()

    logger.info("="*60)
    logger.info("PIPELINE COMPLETE")
    logger.info("="*60)
    logger.info(f"Revenues: {validation.get('revenues', {})}")
    logger.info(f"Movies: {validation.get('movies', {})}")
    
    return 0
    
if __name__ == "__main__":
    sys.exit(main())