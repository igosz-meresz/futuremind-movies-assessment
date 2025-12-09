import logging
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SchemaField

logger = logging.getLogger(__name__)

class BigQueryLoader:
    """
    Loader for BigQuery staging tables.
    
    Usage:
        loader = BigQueryLoader(project_id="my-project", dataset="futuremind_movies")
        loader.load_revenues(records)
        loader.load_movies(metadata_list)
    """

    def __init__(self, project_id: str, dataset: str, location: str = "EU"):
        self.project_id = project_id
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)
        self.revenues_table = f"{project_id}.{dataset}.stg_revenues_raw"
        self.movies_table = f"{project_id}.{dataset}.stg_movies_enriched"

    def load_revenues(self, records: list, write_disposition: WriteDisposition = WriteDisposition.WRITE_TRUNCATE,) -> int:
        """
        Load revenue records to stg_revenues_raw.
        
        Args:
            records: List of RevenueRecord objects from csv_parser
            write_disposition: WRITE_TRUNCATE (replace) or WRITE_APPEND
            
        Returns:
            Number of rows loaded
        """ 
        logger.info(f"Preparing {len(records)} revenue records for BigQuery")
        
        # convert to list of dicts for DataFrame
        rows = []
        for record in records:
            rows.append({
                'id': record.id,
                'date': record.date,
                'title': record.title,
                'revenue': float(record.revenue),
                'theaters': record.theaters,
                'distributor': record.distributor,
                'has_valid_theaters': record.has_valid_theaters,
                'has_valid_distributor': record.has_valid_distributor,
            })

        df = pd.DataFrame(rows)

        # define schema explicitly
        schema = [
            SchemaField("id", "STRING", mode="REQUIRED"),
            SchemaField("date", "DATE", mode="REQUIRED"),
            SchemaField("title", "STRING", mode="REQUIRED"),
            SchemaField("revenue", "FLOAT64", mode="REQUIRED"),
            SchemaField("theaters", "INT64", mode="NULLABLE"),
            SchemaField("distributor", "STRING", mode="NULLABLE"),
            SchemaField("has_valid_theaters", "BOOL", mode="REQUIRED"),
            SchemaField("has_valid_distributor", "BOOL", mode="REQUIRED"),
        ]

        job_config = LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
        )

        logger.info(f"Loading to {self.revenues_table}")

        job = self.client.load_table_from_dataframe(
            df,
            self.revenues_table,
            job_config=job_config,
        )

        job.result() # wait for completion

        table = self.client.get_table(self.revenues_table)
        logger.info(f"Loaded {table.num_rows} rows to {self.movies_table}")

        return table.num_rows

    def load_movies(
        self,
        movies: list,
        write_disposition: WriteDisposition = WriteDisposition.WRITE_TRUNCATE,
    ) -> int:
        """
        Load movie metadata to stg_movies_enriched.
        """
        logger.info(f"Preparing {len(movies)} movie records for BigQuery")

        # convert dataclass object to dicts
        rows = []
        for movie in movies:
            if hasattr(movie, '__dataclass_fields__'):
                row = asdict(movie)
            else:
                row = movie  # already a dict from cache
            rows.append(row)

        # Everything below should be OUTSIDE the for loop (unindented)
        df = pd.DataFrame(rows)

        # define schema
        schema = [
            SchemaField("title", "STRING", mode="REQUIRED"),
            SchemaField("year", "STRING", mode="NULLABLE"),
            SchemaField("rated", "STRING", mode="NULLABLE"),
            SchemaField("released", "STRING", mode="NULLABLE"),
            SchemaField("runtime", "STRING", mode="NULLABLE"),
            SchemaField("genre", "STRING", mode="NULLABLE"),
            SchemaField("director", "STRING", mode="NULLABLE"),
            SchemaField("actors", "STRING", mode="NULLABLE"),
            SchemaField("plot", "STRING", mode="NULLABLE"),
            SchemaField("language", "STRING", mode="NULLABLE"),
            SchemaField("country", "STRING", mode="NULLABLE"),
            SchemaField("awards", "STRING", mode="NULLABLE"),
            SchemaField("poster_url", "STRING", mode="NULLABLE"),
            SchemaField("metascore", "INT64", mode="NULLABLE"),
            SchemaField("imdb_rating", "FLOAT64", mode="NULLABLE"),
            SchemaField("imdb_votes", "INT64", mode="NULLABLE"),
            SchemaField("imdb_id", "STRING", mode="NULLABLE"),
            SchemaField("box_office", "STRING", mode="NULLABLE"),
            SchemaField("enriched_at", "STRING", mode="NULLABLE"),
            SchemaField("api_response_type", "STRING", mode="NULLABLE"),
        ]

        job_config = LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
        )

        logger.info(f"Loading to {self.movies_table}")

        job = self.client.load_table_from_dataframe(
            df,
            self.movies_table,
            job_config=job_config,
        )

        job.result()  # wait for completion

        table = self.client.get_table(self.movies_table)
        logger.info(f"Loaded {table.num_rows} rows to {self.movies_table}")

        return table.num_rows

    def validate_load(self) -> dict:
        """
        Run basic validation queries on loaded data.
        
        Returns:
            Dict with validation results
        """
        results = {}

        # check revenues table
        try:
            query = f"""
                SELECT 
                    COUNT(*) as row_count,
                    COUNT(DISTINCT title) as unique_movies,
                    COUNT(DISTINCT date) as unique_dates,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    SUM(revenue) as total_revenue
                FROM `{self.revenues_table}`
            """
            df = self.client.query(query).to_dataframe()
            results['revenues'] = df.iloc[0].to_dict()
            logger.info(f"Revenues validation: {results['revenues']}")
        except Exception as e:
            logger.error(f"Revenues validation failed: {e}")
            results['revenues'] = {'error': str(e)}
        
        # check movies table
        try:
            query = f"""
                SELECT 
                    COUNT(*) as row_count,
                    COUNTIF(api_response_type = 'match') as matched,
                    COUNTIF(imdb_rating IS NOT NULL) as with_rating
                FROM `{self.movies_table}`
            """
            df = self.client.query(query).to_dataframe()
            results['movies'] = df.iloc[0].to_dict()
            logger.info(f"Movies validation: {results['movies']}")
        except Exception as e:
            logger.error(f"Movies validation failed: {e}")
            results['movies'] = {'error': str(e)}
        
        return results
