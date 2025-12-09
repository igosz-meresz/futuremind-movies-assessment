import csv
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)

@dataclass
class RevenueRecord:
    id: str
    date: date
    title: str
    revenue: Decimal
    theaters: int | None
    distributor: str | None

    # data quality fields
    has_valid_theaters: bool = True
    has_valid_distributor: bool = True

class CSVParseError(Exception):
    """Raised when row cannot be parsed"""
    pass

def parse_revenues_csv(
    file_path: Path,
    skip_zero_revenue: bool = False
) -> Generator[RevenueRecord, None, None]:
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    rows_processed = 0
    rows_skipped = 0
    data_quality_issues = {
        'empty_theaters': 0,
        'zero_revenue': 0,
        'missing_distributor': 0,
    }

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')

        for row_num, row in enumerate(reader, start=2): # account for headers
            try:
                record = _parse_row(row, data_quality_issues)

                if skip_zero_revenue and record.revenue == 0:
                    rows_skipped += 1
                    continue
                
                rows_processed += 1
                yield record
            
            except CSVParseError as e:
                logger.warning(f"Row {row_num}: {e} - {row}")
                rows_skipped += 1
                continue

    logger.info(
        "CSV parsing complete. "
        f"Processed: {rows_processed}, Skipped: {rows_skipped}, "
        f"Data quality issues: {data_quality_issues}"
    )

def _parse_row(row: dict, quality_tracker: dict) -> RevenueRecord:
    """Prase a single row into revenue record"""

    # required fields - fail if missing
    row_id = row.get('id', '').strip()
    if not row_id:
        raise CSVParseError("Missing id")

    date_str = row.get('date', '').strip()
    if not date_str:
        raise CSVParseError("Missing date")

    title = row.get('title', '').strip()
    if not title:
        raise CSVParseError("Missing title")

    # parse date
    try:
        parsed_date = date.fromisoformat(date_str)
    except ValueError:
        raise CSVParseError(f"Invalid date format: {date_str}")

    # parse revenue - default to 0 if empty
    revenue_str = row.get('revenue', '').strip()
    if revenue_str == '' or revenue_str == '0':
        quality_tracker['zero_revenue'] += 1
        revenue = Decimal('0')
    else:
        try:
            revenue = Decimal(revenue_str)
        except Exception:
            raise CSVParseError(f"Invalid revenue: {revenue_str}")
    
    # parse theaters (nullable)
    theaters_str = row.get('theaters', '').strip()
    if theaters_str == '':
        quality_tracker['empty_theaters'] += 1
        theaters = None
        has_valid_theaters = False
    else:
        try:
            theaters = int(theaters_str)
            has_valid_theaters = True
        except ValueError:
            raise CSVParseError(f"Invalid theater count: {theaters_str}")

    # parse distributor (nullable, treat '-' as missing)
    distributor_str = row.get('distributor', '').strip()
    if distributor_str in ('', '-'):
        quality_tracker['missing_distributor'] += 1
        distributor = None
        has_valid_distributor = False
    else:
        distributor = distributor_str
        has_valid_distributor = True

    return RevenueRecord(
        id=row_id,
        date=parsed_date,
        title=title,
        revenue=revenue,
        theaters=theaters,
        distributor=distributor,
        has_valid_theaters=has_valid_theaters,
        has_valid_distributor=has_valid_distributor,
    )

def get_unique_movies_by_revenue(file_path: Path, top_n: int | None = None) -> list[dict]:
    """
    Aggregate CSV to get unique movies ranked by total revenue.
    
    Args:
        file_path: Path to the CSV file
        top_n: If provided, return only top N movies
        
    Returns:
        List of dicts with 'title', 'total_revenue', 'first_date', 'last_date'
    """
    movie_stats: dict[str, dict] = {}

    for record in parse_revenues_csv(file_path):
        title = record.title

        if title not in movie_stats:
            movie_stats[title] = {
                'title': title,
                'total_revenue': Decimal('0'),
                'first_date': record.date,
                'last_date': record.date
            }
        
        movie_stats[title]['total_revenue'] += record.revenue
        movie_stats[title]['first_date'] = min(movie_stats[title]['first_date'], record.date)
        movie_stats[title]['last_date'] = max(movie_stats[title]['last_date'], record.date)

    # sort by revenue descending
    ranked = sorted(
        movie_stats.values(),
        key=lambda x: x['total_revenue'],
        reverse=True
    )

    if top_n:
        ranked = ranked[:top_n]

    logger.info(f"Found {len(movie_stats)} unique movies, returning top {len(ranked)}")

    return ranked

     
