"""
Check command implementation for the Blizzard CLI.
"""

import os
import logging
from pathlib import Path

from src.config.settings import Settings
from src.db.connection import get_db_connection

logger = logging.getLogger(__name__)

def run_diagnostics(credentials_path: str, filing_id: str = None) -> None:
    """
    Run diagnostic checks on the database and/or specific filings.
    
    Args:
        credentials_path: Path to database credentials file
        filing_id: Optional filing ID to check specifically
    """
    # Load configuration
    settings = Settings.from_file(credentials_path)
    
    # Connect to the database
    with get_db_connection(settings.get_db_params()) as conn:
        with conn.cursor() as cursor:
            try:
                # Check database connectivity
                cursor.execute("SELECT 1")
                logger.info("Database connection successful")
                
                # Check schema existence
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'field_definitions'
                    )
                """)
                has_schema = cursor.fetchone()[0]
                if has_schema:
                    logger.info("Schema exists in the database")
                else:
                    logger.warning("Schema does not exist in the database")
                
                # Check filing counts
                cursor.execute("SELECT COUNT(*) FROM filings")
                filing_count = cursor.fetchone()[0]
                logger.info(f"Found {filing_count} filings in the database")
                
                # If filing_id is provided, check that specific filing
                if filing_id:
                    check_filing(cursor, filing_id)
                    
            except Exception as e:
                logger.error(f"Error running diagnostics: {e}")
                raise

def check_filing(cursor, filing_id: str) -> None:
    """Check a specific filing for completeness."""
    # Check filing existence
    cursor.execute("SELECT COUNT(*) FROM filings WHERE filing_id = %s", (filing_id,))
    if cursor.fetchone()[0] == 0:
        logger.warning(f"Filing {filing_id} not found in database")
        return
        
    # Check filing values
    cursor.execute("SELECT COUNT(*) FROM filing_values WHERE filing_id = %s", (filing_id,))
    value_count = cursor.fetchone()[0]
    logger.info(f"Filing {filing_id} has {value_count} values")
    
    # Check repeating groups
    cursor.execute("SELECT COUNT(*) FROM repeating_groups WHERE filing_id = %s", (filing_id,))
    group_count = cursor.fetchone()[0]
    logger.info(f"Filing {filing_id} has {group_count} repeating groups")
    
    # Check repeating group values
    cursor.execute("SELECT COUNT(*) FROM repeating_group_values WHERE filing_id = %s", (filing_id,))
    rg_value_count = cursor.fetchone()[0]
    logger.info(f"Filing {filing_id} has {rg_value_count} repeating group values")
