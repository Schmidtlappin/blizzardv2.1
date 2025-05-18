"""
Setup command implementation for the Blizzard CLI.

This command initializes the database with the required schema.
"""

import os
import logging
import argparse
from pathlib import Path
from typing import Optional

from src.config.settings import Settings
from src.db.connection import initialize_database, transaction, create_tables

logger = logging.getLogger(__name__)

def setup_schema(credentials_path: str, schema_path: str = None, force: bool = False) -> None:
    """
    Set up the database schema.
    
    Args:
        credentials_path: Path to database credentials file
        schema_path: Optional path to schema file (if not provided, use default)
        force: Whether to force re-initialization of the database
    """
    # Use default schema if not specified
    if not schema_path:
        # Default to the schema in the package
        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "schema", "schema.sql"
        )
    
    try:
        logger.info(f"Initializing database with schema {schema_path}")
        initialize_database(credentials_path, schema_path)
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def add_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add command-specific arguments to the parser.
    
    Args:
        parser: The argument parser
    """
    parser.add_argument(
        '--credentials',
        required=True,
        help='Path to database credentials file'
    )
    parser.add_argument(
        '--schema',
        help='Path to SQL schema file (optional, uses default if not provided)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-initialization of the database'
    )

def handle(args: argparse.Namespace) -> int:
    """
    Handle the setup command.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code
    """
    try:
        setup_schema(
            args.credentials,
            args.schema,
            args.force
        )
        return 0
    except Exception as e:
        logger.error(f"Setup command failed: {e}")
        return 1
