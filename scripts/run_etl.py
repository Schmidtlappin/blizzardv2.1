#!/usr/bin/env python3
"""
Full ETL process script for Blizzard 2.1.

This script runs the complete Extract-Transform-Load process for IRS 990 XML data.
It processes XML files, validates them, extracts data, transforms it, and loads it into the database.

Usage:
    python run_etl.py [--config CONFIG_FILE] [--xml-dir XML_DIR] [--batch-size BATCH_SIZE] [--workers WORKERS]
    
By default, the script will look for XML files in the 'xml_files' directory in the root of the Blizzard 2.1 project.
"""

import os
import sys
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config.settings import Settings
from src.etl.extractor import XMLDirectoryExtractor
from src.etl.transformer import IRS990Transformer
from src.etl.loader import PostgreSQLLoader
from src.xml.validator import XMLValidator
from src.repeating_groups.detector import RepeatingGroupDetector
from src.db.connection import transaction

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"etl_process_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('run_etl')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the ETL process for IRS 990 XML files')
    parser.add_argument('--config', help='Path to the configuration file')
    parser.add_argument('--xml-dir', help='Directory containing XML files to process')
    parser.add_argument('--batch-size', type=int, help='Number of files to process in each batch')
    parser.add_argument('--workers', type=int, help='Number of parallel workers')
    return parser.parse_args()

def main():
    """Run the ETL process."""
    args = parse_arguments()
    logger = setup_logging()
    
    start_time = time.time()
    files_processed = 0
    files_skipped = 0
    files_failed = 0
    
    try:
        # Check for existing credentials file
        default_creds_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'db_credentials.txt')
        temp_creds_path = None
        
        if os.path.exists(default_creds_path):
            logger.info(f"Found existing credentials file: {default_creds_path}")
            creds_path = default_creds_path
            
            # Load simple credentials from the file to display database info
            with open(default_creds_path, 'r') as f:
                content = f.read()
                if 'dbname' in content and 'host' in content:
                    dbname = None
                    host = None
                    for line in content.split('\n'):
                        if 'dbname' in line and '=' in line:
                            dbname = line.split('=')[1].strip()
                        if 'host' in line and '=' in line:
                            host = line.split('=')[1].strip()
                    logger.info(f"Using database: {dbname or 'unknown'} on {host or 'unknown'}")
        
        else:
            # Load settings from file or use defaults
            if args.config:
                settings = Settings.from_file(args.config)
                logger.info(f"Loaded configuration from {args.config}")
            else:
                settings = Settings.get_instance()
                logger.info("Using default configuration")
            
            # Get database parameters
            db_params = settings.get_db_params()
            logger.info(f"Using database: {db_params['dbname']} on {db_params['host']}")
            
            # Create a temporary credentials file for the database connection
            temp_creds_path = os.path.join(os.getcwd(), '.db_credentials_temp')
            with open(temp_creds_path, 'w') as f:
                for key, value in db_params.items():
                    f.write(f"{key}={value}\n")
            logger.info("Created temporary credentials file")
            creds_path = temp_creds_path
        
        try:
            # Initialize components
            # Determine XML directory - use argument, or settings, or default path
            if args.xml_dir:
                xml_dir = args.xml_dir
            elif args.config and 'paths' in settings.config and 'xml_dir' in settings.config['paths']:
                xml_dir = settings.get('paths', 'xml_dir')
            else:
                # Use default path in the Blizzard 2.1 root directory
                xml_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xml_files'))
                if not os.path.exists(xml_dir):
                    # Create the directory if it doesn't exist
                    os.makedirs(xml_dir, exist_ok=True)
                    logger.info(f"Created XML files directory: {xml_dir}")
            
            # Determine batch size and workers - use argument, or settings, or default values
            batch_size = args.batch_size or (settings.get('etl', 'batch_size', 100) if 'settings' in locals() else 100)
            workers = args.workers or (settings.get('etl', 'workers', 4) if 'settings' in locals() else 4)
            
            # Create components for the ETL process
            extractor = XMLDirectoryExtractor(xml_dir, batch_size=batch_size)
            validator = XMLValidator()
            transformer = IRS990Transformer()
            repeating_group_detector = RepeatingGroupDetector()
            loader = PostgreSQLLoader(creds_path)
            
            logger.info(f"Starting ETL process from directory: {xml_dir}")
            logger.info(f"Using batch size: {batch_size}, workers: {workers}")
            
            # Process XML files in batches
            for batch_num, batch in enumerate(extractor.extract_in_batches(), 1):
                logger.info(f"Processing batch {batch_num} with {len(batch)} files")
                
                batch_start = time.time()
                batch_processed = 0
                batch_skipped = 0
                batch_failed = 0
                
                for xml_file_path in batch:
                    try:
                        # Validate XML
                        logger.debug(f"Validating: {xml_file_path}")
                        is_valid, form_type, version, tax_year = validator.validate_file(xml_file_path)
                        
                        if not is_valid:
                            logger.warning(f"Skipping invalid XML file: {xml_file_path}")
                            files_skipped += 1
                            batch_skipped += 1
                            continue
                        
                        # Transform XML data
                        logger.debug(f"Transforming: {xml_file_path}")
                        filing_data = transformer.transform_file(xml_file_path)
                        
                        # Detect repeating groups if needed
                        if not filing_data.repeating_groups:
                            logger.debug(f"Detecting repeating groups: {xml_file_path}")
                            repeating_groups = repeating_group_detector.detect_from_file(xml_file_path)
                            if repeating_groups:
                                filing_data.repeating_groups = repeating_groups
                        
                        # Load data into database
                        logger.debug(f"Loading into database: {xml_file_path}")
                        with transaction(creds_path) as conn:
                            loader.load_filing(conn, filing_data)
                        
                        files_processed += 1
                        batch_processed += 1
                        logger.debug(f"Successfully processed: {xml_file_path}")
                        
                    except Exception as e:
                        logger.error(f"Failed to process {xml_file_path}: {str(e)}")
                        files_failed += 1
                        batch_failed += 1
                
                batch_end = time.time()
                batch_duration = batch_end - batch_start
                logger.info(f"Batch {batch_num} completed in {batch_duration:.2f} seconds")
                logger.info(f"Batch statistics: Processed: {batch_processed}, Skipped: {batch_skipped}, Failed: {batch_failed}")
            
        finally:
            # Clean up temporary credentials file if created
            if temp_creds_path and os.path.exists(temp_creds_path):
                os.unlink(temp_creds_path)
                logger.info("Removed temporary credentials file")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        return 1
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    logger.info("ETL process completed")
    logger.info(f"Total duration: {total_duration:.2f} seconds")
    logger.info(f"Files processed: {files_processed}")
    logger.info(f"Files skipped: {files_skipped}")
    logger.info(f"Files failed: {files_failed}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
