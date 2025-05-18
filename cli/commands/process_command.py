"""
Process command implementation for the Blizzard CLI.
"""

import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any

from src.config.settings import Settings
from src.db.connection import transaction
from src.etl.pipeline import Pipeline
from src.etl.extractor import XMLDirectoryExtractor
from src.etl.transformer import IRS990Transformer
from src.etl.loader import PostgreSQLLoader
from src.xml.streaming import StreamingParser
from src.xml.validator import XMLValidator
from src.repeating_groups.detector import RepeatingGroupDetector

logger = logging.getLogger(__name__)

def process_xml_files(
    xml_dir: str, 
    concordance_path: str, 
    credentials_path: str,
    batch_size: int = 100,
    validate_xml: bool = False,
    skip_existing: bool = True,
    max_files: int = None
) -> Dict[str, Any]:
    """
    Process XML files from a directory and load them into the database.
    
    Args:
        xml_dir: Directory containing XML files
        concordance_path: Path to the concordance file
        credentials_path: Path to database credentials file
        batch_size: Number of files to process in each batch
        validate_xml: Whether to validate XML against XSD schemas
        skip_existing: Whether to skip files that are already in the database
        max_files: Maximum number of files to process (None for all)
        
    Returns:
        Dictionary with processing statistics
    """
    import time
    start_time = time.time()
    
    # Load configuration
    settings = Settings.from_file(credentials_path)
    
    # Create pipeline components
    extractor = XMLDirectoryExtractor(xml_dir, batch_size)
    validator = XMLValidator() if validate_xml else None
    transformer = IRS990Transformer(concordance_path)
    loader = PostgreSQLLoader(settings.get_db_params(), batch_size)
    
    # Create and run the pipeline
    pipeline = Pipeline(
        extractors=[extractor],
        transformers=[transformer],
        loaders=[loader]
    )
    
    # Set up tracking metrics
    stats = {
        'total_files': 0,
        'processed_files': 0,
        'failed_files': 0,
        'skipped_files': 0,
        'processed_values': 0,
        'processed_groups': 0
    }
    
    # Run the pipeline on batches of files
    try:
        # First count total files
        all_xml_files = extractor._find_xml_files()
        total_files = len(all_xml_files)
        stats['total_files'] = total_files
        logger.info(f"Found {total_files} XML files in {xml_dir}")
        
        # Process files in batches
        processed_count = 0
        for batch in extractor._batch_files(all_xml_files):
            if max_files and processed_count >= max_files:
                logger.info(f"Reached maximum file limit of {max_files}")
                break
                
            # Only take what we need if max_files is specified
            if max_files:
                remaining = max_files - processed_count
                if remaining < len(batch):
                    batch = batch[:remaining]
            
            # Transform the batch
            transformed_data = transformer.transform(batch)
            
            # Load the data
            load_results = loader.load(transformed_data)
            
            # Update statistics
            for result in load_results:
                if result.get('status') == 'success':
                    stats['processed_files'] += 1
                    stats['processed_values'] += result.get('filing_values_count', 0)
                    stats['processed_groups'] += result.get('repeating_groups_count', 0)
                else:
                    stats['failed_files'] += 1
            
            processed_count += len(batch)
            logger.info(f"Processed {processed_count}/{total_files} files")
            
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        stats['elapsed_time'] = elapsed_time
        stats['files_per_second'] = stats['processed_files'] / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(f"Processed {stats['processed_files']} files successfully in {elapsed_time:.2f} seconds")
        logger.info(f"Failed: {stats['failed_files']}, Skipped: {stats['skipped_files']}")
        logger.info(f"Extracted {stats['processed_values']} values and {stats['processed_groups']} repeating groups")
        
        return stats
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        stats['elapsed_time'] = elapsed_time
        stats['error'] = str(e)
        
        logger.error(f"Error processing XML files: {e}")
        raise
