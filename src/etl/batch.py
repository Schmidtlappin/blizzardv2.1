"""
Batch processing module for the Blizzard ETL pipeline.

This module provides functionality for batch processing of XML files.
"""

from typing import Dict, Any, List, Optional, Union, Iterable
import os
import logging
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.core.exceptions import XMLProcessingError
from src.etl.pipeline import Pipeline
from src.etl.extractor import XMLExtractor
from src.etl.transformer import FilingTransformer
from src.etl.simple_loader import SimplePostgreSQLLoader

logger = logging.getLogger(__name__)

class BatchProcessor:
    """
    Batch processor for XML files.
    
    This class handles batch processing of multiple XML files using a pipeline.
    """
    
    def __init__(self, batch_size: int = 100, max_workers: int = 4):
        """
        Initialize the batch processor.
        
        Args:
            batch_size: Number of files to process in each batch
            max_workers: Maximum number of worker threads
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        
    def process_directory(self, xml_dir: str, concordance_file: str, 
                         credentials_file: str) -> Dict[str, Any]:
        """
        Process all XML files in a directory.
        
        Args:
            xml_dir: Directory containing XML files
            concordance_file: Path to the concordance file
            credentials_file: Path to the database credentials file
            
        Returns:
            Dictionary with processing results
        """
        # Find all XML files in the directory
        xml_files = self._find_xml_files(xml_dir)
        
        if not xml_files:
            logger.warning(f"No XML files found in directory: {xml_dir}")
            return {"success": True, "files_processed": 0}
        
        # Process files in batches
        results = []
        for i in range(0, len(xml_files), self.batch_size):
            batch = xml_files[i:i+self.batch_size]
            batch_result = self._process_batch(batch, concordance_file, credentials_file)
            results.append(batch_result)
        
        # Combine results
        total_processed = sum(r.get("files_processed", 0) for r in results)
        total_succeeded = sum(r.get("files_succeeded", 0) for r in results)
        total_failed = sum(r.get("files_failed", 0) for r in results)
        
        return {
            "success": True,
            "files_processed": total_processed,
            "files_succeeded": total_succeeded,
            "files_failed": total_failed
        }
    
    def _find_xml_files(self, directory: str) -> List[str]:
        """Find all XML files in a directory."""
        pattern = os.path.join(directory, "**", "*.xml")
        return glob.glob(pattern, recursive=True)
    
    def _process_batch(self, files: List[str], concordance_file: str,
                      credentials_file: str) -> Dict[str, Any]:
        """Process a batch of files."""
        # Load concordance data
        concordance = self._load_concordance(concordance_file)
        
        # Set up the pipeline components
        extractor = XMLExtractor(validate=False)
        transformer = FilingTransformer(concordance=concordance)
        loader = SimplePostgreSQLLoader(credentials_file)
        
        # Process files
        succeeded = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self._process_file, file, extractor, transformer, loader): file
                for file in files
            }
            
            # Process results as they complete
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    result = future.result()
                    if result.get("success", False):
                        succeeded += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Error processing file {file}: {e}")
                    failed += 1
        
        # Clean up
        loader.close()
        
        return {
            "files_processed": len(files),
            "files_succeeded": succeeded,
            "files_failed": failed
        }
    
    def _process_file(self, file_path: str, extractor, transformer, loader) -> Dict[str, Any]:
        """Process a single file using the ETL pipeline."""
        try:
            # Extract
            extracted_data = extractor.extract(file_path)
            
            # Transform
            filing = transformer.transform(extracted_data)
            
            # Load
            result = loader.load(filing)
            
            return {"success": True, "filing_id": result.get("filing_id")}
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def _load_concordance(self, concordance_file: str) -> Dict[str, Any]:
        """Load concordance data from file."""
        # Placeholder implementation
        # In a full implementation, this would load and parse the concordance file
        return {}
