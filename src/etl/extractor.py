"""
XML data extractor for the Blizzard ETL pipeline.

This module provides extractor classes for extracting data from XML files.
"""

from typing import Dict, Any, List, Optional, Union, Generator, Tuple
import os
import logging
import glob
from pathlib import Path
import concurrent.futures
from tqdm import tqdm

from src.core.exceptions import XMLProcessingError, ExtractionError
from src.xml.parser import XMLParser
from src.xml.validator import XMLValidator
from src.etl.pipeline import Extractor

logger = logging.getLogger(__name__)

class XMLDirectoryExtractor(Extractor[List[str]]):
    """
    Extractor for processing multiple XML files from a directory.
    
    This class extracts batches of XML file paths from a directory.
    """
    
    def __init__(self, directory_path: str, batch_size: int = 100, file_pattern: str = "*.xml"):
        """
        Initialize the directory extractor.
        
        Args:
            directory_path: Path to directory containing XML files
            batch_size: Number of files to process in each batch
            file_pattern: Glob pattern to match XML files
        """
        self.directory_path = directory_path
        self.batch_size = batch_size
        self.file_pattern = file_pattern
        
    def extract(self, source: Optional[Any] = None) -> List[str]:
        """
        Extract a batch of XML file paths from the directory.
        
        Args:
            source: Optional source parameter (ignored, uses directory_path)
            
        Returns:
            List of file paths for the next batch of XML files
        """
        try:
            # Get all XML files in the directory (recursively)
            xml_files = self._find_xml_files()
            
            if not xml_files:
                logger.warning(f"No XML files found in {self.directory_path}")
                return []
                
            logger.info(f"Found {len(xml_files)} XML files in {self.directory_path}")
            
            # Process files in batches
            for batch in self._batch_files(xml_files):
                # Return one batch at a time
                return batch
                
        except Exception as e:
            logger.error(f"Error extracting XML files: {e}")
            raise ExtractionError(f"Failed to extract XML files: {e}")
            
        return []
    
    def _find_xml_files(self) -> List[str]:
        """Find all XML files in the directory matching the pattern."""
        if not os.path.isdir(self.directory_path):
            raise ExtractionError(f"Directory not found: {self.directory_path}")
            
        path_pattern = os.path.join(self.directory_path, "**", self.file_pattern)
        xml_files = [str(path) for path in Path(self.directory_path).glob(f"**/{self.file_pattern}")]
        return xml_files
    
    def _batch_files(self, file_paths: List[str]) -> Generator[List[str], None, None]:
        """Split file paths into batches of batch_size."""
        for i in range(0, len(file_paths), self.batch_size):
            yield file_paths[i:i + self.batch_size]
            
    def process_all(self, validator: Optional[XMLValidator] = None) -> Dict[str, Any]:
        """
        Process all XML files in the directory.
        
        Args:
            validator: Optional XMLValidator to validate files
            
        Returns:
            Dictionary with processing statistics
        """
        xml_files = self._find_xml_files()
        
        if not xml_files:
            logger.warning(f"No XML files found in {self.directory_path}")
            return {"total": 0, "processed": 0, "failed": 0, "files": []}
            
        total_files = len(xml_files)
        processed_files = 0
        failed_files = 0
        results = []
        
        logger.info(f"Processing {total_files} XML files from {self.directory_path}")
        
        with tqdm(total=total_files, desc="Processing XML files") as progress_bar:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_file = {
                    executor.submit(self._process_file, xml_file, validator): xml_file 
                    for xml_file in xml_files
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    xml_file = future_to_file[future]
                    try:
                        result = future.result()
                        if result.get("status") == "success":
                            processed_files += 1
                        else:
                            failed_files += 1
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing {xml_file}: {e}")
                        failed_files += 1
                        results.append({
                            "file": xml_file,
                            "status": "error",
                            "error": str(e)
                        })
                    
                    progress_bar.update(1)
        
        return {
            "total": total_files,
            "processed": processed_files,
            "failed": failed_files,
            "files": results
        }
    
    def _process_file(self, xml_file: str, validator: Optional[XMLValidator]) -> Dict[str, Any]:
        """Process a single XML file."""
        try:
            # Validate if validator provided
            if validator:
                try:
                    validator.validate(xml_file)
                except XMLProcessingError as e:
                    logger.warning(f"Validation failed for {xml_file}: {e}")
                    # Continue with extraction even if validation fails
            
            return {
                "file": xml_file,
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Failed to process {xml_file}: {e}")
            return {
                "file": xml_file,
                "status": "error",
                "error": str(e)
            }

class XMLExtractor(Extractor[Dict[str, Any]]):
    """
    Extractor for XML files.
    
    This class extracts data from XML files as part of the ETL pipeline.
    """
    
    def __init__(self, validate: bool = False, namespaces=None):
        """
        Initialize the extractor.
        
        Args:
            validate: Whether to validate XML against XSD schema
            namespaces: XML namespaces to use for XPath queries
        """
        self.validate = validate
        self.namespaces = namespaces
        self.parser = XMLParser(namespaces=namespaces)
        
        if validate:
            self.validator = XMLValidator()
        
    def extract(self, source: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract data from an XML file.
        
        Args:
            source: Either a file path or a dictionary with file information
            
        Returns:
            Dictionary with extracted data
        """
        # Determine the file path
        if isinstance(source, str):
            file_path = source
        elif isinstance(source, dict) and "file_path" in source:
            file_path = source["file_path"]
        else:
            raise XMLProcessingError("Invalid source for XMLExtractor")
        
        # Validate if required
        if self.validate:
            try:
                self.validator.validate(file_path)
            except XMLProcessingError as e:
                logger.warning(f"XML validation failed: {e}")
                # Continue with parsing even if validation fails
        
        # Parse the file
        return self.parser.parse_file(file_path)
