"""
Streaming XML parser for memory-efficient XML processing.

This module provides a memory-efficient way to process large XML files
by streaming through the file rather than loading it entirely into memory.
"""

import os
import logging
from lxml import etree
from typing import Iterator, Dict, Any, Optional, List, Tuple, Set, Generator

from src.core.exceptions import XMLProcessingError

logger = logging.getLogger(__name__)

class StreamingParser:
    """
    A memory-efficient XML parser that streams through large XML files.
    
    This class allows for processing very large XML files without loading
    the entire document into memory, which is useful for the large IRS 990
    XML files that can be several megabytes each.
    """
    
    def __init__(self, namespaces=None):
        """
        Initialize the parser with optional namespaces.
        
        Args:
            namespaces: Dictionary mapping namespace prefixes to URIs
        """
        self.namespaces = namespaces or {}
    
    def iterate_elements(self, file_path: str, element_path: str) -> Iterator[etree._Element]:
        """
        Iteratively process a large XML file without loading it entirely into memory.
        
        Args:
            file_path: Path to the XML file
            element_path: XPath or tag name to match elements
            
        Yields:
            Elements matching the specified path
        """
        try:
            # Setup the iterparse
            context = etree.iterparse(
                file_path, 
                events=("end",), 
                tag=element_path if '{' in element_path else None
            )
            
            # Process elements
            for _, element in context:
                # Yield the current element
                yield element
                
                # Clear the element to save memory
                element.clear()
                
                # Also clear ancestors to save more memory
                for ancestor in element.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]
        except Exception as e:
            logger.error(f"Error streaming XML file {file_path}: {e}")
            raise XMLProcessingError(f"Failed to stream XML file {file_path}: {e}")
            
    def find_nested_elements(self, file_path: str, elements_paths: List[str]) -> Dict[str, List[etree._Element]]:
        """
        Find nested elements in an XML file, preserving their relationships.
        
        Args:
            file_path: Path to the XML file
            elements_paths: List of XPath expressions for elements to find
            
        Returns:
            Dictionary mapping path expressions to lists of matching elements
        """
        results = {path: [] for path in elements_paths}
        
        try:
            # Parse the file once rather than streaming
            tree = etree.parse(file_path)
            root = tree.getroot()
            
            # Detect namespaces if not provided
            if not self.namespaces:
                self.namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
                if None in root.nsmap:
                    self.namespaces['default'] = root.nsmap[None]
            
            # Find all elements
            for path in elements_paths:
                try:
                    elements = root.xpath(path, namespaces=self.namespaces)
                    results[path] = elements
                except Exception as e:
                    logger.warning(f"XPath expression failed: {path} - {e}")
                    
        except Exception as e:
            logger.error(f"Error parsing XML file {file_path}: {e}")
            raise XMLProcessingError(f"Failed to parse XML file {file_path}: {e}")
            
        return results
        
    def extract_values(self, file_path: str, value_paths: Dict[str, str]) -> Dict[str, Optional[str]]:
        """
        Extract specific values from an XML file.
        
        Args:
            file_path: Path to the XML file
            value_paths: Dictionary mapping field names to XPath expressions
            
        Returns:
            Dictionary mapping field names to extracted values
        """
        results = {field: None for field in value_paths}
        
        try:
            tree = etree.parse(file_path)
            root = tree.getroot()
            
            # Detect namespaces if not provided
            namespaces = self.namespaces
            if not namespaces:
                namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
                if None in root.nsmap:
                    namespaces['default'] = root.nsmap[None]
            
            # Extract each value
            for field, path in value_paths.items():
                try:
                    elements = root.xpath(path, namespaces=namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        results[field] = elements[0].text.strip()
                except Exception as e:
                    logger.debug(f"Failed to extract {field} with path {path}: {e}")
                    
                    # Try alternative approach with local-name()
                    try:
                        local_path = path
                        for prefix in namespaces:
                            if f"{prefix}:" in local_path:
                                tag_name = local_path.split(f"{prefix}:")[1].split("[")[0].split("/")[-1]
                                local_path = local_path.replace(f"{prefix}:{tag_name}", f"*[local-name()='{tag_name}']")
                        
                        elements = root.xpath(local_path)
                        if elements and hasattr(elements[0], 'text') and elements[0].text:
                            results[field] = elements[0].text.strip()
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.error(f"Error extracting values from {file_path}: {e}")
            raise XMLProcessingError(f"Failed to extract values from {file_path}: {e}")
            
        return results
        
    def get_root_element(self, file_path: str) -> etree._Element:
        """
        Get the root element of an XML file.
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            Root element of the XML document
        """
        try:
            tree = etree.parse(file_path)
            return tree.getroot()
        except Exception as e:
            logger.error(f"Error parsing XML file {file_path}: {e}")
            raise XMLProcessingError(f"Failed to parse XML file {file_path}: {e}")
