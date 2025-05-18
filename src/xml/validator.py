"""
XML schema validation for the Blizzard system.

This module provides functionality to validate XML files against XSD schemas.
"""

import os
from typing import Optional, Dict, List, Any
from lxml import etree
import logging

from src.core.exceptions import XMLProcessingError
from src.core.constants import XSD_DIR

logger = logging.getLogger(__name__)

class XMLValidator:
    """
    Validator for IRS 990 XML files against XSD schemas.
    
    This class provides methods to validate XML files against their corresponding
    XSD schemas, with specific support for different versions of IRS 990 forms.
    """
    
    def __init__(self, xsd_dir: Optional[str] = None):
        """
        Initialize the validator.
        
        Args:
            xsd_dir: Directory containing XSD schema files (default: XSD_DIR constant)
        """
        self.xsd_dir = xsd_dir or XSD_DIR
        self.schema_cache = {}  # Cache of parsed XSD schemas
        
    def validate(self, xml_path: str, schema_path: Optional[str] = None) -> bool:
        """
        Validate an XML file against an XSD schema.
        
        Args:
            xml_path: Path to the XML file
            schema_path: Path to the XSD schema (if None, will be auto-detected)
            
        Returns:
            True if validation succeeded, raises exception otherwise
        """
        try:
            # Parse the XML
            xml_doc = etree.parse(xml_path)
            
            # Determine the schema to use
            if not schema_path:
                schema_path = self._detect_schema(xml_doc)
            
            # Get or create the schema
            schema = self._get_schema(schema_path)
            
            # Validate
            schema.assertValid(xml_doc)
            
            logger.debug(f"XML file {xml_path} successfully validated against {schema_path}")
            return True
            
        except etree.DocumentInvalid as e:
            logger.error(f"XML validation failed: {e}")
            raise XMLProcessingError(f"XML validation failed: {e}")
        except Exception as e:
            logger.error(f"Error during XML validation: {e}")
            raise XMLProcessingError(f"Error during XML validation: {e}")
    
    def _get_schema(self, schema_path: str) -> etree.XMLSchema:
        """
        Get a parsed XSD schema, using cache if available.
        
        Args:
            schema_path: Path to the XSD schema file
            
        Returns:
            Parsed XMLSchema object
        """
        if schema_path not in self.schema_cache:
            try:
                schema_doc = etree.parse(schema_path)
                self.schema_cache[schema_path] = etree.XMLSchema(schema_doc)
            except Exception as e:
                raise XMLProcessingError(f"Failed to parse XSD schema {schema_path}: {e}")
        
        return self.schema_cache[schema_path]
    
    def _detect_schema(self, xml_doc: etree._ElementTree) -> str:
        """
        Automatically detect the appropriate XSD schema for an XML document.
        
        Args:
            xml_doc: Parsed XML document
            
        Returns:
            Path to the appropriate XSD schema file
        """
        # Extract form type and version from the XML
        root = xml_doc.getroot()
        form_type = self._extract_form_type(root)
        form_version = self._extract_form_version(root)
        
        # Construct schema filename based on form type and version
        schema_filename = f"IRS990_{form_type}_{form_version}.xsd"
        schema_path = os.path.join(self.xsd_dir, schema_filename)
        
        # Check if the schema file exists
        if not os.path.isfile(schema_path):
            # Fall back to generic schema if specific one not found
            schema_path = os.path.join(self.xsd_dir, "IRS990.xsd")
            if not os.path.isfile(schema_path):
                raise XMLProcessingError(f"Could not find XSD schema for {form_type} version {form_version}")
        
        return schema_path
    
    def _extract_form_type(self, root: etree._Element) -> str:
        """
        Extract form type from XML root element.
        
        Returns:
            Form type (e.g., "990", "990EZ", "990PF", "990T", "990N")
        """
        # Try different namespaces and paths
        namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
        if None in root.nsmap:
            namespaces['default'] = root.nsmap[None]
        
        # Try to find ReturnTypeCd element
        form_type_paths = [
            "//*[local-name()='ReturnTypeCd']",
            "//irs:ReturnHeader/irs:ReturnTypeCd",
            "//default:ReturnHeader/default:ReturnTypeCd"
        ]
        
        for path in form_type_paths:
            try:
                elements = root.xpath(path, namespaces=namespaces)
                if elements and hasattr(elements[0], 'text') and elements[0].text:
                    return elements[0].text.strip()
            except Exception:
                continue
        
        # Default to 990 if not found
        logger.warning("Could not determine form type, defaulting to 990")
        return "990"
    
    def _extract_form_version(self, root: etree._Element) -> str:
        """
        Extract form version from XML root element.
        
        Returns:
            Form version (e.g., "2020v1.0")
        """
        # Look for version in root element attributes
        version = root.get('returnVersion')
        if not version:
            for ns in root.nsmap.values():
                version = root.get(f"{{{ns}}}returnVersion")
                if version:
                    break
        
        # If still not found, try to extract tax year from return period
        tax_year = self._extract_tax_year(root)
        
        if version:
            return f"{tax_year}{version}" if tax_year else version
        
        # Default version based on tax year
        if tax_year:
            return f"{tax_year}v1.0"
        
        # Last resort default
        logger.warning("Could not determine form version, using default")
        return "2020v1.0"
    
    def _extract_tax_year(self, root: etree._Element) -> Optional[str]:
        """Extract tax year from XML root element."""
        namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
        if None in root.nsmap:
            namespaces['default'] = root.nsmap[None]
            
        # Try to find tax period end date
        tax_period_paths = [
            "//*[local-name()='TaxPeriodEndDt']",
            "//irs:ReturnHeader/irs:TaxPeriodEndDt",
            "//default:ReturnHeader/default:TaxPeriodEndDt"
        ]
        
        for path in tax_period_paths:
            try:
                elements = root.xpath(path, namespaces=namespaces)
                if elements and hasattr(elements[0], 'text') and elements[0].text:
                    tax_period = elements[0].text.strip()
                    # Extract year from YYYY-MM-DD format
                    if '-' in tax_period:
                        return tax_period.split('-')[0]
            except Exception:
                continue
        
        return None
