"""
XML parsing utilities for the Blizzard system.

This module provides functions and classes for parsing and processing XML files,
with specific support for IRS 990 XML data.
"""

from typing import Dict, Any, List, Optional, Iterator, Union
from lxml import etree
import os
import logging

from src.core.constants import NAMESPACES
from src.core.exceptions import XMLProcessingError
from src.xml.streaming import StreamingParser

logger = logging.getLogger(__name__)

class XMLParser:
    """
    Parser for IRS 990 XML files.
    
    This class provides methods for parsing and extracting data from IRS 990 XML files.
    """
    
    def __init__(self, namespaces=None):
        """
        Initialize the parser with optional namespaces.
        
        Args:
            namespaces: XML namespaces to use for XPath queries
        """
        self.namespaces = namespaces or NAMESPACES
        self.streaming_parser = StreamingParser(namespaces=self.namespaces)
        
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse an XML file and extract key data.
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            Dictionary with extracted data
        """
        try:
            tree = etree.parse(file_path)
            root = tree.getroot()
            
            # Extract filing metadata
            metadata = self._extract_metadata(root)
            
            # Extract form data
            form_data = self._extract_form_data(root)
            
            return {
                "metadata": metadata,
                "form_data": form_data,
                "file_path": file_path
            }
        except Exception as e:
            raise XMLProcessingError(f"Failed to parse XML file {file_path}: {e}")
    
    def _extract_metadata(self, root: etree._Element) -> Dict[str, Any]:
        """
        Extract metadata from the XML root element.
        
        Args:
            root: Root element of the XML document
            
        Returns:
            Dictionary with metadata
        """
        metadata = {}
        
        # Update namespaces from the document if they weren't provided
        if not self.namespaces:
            self.namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
            if None in root.nsmap:
                self.namespaces['default'] = root.nsmap[None]
                self.namespaces['irs'] = root.nsmap[None]  # Common IRS namespace
                
        # Function to try multiple XPath patterns
        def get_text(patterns):
            for pattern in patterns:
                try:
                    elements = root.xpath(pattern, namespaces=self.namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        return elements[0].text.strip()
                except Exception:
                    continue
            return None
                
        # Extract EIN
        ein_patterns = [
            "//*[local-name()='EIN']",
            "//irs:ReturnHeader/irs:Filer/irs:EIN",
            "//default:ReturnHeader/default:Filer/default:EIN",
            "//ein",
            "//EIN"
        ]
        metadata["ein"] = get_text(ein_patterns)
        
        # Extract tax period
        tax_period_patterns = [
            "//*[local-name()='TaxPeriodEndDt']",
            "//irs:ReturnHeader/irs:TaxPeriodEndDt",
            "//default:ReturnHeader/default:TaxPeriodEndDt",
            "//taxPeriodEndDate",
            "//TaxPeriodEndDt"
        ]
        metadata["tax_period"] = get_text(tax_period_patterns)
        
        # Extract tax year
        if metadata.get("tax_period"):
            try:
                metadata["tax_year"] = metadata["tax_period"].split('-')[0]
            except (IndexError, AttributeError):
                pass
        
        # Extract form type
        form_type_patterns = [
            "//*[local-name()='ReturnTypeCd']",
            "//irs:ReturnHeader/irs:ReturnTypeCd",
            "//default:ReturnHeader/default:ReturnTypeCd",
            "//returnType",
            "//ReturnTypeCd"
        ]
        metadata["form_type"] = get_text(form_type_patterns)
        
        # Extract form version
        metadata["form_version"] = root.get('returnVersion')
        if not metadata["form_version"]:
            for ns in root.nsmap.values():
                form_version = root.get(f"{{{ns}}}returnVersion")
                if form_version:
                    metadata["form_version"] = form_version
                    break
        
        if not metadata["form_version"]:
            metadata["form_version"] = "Unknown"
            
        # Extract submission date
        submission_patterns = [
            "//*[local-name()='ReturnTs']",
            "//irs:ReturnHeader/irs:ReturnTs",
            "//default:ReturnHeader/default:ReturnTs",
            "//submissionDate",
            "//ReturnTs"
        ]
        metadata["submission_date"] = get_text(submission_patterns)
        
        # Generate filing ID
        if metadata.get("ein") and metadata.get("tax_period") and metadata.get("form_type"):
            metadata["filing_id"] = f"{metadata['ein']}_{metadata['tax_period']}_{metadata['form_type']}"
        
        return metadata
    
    def _extract_form_data(self, root: etree._Element) -> Dict[str, Any]:
        """
        Extract form data from the XML root element.
        
        Args:
            root: Root element of the XML document
            
        Returns:
            Dictionary with form data
        """
        form_data = {}
        
        # Extract organization info
        org_info = self._extract_organization_info(root)
        form_data["organization"] = org_info
        
        # Extract selected key fields that are commonly used
        form_data["key_fields"] = self._extract_key_fields(root)
        
        # Look for ReturnData element which often contains all data
        return_data_elem = None
        return_data_patterns = [
            "//*[local-name()='ReturnData']",
            "//irs:ReturnData",
            "//default:ReturnData"
        ]
        
        for pattern in return_data_patterns:
            try:
                elements = root.xpath(pattern, namespaces=self.namespaces)
                if elements:
                    return_data_elem = elements[0]
                    break
            except Exception:
                pass
        
        # Extract repeating groups if ReturnData was found
        if return_data_elem is not None:
            form_data["repeating_groups"] = self._extract_repeating_groups(return_data_elem)
        
        return form_data
        
    def _extract_organization_info(self, root: etree._Element) -> Dict[str, Any]:
        """Extract organization information from the XML root."""
        org_info = {}
        
        # Function to try multiple XPath patterns
        def get_text(patterns):
            for pattern in patterns:
                try:
                    elements = root.xpath(pattern, namespaces=self.namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        return elements[0].text.strip()
                except Exception:
                    continue
            return None
            
        # Extract organization name
        org_info["name"] = get_text([
            "//*[local-name()='BusinessNameLine1Txt']",
            "//irs:ReturnHeader/irs:Filer/irs:Name/irs:BusinessNameLine1Txt",
            "//default:ReturnHeader/default:Filer/default:Name/default:BusinessNameLine1Txt",
            "//irs:ReturnHeader/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt"
        ])
        
        # Extract address
        org_info["address_line1"] = get_text([
            "//*[local-name()='USAddress']/*[local-name()='AddressLine1Txt']",
            "//*[local-name()='ForeignAddress']/*[local-name()='AddressLine1Txt']"
        ])
        
        org_info["address_line2"] = get_text([
            "//*[local-name()='USAddress']/*[local-name()='AddressLine2Txt']",
            "//*[local-name()='ForeignAddress']/*[local-name()='AddressLine2Txt']"
        ])
        
        org_info["city"] = get_text([
            "//*[local-name()='USAddress']/*[local-name()='CityNm']",
            "//*[local-name()='ForeignAddress']/*[local-name()='CityNm']"
        ])
        
        org_info["state"] = get_text([
            "//*[local-name()='USAddress']/*[local-name()='StateAbbreviationCd']"
        ])
        
        org_info["zip_code"] = get_text([
            "//*[local-name()='USAddress']/*[local-name()='ZIPCd']"
        ])
        
        country = get_text([
            "//*[local-name()='ForeignAddress']/*[local-name()='CountryCd']"
        ])
        org_info["country"] = country if country else 'US'
        
        org_info["website"] = get_text([
            "//*[local-name()='WebsiteAddressTxt']"
        ])
        
        return org_info
        
    def _extract_key_fields(self, root: etree._Element) -> Dict[str, Any]:
        """Extract key fields that are commonly referenced."""
        key_fields = {}
        
        # Function to try multiple XPath patterns
        def get_text(patterns):
            for pattern in patterns:
                try:
                    elements = root.xpath(pattern, namespaces=self.namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        return elements[0].text.strip()
                except Exception:
                    continue
            return None
            
        # Extract total revenue
        key_fields["total_revenue"] = get_text([
            "//*[local-name()='TotalRevenueAmt']",
            "//*[local-name()='CYTotalRevenueAmt']"
        ])
        
        # Extract total assets
        key_fields["total_assets"] = get_text([
            "//*[local-name()='TotalAssetsEOYAmt']",
            "//*[local-name()='TotalAssetsAmt']"
        ])
        
        # Extract total liabilities
        key_fields["total_liabilities"] = get_text([
            "//*[local-name()='TotalLiabilitiesEOYAmt']",
            "//*[local-name()='TotalLiabilitiesAmt']"
        ])
        
        # Extract mission statement
        key_fields["mission"] = get_text([
            "//*[local-name()='ActivityOrMissionDesc']",
            "//*[local-name()='MissionDesc']"
        ])
        
        return key_fields
        
    def _extract_repeating_groups(self, element: etree._Element) -> Dict[str, List[Dict[str, Any]]]:
        """Identify and extract repeating groups of data."""
        repeating_groups = {}
        
        # Look for elements with "Grp" in the name
        grp_elements = element.xpath(".//*[contains(local-name(), 'Grp')]")
        
        for grp_element in grp_elements:
            # Check if this might be a repeating group by looking at children
            children = list(grp_element)
            if not children or len(children) < 2:
                continue
            
            # Check if children have consistent structure
            first_child = children[0]
            first_child_tags = {etree.QName(e).localname for e in first_child}
            
            # If all children have similar structure, treat as repeating group
            group_name = etree.QName(grp_element).localname
            group_data = []
            
            for child in children:
                item_data = {}
                for grandchild in child:
                    field_name = etree.QName(grandchild).localname
                    item_data[field_name] = grandchild.text if hasattr(grandchild, "text") else None
                    
                if item_data:  # Only add if we extracted some data
                    group_data.append(item_data)
                    
            if group_data:  # Only add the group if we found data
                repeating_groups[group_name] = group_data
                
        return repeating_groups
