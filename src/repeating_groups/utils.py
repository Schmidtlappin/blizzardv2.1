"""
Utility functions for processing repeating groups in IRS 990 XML files.

This module provides various helper functions for identifying, extracting,
and processing repeating element groups in IRS 990 XML filings.
"""

import re
import logging
from typing import Dict, Any, Optional, List, Tuple, Union
from lxml import etree

logger = logging.getLogger(__name__)

def clean_element_name(name: str) -> str:
    """
    Clean up XML element name for use as a database field name.
    
    Args:
        name: XML element name
        
    Returns:
        Cleaned name suitable for database use
    """
    # Remove common prefixes and suffixes
    name = re.sub(r'^(Frm|Form|Irs|IRS|ReturnHeader|Return|ReturnData)', '', name)
    name = re.sub(r'(Ind|Amt|Txt|Num|Desc|Grp|Group|Ind)$', '', name)
    
    # Convert camel case to snake case
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    
    # Remove any non-alphanumeric characters
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    
    return name

def guess_data_type(value: Optional[str]) -> str:
    """
    Make a best guess at what data type a value should be.
    
    Args:
        value: The value to analyze
        
    Returns:
        Data type string (text, number, boolean, or date)
    """
    if value is None:
        return 'text'
    
    # Convert value to string if it's not already
    if not isinstance(value, str):
        value = str(value)
        
    # Check if it's a number
    try:
        # Remove common currency and formatting characters
        cleaned_value = re.sub(r'[,$%()]', '', value)
        float(cleaned_value)
        return 'number'
    except ValueError:
        pass
    
    # Check if it's a boolean
    lower_value = value.lower()
    if lower_value in ('true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'):
        return 'boolean'
    
    # Check if it's a date (simple check)
    if re.match(r'\d{4}-\d{2}-\d{2}', value) or re.match(r'\d{2}/\d{2}/\d{4}', value):
        return 'date'
    
    # Default to text
    return 'text'

def guess_table_name(group_name: str) -> str:
    """
    Make a best guess at what table name to use for a repeating group.
    
    Args:
        group_name: Name of the repeating group element
        
    Returns:
        Table name for database storage
    """
    # Common mappings from XML element names to table names
    mappings = {
        'Form990PartVIISectionAGrp': 'compensation_officers',
        'OfficerDirectorTrusteeKeyEmpl': 'compensation_officers',
        'OtherExpensesGrp': 'expenses_other',
        'GrantsToOrgOutsideUSGrp': 'foreign_org_grants',
        'SupplementalInformationDetail': 'supplemental_info',
        'ProgramServiceRevenueGrp': 'program_service_revenue',
        'RelatedOrgInformationGrp': 'related_organizations',
        'UnrelatedOrgTxblPartnershipGrp': 'unrelated_partnerships',
        'ReceivablesFromOfficersGrp': 'receivables_from_officers',
        'CompOfHighestPaidEmplGrp': 'highest_compensated_employees',
        'IndependentContractorCompGrp': 'independent_contractors',
    }
    
    # Try direct lookup
    if group_name in mappings:
        return mappings[group_name]
    
    # Try partial matches
    for key, value in mappings.items():
        if key in group_name or group_name in key:
            return value
    
    # Generate a generic table name as fallback
    clean_name = clean_element_name(group_name)
    return f"repeating_{clean_name}"

def extract_value(element: etree._Element, xpath: str, 
                 namespaces: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Extract value from an XML element using an XPath.
    
    Args:
        element: XML element to extract from
        xpath: XPath to use for extraction
        namespaces: XML namespaces to use
        
    Returns:
        Extracted value as string, or None if not found
    """
    try:
        # Try direct XPath
        result = element.xpath(xpath, namespaces=namespaces or {})
        if result:
            if isinstance(result[0], str):
                return result[0].strip()
            elif hasattr(result[0], 'text') and result[0].text:
                return result[0].text.strip()
        
        # Try with local-name() for namespace independence
        if '/' in xpath:
            parts = xpath.split('/')
            local_name_parts = []
            for part in parts:
                if part and not part.startswith('@') and not part == '.':
                    local_name_parts.append(f"*[local-name()='{part}']")
                else:
                    local_name_parts.append(part)
            
            local_name_xpath = '/'.join(local_name_parts)
            if not local_name_xpath.startswith('/'):
                local_name_xpath = f"./{local_name_xpath}"
                
            result = element.xpath(local_name_xpath, namespaces=namespaces or {})
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                elif hasattr(result[0], 'text') and result[0].text:
                    return result[0].text.strip()
    
    except Exception as e:
        logger.debug(f"Error extracting value with xpath {xpath}: {e}")
        
    return None

def convert_value(value: str, data_type: str) -> Tuple[Optional[str], Optional[float], 
                                                     Optional[bool], Optional[str]]:
    """
    Convert string value to appropriate database type based on data_type.
    
    Args:
        value: String value to convert
        data_type: Target data type ('text', 'number', 'boolean', 'date')
        
    Returns:
        Tuple of (text_value, numeric_value, boolean_value, date_value)
    """
    if value is None:
        return None, None, None, None
    
    text_value = None
    numeric_value = None
    boolean_value = None
    date_value = None
    
    if data_type == 'number':
        try:
            # Remove any non-numeric characters except decimal point
            cleaned_value = re.sub(r'[^0-9.-]', '', value)
            numeric_value = float(cleaned_value)
            text_value = str(numeric_value)
        except ValueError:
            text_value = value
    elif data_type == 'boolean':
        try:
            lower_value = value.lower()
            if lower_value in ('true', 'yes', '1', 't', 'y'):
                boolean_value = True
                text_value = 'true'
            elif lower_value in ('false', 'no', '0', 'f', 'n'):
                boolean_value = False
                text_value = 'false'
            else:
                text_value = value
        except Exception:
            text_value = value
    elif data_type == 'date':
        date_value = value  # Store as string, database will validate
        text_value = value
    else:  # Default to text
        text_value = value
    
    return text_value, numeric_value, boolean_value, date_value

def guess_data_type(element_name: str, value: str) -> str:
    """
    Guess the data type based on element name and value.
    
    Args:
        element_name: Name of the XML element
        value: Element value
        
    Returns:
        Guessed data type ('text', 'number', 'boolean', or 'date')
    """
    if value is None:
        return 'text'
    
    # Check element name for clues
    lower_name = element_name.lower()
    
    # Check for boolean indicators
    if ('ind' in lower_name or lower_name.endswith('ind') or 
            lower_name.endswith('flag') or 'bool' in lower_name):
        return 'boolean'
    
    # Check for amount/numeric indicators
    if ('amt' in lower_name or lower_name.endswith('amt') or 
            'amount' in lower_name or 'total' in lower_name or
            'num' in lower_name or 'count' in lower_name):
        return 'number'
    
    # Check for date indicators
    if ('date' in lower_name or 'dt' in lower_name or 
            'year' in lower_name):
        return 'date'
    
    # Analyze the value
    if value:
        # Check if it's a number
        try:
            # Remove common currency and formatting characters
            cleaned_value = re.sub(r'[,$%()]', '', value)
            float(cleaned_value)
            return 'number'
        except ValueError:
            pass
        
        # Check if it's a boolean
        lower_value = value.lower()
        if lower_value in ('true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'):
            return 'boolean'
        
        # Check if it's a date (simple check)
        if re.match(r'\d{4}-\d{2}-\d{2}', value) or re.match(r'\d{2}/\d{2}/\d{4}', value):
            return 'date'
    
    # Default to text
    return 'text'
