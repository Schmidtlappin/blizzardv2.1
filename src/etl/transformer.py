"""
Data transformer for the Blizzard ETL pipeline.

This module provides transformer classes for transforming extracted data.
"""

from typing import Dict, Any, List, Optional, Union, Tuple
import logging
import re
import os
import pandas as pd
from lxml import etree
import hashlib
from datetime import datetime

from src.core.exceptions import XMLProcessingError, ConcordanceError
from src.etl.pipeline import Transformer
from src.db.models import FieldDefinition
from src.xml.streaming import StreamingParser

logger = logging.getLogger(__name__)

class FilingTransformer(Transformer[Dict[str, Any], Filing]):
    """
    Base transformer for converting extracted XML data to filing models.
    
    This class transforms data extracted from XML files into filing models
    that can be loaded into the database.
    """
    
    def __init__(self, concordance: Dict[str, Any] = None):
        """
        Initialize the transformer.
        
        Args:
            concordance: Dictionary mapping XML paths to field definitions
        """
        self.concordance = concordance or {}
        
    def transform(self, data: Dict[str, Any]) -> Filing:
        """
        Transform extracted data into a filing model.
        
        Args:
            data: Dictionary with extracted XML data
            
        Returns:
            Filing object
        """
        # Extract metadata
        metadata = data.get("metadata", {})
        ein = metadata.get("ein", "")
        tax_year = metadata.get("tax_year", "")
        form_type = metadata.get("form_type", "")
        
        # Create filing model
        filing = Filing(
            filing_id=metadata.get("filing_id", ""),
            ein=ein, 
            tax_year=tax_year, 
            form_type=form_type,
            tax_period=metadata.get("tax_period", ""),
            submission_date=metadata.get("submission_date", "")
        )
        
        return filing


class IRS990Transformer(Transformer[List[str], List[Dict[str, Any]]]):
    """
    Transformer for processing IRS 990 XML files into database-ready objects.
    
    This transformer handles:
    1. Extracting metadata from XML files
    2. Creating organization records
    3. Creating filing records
    4. Extracting field values based on concordance mappings
    5. Detecting and processing repeating groups
    """
    
    def __init__(self, concordance_path: str):
        """
        Initialize the IRS990Transformer.
        
        Args:
            concordance_path: Path to the concordance CSV file
        """
        self.concordance_path = concordance_path
        self.field_ids = {}  # Variable name to field_id mapping
        self.data_types = {}  # Variable name to data_type mapping
        self.tables = {}  # Table name to field list mapping
        self.field_loaded = False
        
        # Load the concordance file
        self._load_concordance()
        
    def _load_concordance(self):
        """Load the concordance file into memory."""
        try:
            if not os.path.exists(self.concordance_path):
                raise ConcordanceError(f"Concordance file not found: {self.concordance_path}")
                
            # Read the concordance file
            concordance_df = pd.read_csv(self.concordance_path)
            logger.info(f"Loaded {len(concordance_df)} records from concordance file")
            
            # Determine column names
            var_name_col = 'variable_name' if 'variable_name' in concordance_df.columns else 'VAR_NAME'
            xpath_col = 'xpath' if 'xpath' in concordance_df.columns else 'XPATH'
            desc_col = 'description' if 'description' in concordance_df.columns else 'DESCRIPTION'
            data_type_col = 'data_type_simple' if 'data_type_simple' in concordance_df.columns else 'DATA_TYPE_SIMPLE'
            table_col = 'rdb_table' if 'rdb_table' in concordance_df.columns else 'DATABASE_TABLE'
            relation_col = 'rdb_relationship' if 'rdb_relationship' in concordance_df.columns else 'RELATIONSHIP'
            
            # Process each row in the concordance
            field_id = 1  # Start with a simple counter for now
            for _, row in concordance_df.iterrows():
                var_name = row[var_name_col] if var_name_col in row and pd.notna(row[var_name_col]) else None
                xpath = row[xpath_col] if xpath_col in row and pd.notna(row[xpath_col]) else None
                description = row[desc_col] if desc_col in row and pd.notna(row[desc_col]) else None
                data_type = row[data_type_col] if data_type_col in row and pd.notna(row[data_type_col]) else 'text'
                rdb_table = row[table_col] if table_col in row and pd.notna(row[table_col]) else 'filing_values'
                relationship = row[relation_col] if relation_col in row and pd.notna(row[relation_col]) else 'ONE'
                
                # Skip if missing required fields
                if not var_name or not xpath:
                    continue
                    
                # Store mapping information
                self.field_ids[var_name] = field_id
                self.data_types[var_name] = data_type
                
                # Organize by table and relationship
                if rdb_table and rdb_table != 'filing_values':
                    if rdb_table not in self.tables:
                        self.tables[rdb_table] = {'ONE': [], 'MANY': []}
                    rel = 'MANY' if relationship == 'MANY' else 'ONE'
                    self.tables[rdb_table][rel].append({
                        'var_name': var_name,
                        'field_id': field_id,
                        'xpath': xpath,
                        'data_type': data_type
                    })
                
                field_id += 1
            
            self.field_loaded = True
            logger.info(f"Loaded {len(self.field_ids)} field definitions into memory")
            
        except Exception as e:
            logger.error(f"Error loading concordance data: {e}")
            raise ConcordanceError(f"Failed to load concordance: {e}")
            
    def transform(self, xml_files: List[str]) -> List[Dict[str, Any]]:
        """
        Transform a batch of XML files into processed data objects.
        
        Args:
            xml_files: List of paths to XML files
            
        Returns:
            List of processed data dictionaries containing organizations, filings, values, and groups
        """
        results = []
        
        for xml_file in xml_files:
            try:
                # Process each XML file
                processed_data = self._process_xml_file(xml_file)
                if processed_data:
                    results.append(processed_data)
                    logger.info(f"Successfully transformed {xml_file}")
                else:
                    logger.warning(f"Failed to transform {xml_file}")
            except Exception as e:
                logger.error(f"Error transforming XML file {xml_file}: {e}")
                
        return results
    
    def _process_xml_file(self, xml_file: str) -> Dict[str, Any]:
        """
        Process a single XML file and transform it to database objects.
        
        Args:
            xml_file: Path to the XML file
            
        Returns:
            Dictionary containing processed data
        """
        try:
            # Parse the XML file
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(xml_file, parser)
            root = tree.getroot()
            
            # Extract namespaces
            namespaces = {prefix: uri for prefix, uri in root.nsmap.items() if prefix is not None}
            # Add default namespace if it exists
            if None in root.nsmap:
                namespaces['default'] = root.nsmap[None]
            
            # If default namespace exists, add it as 'irs' too
            if 'default' in namespaces:
                namespaces['irs'] = namespaces['default']
            
            # Extract metadata using flexible XPath patterns
            file_name = os.path.basename(xml_file)
            metadata = self._extract_metadata(root, namespaces, file_name)
            
            # Extract organization info
            organization = self._extract_organization_info(root, namespaces, metadata['ein'])
            
            # Extract filing values
            filing_values = self._extract_filing_values(root, metadata['filing_id'], namespaces)
            
            # Detect and process repeating groups
            repeating_groups, group_values = self._process_repeating_groups(root, metadata['filing_id'], namespaces)
            
            return {
                'metadata': metadata,
                'organization': organization,
                'filing_values': filing_values,
                'repeating_groups': repeating_groups,
                'group_values': group_values,
                'xml_hash': hashlib.sha256(etree.tostring(root)).hexdigest()
            }
            
        except Exception as e:
            logger.error(f"Error processing XML file {xml_file}: {e}")
            return None
            
    def _extract_metadata(self, root, namespaces, file_name) -> Dict[str, Any]:
        """
        Extract metadata from the XML root.
        
        Args:
            root: XML root element
            namespaces: XML namespaces
            file_name: Name of the XML file
            
        Returns:
            Dictionary with metadata
        """
        # Function to find text with multiple xpath patterns
        def get_text(patterns):
            for pattern in patterns:
                try:
                    elements = root.xpath(pattern, namespaces=namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        return elements[0].text.strip()
                except Exception:
                    continue
            return None
        
        # Get EIN
        ein_patterns = [
            "//*[local-name()='EIN']",
            "//irs:ReturnHeader/irs:Filer/irs:EIN",
            "//default:ReturnHeader/default:Filer/default:EIN"
        ]
        ein = get_text(ein_patterns)
        if not ein:
            raise XMLProcessingError("EIN not found in XML")
        
        # Get tax period
        tax_period_patterns = [
            "//*[local-name()='TaxPeriodEndDt']",
            "//irs:ReturnHeader/irs:TaxPeriodEndDt",
            "//default:ReturnHeader/default:TaxPeriodEndDt"
        ]
        tax_period = get_text(tax_period_patterns)
        if not tax_period:
            raise XMLProcessingError("Tax period not found in XML")
        
        # Get form type
        form_type_patterns = [
            "//*[local-name()='ReturnTypeCd']",
            "//irs:ReturnHeader/irs:ReturnTypeCd",
            "//default:ReturnHeader/default:ReturnTypeCd"
        ]
        form_type = get_text(form_type_patterns)
        if not form_type:
            raise XMLProcessingError("Form type not found in XML")
        
        # Get form version
        form_version = root.get('returnVersion')
        if not form_version:
            for ns in namespaces.values():
                form_version = root.get(f"{{{ns}}}returnVersion")
                if form_version:
                    break
        if not form_version:
            form_version = 'Unknown'
        
        # Get tax year
        tax_year = None
        if tax_period:
            try:
                tax_year = int(tax_period.split('-')[0])
            except (IndexError, ValueError):
                pass
                
        # Get submission date
        submission_patterns = [
            "//*[local-name()='ReturnTs']",
            "//irs:ReturnHeader/irs:ReturnTs",
            "//default:ReturnHeader/default:ReturnTs"
        ]
        submission_date = get_text(submission_patterns)
        
        # Generate filing ID
        filing_id = f"{ein}_{tax_period}_{form_type}"
        
        return {
            'filing_id': filing_id,
            'ein': ein,
            'tax_period': tax_period,
            'form_type': form_type,
            'form_version': form_version,
            'tax_year': tax_year,
            'submission_date': submission_date,
            'object_id': file_name
        }
        
    def _extract_organization_info(self, root, namespaces, ein) -> Dict[str, Any]:
        """
        Extract organization info from the XML.
        
        Args:
            root: XML root element
            namespaces: XML namespaces
            ein: Employer Identification Number
            
        Returns:
            Dictionary with organization info
        """
        # Function to find text with multiple xpath patterns
        def get_text(patterns):
            for pattern in patterns:
                try:
                    elements = root.xpath(pattern, namespaces=namespaces)
                    if elements and hasattr(elements[0], 'text') and elements[0].text:
                        return elements[0].text.strip()
                except Exception:
                    continue
            return None
        
        # Extract organization name
        org_name = get_text([
            "//*[local-name()='BusinessNameLine1Txt']",
            "//irs:ReturnHeader/irs:Filer/irs:Name/irs:BusinessNameLine1Txt",
            "//default:ReturnHeader/default:Filer/default:Name/default:BusinessNameLine1Txt",
            "//irs:ReturnHeader/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt"
        ])
        
        # Extract address
        address_line1 = get_text([
            "//*[local-name()='USAddress']/*[local-name()='AddressLine1Txt']",
            "//*[local-name()='ForeignAddress']/*[local-name()='AddressLine1Txt']"
        ])
        
        address_line2 = get_text([
            "//*[local-name()='USAddress']/*[local-name()='AddressLine2Txt']",
            "//*[local-name()='ForeignAddress']/*[local-name()='AddressLine2Txt']"
        ])
        
        city = get_text([
            "//*[local-name()='USAddress']/*[local-name()='CityNm']",
            "//*[local-name()='ForeignAddress']/*[local-name()='CityNm']"
        ])
        
        state = get_text([
            "//*[local-name()='USAddress']/*[local-name()='StateAbbreviationCd']"
        ])
        
        zip_code = get_text([
            "//*[local-name()='USAddress']/*[local-name()='ZIPCd']"
        ])
        
        country = get_text([
            "//*[local-name()='ForeignAddress']/*[local-name()='CountryCd']"
        ])
        if not country:
            country = 'US'
        
        website = get_text([
            "//*[local-name()='WebsiteAddressTxt']"
        ])
        
        return {
            'ein': ein,
            'name': org_name,
            'address_line1': address_line1,
            'address_line2': address_line2,
            'city': city,
            'state': state,
            'zip': zip_code,
            'country': country,
            'website': website
        }
        
    def _extract_filing_values(self, root, filing_id, namespaces) -> List[Dict[str, Any]]:
        """
        Extract filing values based on concordance mappings.
        
        Args:
            root: XML root element
            filing_id: Filing ID
            namespaces: XML namespaces
            
        Returns:
            List of filing values
        """
        filing_values = []
        
        # Make sure concordance is loaded
        if not self.field_loaded:
            logger.warning("Concordance not loaded, can't extract filing values")
            return filing_values
            
        # Process each field in concordance
        for var_name, field_id in self.field_ids.items():
            # Skip fields that belong to repeating groups (MANY relationship)
            skip_field = False
            for table in self.tables.values():
                for field_info in table['MANY']:
                    if field_info['var_name'] == var_name:
                        skip_field = True
                        break
                if skip_field:
                    break
            
            if skip_field:
                continue
                
            # Get data type for this field
            data_type = self.data_types.get(var_name, 'text')
            
            # Create xpath variations and try to extract value
            value = self._extract_value_from_xml(root, var_name, namespaces)
            
            if value is not None:
                # Convert value based on data type
                text_value, numeric_value, boolean_value, date_value = self._convert_value(value, data_type)
                
                filing_values.append({
                    'filing_id': filing_id,
                    'field_id': field_id,
                    'text_value': text_value,
                    'numeric_value': numeric_value,
                    'boolean_value': boolean_value,
                    'date_value': date_value
                })
        
        return filing_values
        
    def _process_repeating_groups(self, root, filing_id, namespaces) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Detect and process repeating groups in the XML.
        
        Args:
            root: XML root element
            filing_id: Filing ID
            namespaces: XML namespaces
            
        Returns:
            Tuple of (repeating groups, repeating group values)
        """
        repeating_groups = []
        group_values = []
        
        # First look for ReturnData element which often contains all data
        return_data_elem = None
        return_data_patterns = [
            "//*[local-name()='ReturnData']",
            "//irs:ReturnData",
            "//default:ReturnData"
        ]
        
        for pattern in return_data_patterns:
            try:
                elements = root.xpath(pattern, namespaces=namespaces)
                if elements:
                    return_data_elem = elements[0]
                    break
            except Exception:
                pass
        
        # Use ReturnData as root for repeating groups if found, otherwise use original root
        repeating_root = return_data_elem if return_data_elem is not None else root
        
        # Detect repeating groups
        detected_groups = self._detect_repeating_groups(repeating_root, namespaces)
        logger.info(f"Detected {len(detected_groups)} potential repeating group types")
        
        # Process each detected repeating group
        group_id = 1  # Simple counter for now
        for group_xpath, group_elements in detected_groups.items():
            # Figure out which table this group might correspond to
            table_name = self._guess_table_name(group_xpath)
            if not table_name:
                continue
                
            logger.info(f"Processing repeating group: {group_xpath} as table {table_name}")
            
            # Get field definitions for this table
            table_fields = self._get_fields_for_table(table_name)
            
            # Create a repeating group record
            repeating_groups.append({
                'group_id': group_id,
                'filing_id': filing_id,
                'name': table_name,
                'xpath': group_xpath
            })
            
            # Process each element in this group
            for group_element in group_elements:
                for field_info in table_fields:
                    field_id = field_info['field_id']
                    var_name = field_info['var_name']
                    
                    # Extract value from this element
                    element_value = self._extract_value_from_element(group_element, var_name, namespaces)
                    
                    if element_value is not None:
                        # Convert value based on data type
                        data_type = field_info.get('data_type', 'text')
                        text_value, numeric_value, boolean_value, date_value = self._convert_value(element_value, data_type)
                        
                        # Add to group values
                        group_values.append({
                            'group_id': group_id,
                            'field_id': field_id,
                            'text_value': text_value,
                            'numeric_value': numeric_value,
                            'boolean_value': boolean_value,
                            'date_value': date_value
                        })
            
            group_id += 1
            
        return repeating_groups, group_values
        
    def _detect_repeating_groups(self, root, namespaces) -> Dict[str, List[etree._Element]]:
        """
        Detect repeating groups in the XML.
        
        Args:
            root: XML root element
            namespaces: XML namespaces
            
        Returns:
            Dictionary mapping group XPaths to lists of element nodes
        """
        repeating_groups = {}
        
        # Common patterns for repeating groups
        patterns = [
            "//*[contains(local-name(), 'Grp')]",
            "//*[contains(local-name(), 'Group')]",
            "//*[contains(local-name(), 'Table')]",
            "//irs:*[contains(local-name(), 'Grp')]",
            "//default:*[contains(local-name(), 'Grp')]"
        ]
        
        for pattern in patterns:
            try:
                elements = root.xpath(pattern, namespaces=namespaces)
                for element in elements:
                    # Check if this element has multiple children with the same name
                    child_counts = {}
                    for child in element:
                        local_name = etree.QName(child).localname
                        if local_name in child_counts:
                            child_counts[local_name] += 1
                        else:
                            child_counts[local_name] = 1
                    
                    # If we have repeating children, consider this a repeating group
                    for name, count in child_counts.items():
                        if count > 1:
                            element_path = root.getroottree().getpath(element)
                            repeating_groups[element_path] = list(element)
                            break
            except Exception as e:
                logger.debug(f"Error in detecting repeating groups with pattern {pattern}: {e}")
        
        return repeating_groups
        
    def _guess_table_name(self, group_path) -> str:
        """
        Guess the table name for a repeating group based on its path.
        
        Args:
            group_path: XPath of the repeating group
            
        Returns:
            Table name
        """
        # Mapping of group path patterns to table names
        mappings = {
            'OfficerDirectorTrustee': 'compensation_officers',
            'CompensationHighest': 'compensation_highest',
            'GrantsToDomesticOrg': 'grants_domestic',
            'GrantsToForeignOrg': 'foreign_org_grants',
            'ForeignActivities': 'foreign_activities',
            'ExpenseOther': 'expenses_other',
            'ProgSrvcAccomplishment': 'program_service_accomplishment',
            'RelatedOrgInfo': 'related_org_info',
            'UnrelatedBusiness': 'unrelated_business',
            'SupplementalInfo': 'supplemental_info',
            'DisregardedEntityGrp': 'disregarded_entities',
            'LandBuildingEquipmentGrp': 'land_buildings_equipment',
            'InvestmentIncomeGrp': 'investment_income',
            'OtherRevenueGrp': 'revenue_misc',
            'FunctionalExpenseGrp': 'functional_expenses'
        }
        
        # First, try direct lookup
        for key, value in mappings.items():
            if key in group_path:
                return value
        
        # If no match found, make a generic table name from the group path
        # Remove special characters and convert to lowercase
        import re
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', group_path).lower()
        clean_name = re.sub(r'_+', '_', clean_name)  # Replace multiple underscores with single
        clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
        
        # For simplicity in testing, we'll use a few common table names for well-known group types
        if "PartVII" in group_path or "Compensation" in group_path or "Officer" in group_path:
            return "compensation_officers"
        elif "Expense" in group_path:
            return "expenses_other"
        elif "Grant" in group_path or "Foreign" in group_path:
            return "foreign_org_grants"
        elif "Supplemental" in group_path or "Information" in group_path:
            return "supplemental_info"
        else:
            return f"repeating_{clean_name[-50:]}"  # Ensure name isn't too long
    
    def _get_fields_for_table(self, table_name) -> List[Dict[str, Any]]:
        """
        Get field definitions for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of field definitions
        """
        # Check if we have field definitions for this table in concordance
        fields = []
        if table_name in self.tables and self.tables[table_name]['MANY']:
            return self.tables[table_name]['MANY']
        
        # If no fields found, create generic field definitions
        logger.warning(f"No MANY fields found for table {table_name}, using generic field definitions")
        
        # Common field patterns for different repeating groups
        common_fields = {
            'compensation_officers': [
                {'name': 'PersonNm', 'path': './PersonNm', 'type': 'text', 'description': 'Person Name'},
                {'name': 'TitleTxt', 'path': './TitleTxt', 'type': 'text', 'description': 'Title'},
                {'name': 'OfficerInd', 'path': './OfficerInd', 'type': 'boolean', 'description': 'Officer Indicator'},
                {'name': 'ReportableCompFromOrgAmt', 'path': './ReportableCompFromOrgAmt', 'type': 'numeric', 'description': 'Reportable Compensation From Organization'},
                {'name': 'ReportableCompFromRltdOrgAmt', 'path': './ReportableCompFromRltdOrgAmt', 'type': 'numeric', 'description': 'Reportable Compensation From Related Organizations'}
            ],
            'expenses_other': [
                {'name': 'Desc', 'path': './Desc', 'type': 'text', 'description': 'Description'},
                {'name': 'TotalAmt', 'path': './TotalAmt', 'type': 'numeric', 'description': 'Total Amount'},
                {'name': 'ProgramServicesAmt', 'path': './ProgramServicesAmt', 'type': 'numeric', 'description': 'Program Services Amount'},
                {'name': 'MgmtAndGeneralAmt', 'path': './MgmtAndGeneralAmt', 'type': 'numeric', 'description': 'Management and General Amount'},
                {'name': 'FundraisingAmt', 'path': './FundraisingAmt', 'type': 'numeric', 'description': 'Fundraising Amount'}
            ],
            'foreign_org_grants': [
                {'name': 'RegionTxt', 'path': './RegionTxt', 'type': 'text', 'description': 'Region'},
                {'name': 'PurposeOfGrantTxt', 'path': './PurposeOfGrantTxt', 'type': 'text', 'description': 'Purpose of Grant'},
                {'name': 'CashGrantAmt', 'path': './CashGrantAmt', 'type': 'numeric', 'description': 'Cash Grant Amount'},
                {'name': 'NonCashAssistanceAmt', 'path': './NonCashAssistanceAmt', 'type': 'numeric', 'description': 'Non-cash Assistance Amount'},
                {'name': 'MannerOfCashDisbursementTxt', 'path': './MannerOfCashDisbursementTxt', 'type': 'text', 'description': 'Manner of Cash Disbursement'}
            ],
            'supplemental_info': [
                {'name': 'FormAndLineReferenceDesc', 'path': './FormAndLineReferenceDesc', 'type': 'text', 'description': 'Form and Line Reference'},
                {'name': 'ExplanationTxt', 'path': './ExplanationTxt', 'type': 'text', 'description': 'Explanation Text'}
            ]
        }
        
        # Default fields for unknown tables
        default_fields = [
            {'name': 'Value', 'path': '.', 'type': 'text', 'description': 'Value'},
            {'name': 'Amount', 'path': './Amt', 'type': 'numeric', 'description': 'Amount'},
            {'name': 'Description', 'path': './Desc', 'type': 'text', 'description': 'Description'},
            {'name': 'Name', 'path': './Nm', 'type': 'text', 'description': 'Name'},
            {'name': 'Text', 'path': './Txt', 'type': 'text', 'description': 'Text'}
        ]
        
        # Use common fields for known tables
        if table_name in common_fields:
            field_specs = common_fields[table_name]
        else:
            field_specs = default_fields
        
        # Create field entries
        next_field_id = len(self.field_ids) + 1
        for field_spec in field_specs:
            var_name = f"{table_name}_{field_spec['name']}"
            
            # Skip if we already have this field
            if var_name in self.field_ids:
                continue
            
            field_id = next_field_id
            next_field_id += 1
            
            # Add field to our in-memory maps
            self.field_ids[var_name] = field_id
            self.data_types[var_name] = field_spec['type']
            
            # Add to our result list
            fields.append({
                'field_id': field_id,
                'var_name': var_name,
                'xpath': field_spec['path'],
                'data_type': field_spec['type']
            })
            
        return fields
    
    def _extract_value_from_xml(self, root, var_name, namespaces) -> Optional[str]:
        """
        Extract a value from XML for a field from the concordance.
        
        Args:
            root: XML root element
            var_name: Variable name from concordance
            namespaces: XML namespaces
            
        Returns:
            Extracted value or None
        """
        # Try to get the xpath for this field
        xpath = None
        for table in self.tables.values():
            for rel in ('ONE', 'MANY'):
                for field_info in table[rel]:
                    if field_info['var_name'] == var_name and 'xpath' in field_info:
                        xpath = field_info['xpath']
                        break
                if xpath:
                    break
            if xpath:
                break
        
        if not xpath:
            return None
        
        # Create variations of the XPath to try
        variations = self._create_xpath_variations(xpath, namespaces)
        
        # Try each variation
        for variation in variations:
            try:
                elements = root.xpath(variation, namespaces=namespaces)
                if elements and hasattr(elements[0], 'text') and elements[0].text:
                    return elements[0].text.strip()
            except Exception as e:
                logger.debug(f"XPath {variation} failed: {e}")
        
        return None
        
    def _extract_value_from_element(self, element, field_name, namespaces) -> Optional[str]:
        """
        Extract a value from an element using the field name.
        
        Args:
            element: XML element
            field_name: Name of the field to extract
            namespaces: XML namespaces
            
        Returns:
            Extracted value or None
        """
        # Try direct child elements first
        for child in element:
            local_name = etree.QName(child).localname
            if local_name == field_name:
                if hasattr(child, 'text') and child.text:
                    return child.text.strip()
        
        # Try with namespaces
        for prefix in namespaces:
            try:
                ns_xpath = f".//{prefix}:{field_name}"
                elements = element.xpath(ns_xpath, namespaces=namespaces)
                if elements and hasattr(elements[0], 'text') and elements[0].text:
                    return elements[0].text.strip()
            except Exception:
                pass
        
        # Try with local-name
        try:
            local_xpath = f".//*[local-name()='{field_name}']"
            elements = element.xpath(local_xpath)
            if elements and hasattr(elements[0], 'text') and elements[0].text:
                return elements[0].text.strip()
        except Exception:
            pass
        
        # Try partial matches on local-name
        try:
            # First try contains
            contains_xpath = f".//*[contains(local-name(), '{field_name}')]"
            elements = element.xpath(contains_xpath)
            if elements and hasattr(elements[0], 'text') and elements[0].text:
                return elements[0].text.strip()
                
            # Then try ends-with if available
            try:
                ends_with_xpath = f".//*[ends-with(local-name(), '{field_name}')]"
                elements = element.xpath(ends_with_xpath)
                if elements and hasattr(elements[0], 'text') and elements[0].text:
                    return elements[0].text.strip()
            except Exception:
                # ends-with is not supported in XPath 1.0
                pass
        except Exception:
            pass
        
        return None

    def _create_xpath_variations(self, xpath, namespaces) -> List[str]:
        """
        Create multiple variations of an XPath to try.
        
        Args:
            xpath: Original XPath
            namespaces: XML namespaces
            
        Returns:
            List of XPath variations
        """
        variations = [xpath]  # Start with original
        
        # Remove namespace prefixes for local-name approach
        local_name_xpath = xpath
        for prefix in namespaces:
            if prefix and f"{prefix}:" in local_name_xpath:
                parts = []
                prev_end = 0
                for match in re.finditer(f"{prefix}:([a-zA-Z0-9_]+)", local_name_xpath):
                    start, end = match.span()
                    tag_name = match.group(1)
                    parts.append(local_name_xpath[prev_end:start])
                    parts.append(f"*[local-name()='{tag_name}']")
                    prev_end = end
                parts.append(local_name_xpath[prev_end:])
                local_name_xpath = "".join(parts)
        
        variations.append(local_name_xpath)
        
        # Try different namespace prefixes
        for prefix in namespaces:
            if prefix:
                new_xpath = xpath
                for other_prefix in namespaces:
                    if other_prefix and other_prefix != prefix:
                        new_xpath = new_xpath.replace(f"{other_prefix}:", f"{prefix}:")
                if new_xpath != xpath:
                    variations.append(new_xpath)
        
        # Add fully qualified local-name version
        local_name_parts = []
        prev_end = 0
        for match in re.finditer(r"/([^/]+)", xpath):
            start, end = match.span()
            tag = match.group(1)
            if ":" in tag and not tag.startswith("@"):
                prefix, name = tag.split(":", 1)
                local_name_parts.append(xpath[prev_end:start])
                local_name_parts.append(f"/*[local-name()='{name}']")
            else:
                local_name_parts.append(xpath[prev_end:end])
            prev_end = end
        local_name_parts.append(xpath[prev_end:])
        variations.append("".join(local_name_parts))
            
        return variations
    
    def _convert_value(self, value, data_type) -> Tuple[Optional[str], Optional[float], Optional[bool], Optional[str]]:
        """
        Convert a value to the appropriate type based on the data type.
        
        Args:
            value: Value to convert
            data_type: Data type
            
        Returns:
            Tuple of (text_value, numeric_value, boolean_value, date_value)
        """
        text_value = None
        numeric_value = None
        boolean_value = None
        date_value = None
        
        if not data_type or data_type == 'text':
            text_value = value
        elif data_type == 'numeric':
            try:
                # Remove non-numeric characters like $, %, and commas
                clean_value = re.sub(r'[^\d.-]', '', value)
                numeric_value = float(clean_value)
            except ValueError:
                text_value = value
        elif data_type == 'checkbox' or data_type == 'boolean':
            lower_val = value.lower() if isinstance(value, str) else str(value).lower()
            boolean_value = lower_val in ('true', 'yes', '1', 't', 'y', 'x')
        elif data_type == 'date':
            date_value = value
        else:
            text_value = value
            
        return text_value, numeric_value, boolean_value, date_value
        
        # Transform form data
        form_data = data.get("form_data", {})
        self._transform_form_data(filing, form_data)
        
        return filing
    
    def _transform_form_data(self, filing: FilingModel, form_data: Dict[str, Any]) -> None:
        """
        Transform form data and add it to the filing model.
        
        Args:
            filing: FilingModel to add values to
            form_data: Dictionary with form data
        """
        # Placeholder implementation
        # In a full implementation, this would transform extracted XML data
        # into fields and values in the filing model
        
        # Just pass through any simple key-value pairs for now
        for key, value in form_data.items():
            filing.add_value(key, value)
