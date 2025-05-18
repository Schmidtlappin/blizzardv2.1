"""
Module for processing and managing repeating groups in IRS 990 XML files.

This module handles the extraction, transformation, and loading of repeating
element groups from XML into database tables, including nested repeating groups.
"""

from typing import List, Dict, Any, Optional, Tuple
from lxml import etree
import psycopg2
import logging
import os

from src.core.exceptions import RepeatingGroupError
from src.db.connection_pool import ConnectionPool
from src.repeating_groups.detector import RepeatingGroupDetector
from src.repeating_groups.utils import guess_table_name, extract_value, convert_value, guess_data_type

logger = logging.getLogger(__name__)

class RepeatingGroupProcessor:
    """
    Processor for handling repeating groups in IRS 990 XML files.
    
    This class handles the extraction of repeating element groups from XML
    and their storage in the database, including nested repeating groups.
    """
    
    def __init__(self, db_conn=None, namespaces=None, max_nesting_level=3):
        """
        Initialize the processor with optional database connection and namespaces.
        
        Args:
            db_conn: Database connection (if None, one will be obtained from the pool)
            namespaces: XML namespaces to use for XPath queries
            max_nesting_level: Maximum nesting level to process (default: 3)
        """
        self.db_conn = db_conn
        self.namespaces = namespaces or {}
        self.detector = RepeatingGroupDetector(namespaces=self.namespaces, max_nesting_level=max_nesting_level)
        self.max_nesting_level = max_nesting_level
        
    def process_repeating_groups(self, xml_file_path: str, filing_id: str, include_nested: bool = True) -> List[Dict[str, Any]]:
        """
        Process all repeating groups in an XML file.
        
        Args:
            xml_file_path: Path to the XML file
            filing_id: ID of the filing being processed
            include_nested: Whether to process nested repeating groups
            
        Returns:
            List of dictionaries with information about processed repeating groups
        """
        # Detect repeating groups
        try:
            if include_nested:
                repeating_groups = self.detector.find_nested_groups(xml_file_path)
                logger.info(f"Found repeating groups with nested structure in {os.path.basename(xml_file_path)}")
            else:
                repeating_groups = self.detector.find_repeating_groups(xml_file_path)
                logger.info(f"Found {len(repeating_groups)} potential repeating groups in {os.path.basename(xml_file_path)}")
                
        except RepeatingGroupError as e:
            logger.error(f"Failed to detect repeating groups: {e}")
            return []
        
        # Process each repeating group
        results = []
        for group in repeating_groups:
            group_result = self._process_group(group, xml_file_path, filing_id)
            if group_result:
                # Process nested groups if available
                if include_nested and 'nested_groups' in group:
                    nested_results = []
                    for nested_group in group['nested_groups']:
                        nested_result = self._process_nested_group(
                            nested_group, 
                            xml_file_path, 
                            filing_id, 
                            parent_group_id=group_result.get('group_id')
                        )
                        if nested_result:
                            nested_results.append(nested_result)
                    
                    if nested_results:
                        group_result['nested_groups'] = nested_results
                
                results.append(group_result)
                
        logger.info(f"Successfully processed {len(results)} repeating groups for filing {filing_id}")
        return results
    
    def _process_group(self, group: Dict[str, Any], xml_file_path: str, 
                       filing_id: str) -> Dict[str, Any]:
        """
        Process a single repeating group.
        
        Args:
            group: Dictionary with repeating group information
            xml_file_path: Path to the XML file
            filing_id: ID of the filing being processed
            
        Returns:
            Dictionary with information about the processed repeating group
        """
        try:
            # Generate a table name for this group
            group_name = group.get("name", "")
            table_name = guess_table_name(group_name)
            
            # Extract all values from the XML
            values = self._extract_values(group, xml_file_path)
            
            if not values:
                logger.warning(f"No values extracted for group {group_name} in filing {filing_id}")
                return None
            
            # Store values in the database
            result = self._store_values(group, values, filing_id, table_name)
            
            return {
                "group_name": group_name,
                "table_name": table_name,
                "count": len(values),
                "fields": list(values[0].keys()) if values else [],
                "values": values[:3]  # Include sample values in the result
            }
        except Exception as e:
            logger.error(f"Error processing repeating group {group.get('name', 'unknown')}: {e}")
            return None
    
    def _process_nested_group(self, group: Dict[str, Any], xml_file_path: str, 
                              filing_id: str, parent_group_id: int) -> Dict[str, Any]:
        """
        Process a nested repeating group.
        
        Args:
            group: Dictionary with repeating group information
            xml_file_path: Path to the XML file
            filing_id: ID of the filing being processed
            parent_group_id: ID of the parent repeating group
            
        Returns:
            Dictionary with information about the processed nested repeating group
        """
        try:
            # Generate a table name for this group
            group_name = group.get("name", "")
            table_name = guess_table_name(group_name)
            
            # Extract all values from the XML
            values = self._extract_values(group, xml_file_path)
            
            if not values:
                logger.warning(f"No values extracted for nested group {group_name} in filing {filing_id}")
                return None
            
            # Store values in the database with parent group reference
            result = self._store_nested_values(group, values, filing_id, table_name, parent_group_id)
            
            # Process further nested groups if available
            if 'nested_groups' in group and len(group['nested_groups']) > 0:
                nested_results = []
                for nested_group in group['nested_groups']:
                    nested_result = self._process_nested_group(
                        nested_group,
                        xml_file_path,
                        filing_id,
                        result.get('group_id')
                    )
                    if nested_result:
                        nested_results.append(nested_result)
                
                if nested_results:
                    result['nested_groups'] = nested_results
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing nested group {group.get('name', 'unknown')}: {e}")
            return None
    
    def _extract_values(self, group: Dict[str, Any], xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Extract values for a repeating group from an XML file.
        
        Args:
            group: Dictionary with repeating group information
            xml_file_path: Path to the XML file
            
        Returns:
            List of dictionaries with values from the repeating group
        """
        element = group.get("element")
        if element is None:
            logger.warning("No element reference found in group info")
            return []
        
        # Extract values from each child element (which represents a row in the group)
        result = []
        for child_element in element.iterchildren():
            row_values = {}
            
            # Process each field in the child element
            for grandchild in child_element.iterchildren():
                field_name = etree.QName(grandchild).localname
                field_value = grandchild.text.strip() if hasattr(grandchild, "text") and grandchild.text else None
                
                if field_value is not None:
                    # Use field names from the first occurrence
                    row_values[field_name] = field_value
                    
                    # Check for nested fields (for depth 2)
                    for great_grandchild in grandchild.iterchildren():
                        nested_field_name = etree.QName(great_grandchild).localname
                        nested_field_value = great_grandchild.text.strip() if hasattr(great_grandchild, "text") and great_grandchild.text else None
                        
                        if nested_field_value is not None:
                            row_values[f"{field_name}_{nested_field_name}"] = nested_field_value
                
            if row_values:  # Only add if there are actual values
                result.append(row_values)
                
        return result
    
    def _store_values(self, group: Dict[str, Any], values: List[Dict[str, Any]], 
                     filing_id: str, table_name: str) -> Dict[str, Any]:
        """
        Store repeating group values in the database.
        
        Args:
            group: Dictionary with repeating group information
            values: List of dictionaries with values from the repeating group
            filing_id: ID of the filing being processed
            table_name: Name of the table to store the values in
            
        Returns:
            Dictionary with information about the stored values
        """
        if not self.db_conn:
            logger.warning("No database connection available. Values not stored.")
            return {
                "stored": False,
                "reason": "No database connection",
                "group_name": group.get("name", ""),
                "count": len(values) if values else 0
            }
            
        try:
            cursor = self.db_conn.cursor()
            
            # Create the repeating group entry
            cursor.execute(
                """
                INSERT INTO repeating_groups (filing_id, name, xpath)
                VALUES (%s, %s, %s)
                RETURNING group_id
                """,
                (filing_id, group.get("name"), group.get("path", ""))
            )
            group_id = cursor.fetchone()[0]
            
            # Process each row in the repeating group
            for idx, row_values in enumerate(values):
                # Process each field in this row
                for field_name, field_value in row_values.items():
                    # First, check if field definition exists
                    cursor.execute(
                        """
                        SELECT field_id FROM field_definitions
                        WHERE name = %s
                        """,
                        (field_name,)
                    )
                    field_result = cursor.fetchone()
                    
                    if field_result:
                        field_id = field_result[0]
                    else:
                        # Create new field definition
                        data_type = guess_data_type(field_name, field_value)
                        cursor.execute(
                            """
                            INSERT INTO field_definitions (name, xpath, field_type)
                            VALUES (%s, %s, %s)
                            RETURNING field_id
                            """,
                            (field_name, f"//{field_name}", data_type)
                        )
                        field_id = cursor.fetchone()[0]
                    
                    # Convert value based on type
                    text_value, numeric_value, boolean_value, date_value = None, None, None, None
                    if field_value is not None:
                        if isinstance(field_value, str):
                            text_value = field_value
                        else:
                            text_value = str(field_value)                        # Insert the value
                        cursor.execute(
                            """
                            INSERT INTO repeating_group_values (group_id, field_id, value, instance_index)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (group_id, field_id, text_value, idx)
                        )
            
            # Commit the transaction
            self.db_conn.commit()
            
            return {
                "stored": True,
                "group_id": group_id,
                "group_name": group.get("name", ""),
                "table_name": table_name,
                "rows_processed": len(values)
            }
            
        except Exception as e:
            logger.error(f"Error storing repeating group values: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return {
                "stored": False,
                "reason": str(e),
                "group_name": group.get("name", ""),
                "count": len(values) if values else 0
            }
    
    def _store_nested_values(self, group: Dict[str, Any], values: List[Dict[str, Any]], 
                            filing_id: str, table_name: str, parent_group_id: int) -> Dict[str, Any]:
        """
        Store nested repeating group values in the database.
        
        Args:
            group: Dictionary with repeating group information
            values: List of dictionaries with field values
            filing_id: ID of the filing being processed
            table_name: Table name for the repeating group
            parent_group_id: ID of the parent repeating group
            
        Returns:
            Dictionary with information about the stored repeating group
        """
        if not self.db_conn:
            try:
                from src.db.connection_pool import ConnectionPool
                pool = ConnectionPool()
                conn = pool.get_connection()
            except ImportError:
                logger.warning("Connection pool not available, trying to use psycopg2 directly")
                try:
                    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
                except Exception as e:
                    logger.error(f"Failed to connect to database: {e}")
                    return None
        else:
            conn = self.db_conn
            
        try:
            # Create group record with parent reference
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO repeating_groups (filing_id, name, xpath, parent_group_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING group_id
                    """,
                    (filing_id, group.get('name', ''), group.get('path', ''), parent_group_id)
                )
                group_id = cur.fetchone()[0]
                
            # Store values for each instance
            field_count = 0
            for instance_idx, instance_values in enumerate(values):
                for field_name, field_value in instance_values.items():
                    if field_value is None:
                        continue
                        
                    # Get field definition ID
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT field_id FROM field_definitions 
                            WHERE name = %s
                            """,
                            (field_name,)
                        )
                        result = cur.fetchone()
                        
                        if result:
                            field_id = result[0]
                        else:
                            data_type = guess_data_type(field_value)
                            xpath = f"//*[local-name()='{field_name}']"
                            
                            cur.execute(
                                """
                                INSERT INTO field_definitions (name, field_type, xpath)
                                VALUES (%s, %s, %s)
                                RETURNING field_id
                                """,
                                (field_name, data_type, xpath)
                            )
                            field_id = cur.fetchone()[0]
                            
                    # Insert value
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO repeating_group_values (group_id, field_id, value, instance_index)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (group_id, field_id, str(field_value), instance_idx)
                        )
                        field_count += 1
            
            if not self.db_conn:
                conn.commit()
                
            return {
                "group_id": group_id,
                "name": group.get('name', ''),
                "table_name": table_name,
                "count": len(values),
                "field_count": field_count,
                "parent_group_id": parent_group_id
            }
                
        except Exception as e:
            if not self.db_conn:
                conn.rollback()
            logger.error(f"Error storing nested group values: {e}")
            return None
            
        finally:
            if not self.db_conn and conn:
                conn.close()
