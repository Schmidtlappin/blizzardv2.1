"""
Database loader for the Blizzard 2.1 ETL pipeline.

This module provides a consolidated, production-ready loader 
for loading transformed data into the database.
"""

from typing import Dict, Any, List, Optional, Union
import logging
import psycopg2
from psycopg2.extras import execute_values

from src.core.exceptions import DatabaseError
from src.etl.pipeline import Loader
from src.db.connection import get_db_connection

logger = logging.getLogger(__name__)

class PostgreSQLLoader(Loader[List[Dict[str, Any]]]):
    """
    Production-ready loader for inserting IRS 990 data into a PostgreSQL database.
    
    This class handles loading organizations, filings, and their related data
    into a PostgreSQL database as part of the ETL pipeline.
    """
    
    def __init__(self, db_params: Dict[str, Any], batch_size: int = 100):
        """
        Initialize the loader.
        
        Args:
            db_params: PostgreSQL connection parameters
            batch_size: Batch size for database operations
        """
        self.db_params = db_params
        self.batch_size = batch_size
        
    def load(self, processed_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Load processed data into the database.
        
        Args:
            processed_data_list: List of processed data dictionaries
            
        Returns:
            List of results with loading status
        """
        results = []
        
        for processed_data in processed_data_list:
            try:
                # Load the individual filing
                result = self._load_filing(processed_data)
                results.append(result)
            except Exception as e:
                logger.error(f"Error loading filing: {e}")
                results.append({
                    'status': 'error',
                    'error': str(e),
                    'filing_id': processed_data.get('metadata', {}).get('filing_id', 'unknown')
                })
                
        return results
    
    def _load_filing(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load a single filing into the database.
        
        Args:
            processed_data: Dictionary with processed data
            
        Returns:
            Dictionary with loading result
        """
        metadata = processed_data.get('metadata', {})
        organization = processed_data.get('organization', {})
        filing_values = processed_data.get('filing_values', [])
        repeating_groups = processed_data.get('repeating_groups', [])
        group_values = processed_data.get('group_values', [])
        
        filing_id = metadata.get('filing_id')
        if not filing_id:
            raise DatabaseError("Missing filing_id in metadata")
        
        with get_db_connection(self.db_params) as conn:
            try:
                # Set the schema search path
                with conn.cursor() as cursor:
                    cursor.execute("SET search_path TO irs990, public;")
                
                # Load organization
                self._load_organization(conn, organization)
                
                # Load filing
                self._load_filing_record(conn, metadata)
                
                # Load filing values
                self._load_filing_values(conn, filing_values)
                
                # Load repeating groups and their values
                self._load_repeating_groups(conn, repeating_groups, group_values)
                
                # Store XML content if provided
                if 'xml_hash' in processed_data:
                    self._store_xml_metadata(conn, filing_id, processed_data['xml_hash'])
                
                # Commit transaction
                conn.commit()
                
                return {
                    'status': 'success',
                    'filing_id': filing_id,
                    'filing_values_count': len(filing_values),
                    'repeating_groups_count': len(repeating_groups),
                    'group_values_count': len(group_values)
                }
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error loading filing {filing_id}: {e}")
                raise DatabaseError(f"Error loading filing {filing_id}: {e}")
    
    def _load_organization(self, conn, organization: Dict[str, Any]) -> None:
        """Load organization data into the database."""
        ein = organization.get('ein')
        if not ein:
            raise DatabaseError("Missing EIN in organization data")
            
        with conn.cursor() as cursor:
            # Check if organization exists
            cursor.execute("SELECT ein FROM organizations WHERE ein = %s", (ein,))
            if cursor.fetchone():
                # Update existing organization
                cursor.execute("""
                    UPDATE organizations SET
                        name = COALESCE(%s, name),
                        address_line1 = COALESCE(%s, address_line1),
                        address_line2 = COALESCE(%s, address_line2),
                        city = COALESCE(%s, city),
                        state = COALESCE(%s, state),
                        zip = COALESCE(%s, zip),
                        country = COALESCE(%s, country),
                        website = COALESCE(%s, website),
                        last_updated_timestamp = CURRENT_TIMESTAMP
                    WHERE ein = %s
                """, (
                    organization.get('name'),
                    organization.get('address_line1'),
                    organization.get('address_line2'),
                    organization.get('city'),
                    organization.get('state'),
                    organization.get('zip'),
                    organization.get('country'),
                    organization.get('website'),
                    ein
                ))
            else:
                # Insert new organization
                cursor.execute("""
                    INSERT INTO organizations (
                        ein, name, address_line1, address_line2, city, state, zip, country, website
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    ein,
                    organization.get('name'),
                    organization.get('address_line1'),
                    organization.get('address_line2'),
                    organization.get('city'),
                    organization.get('state'),
                    organization.get('zip'),
                    organization.get('country', 'US'),
                    organization.get('website')
                ))
    
    def _load_filing_record(self, conn, metadata: Dict[str, Any]) -> None:
        """Load filing record into the database."""
        filing_id = metadata.get('filing_id')
        ein = metadata.get('ein')
        
        if not filing_id or not ein:
            raise DatabaseError("Missing filing_id or EIN in metadata")
            
        with conn.cursor() as cursor:
            # Check if filing exists
            cursor.execute("SELECT filing_id FROM filings WHERE filing_id = %s", (filing_id,))
            if cursor.fetchone():
                # Update existing filing
                cursor.execute("""
                    UPDATE filings SET
                        object_id = %s,
                        form_version = %s,
                        tax_year = %s,
                        processed_timestamp = CURRENT_TIMESTAMP
                    WHERE filing_id = %s
                """, (
                    metadata.get('object_id'),
                    metadata.get('form_version'),
                    metadata.get('tax_year'),
                    filing_id
                ))
            else:
                # Insert new filing
                cursor.execute("""
                    INSERT INTO filings (
                        filing_id, object_id, ein, tax_period, form_type, form_version, tax_year, submission_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    filing_id,
                    metadata.get('object_id'),
                    ein,
                    metadata.get('tax_period'),
                    metadata.get('form_type'),
                    metadata.get('form_version'),
                    metadata.get('tax_year'),
                    metadata.get('submission_date')
                ))
    
    def _load_filing_values(self, conn, filing_values: List[Dict[str, Any]]) -> None:
        """Load filing values into the database."""
        if not filing_values:
            return
            
        with conn.cursor() as cursor:
            # Process in batches for efficiency
            batch_size = self.batch_size
            for i in range(0, len(filing_values), batch_size):
                batch = filing_values[i:i+batch_size]
                
                # Prepare batch values
                batch_values = []
                for value in batch:
                    batch_values.append((
                        value['filing_id'],
                        value['field_id'],
                        value.get('text_value'),
                        value.get('numeric_value'),
                        value.get('boolean_value'),
                        value.get('date_value')
                    ))
                
                # Batch insert
                execute_values(
                    cursor,
                    """
                    INSERT INTO filing_values (
                        filing_id, field_id, text_value, numeric_value, boolean_value, date_value
                    ) VALUES %s
                    ON CONFLICT (filing_id, field_id) DO UPDATE SET
                        text_value = EXCLUDED.text_value,
                        numeric_value = EXCLUDED.numeric_value,
                        boolean_value = EXCLUDED.boolean_value,
                        date_value = EXCLUDED.date_value
                    """,
                    batch_values
                )
    
    def _load_repeating_groups(self, conn, repeating_groups: List[Dict[str, Any]], 
                              group_values: List[Dict[str, Any]]) -> None:
        """Load repeating groups and their values into the database."""
        if not repeating_groups:
            return
            
        with conn.cursor() as cursor:
            # Process repeating groups
            group_id_map = {}  # Map from client-side IDs to database IDs
            
            for group in repeating_groups:
                client_group_id = group['group_id']
                
                # Insert the group
                cursor.execute("""
                    INSERT INTO repeating_groups (
                        filing_id, parent_group_id, name, xpath
                    ) VALUES (%s, %s, %s, %s)
                    RETURNING group_id
                """, (
                    group['filing_id'],
                    group.get('parent_group_id'),
                    group.get('name'),
                    group.get('xpath')
                ))
                
                # Remember the database-assigned ID
                db_group_id = cursor.fetchone()[0]
                group_id_map[client_group_id] = db_group_id
            
            # Process group values
            if not group_values:
                return
                
            # Process in batches for efficiency
            batch_size = self.batch_size
            for i in range(0, len(group_values), batch_size):
                batch = group_values[i:i+batch_size]
                
                # Prepare batch values
                batch_values = []
                for value in batch:
                    # Map the client-side group ID to the database ID
                    client_group_id = value['group_id']
                    if client_group_id not in group_id_map:
                        logger.warning(f"Group ID {client_group_id} not found in map")
                        continue
                        
                    db_group_id = group_id_map[client_group_id]
                    batch_values.append((
                        db_group_id,
                        value['field_id'],
                        value.get('text_value'),
                        value.get('numeric_value'),
                        value.get('boolean_value'),
                        value.get('date_value')
                    ))
                
                if batch_values:
                    # Batch insert
                    execute_values(
                        cursor,
                        """
                        INSERT INTO repeating_group_values (
                            group_id, field_id, text_value, numeric_value, boolean_value, date_value
                        ) VALUES %s
                        ON CONFLICT (group_id, field_id) DO UPDATE SET
                            text_value = EXCLUDED.text_value,
                            numeric_value = EXCLUDED.numeric_value,
                            boolean_value = EXCLUDED.boolean_value,
                            date_value = EXCLUDED.date_value
                        """,
                        batch_values
                    )
    
    def _store_xml_metadata(self, conn, filing_id: str, xml_hash: str) -> None:
        """Store XML metadata in the database."""
        with conn.cursor() as cursor:
            # Check if XML storage exists
            cursor.execute("SELECT filing_id FROM xml_metadata WHERE filing_id = %s", (filing_id,))
            if cursor.fetchone():
                # Update XML metadata
                cursor.execute("""
                    UPDATE xml_metadata SET
                        xml_hash = %s,
                        storage_date = CURRENT_TIMESTAMP
                    WHERE filing_id = %s
                """, (xml_hash, filing_id))
            else:
                # Insert XML metadata
                cursor.execute("""
                    INSERT INTO xml_metadata (
                        filing_id, xml_hash, storage_date
                    ) VALUES (%s, %s, CURRENT_TIMESTAMP)
                """, (filing_id, xml_hash))
