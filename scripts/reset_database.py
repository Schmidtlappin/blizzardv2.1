#!/usr/bin/env python3
"""
Database reset script for Blizzard 2.0.

This script connects to the PostgreSQL database and purges all records
to prepare for testing the Blizzard 2.0 ETL process.

Usage:
    python reset_database.py
"""

import os
import sys
import logging
from pathlib import Path
import psycopg2
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'db_reset_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('reset_database')
    logger.info(f"Logging to file: {log_file}")
    
    return logger

def load_credentials(credentials_file):
    """Load database credentials from a file."""
    if not os.path.exists(credentials_file):
        raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
    
    creds = {}
    section = None
    
    with open(credentials_file, 'r') as f:
        for line in f:
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('//') or line.startswith('#'):
                continue
            
            # Check for section headers
            if line.startswith('[') and line.endswith(']'):
                section = line[1:-1]
                continue
            
            # Parse key=value pairs
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                if section == 'database':
                    creds[key] = value
                else:
                    # If no section is specified, assume it's a direct connection parameter
                    creds[key] = value
    
    return creds

def create_schema(cursor):
    """Create the database schema if it doesn't exist."""
    schema_sql = """
-- IRS 990 Dynamic Schema
-- Version: 2.0

-- Organizations table
CREATE TABLE IF NOT EXISTS organizations (
    ein VARCHAR(9) PRIMARY KEY,
    name TEXT,
    city TEXT,
    state TEXT
);

-- Filings table
CREATE TABLE IF NOT EXISTS filings (
    filing_id VARCHAR(30) PRIMARY KEY,
    ein VARCHAR(9) REFERENCES organizations(ein),
    form_type VARCHAR(10),
    tax_period VARCHAR(6),
    submission_date DATE,
    tax_year INTEGER
);

-- Field definitions table
CREATE TABLE IF NOT EXISTS field_definitions (
    field_id SERIAL PRIMARY KEY,
    xpath TEXT NOT NULL,
    name TEXT,
    description TEXT,
    form_type VARCHAR(10),
    field_type VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_field_definitions_xpath ON field_definitions(xpath);

-- Filing values table (EAV model)
CREATE TABLE IF NOT EXISTS filing_values (
    filing_id VARCHAR(30) REFERENCES filings(filing_id),
    field_id INTEGER REFERENCES field_definitions(field_id),
    value TEXT,
    PRIMARY KEY (filing_id, field_id)
);
CREATE INDEX IF NOT EXISTS idx_filing_values_filing_id ON filing_values(filing_id);

-- Repeating groups table
CREATE TABLE IF NOT EXISTS repeating_groups (
    group_id SERIAL PRIMARY KEY,
    filing_id VARCHAR(30) REFERENCES filings(filing_id),
    parent_group_id INTEGER REFERENCES repeating_groups(group_id),
    name TEXT,
    xpath TEXT
);
CREATE INDEX IF NOT EXISTS idx_repeating_groups_filing_id ON repeating_groups(filing_id);

-- Repeating group values table
CREATE TABLE IF NOT EXISTS repeating_group_values (
    group_id INTEGER REFERENCES repeating_groups(group_id),
    field_id INTEGER REFERENCES field_definitions(field_id),
    value TEXT,
    PRIMARY KEY (group_id, field_id)
);
CREATE INDEX IF NOT EXISTS idx_repeating_group_values_group_id ON repeating_group_values(group_id);

-- Performance indexes for common operations
CREATE INDEX IF NOT EXISTS idx_organizations_ein ON organizations(ein);
CREATE INDEX IF NOT EXISTS idx_filings_ein ON filings(ein);
CREATE INDEX IF NOT EXISTS idx_filings_tax_year ON filings(tax_year);
CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type);
    """
    
    print("Creating database schema...")
    cursor.execute(schema_sql)
    print("Schema created successfully")

def check_tables_exist(cursor):
    """Check if the required tables exist in the database."""
    tables = [
        "repeating_group_values",
        "repeating_groups",
        "filing_values",
        "field_definitions",
        "filings",
        "organizations"
    ]
    
    existing_tables = []
    for table in tables:
        cursor.execute(f"SELECT to_regclass('public.{table}')")
        if cursor.fetchone()[0]:
            existing_tables.append(table)
    
    return existing_tables

def reset_database(credentials):
    """Reset the database by purging all records and recreating schema if needed."""
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(**credentials)
        conn.autocommit = False
        
        # Create a cursor
        with conn.cursor() as cursor:
            # Check if tables exist
            existing_tables = check_tables_exist(cursor)
            
            if not existing_tables:
                print("No tables found. Creating schema...")
                create_schema(cursor)
                conn.commit()
                print("Schema created successfully")
                return
            
            print(f"Found existing tables: {', '.join(existing_tables)}")
            
            # Truncate existing tables in reverse order of dependency
            for table in existing_tables:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                    print(f"Truncated table: {table}")
                except Exception as e:
                    print(f"Error truncating {table}: {e}")
            
            # Reset sequences
            cursor.execute("""
                DO $$
                DECLARE
                    seq_rec RECORD;
                BEGIN
                    FOR seq_rec IN 
                        SELECT sequence_name 
                        FROM information_schema.sequences 
                        WHERE sequence_schema = 'public'
                    LOOP
                        EXECUTE 'ALTER SEQUENCE ' || seq_rec.sequence_name || ' RESTART WITH 1';
                    END LOOP;
                END $$;
            """)
            print("Reset all sequences to 1")
            
            # Commit the transaction
            conn.commit()
            print("Database reset completed successfully")
            
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def main():
    """Run the database reset process."""
    logger = setup_logging()
    
    try:
        # Get the path to the credentials file
        credentials_file = os.path.abspath("/workspaces/blizzard/2.0/config/db_credentials.txt")
        
        # Load credentials
        logger.info(f"Loading credentials from {credentials_file}")
        credentials = load_credentials(credentials_file)
        
        # Confirm the operation
        print(f"This will PURGE ALL RECORDS from database '{credentials.get('dbname')}' on '{credentials.get('host')}'.")
        print("This action CANNOT BE UNDONE.")
        response = input("Are you sure you want to continue? [yes/NO]: ")
        
        if response.lower() != 'yes':
            logger.info("Operation cancelled by user")
            print("Operation cancelled.")
            return 0
        
        # Reset the database
        logger.info("Resetting database...")
        reset_database(credentials)
        logger.info("Database reset completed successfully")
        
        return 0
    except Exception as e:
        logger.error(f"Database reset failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
