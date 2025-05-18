#!/usr/bin/env python3
"""
Simple script to check the database contents for Blizzard 2.0.
"""

import os
import sys
import logging
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2

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

def main():
    """Check the database contents."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logger = logging.getLogger('check_database')
    
    try:
        # Get credentials file
        creds_path = os.path.abspath("/workspaces/blizzard/2.0/config/db_credentials.txt")
        if not os.path.exists(creds_path):
            logger.error(f"Credentials file not found: {creds_path}")
            return 1
        
        # Load credentials
        credentials = load_credentials(creds_path)
        conn = psycopg2.connect(**credentials)
        cur = conn.cursor()
        
        # Query organizations
        print("\n--- ORGANIZATIONS ---")
        cur.execute("SELECT * FROM organizations")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No organization records found.")
            
        # Query filings
        print("\n--- FILINGS ---")
        cur.execute("SELECT * FROM filings")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No filing records found.")
        
        # Query field definitions
        print("\n--- FIELD DEFINITIONS ---")
        cur.execute("SELECT * FROM field_definitions")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[2]}, Type: {row[5]}, XPath: {row[1]}")
        else:
            print("No field definition records found.")
        
        # Query filing values
        print("\n--- FILING VALUES ---")
        cur.execute("""
            SELECT fv.filing_id, fd.name, fv.value 
            FROM filing_values fv 
            JOIN field_definitions fd ON fv.field_id = fd.field_id
        """)
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No filing value records found.")
        
        conn.close()
        
        return 0
        
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
