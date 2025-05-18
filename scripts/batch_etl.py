#!/usr/bin/env python3
"""
Batch ETL script for Blizzard 2.0.

This script processes multiple XML files and generates a detailed report.
It's designed to be used for production data loading.

Usage:
    python batch_etl.py [--xml-dir XML_DIR] [--limit LIMIT] [--reset]
"""

import os
import sys
import time
import argparse
import logging
import traceback
import json
from pathlib import Path
from datetime import datetime
import psycopg2
from contextlib import contextmanager
import pandas as pd

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.simple_transformer import IRS990Transformer
from src.etl.dev_loader import DevPostgreSQLLoader

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'batch_etl_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('batch_etl')
    logger.info(f"Logging to file: {log_file}")
    
    return logger, log_file

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Batch ETL process for IRS 990 XML files')
    parser.add_argument('--xml-dir', help='Directory containing XML files to process')
    parser.add_argument('--limit', type=int, default=None, help='Maximum number of files to process')
    parser.add_argument('--reset', action='store_true', help='Reset the database before processing')
    parser.add_argument('--report', action='store_true', help='Generate a detailed report after processing')
    parser.add_argument('--credentials', help='Path to database credentials file')
    return parser.parse_args()

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

@contextmanager
def db_connection(credentials_file):
    """Create a database connection context manager."""
    credentials = load_credentials(credentials_file)
    conn = psycopg2.connect(**credentials)
    conn.autocommit = False
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def find_xml_files(directory=None, limit=None):
    """Find XML files in a directory (recursively)."""
    # If no directory provided, use the default xml_files directory
    if directory is None:
        directory = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xml_files'))
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
    
    xml_files = []
    for root, _, files in os.walk(directory):
        for file in sorted(files):
            if file.lower().endswith('.xml'):
                xml_files.append(os.path.join(root, file))
                if limit and len(xml_files) >= limit:
                    break
        if limit and len(xml_files) >= limit:
            break
    return xml_files

def reset_database(creds_path, logger):
    """Reset the database before processing."""
    logger.info("Resetting database...")
    
    try:
        with db_connection(creds_path) as conn:
            with conn.cursor() as cur:
                # Get existing tables
                cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = [row[0] for row in cur.fetchall()]
                
                # Define the order of tables to truncate based on foreign key constraints
                ordered_tables = [
                    'repeating_group_values',
                    'repeating_groups',
                    'filing_values',
                    'field_definitions',
                    'filings',
                    'organizations'
                ]
                
                # Filter ordered_tables to only include tables that actually exist
                tables_to_truncate = [table for table in ordered_tables if table in tables]
                
                logger.info(f"Found existing tables: {', '.join(tables_to_truncate)}")
                
                # Truncate tables
                for table in tables_to_truncate:
                    cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                    logger.info(f"Truncated table: {table}")
                
                # Reset sequences
                cur.execute("""
                    SELECT sequence_name FROM information_schema.sequences
                    WHERE sequence_schema = 'public'
                """)
                sequences = [row[0] for row in cur.fetchall()]
                
                for sequence in sequences:
                    cur.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")
                
                logger.info("Reset all sequences to 1")
                
                conn.commit()
                logger.info("Database reset completed successfully")
                
    except Exception as e:
        logger.error(f"Failed to reset database: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def generate_report(results, log_file, logger):
    """Generate a detailed report."""
    report_dir = os.path.abspath(os.path.join(os.path.dirname(log_file), '..', 'reports'))
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(report_dir, f'etl_report_{timestamp}.md')
    
    form_types = {}
    success_count = 0
    failure_count = 0
    total_fields = 0
    
    for result in results:
        if result['success']:
            success_count += 1
            form_type = result.get('form_type', 'Unknown')
            form_types[form_type] = form_types.get(form_type, 0) + 1
            total_fields += result.get('field_count', 0)
        else:
            failure_count += 1
    
    with open(report_file, 'w') as f:
        f.write(f"# Blizzard 2.1 ETL Processing Report\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Summary\n\n")
        f.write(f"- Total files processed: {len(results)}\n")
        f.write(f"- Successful: {success_count}\n")
        f.write(f"- Failed: {failure_count}\n")
        f.write(f"- Success rate: {success_count / len(results) * 100:.1f}%\n")
        f.write(f"- Total fields extracted: {total_fields}\n\n")
        
        f.write("## Form Type Distribution\n\n")
        f.write("| Form Type | Count | Percentage |\n")
        f.write("|-----------|-------|------------|\n")
        for form_type, count in sorted(form_types.items()):
            f.write(f"| {form_type} | {count} | {count / success_count * 100:.1f}% |\n")
        
        f.write("\n## Processing Details\n\n")
        f.write("| File | Status | Organization | EIN | Form Type | Field Count |\n")
        f.write("|------|--------|--------------|-----|-----------|------------|\n")
        
        for result in results:
            status = "✅ Success" if result['success'] else "❌ Failed"
            org_name = result.get('organization_name', 'N/A')
            ein = result.get('ein', 'N/A')
            form_type = result.get('form_type', 'N/A')
            field_count = result.get('field_count', 0)
            
            f.write(f"| {os.path.basename(result['file'])} | {status} | {org_name} | {ein} | {form_type} | {field_count} |\n")
        
        f.write("\n## Error Details\n\n")
        for result in results:
            if not result['success']:
                f.write(f"### {os.path.basename(result['file'])}\n\n")
                f.write(f"```\n{result['error']}\n```\n\n")
    
    logger.info(f"Generated report at: {report_file}")
    return report_file

def generate_csv_report(results, log_file):
    """Generate a CSV report of processing results."""
    report_dir = os.path.abspath(os.path.join(os.path.dirname(log_file), '..', 'reports'))
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = os.path.join(report_dir, f'etl_report_{timestamp}.csv')
    
    # Create a DataFrame from the results
    report_data = []
    for result in results:
        report_data.append({
            'file': os.path.basename(result['file']),
            'success': result['success'],
            'organization_name': result.get('organization_name', 'N/A'),
            'ein': result.get('ein', 'N/A'),
            'form_type': result.get('form_type', 'N/A'),
            'field_count': result.get('field_count', 0),
            'processing_time': result.get('processing_time', 0),
            'error': '' if result['success'] else result['error']
        })
    
    df = pd.DataFrame(report_data)
    df.to_csv(csv_file, index=False)
    
    return csv_file

def generate_database_stats(creds_path, logger):
    """Generate database statistics."""
    stats = {}
    
    try:
        with db_connection(creds_path) as conn:
            with conn.cursor() as cur:
                # Count organizations
                cur.execute("SELECT COUNT(*) FROM organizations")
                stats['organization_count'] = cur.fetchone()[0]
                
                # Count filings by form type
                cur.execute("""
                    SELECT form_type, COUNT(*) 
                    FROM filings 
                    GROUP BY form_type
                    ORDER BY COUNT(*) DESC
                """)
                stats['filings_by_type'] = {row[0]: row[1] for row in cur.fetchall()}
                
                # Count total filings
                stats['filing_count'] = sum(stats['filings_by_type'].values())
                
                # Count field definitions
                cur.execute("SELECT COUNT(*) FROM field_definitions")
                stats['field_definition_count'] = cur.fetchone()[0]
                
                # Count filing values
                cur.execute("SELECT COUNT(*) FROM filing_values")
                stats['filing_value_count'] = cur.fetchone()[0]
                
                # Top fields by frequency
                cur.execute("""
                    SELECT fd.name, COUNT(*) as count
                    FROM filing_values fv
                    JOIN field_definitions fd ON fv.field_id = fd.field_id
                    GROUP BY fd.name
                    ORDER BY count DESC
                    LIMIT 20
                """)
                stats['top_fields'] = {row[0]: row[1] for row in cur.fetchall()}
                
        return stats
        
    except Exception as e:
        logger.error(f"Failed to generate database statistics: {str(e)}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}

def main():
    """Run the batch ETL process."""
    args = parse_arguments()
    logger, log_file = setup_logging()
    
    start_time = time.time()
    results = []
    
    try:
        # Get credentials file
        creds_path = args.credentials or os.path.abspath("/workspaces/blizzard/2.0/config/db_credentials.txt")
        if not os.path.exists(creds_path):
            logger.error(f"Credentials file not found: {creds_path}")
            return 1
        
        # Get XML directory
        xml_dir = args.xml_dir or '/workspaces/blizzard/xml_files/2023'
        if not os.path.exists(xml_dir):
            logger.error(f"XML directory not found: {xml_dir}")
            return 1
        
        # Reset database if requested
        if args.reset:
            reset_database(creds_path, logger)
        
        # Create ETL components
        transformer = IRS990Transformer()
        loader = DevPostgreSQLLoader(creds_path)
        
        # Find XML files
        logger.info(f"Finding XML files in {xml_dir}" + (f" (limit: {args.limit})" if args.limit else ""))
        xml_files = find_xml_files(xml_dir, args.limit)
        logger.info(f"Found {len(xml_files)} XML files")
        
        # Process XML files
        for i, xml_file in enumerate(xml_files, 1):
            file_start_time = time.time()
            result = {
                'file': xml_file,
                'success': False,
                'processing_time': 0
            }
            
            try:
                logger.info(f"Processing file {i}/{len(xml_files)}: {xml_file}")
                
                # Transform XML data
                filing_data = transformer.transform_file(xml_file)
                
                # Extract key information for reporting
                result['organization_name'] = filing_data['organization'].get('name')
                result['ein'] = filing_data['organization'].get('ein')
                result['form_type'] = filing_data['filing'].get('form_type')
                result['field_count'] = len(filing_data['values'])
                
                # Load data into database
                with db_connection(creds_path) as conn:
                    loader.load_filing(conn, filing_data)
                    conn.commit()
                
                result['success'] = True
                logger.info(f"Successfully processed: {xml_file}")
                logger.info(f"Organization: {filing_data['organization'].get('name')} ({filing_data['organization'].get('ein')})")
                logger.info(f"Form type: {filing_data['filing'].get('form_type')}, Fields: {len(filing_data['values'])}")
                
            except Exception as e:
                logger.error(f"Failed to process {xml_file}: {str(e)}")
                logger.error(traceback.format_exc())
                result['error'] = str(e) + "\n" + traceback.format_exc()
            
            result['processing_time'] = time.time() - file_start_time
            results.append(result)
        
        # Display statistics
        end_time = time.time()
        duration = end_time - start_time
        success_count = sum(1 for r in results if r['success'])
        failure_count = len(results) - success_count
        
        logger.info("Batch ETL completed")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Files processed: {len(results)}")
        logger.info(f"Files successful: {success_count}")
        logger.info(f"Files failed: {failure_count}")
        
        # Generate report if requested
        if args.report:
            report_file = generate_report(results, log_file, logger)
            csv_file = generate_csv_report(results, log_file)
            logger.info(f"CSV report saved to: {csv_file}")
            
            # Generate database statistics
            logger.info("Generating database statistics...")
            db_stats = generate_database_stats(creds_path, logger)
            
            # Log basic stats
            logger.info(f"Organizations in database: {db_stats.get('organization_count', 'N/A')}")
            logger.info(f"Filings in database: {db_stats.get('filing_count', 'N/A')}")
            logger.info(f"Field definitions: {db_stats.get('field_definition_count', 'N/A')}")
            logger.info(f"Filing values: {db_stats.get('filing_value_count', 'N/A')}")
        
        # Success if at least one file was processed
        return 0 if success_count > 0 else 1
        
    except Exception as e:
        logger.error(f"Batch ETL failed: {e}")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
