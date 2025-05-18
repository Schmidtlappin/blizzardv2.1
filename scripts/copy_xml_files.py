#!/usr/bin/env python3
"""
Script to copy XML files from the main workspace into Blizzard 2.1's xml_files directory.

This script helps copy a subset of files from the main xml_files directory to the
Blizzard 2.1 xml_files directory for processing.

Usage:
    python copy_xml_files.py [--source SOURCE_DIR] [--year YEAR] [--limit LIMIT] [--form-type FORM_TYPE]
"""

import os
import sys
import shutil
import argparse
import random
import logging
from pathlib import Path
from datetime import datetime

# Add the parent directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"copy_xml_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('copy_xml')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Copy XML files to the Blizzard 2.1 processing directory')
    parser.add_argument('--source', default='/workspaces/blizzard/xml_files', 
                        help='Source directory containing XML files (default: /workspaces/blizzard/xml_files)')
    parser.add_argument('--year', help='Specific year to copy (e.g., 2023)')
    parser.add_argument('--limit', type=int, default=100, 
                        help='Maximum number of files to copy (default: 100)')
    parser.add_argument('--form-type', help='Filter by form type (e.g., 990, 990EZ, 990PF)')
    parser.add_argument('--random', action='store_true', help='Select random files instead of sequential')
    parser.add_argument('--destination', default=None, 
                        help='Destination directory (default: [project_root]/xml_files)')
    return parser.parse_args()

def find_xml_files(source_dir, year=None, form_type=None):
    """Find XML files matching criteria."""
    xml_files = []
    
    # If year is specified, look in that subdirectory
    if year and os.path.isdir(os.path.join(source_dir, year)):
        source_dir = os.path.join(source_dir, year)
    
    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.lower().endswith('.xml'):
                # Filter by form type if specified
                if form_type:
                    form_type_lower = form_type.lower()
                    file_lower = file.lower()
                    if not (form_type_lower in file_lower or 
                            (form_type_lower == '990' and 'ez' not in file_lower and 'pf' not in file_lower)):
                        continue
                
                xml_files.append(os.path.join(root, file))
    
    return xml_files

def main():
    """Run the copy process."""
    args = parse_arguments()
    logger = setup_logging()
    
    # Determine source directory
    source_dir = args.source
    if not os.path.isdir(source_dir):
        logger.error(f"Source directory not found: {source_dir}")
        return 1
    
    # Determine destination directory
    if args.destination:
        dest_dir = args.destination
    else:
        # Use default xml_files directory in the project root
        dest_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xml_files'))
    
    # Create destination directory if it doesn't exist
    os.makedirs(dest_dir, exist_ok=True)
    
    # If year is specified, create that subdirectory
    if args.year:
        year_dir = os.path.join(dest_dir, args.year)
        os.makedirs(year_dir, exist_ok=True)
        dest_dir = year_dir
    
    # Find XML files
    logger.info(f"Searching for XML files in {source_dir}...")
    xml_files = find_xml_files(source_dir, args.year, args.form_type)
    
    if not xml_files:
        logger.error("No XML files found matching the criteria")
        return 1
    
    logger.info(f"Found {len(xml_files)} XML files")
    
    # Limit the number of files if needed
    if args.limit and args.limit < len(xml_files):
        if args.random:
            xml_files = random.sample(xml_files, args.limit)
        else:
            xml_files = xml_files[:args.limit]
    
    # Copy files
    logger.info(f"Copying {len(xml_files)} files to {dest_dir}...")
    copied_count = 0
    
    for file_path in xml_files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(dest_dir, file_name)
        
        try:
            shutil.copy2(file_path, dest_path)
            copied_count += 1
            logger.debug(f"Copied: {file_name}")
        except Exception as e:
            logger.error(f"Error copying {file_name}: {e}")
    
    logger.info(f"Successfully copied {copied_count} of {len(xml_files)} files")
    logger.info(f"Files are ready for processing in {dest_dir}")
    logger.info("You can now run the ETL process:")
    logger.info("python -m scripts.run_etl")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
