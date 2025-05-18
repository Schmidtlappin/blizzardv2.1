#!/usr/bin/env python3
"""
Script to check and validate XML files in the Blizzard 2.1 xml_files directory.

This script helps users verify that their XML files are properly staged for processing.

Usage:
    python check_xml_files.py [--validate]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET
from collections import defaultdict

# Add the parent directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Try to import the XMLValidator if requested
try:
    from src.xml.validator import XMLValidator
    VALIDATOR_AVAILABLE = True
except ImportError:
    VALIDATOR_AVAILABLE = False

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"check_xml_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('check_xml')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Check XML files in the processing directory')
    parser.add_argument('--validate', action='store_true', help='Perform basic XML validation')
    parser.add_argument('--dir', default=None, help='XML files directory to check (default: project xml_files directory)')
    return parser.parse_args()

def check_xml_format(file_path):
    """Perform a basic check of XML format."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        return True, root.tag
    except Exception as e:
        return False, str(e)

def get_form_info(file_path, root_tag):
    """Extract basic information from the XML file."""
    info = {
        'form_type': 'Unknown',
        'ein': 'Unknown',
        'tax_year': 'Unknown',
        'organization': 'Unknown'
    }
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Try to find EIN
        ein_elements = root.findall(".//EIN") or root.findall(".//*[local-name()='EIN']")
        if ein_elements:
            info['ein'] = ein_elements[0].text
        
        # Try to find form type
        form_elements = root.findall(".//ReturnType") or root.findall(".//*[local-name()='ReturnType']")
        if form_elements:
            info['form_type'] = form_elements[0].text
        
        # Try to find tax year
        year_elements = (
            root.findall(".//TaxYr") or 
            root.findall(".//TaxYear") or
            root.findall(".//*[local-name()='TaxYr']") or
            root.findall(".//*[local-name()='TaxYear']")
        )
        if year_elements:
            info['tax_year'] = year_elements[0].text
        
        # Try to find organization name
        name_elements = (
            root.findall(".//BusinessName/BusinessNameLine1") or
            root.findall(".//Name/BusinessNameLine1") or
            root.findall(".//*[local-name()='BusinessNameLine1']")
        )
        if name_elements:
            info['organization'] = name_elements[0].text
    except:
        pass
    
    return info

def main():
    """Run the check process."""
    args = parse_arguments()
    logger = setup_logging()
    
    # Determine XML files directory
    if args.dir:
        xml_dir = args.dir
    else:
        xml_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xml_files'))
    
    if not os.path.isdir(xml_dir):
        logger.error(f"XML files directory not found: {xml_dir}")
        return 1
    
    # Find XML files
    logger.info(f"Checking XML files in: {xml_dir}")
    
    xml_files = []
    for root, _, files in os.walk(xml_dir):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    if not xml_files:
        logger.warning(f"No XML files found in {xml_dir}")
        print(f"\nNo XML files found in {xml_dir}")
        print("\nTo add XML files, you can:")
        print("1. Copy files directly to the xml_files directory")
        print("2. Use the copy_xml_files.py script:")
        print("   python -m scripts.copy_xml_files --year 2023 --limit 50")
        return 0
    
    logger.info(f"Found {len(xml_files)} XML files")
    print(f"\nFound {len(xml_files)} XML files in {xml_dir}")
    
    # Check files structure
    form_types = defaultdict(int)
    tax_years = defaultdict(int)
    valid_files = 0
    invalid_files = 0
    
    # Display a summary of the first few files
    print("\nSample files:")
    for i, file_path in enumerate(xml_files[:5]):
        print(f"  - {os.path.basename(file_path)}")
    
    if len(xml_files) > 5:
        print(f"  - ... and {len(xml_files) - 5} more")
    
    # Check XML format and extract basic info
    if args.validate:
        print("\nValidating XML format...")
        for file_path in xml_files:
            is_valid, root_tag = check_xml_format(file_path)
            
            if is_valid:
                valid_files += 1
                info = get_form_info(file_path, root_tag)
                form_types[info['form_type']] += 1
                tax_years[info['tax_year']] += 1
            else:
                invalid_files += 1
                logger.warning(f"Invalid XML file: {file_path}: {root_tag}")
        
        print(f"\nValidation results:")
        print(f"  - Valid XML files: {valid_files}")
        print(f"  - Invalid XML files: {invalid_files}")
        
        if form_types:
            print("\nForm types:")
            for form_type, count in sorted(form_types.items()):
                print(f"  - {form_type}: {count} files")
        
        if tax_years:
            print("\nTax years:")
            for year, count in sorted(tax_years.items()):
                print(f"  - {year}: {count} files")
    
    # Next steps
    print("\nNext steps:")
    print("1. Process the XML files:")
    print("   python -m scripts.run_etl")
    print("\n2. Or use the batch ETL process:")
    print("   python -m scripts.batch_etl --report")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
