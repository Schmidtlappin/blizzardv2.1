#!/usr/bin/env python3
"""
Unified command-line interface     # Execute the appropriate command
    try:
        if args.command == 'process':
            logger.info(f"Processing XML files from directory: {args.xmldir}")
            # Import the process command module
            process_cmd = importlib.import_module('cli.commands.process_command')
            process_cmd.process_xml_files(
                args.xmldir, args.concordance, args.credentials, args.batch_size
            )
        
        elif args.command == 'setup':
            logger.info("Setting up database schema")
            # Import the setup command module
            setup_cmd = importlib.import_module('cli.commands.setup_command')
            setup_cmd.setup_schema(args.credentials, args.schema)
        
        elif args.command == 'check':
            logger.info("Running diagnostics")
            # Import the check command module
            check_cmd = importlib.import_module('cli.commands.check_command')
            check_cmd.run_diagnostics(args.credentials, args.filing_id)
    
    except ImportError as e:
        logger.error(f"Error importing command module: {e}")
        print(f"Error: Command implementation not found. Make sure the command modules are properly installed.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        print(f"Error: {str(e)}")
        sys.exit(1)S 990 XML processing system version 2.0.

This module provides a centralized entry point for various operations:
- Process XML files (individual or batches)
- Handle repeating groups
- Set up the database schema
- Run diagnostics
"""

import sys
import argparse
import logging
import importlib
from pathlib import Path

from src.core.constants import LOG_DIR
from src.logging import setup_logging
from src.config.settings import Settings

logger = setup_logging(__name__)

def main():
    """Main entry point for the Blizzard CLI."""
    parser = argparse.ArgumentParser(
        description='Blizzard IRS 990 XML Processing System',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Set up subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Process command
    process_parser = subparsers.add_parser('process', help='Process XML files')
    process_parser.add_argument('--xmldir', required=True, help='Directory containing XML files')
    process_parser.add_argument('--concordance', required=True, help='Path to concordance file')
    process_parser.add_argument('--credentials', required=True, help='Path to database credentials file')
    process_parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up database schema')
    setup_parser.add_argument('--credentials', required=True, help='Path to database credentials file')
    setup_parser.add_argument('--schema', help='Path to schema file (optional)')
    
    # Check command
    check_parser = subparsers.add_parser('check', help='Run diagnostics')
    check_parser.add_argument('--credentials', required=True, help='Path to database credentials file')
    check_parser.add_argument('--filing-id', help='Filing ID to check')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Check if a command was specified
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the appropriate command
    if args.command == 'process':
        logger.info(f"Processing XML files from directory: {args.xmldir}")
        from .commands.process_command import process_xml_files
        process_xml_files(args.xmldir, args.concordance, args.credentials, args.batch_size)
    
    elif args.command == 'setup':
        logger.info("Setting up database schema")
        from .commands.setup_command import setup_schema
        setup_schema(args.credentials, args.schema)
    
    elif args.command == 'check':
        logger.info("Running diagnostics")
        from .commands.check_command import run_diagnostics
        run_diagnostics(args.credentials, args.filing_id)

if __name__ == "__main__":
    main()
